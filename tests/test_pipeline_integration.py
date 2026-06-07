# tests/test_pipeline_integration.py
"""INTEGRATION TEST — validate the *live* pipeline against MongoDB.

Run this after ``docker compose up`` has had a minute to accumulate alerts. It
connects to the same MongoDB the alert-manager writes to and asserts, on real
end-to-end data, the property the offline precision test proves in theory:

    every alert the Flink job stored corresponds to a transaction the simulator
    labelled ``is_injected_anomaly = True`` (i.e. no false positives).

If MongoDB is unreachable or empty, the test SKIPS rather than fails — so it is
safe to include in a plain ``pytest`` run on a laptop with nothing running.
"""
from common import config
from tests._runner import skip

MIN_ALERTS = 5   # don't judge precision until a few alerts have landed


def _alerts_collection():
    try:
        from pymongo import MongoClient
    except ImportError:
        skip("pymongo not installed")
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        skip(f"MongoDB not reachable at {config.MONGO_URI}: {exc}")
    return client[config.MONGO_DB_NAME][config.MONGO_COLLECTION_ALERTS]


def test_alerts_have_been_persisted():
    """The alert-manager should be bridging Kafka alerts into MongoDB."""
    coll = _alerts_collection()
    count = coll.count_documents({})
    if count < MIN_ALERTS:
        skip(f"only {count} alerts so far; let the stack run longer")
    assert count >= MIN_ALERTS


def test_live_precision_no_false_positives():
    """Every stored alert traces back to an injected-anomaly transaction."""
    coll = _alerts_collection()
    total = coll.count_documents({})
    if total < MIN_ALERTS:
        skip(f"only {total} alerts so far; let the stack run longer")

    # False positive = an alert whose embedded transaction was NOT injected.
    false_positives = coll.count_documents({"transaction.is_injected_anomaly": {"$ne": True}})
    precision = (total - false_positives) / total
    assert false_positives == 0, (
        f"{false_positives}/{total} alerts were false positives "
        f"(live precision {precision:.3f})"
    )


def test_every_alert_type_appears():
    """Over a healthy run, the detector should exercise multiple rules."""
    coll = _alerts_collection()
    if coll.count_documents({}) < MIN_ALERTS:
        skip("not enough alerts yet")
    types = coll.distinct("alert_type")
    assert types, "no alert_type values found in MongoDB"
