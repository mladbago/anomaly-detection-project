# tests/test_precision_offline.py
"""PRECISION TEST (offline) — is the detector missing anomalies or crying wolf?

This replays simulator-generated streams through the :class:`ReferenceDetector`
(the faithful, rule-sharing twin of the Flink job) and checks both directions:

  * PRECISION — every alert must land on a transaction the simulator labelled
    ``is_injected_anomaly = True``. A flagged *normal* charge is a false positive.
  * RECALL    — each of the six injected anomaly types must be detected.

It needs no Kafka/Flink/Mongo, so it runs anywhere and is fully deterministic.
The live equivalent (querying MongoDB on the running stack) is in
``test_pipeline_integration.py`` and documented in the README Testing Guide.
"""
import random

from common import constants
from simulator.generator import TransactionGenerator
from tests.reference_detector import ReferenceDetector

ALL_ANOMALIES = [
    constants.ANOMALY_IMPOSSIBLE_TRAVEL,
    constants.ANOMALY_CARD_TESTING,
    constants.ANOMALY_FREQUENCY_BURST,
    constants.ANOMALY_MAX_OUT,
    constants.ANOMALY_NIGHT_OWL,
    constants.ANOMALY_AMOUNT_SPIKE,
]


def test_no_false_positives_on_pure_normal_stream():
    """A long run of well-spaced normal charges must raise zero alerts."""
    gen = TransactionGenerator(num_cards=3000)
    det = ReferenceDetector()
    base = 1_700_000_000_000   # fixed epoch-ms so the test is deterministic
    false_positives = 0
    for i in range(600):
        tx = gen.generate_normal_transaction()
        tx["timestamp"] = base + i * 60_000   # 1 minute apart -> no bursts/teleports
        false_positives += len(det.process(tx))
    assert false_positives == 0, f"detector flagged {false_positives} normal charges"


def test_every_injected_anomaly_is_caught_and_only_injected_is_flagged():
    """Per anomaly type: the expected alert fires, and only on injected charges."""
    for anomaly in ALL_ANOMALIES:
        gen = TransactionGenerator(num_cards=500)
        det = ReferenceDetector()
        batch = gen.generate_anomaly(anomaly)

        fired_types = set()
        for tx in batch:
            alerts = det.process(tx)
            if alerts:
                # PRECISION: alerts only ever appear on labelled trigger charges.
                assert tx["is_injected_anomaly"] is True, (
                    f"{anomaly}: alert {alerts} fired on a non-injected charge {tx}"
                )
                fired_types.update(alerts)
        # RECALL: the rule this batch targets must have fired.
        assert anomaly in fired_types, f"{anomaly}: detector missed it (got {fired_types})"


def test_aggregate_precision_and_recall_over_all_scenarios():
    """Across many independent episodes: 100% precision, every type recalled.

    The simulator builds each anomaly as a *self-contained episode* on a card
    (impossible-travel even carries its own benign setup charge), so each batch
    is replayed through a fresh detector — exactly how a card with no prior
    baseline experiences it. We run 20 episodes of every type plus a large
    normal-only stream and tally precision/recall over the lot.
    """
    random.seed(1234)   # deterministic
    total_alerts = injected_alerts = 0
    recalled = set()

    # A big normal-only stream must contribute zero alerts (no false positives).
    gen = TransactionGenerator(num_cards=3000)
    det = ReferenceDetector()
    base = 1_700_000_000_000
    for i in range(500):
        tx = gen.generate_normal_transaction()
        tx["timestamp"] = base + i * 60_000
        total_alerts += len(det.process(tx))

    # 20 independent episodes of each anomaly type, each on a fresh detector.
    for anomaly in ALL_ANOMALIES:
        for _ in range(20):
            det = ReferenceDetector()
            for tx in TransactionGenerator(num_cards=500).generate_anomaly(anomaly):
                alerts = det.process(tx)
                total_alerts += len(alerts)
                if alerts:
                    assert tx["is_injected_anomaly"] is True
                    injected_alerts += len(alerts)
                    recalled.update(alerts)

    precision = injected_alerts / total_alerts if total_alerts else 1.0
    assert total_alerts > 0
    assert precision == 1.0, f"precision {precision:.3f} < 1.0 (false positives present)"
    assert set(ALL_ANOMALIES).issubset(recalled), f"missed types: {set(ALL_ANOMALIES) - recalled}"
