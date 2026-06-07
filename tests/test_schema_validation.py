# tests/test_schema_validation.py
"""VALIDATION TEST — does the simulator's JSON match what Flink expects?

Two complementary guarantees:

  1. Every payload the simulator emits (normal + all six anomaly types) is valid
     against the canonical ``common.schema.TRANSACTION_SCHEMA``.
  2. The schema's ``required`` set is a superset of ``FLINK_REQUIRED_FIELDS`` --
     the exact keys the Flink job dereferences. If anyone adds a field read in
     ``job.py`` without declaring it in the contract, this test fails.
"""
import jsonschema

from common import constants
from common.schema import FLINK_REQUIRED_FIELDS, TRANSACTION_SCHEMA
from simulator.generator import TransactionGenerator

ALL_ANOMALIES = [
    constants.ANOMALY_IMPOSSIBLE_TRAVEL,
    constants.ANOMALY_CARD_TESTING,
    constants.ANOMALY_FREQUENCY_BURST,
    constants.ANOMALY_MAX_OUT,
    constants.ANOMALY_NIGHT_OWL,
    constants.ANOMALY_AMOUNT_SPIKE,
]


def _sample_payloads():
    """A representative mix: many normals plus every anomaly batch."""
    gen = TransactionGenerator(num_cards=200)
    payloads = [gen.generate_normal_transaction() for _ in range(50)]
    for anomaly in ALL_ANOMALIES:
        payloads.extend(gen.generate_anomaly(anomaly))
    return payloads


def test_every_payload_matches_schema():
    """Each generated transaction validates against the JSON contract."""
    validator = jsonschema.Draft7Validator(TRANSACTION_SCHEMA)
    for tx in _sample_payloads():
        errors = sorted(validator.iter_errors(tx), key=lambda e: e.path)
        assert not errors, f"schema violation in {tx}: {[e.message for e in errors]}"


def test_schema_covers_every_field_flink_reads():
    """The contract must require everything the Flink job actually consumes."""
    required = set(TRANSACTION_SCHEMA["required"])
    missing = set(FLINK_REQUIRED_FIELDS) - required
    assert not missing, f"schema is missing fields Flink reads: {missing}"


def test_injected_labels_are_consistent():
    """Normals are unlabelled; injected anomalies name an expected rule."""
    gen = TransactionGenerator(num_cards=200)
    for _ in range(100):
        tx = gen.generate_normal_transaction()
        assert tx["is_injected_anomaly"] is False
        assert tx["expected_anomaly"] is None

    for anomaly in ALL_ANOMALIES:
        batch = gen.generate_anomaly(anomaly)
        injected = [t for t in batch if t["is_injected_anomaly"]]
        assert injected, f"{anomaly} produced no labelled trigger transaction"
        for t in injected:
            assert t["expected_anomaly"] == anomaly
