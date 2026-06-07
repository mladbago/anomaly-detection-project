# common/schema.py
"""Canonical contract for a transaction payload on `payment_transactions`.

This is the single source of truth shared by the producer, the Flink detector
and the test-suite. ``TRANSACTION_SCHEMA`` is a JSON-Schema (Draft 7) document;
``FLINK_REQUIRED_FIELDS`` enumerates exactly the keys the Flink job dereferences
in ``process_element`` / the rule functions. The validation test asserts both
that the simulator emits schema-valid JSON *and* that the schema's required set
matches what Flink actually reads -- so a drift on either side fails the build.
"""

# Keys that flink_detector.job.FraudDetector reads off every transaction.
FLINK_REQUIRED_FIELDS = [
    "card_id",
    "transaction_value",
    "available_limit",
    "timestamp",
    "location",
]

TRANSACTION_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "PaymentTransaction",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "card_id",
        "user_id",
        "location",
        "transaction_value",
        "available_limit",
        "timestamp",
        "is_injected_anomaly",
        "expected_anomaly",
    ],
    "properties": {
        "card_id": {"type": "string", "pattern": r"^CARD-\d{5}$"},
        "user_id": {"type": "string", "pattern": r"^USER-\d{4}$"},
        "location": {
            "type": "object",
            "additionalProperties": False,
            "required": ["lat", "lon"],
            "properties": {
                "lat": {"type": "number", "minimum": -90, "maximum": 90},
                "lon": {"type": "number", "minimum": -180, "maximum": 180},
            },
        },
        "transaction_value": {"type": "number", "minimum": 0},
        "available_limit": {"type": "number", "exclusiveMinimum": 0},
        "timestamp": {"type": "integer", "minimum": 0},
        # Ground-truth labels for the testing strategy.
        "is_injected_anomaly": {"type": "boolean"},
        "expected_anomaly": {"type": ["string", "null"]},
    },
}
