# tests/reference_detector.py
"""A PyFlink-free twin of the streaming detector, for offline testing.

The production detector (:class:`flink_detector.job.FraudDetector`) is a stateful
PyFlink ``KeyedProcessFunction`` — you cannot run it without a cluster. But all
of its *decisions* are delegated to :mod:`flink_detector.rules`, and its only
other responsibility is per-``card_id`` keyed state plus a fixed check order.

This class reproduces exactly that orchestration over plain dictionaries, calling
the very same rule functions in the very same order as ``process_element``. So a
stream replayed through here yields the same alerts the Flink job would — which
is what makes the offline precision test a faithful proxy for the real pipeline.
"""
from typing import Dict, List, Optional

from common import constants
from flink_detector import rules


class _CardState:
    """Mirror of the six ValueState handles the Flink job keeps per card."""

    def __init__(self):
        self.last_tx: Optional[dict] = None
        self.count: int = 0
        self.window_start: Optional[int] = None
        self.amt_count: int = 0
        self.amt_mean: float = 0.0
        self.amt_m2: float = 0.0


class ReferenceDetector:
    """Replays a transaction stream and emits the same alerts Flink would."""

    def __init__(self):
        self._state: Dict[str, _CardState] = {}

    def process(self, tx: dict) -> List[str]:
        """Run all six checks for one transaction, in the job's exact order."""
        st = self._state.setdefault(tx["card_id"], _CardState())
        last = st.last_tx
        anomalies = [
            self._max_out(tx),
            self._night_owl(tx),
            self._impossible_travel(tx, last),
            self._card_testing(tx, last),
            self._frequency_burst(st, tx),
            self._amount_spike(st, tx),
        ]
        st.last_tx = tx
        return [a for a in anomalies if a is not None]

    # --- individual checks (delegate to the shared rules) -------------------
    @staticmethod
    def _max_out(tx) -> Optional[str]:
        if rules.exceeds_max_out(tx["transaction_value"], tx["available_limit"]):
            return constants.ANOMALY_MAX_OUT
        return None

    @staticmethod
    def _night_owl(tx) -> Optional[str]:
        import time
        hour = time.localtime(tx["timestamp"] / 1000.0).tm_hour
        if rules.is_night_owl(tx["transaction_value"], hour):
            return constants.ANOMALY_NIGHT_OWL
        return None

    @staticmethod
    def _impossible_travel(tx, last) -> Optional[str]:
        if last is None:
            return None
        distance, speed = rules.required_speed_kmh(
            last["location"]["lat"], last["location"]["lon"], last["timestamp"],
            tx["location"]["lat"], tx["location"]["lon"], tx["timestamp"],
        )
        if rules.is_impossible_travel(distance, speed):
            return constants.ANOMALY_IMPOSSIBLE_TRAVEL
        return None

    @staticmethod
    def _card_testing(tx, last) -> Optional[str]:
        if last is None:
            return None
        if rules.is_card_testing(last["transaction_value"], tx["transaction_value"]):
            return constants.ANOMALY_CARD_TESTING
        return None

    @staticmethod
    def _frequency_burst(st: _CardState, tx) -> Optional[str]:
        if rules.window_should_reset(st.window_start, tx["timestamp"]):
            st.window_start = tx["timestamp"]
            st.count = 0
        st.count += 1
        if rules.is_frequency_burst(st.count):
            return constants.ANOMALY_FREQUENCY_BURST
        return None

    @staticmethod
    def _amount_spike(st: _CardState, tx) -> Optional[str]:
        amount = float(tx["transaction_value"])
        hit = rules.is_amount_spike(amount, st.amt_mean, st.amt_m2, st.amt_count)
        st.amt_count, st.amt_mean, st.amt_m2 = rules.welford_update(
            st.amt_count, st.amt_mean, st.amt_m2, amount
        )
        return constants.ANOMALY_AMOUNT_SPIKE if hit else None
