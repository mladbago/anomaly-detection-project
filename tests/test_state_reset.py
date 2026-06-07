# tests/test_state_reset.py
"""STATE TEST — does Flink's temporary per-card memory reset correctly?

The frequency-burst detector keeps a count inside a 2-second window. The risk is
that stale counts leak across window boundaries (a slow trickle of charges would
eventually, wrongly, look like a burst). We drive the keyed-state orchestration
(via the faithful :class:`ReferenceDetector` twin) and assert the count drops to
zero the instant a new window opens — and that state is isolated per card.
"""
from common import constants
from flink_detector import rules
from tests.reference_detector import ReferenceDetector, _CardState


def _tx(card, value, ts, lat=52.0, lon=21.0, limit=10000.0):
    return {
        "card_id": card, "user_id": "USER-0001",
        "location": {"lat": lat, "lon": lon},
        "transaction_value": value, "available_limit": limit, "timestamp": ts,
    }


def test_window_counter_resets_between_windows():
    """Four charges, then a long gap, then four more -> never reaches 5."""
    det = ReferenceDetector()
    base = 1_000_000
    # 4 charges inside one 2s window: count climbs to 4, no burst.
    for i in range(4):
        assert constants.ANOMALY_FREQUENCY_BURST not in det.process(_tx("CARD-1", 50, base + i * 100))

    # Jump well past the window; the counter must restart from zero.
    far = base + 10_000
    alerts = det.process(_tx("CARD-1", 50, far))
    assert constants.ANOMALY_FREQUENCY_BURST not in alerts

    state = det._state["CARD-1"]
    assert state.window_start == far
    assert state.count == 1, "counter did not reset when the new window opened"


def test_burst_fires_only_inside_one_window():
    """Five charges packed into 2s do trip the burst rule."""
    det = ReferenceDetector()
    base = 2_000_000
    fired = False
    for i in range(5):
        if constants.ANOMALY_FREQUENCY_BURST in det.process(_tx("CARD-9", 50, base + i * 100)):
            fired = True
    assert fired, "five charges within 2s should raise a frequency burst"


def test_state_is_isolated_per_card():
    """One card's burst must not contaminate another card's counter."""
    det = ReferenceDetector()
    base = 3_000_000
    for i in range(4):
        det.process(_tx("CARD-A", 50, base + i * 100))
    # A different card starting fresh should be at count 1, not inherit CARD-A's 4.
    det.process(_tx("CARD-B", 50, base))
    assert det._state["CARD-B"].count == 1
    assert det._state["CARD-A"].count == 4


def test_welford_state_is_finite_and_monotonic_in_count():
    """The rolling Z-score accumulator stays well-formed as samples fold in."""
    st = _CardState()
    for x in [40, 60, 50, 55, 45, 50, 48, 52, 51]:
        st.amt_count, st.amt_mean, st.amt_m2 = rules.welford_update(
            st.amt_count, st.amt_mean, st.amt_m2, x
        )
    assert st.amt_count == 9
    assert st.amt_m2 >= 0.0                 # variance accumulator never negative
    assert 40 <= st.amt_mean <= 60
