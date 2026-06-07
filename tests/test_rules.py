# tests/test_rules.py
"""Unit tests for the statistical rules (the maths, in isolation).

These pin down each detector's decision boundary without any Kafka/Flink/Mongo.
"""
import math

from common import constants
from flink_detector import rules


def test_max_out_boundary():
    """Fires at exactly 95% of the limit, not just below it."""
    limit = 1000.0
    assert rules.exceeds_max_out(950.0, limit) is True
    assert rules.exceeds_max_out(949.99, limit) is False


def test_night_owl_needs_value_and_hour():
    """Both the high value AND the 3 AM hour are required."""
    assert rules.is_night_owl(2000.0, 3) is True
    assert rules.is_night_owl(2000.0, 4) is False     # wrong hour
    assert rules.is_night_owl(500.0, 3) is False       # too cheap


def test_impossible_travel_requires_distance_and_speed():
    """A tiny GPS drift never trips, even at absurd implied speed."""
    # ~3 km in 0.1 s -> ~108,000 km/h, but distance is below the floor.
    dist, speed = rules.required_speed_kmh(52.0, 21.0, 0, 52.02, 21.02, 100)
    assert speed > constants.MAX_SPEED_KMH
    assert rules.is_impossible_travel(dist, speed) is False

    # ~1,600 km in 1 s -> genuine teleportation.
    dist, speed = rules.required_speed_kmh(52.0, 21.0, 0, 52.0, 45.0, 1000)
    assert rules.is_impossible_travel(dist, speed) is True


def test_card_testing_micro_then_spike():
    assert rules.is_card_testing(0.5, 800.0) is True
    assert rules.is_card_testing(5.0, 800.0) is False   # last charge not a micro
    assert rules.is_card_testing(0.5, 100.0) is False   # follow-up not a spike


def test_frequency_window_resets_after_two_seconds():
    assert rules.window_should_reset(None, 1000) is True            # no window yet
    assert rules.window_should_reset(1000, 2999) is False           # still inside
    assert rules.window_should_reset(1000, 3000) is True            # exactly 2s later


def test_zscore_matches_textbook_value():
    """Welford-accumulated stats reproduce the closed-form Z-score."""
    data = [40, 42, 41, 43, 39, 44, 40, 41]
    n = mean = m2 = 0
    for x in data:
        n, mean, m2 = rules.welford_update(n, mean, m2, x)
    pop_std = math.sqrt(sum((x - mean) ** 2 for x in data) / len(data))
    expected = (1500 - mean) / pop_std
    assert math.isclose(rules.zscore(1500, mean, m2, n), expected, rel_tol=1e-9)


def test_amount_spike_respects_warmup():
    """Below the warm-up sample count, nothing is ever flagged."""
    n = mean = m2 = 0
    for x in [50, 51, 49]:                      # only 3 samples << warm-up
        n, mean, m2 = rules.welford_update(n, mean, m2, x)
    assert rules.is_amount_spike(5000, mean, m2, n) is False

    for x in [50, 51, 49, 50, 52, 48, 50, 51, 49]:   # now well past warm-up
        n, mean, m2 = rules.welford_update(n, mean, m2, x)
    assert rules.is_amount_spike(5000, mean, m2, n) is True
    assert rules.is_amount_spike(51, mean, m2, n) is False
