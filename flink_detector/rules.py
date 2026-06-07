# flink_detector/rules.py
"""Pure, side-effect-free statistical rules used by the fraud detector.

This module deliberately imports **no PyFlink**. Every detection decision the
streaming job makes is expressed here as a small, deterministic function over
plain numbers, so the exact same logic can be:

  * called by the stateful :class:`FraudDetector` running inside Flink, and
  * unit-tested on a laptop with nothing but ``pytest`` (no cluster, no Kafka).

Keeping the maths in one importable place is what lets the test-suite prove the
statistical algorithms are correct independently of the distributed runtime.
"""
import math
from typing import Tuple

from common import constants
from common.geo import haversine_km


# --- MAX_OUT -----------------------------------------------------------------
def exceeds_max_out(value: float, limit: float) -> bool:
    """A single charge consumes >= 95% of the card's available limit."""
    return value >= constants.MAX_OUT_RATIO * limit


# --- NIGHT_OWL ---------------------------------------------------------------
def is_night_owl(value: float, local_hour: int) -> bool:
    """High-value charge landing in the small-hours local-time window."""
    return value > constants.NIGHT_OWL_MIN_VALUE and local_hour == constants.NIGHT_OWL_HOUR


# --- IMPOSSIBLE_TRAVEL (Haversine speed limit) -------------------------------
def required_speed_kmh(lat1: float, lon1: float, ts1_ms: int,
                       lat2: float, lon2: float, ts2_ms: int) -> Tuple[float, float]:
    """Return ``(distance_km, implied_speed_kmh)`` between two timed fixes.

    A non-positive time delta yields infinite speed (teleportation).
    """
    distance = haversine_km(lat1, lon1, lat2, lon2)
    hours = (ts2_ms - ts1_ms) / 3_600_000.0
    speed = float("inf") if hours <= 0 else distance / hours
    return distance, speed


def is_impossible_travel(distance_km: float, speed_kmh: float) -> bool:
    """Flag only a *substantial* jump taken faster than is physically possible."""
    return distance_km >= constants.IMPOSSIBLE_TRAVEL_MIN_KM and speed_kmh > constants.MAX_SPEED_KMH


# --- CARD_TESTING ------------------------------------------------------------
def is_card_testing(last_value: float, value: float) -> bool:
    """A throwaway micro-charge immediately followed by a big-ticket spike."""
    return last_value < constants.MICRO_CHARGE_MAX and value > constants.SPIKE_MIN_VALUE


# --- FREQUENCY_BURST (sliding count window) ----------------------------------
def window_should_reset(window_start_ms, ts_ms: int,
                        window_ms: int = constants.FREQUENCY_WINDOW_MS) -> bool:
    """True when ``ts`` falls outside the current counting window (or none yet)."""
    return window_start_ms is None or ts_ms - window_start_ms >= window_ms


def is_frequency_burst(count_in_window: int) -> bool:
    """5+ charges observed inside a single 2-second window."""
    return count_in_window >= constants.FREQUENCY_BURST_THRESHOLD


# --- AMOUNT_SPIKE (rolling Welford Z-score) ----------------------------------
def welford_update(n: int, mean: float, m2: float, x: float) -> Tuple[int, float, float]:
    """Fold observation ``x`` into Welford running ``(count, mean, M2)``."""
    n += 1
    delta = x - mean
    mean += delta / n
    m2 += delta * (x - mean)
    return n, mean, m2


def zscore(value: float, mean: float, m2: float, n: int) -> float:
    """Standard score of ``value`` against a Welford-accumulated distribution."""
    if n < 1:
        return 0.0
    std = math.sqrt(m2 / n)
    return 0.0 if std == 0.0 else (value - mean) / std


def is_amount_spike(value: float, mean: float, m2: float, n: int) -> bool:
    """Charge sits >= 3.5 rolling std-devs above the per-card mean.

    The card must be past its warm-up (``ZSCORE_MIN_SAMPLES``) before any score
    is trusted, otherwise a thin sample would manufacture false outliers.
    """
    if n < constants.ZSCORE_MIN_SAMPLES:
        return False
    return zscore(value, mean, m2, n) >= constants.ZSCORE_THRESHOLD
