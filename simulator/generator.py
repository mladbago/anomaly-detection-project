# simulator/generator.py
import random
import time
from typing import Dict, Any, List
from common import constants

class TransactionGenerator:
    def __init__(self, num_cards: int = constants.TOTAL_CARDS):
        self.num_cards = num_cards
        self.cards = self._initialize_cards()
        
        self._anomaly_handlers = {
            constants.ANOMALY_IMPOSSIBLE_TRAVEL: self._handle_impossible_travel,
            constants.ANOMALY_CARD_TESTING: self._handle_card_testing,
            constants.ANOMALY_FREQUENCY_BURST: self._handle_frequency_burst,
            constants.ANOMALY_MAX_OUT: self._handle_max_out,
            constants.ANOMALY_NIGHT_OWL: self._handle_night_owl,
            constants.ANOMALY_AMOUNT_SPIKE: self._handle_amount_spike
        }
        print(f"Initialized {self.num_cards} virtual payment cards.")

    def _initialize_cards(self) -> Dict[str, dict]:
        return {
            f"CARD-{i:05d}": self._create_card_profile(i)
            for i in range(1, self.num_cards + 1)
        }

    def _create_card_profile(self, index: int) -> dict:
        return {
            "card_id": f"CARD-{index:05d}",
            "user_id": f"USER-{random.randint(1, constants.TOTAL_USERS):04d}",
            "limit": random.choice([2000.0, 5000.0, 10000.0, 25000.0]),
            "current_lat": random.uniform(constants.POLAND_LAT_MIN, constants.POLAND_LAT_MAX),
            "current_lon": random.uniform(constants.POLAND_LON_MIN, constants.POLAND_LON_MAX)
        }

    def _get_drifted_location(self, lat: float, lon: float) -> tuple:
        """Simulates natural local movement within Poland."""
        new_lat = max(constants.POLAND_LAT_MIN, min(constants.POLAND_LAT_MAX, lat + random.uniform(-0.02, 0.02)))
        new_lon = max(constants.POLAND_LON_MIN, min(constants.POLAND_LON_MAX, lon + random.uniform(-0.02, 0.02)))
        return round(new_lat, 6), round(new_lon, 6)

    def _build_payload(self, card: dict, amount: float, lat: float, lon: float, timestamp: int,
                       injected: bool = False, expected: str = None) -> dict:
        """Build one transaction payload.

        ``injected``/``expected`` are the ground-truth labels the testing
        strategy relies on: ``is_injected_anomaly`` marks a charge that was
        deliberately fabricated to trip a detector, and ``expected_anomaly``
        names which rule it is engineered to fire. They ride along inside the
        alert envelope, letting the test-suite measure detector precision
        without any external bookkeeping.
        """
        return {
            "card_id": card["card_id"],
            "user_id": card["user_id"],
            "location": {"lat": lat, "lon": lon},
            "transaction_value": round(amount, 2),
            "available_limit": card["limit"],
            "timestamp": timestamp,
            "is_injected_anomaly": injected,
            "expected_anomaly": expected,
        }

    def generate_normal_transaction(self) -> Dict[str, Any]:
        """Generates one standard transaction and updates the card's physical location."""
        card = random.choice(list(self.cards.values()))
        amount = random.uniform(constants.MIN_NORMAL_TX_VALUE, constants.MAX_NORMAL_TX_VALUE)
        lat, lon = self._get_drifted_location(card["current_lat"], card["current_lon"])
        
        card["current_lat"], card["current_lon"] = lat, lon
        return self._build_payload(card, amount, lat, lon, int(time.time() * 1000))


    def _handle_impossible_travel(self, card: dict, base_time: int) -> List[dict]:
        """Kinematic anomaly: Normal transaction, then a massive coordinate jump (1500+ km) instantly."""
        normal_lat, normal_lon = self._get_drifted_location(card["current_lat"], card["current_lon"])
        tx1 = self._build_payload(card, 25.0, normal_lat, normal_lon, base_time)  # benign setup charge

        jump_lat = round(normal_lat + random.choice([-15.0, 15.0]), 6)
        jump_lon = round(normal_lon + random.choice([-15.0, 15.0]), 6)
        # Only 1 second later -> the jump is the charge that must be flagged.
        tx2 = self._build_payload(card, 300.0, jump_lat, jump_lon, base_time + 1000,
                                  injected=True, expected=constants.ANOMALY_IMPOSSIBLE_TRAVEL)

        card["current_lat"], card["current_lon"] = jump_lat, jump_lon
        return [tx1, tx2]

    def _handle_card_testing(self, card: dict, base_time: int) -> List[dict]:
        """Bot testing: Two micro-charges followed instantly by a massive purchase."""
        lat, lon = self._get_drifted_location(card["current_lat"], card["current_lon"])
        return [
            self._build_payload(card, 0.50, lat, lon, base_time),          # micro probe (setup)
            self._build_payload(card, 0.80, lat, lon, base_time + 500),    # micro probe (setup)
            self._build_payload(card, card["limit"] * 0.8, lat, lon, base_time + 1000,
                                injected=True, expected=constants.ANOMALY_CARD_TESTING),
        ]

    def _handle_frequency_burst(self, card: dict, base_time: int) -> List[dict]:
        """A rapid burst of charges; every one is part of the fraudulent episode."""
        lat, lon = self._get_drifted_location(card["current_lat"], card["current_lon"])
        return [
            self._build_payload(card, random.uniform(50.0, 100.0), lat, lon, base_time + (i * 150),
                                injected=True, expected=constants.ANOMALY_FREQUENCY_BURST)
            for i in range(6)
        ]

    def _handle_max_out(self, card: dict, base_time: int) -> List[dict]:
        lat, lon = self._get_drifted_location(card["current_lat"], card["current_lon"])
        return [self._build_payload(card, card["limit"] * 0.99, lat, lon, base_time,
                                    injected=True, expected=constants.ANOMALY_MAX_OUT)]

    def _handle_night_owl(self, card: dict, base_time: int) -> List[dict]:
        """A massive purchase forced to happen at 3:30 AM local time."""
        lat, lon = self._get_drifted_location(card["current_lat"], card["current_lon"])

        now = time.localtime()
        night_time = time.mktime((now.tm_year, now.tm_mon, now.tm_mday, 3, 30, 0, now.tm_wday, now.tm_yday, now.tm_isdst))

        return [self._build_payload(card, random.uniform(2000.0, card["limit"]), lat, lon, int(night_time * 1000),
                                    injected=True, expected=constants.ANOMALY_NIGHT_OWL)]

    def _handle_amount_spike(self, card: dict, base_time: int) -> List[dict]:
        """Builds a calm spending history, then one charge far above the norm.

        The detector needs a per-card baseline before a Z-score means anything,
        so we emit a run of tight, low-value charges (well spaced to avoid the
        frequency-burst rule) and finish with a single statistical outlier. Only
        that final outlier is labelled as the injected anomaly.
        """
        lat, lon = self._get_drifted_location(card["current_lat"], card["current_lon"])
        history = [
            self._build_payload(card, random.uniform(40.0, 60.0), lat, lon, base_time + (i * 5000))
            for i in range(constants.ZSCORE_MIN_SAMPLES + 2)
        ]
        spike_time = history[-1]["timestamp"] + 5000
        history.append(self._build_payload(card, 1500.0, lat, lon, spike_time,
                                            injected=True, expected=constants.ANOMALY_AMOUNT_SPIKE))
        return history

    def generate_anomaly(self, anomaly_type: str) -> List[Dict[str, Any]]:
        """Routes the anomaly request to the specific mathematical handler."""
        card = random.choice(list(self.cards.values()))
        current_time_ms = int(time.time() * 1000)
        
        handler = self._anomaly_handlers.get(anomaly_type)
        if not handler:
            raise ValueError(f"Unknown anomaly type: {anomaly_type}")
            
        return handler(card, current_time_ms)