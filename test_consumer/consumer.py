# test_consumer/consumer.py
"""Live correctness/visualization consumer for the `payment_transactions` topic.

This is the "test consumer" from the spec: it lets you eyeball that the
simulator is producing well-formed data by rendering a self-refreshing console
dashboard -- an ASCII histogram of transaction values plus rolling counters.
"""
import json
import os
import sys
import time
from collections import Counter, deque

from confluent_kafka import Consumer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import config

# Histogram buckets for transaction value (in currency units). The last bucket
# is an open-ended catch-all for big-ticket / anomalous charges.
VALUE_BUCKETS = [
    (0, 1, "  <1   "),
    (1, 50, "  1-50 "),
    (50, 150, " 50-150"),
    (150, 500, "150-500"),
    (500, 2000, "0.5k-2k"),
    (2000, float("inf"), "  >2k  "),
]
BAR_WIDTH = 50
REFRESH_EVERY = 25          # redraw after this many messages
RECENT_FEED_SIZE = 8        # rows in the live transaction feed


class TransactionDashboard:
    """Consumes transactions and paints a live ASCII dashboard of the stream."""

    def __init__(self):
        self.consumer = Consumer(self._consumer_config())
        self.buckets = Counter()
        self.unique_cards = set()
        self.recent = deque(maxlen=RECENT_FEED_SIZE)
        self.total = 0
        self.sum_value = 0.0
        self.started_at = time.time()
        print(f"Test consumer connected to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}")

    @staticmethod
    def _consumer_config() -> dict:
        """SRP: build the Kafka consumer settings."""
        return {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "test-consumer-dashboard",
            "auto.offset.reset": "latest",
        }

    @staticmethod
    def _bucket_for(value: float) -> str:
        """SRP: map a transaction value onto its histogram bucket label."""
        for low, high, label in VALUE_BUCKETS:
            if low <= value < high:
                return label
        return VALUE_BUCKETS[-1][2]

    def _record(self, tx: dict):
        """SRP: fold a single transaction into the aggregate counters."""
        value = float(tx["transaction_value"])
        self.total += 1
        self.sum_value += value
        self.buckets[self._bucket_for(value)] += 1
        self.unique_cards.add(tx["card_id"])
        self.recent.appendleft(tx)

    def _render(self):
        """SRP: clear the screen and paint the full dashboard."""
        os.system("cls" if os.name == "nt" else "clear")
        print(self._header())
        print(self._histogram())
        print(self._feed())

    def _header(self) -> str:
        """SRP: render the top summary line of running totals."""
        elapsed = max(time.time() - self.started_at, 1e-9)
        rate = self.total / elapsed
        avg = self.sum_value / self.total if self.total else 0.0
        return (
            "=" * 70 + "\n"
            "  LIVE TRANSACTION STREAM  (topic: "
            f"{config.TOPIC_TRANSACTIONS})\n"
            "=" * 70 + "\n"
            f"  total: {self.total:<8d}  rate: {rate:6.1f} tx/s  "
            f"avg: ${avg:8.2f}  cards: {len(self.unique_cards):<6d}\n"
        )

    def _histogram(self) -> str:
        """SRP: render the ASCII bar chart of value distribution."""
        peak = max(self.buckets.values(), default=1)
        lines = ["  TRANSACTION VALUE DISTRIBUTION", "  " + "-" * 66]
        for _, _, label in VALUE_BUCKETS:
            count = self.buckets[label]
            filled = int(BAR_WIDTH * count / peak) if peak else 0
            bar = "#" * filled
            lines.append(f"  {label} | {bar:<{BAR_WIDTH}} {count}")
        return "\n".join(lines) + "\n"

    def _feed(self) -> str:
        """SRP: render the most recent transactions as a scrolling feed."""
        lines = ["  RECENT TRANSACTIONS", "  " + "-" * 66]
        for tx in self.recent:
            loc = tx["location"]
            lines.append(
                f"  {tx['card_id']:<11} ${tx['transaction_value']:>9.2f}  "
                f"({loc['lat']:7.3f}, {loc['lon']:7.3f})  limit ${tx['available_limit']:,.0f}"
            )
        return "\n".join(lines)

    def _poll_once(self):
        """SRP: process a single poll cycle using guard clauses."""
        msg = self.consumer.poll(1.0)
        if msg is None:
            return
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            return
        self._record(json.loads(msg.value().decode("utf-8")))
        if self.total % REFRESH_EVERY == 0:
            self._render()

    def run(self):
        """SRP: the main consume loop."""
        self.consumer.subscribe([config.TOPIC_TRANSACTIONS])
        print(f"Listening on '{config.TOPIC_TRANSACTIONS}'. Press Ctrl+C to stop.")
        try:
            while True:
                self._poll_once()
        except KeyboardInterrupt:
            print("\nStopping test consumer gracefully...")
        finally:
            self.consumer.close()
            print("Consumer closed.")


if __name__ == "__main__":
    TransactionDashboard().run()
