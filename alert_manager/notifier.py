# alert_manager/notifier.py
"""Alert dashboard: consumes fraud alerts, visualizes them, and persists to Mongo.

This is the spec's "program for reading alerts, visualization, and notifying".
It bridges the `transaction_alerts` topic into the MongoDB `alerts` collection
while painting a self-refreshing console dashboard: an ASCII breakdown of alert
counts by type plus a live feed of the most recent fraud notifications.
"""
import json
import os
import sys
import time
from collections import Counter, deque

from confluent_kafka import Consumer
from pymongo import MongoClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import config

BAR_WIDTH = 45
RECENT_FEED_SIZE = 12


class AlertDashboard:
    """Bridges the `transaction_alerts` topic into MongoDB and the console."""

    def __init__(self):
        self.consumer = Consumer(self._consumer_config())
        self.collection = self._connect_collection()
        self.counts = Counter()
        self.recent = deque(maxlen=RECENT_FEED_SIZE)
        self.total = 0
        self.started_at = time.time()
        print(f"Alert dashboard connected to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}")

    @staticmethod
    def _consumer_config() -> dict:
        """SRP: build the Kafka consumer settings."""
        return {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "alert-dashboard",
            "auto.offset.reset": "earliest",
        }

    @staticmethod
    def _connect_collection():
        """SRP: open the MongoDB handle for the alerts collection."""
        client = MongoClient(config.MONGO_URI)
        return client[config.MONGO_DB_NAME][config.MONGO_COLLECTION_ALERTS]

    @staticmethod
    def _parse(msg) -> dict:
        """SRP: decode a raw Kafka message into an alert document."""
        return json.loads(msg.value().decode("utf-8"))

    def _store_alert(self, alert: dict):
        """SRP: persist one alert document into MongoDB."""
        self.collection.insert_one(dict(alert))

    def _record(self, alert: dict):
        """SRP: fold an alert into the in-memory aggregates."""
        self.total += 1
        self.counts[alert["alert_type"]] += 1
        self.recent.appendleft(alert)

    def _render(self):
        """SRP: clear the screen and paint the alert dashboard."""
        os.system("cls" if os.name == "nt" else "clear")
        print(self._header())
        print(self._breakdown())
        print(self._feed())

    def _header(self) -> str:
        """SRP: render the top summary banner."""
        elapsed = max(time.time() - self.started_at, 1e-9)
        rate = self.total * 60.0 / elapsed
        return (
            "!" * 70 + "\n"
            "  FRAUD ALERT DASHBOARD  (topic: "
            f"{config.TOPIC_ALERTS} -> mongo: {config.MONGO_DB_NAME}.{config.MONGO_COLLECTION_ALERTS})\n"
            "!" * 70 + "\n"
            f"  total alerts: {self.total:<8d}  rate: {rate:6.1f} alerts/min\n"
        )

    def _breakdown(self) -> str:
        """SRP: render the ASCII bar chart of alert counts by type."""
        peak = max(self.counts.values(), default=1)
        lines = ["  ALERTS BY TYPE", "  " + "-" * 66]
        for alert_type, count in sorted(self.counts.items(), key=lambda kv: -kv[1]):
            filled = int(BAR_WIDTH * count / peak) if peak else 0
            lines.append(f"  {alert_type:<18} | {'#' * filled:<{BAR_WIDTH}} {count}")
        return "\n".join(lines) + "\n"

    def _feed(self) -> str:
        """SRP: render the most recent alerts as a scrolling feed."""
        lines = ["  RECENT ALERTS", "  " + "-" * 66]
        for alert in self.recent:
            tx = alert.get("transaction", {})
            lines.append(
                f"  [{alert['alert_type']:<17}] {alert['card_id']:<11} "
                f"${tx.get('transaction_value', 0):>9.2f}  ts={alert['timestamp']}"
            )
        return "\n".join(lines)

    def _handle_message(self, msg):
        """SRP: turn a valid message into a stored + visualized + logged alert."""
        alert = self._parse(msg)
        self._store_alert(alert)
        self._record(alert)
        # A plain log line guarantees the notification is captured even when
        # the console is not being watched (e.g. `docker logs`).
        print(f"[ALERT] {alert['alert_type']} | {alert['card_id']} | ts={alert['timestamp']}")
        self._render()

    def _poll_once(self):
        """SRP: process a single poll cycle using guard clauses."""
        msg = self.consumer.poll(1.0)
        if msg is None:
            return
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            return
        self._handle_message(msg)

    def run(self):
        """SRP: the main consume loop."""
        self.consumer.subscribe([config.TOPIC_ALERTS])
        print(f"Listening on '{config.TOPIC_ALERTS}'. Press Ctrl+C to stop.")
        try:
            while True:
                self._poll_once()
        except KeyboardInterrupt:
            print("\nStopping alert dashboard gracefully...")
        finally:
            self.consumer.close()
            print("Consumer closed.")


if __name__ == "__main__":
    AlertDashboard().run()
