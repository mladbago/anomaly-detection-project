import json
import time
import random
import sys
import os
from confluent_kafka import Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import config, constants
from simulator.generator import TransactionGenerator

class StreamProducer:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS})
        self.generator = TransactionGenerator()
        print(f"Connected to Kafka broker at {config.KAFKA_BOOTSTRAP_SERVERS}")

    @staticmethod
    def _delivery_report(err, msg):
        """SRP: Handles only the callback status of a message."""
        if err is not None:
            print(f"Message delivery failed: {err}")

    # Every anomaly the simulator knows how to fabricate; one is picked at
    # random whenever the dice say this tick should be fraudulent.
    ANOMALY_TYPES = [
        constants.ANOMALY_IMPOSSIBLE_TRAVEL,
        constants.ANOMALY_CARD_TESTING,
        constants.ANOMALY_FREQUENCY_BURST,
        constants.ANOMALY_MAX_OUT,
        constants.ANOMALY_NIGHT_OWL,
        constants.ANOMALY_AMOUNT_SPIKE,
    ]

    def _get_next_transactions(self) -> list:
        """SRP: Handles only the business logic of what to generate next.

        ~8% of ticks fabricate a randomly chosen anomaly; the rest are normal.
        """
        if random.random() < 0.08:
            anomaly_type = random.choice(self.ANOMALY_TYPES)
            print(f"--- Injecting anomaly: {anomaly_type} ---")
            return self.generator.generate_anomaly(anomaly_type)
        return [self.generator.generate_normal_transaction()]

    def _publish_transaction(self, tx: dict):
        """SRP: Handles only the formatting and pushing of a single payload."""
        self.producer.produce(
            topic=config.TOPIC_TRANSACTIONS,
            key=tx["card_id"],
            value=json.dumps(tx),
            callback=self._delivery_report
        )
        print(f"Sent: {tx['card_id']} | ${tx['transaction_value']} | Lat: {tx['location']['lat']} | Lon: {tx['location']['lon']}")

    def run(self):
        """SRP: The main orchestration loop."""
        print(f"Starting stream to '{config.TOPIC_TRANSACTIONS}'. Press Ctrl+C to stop.")
        try:
            while True:
                transactions = self._get_next_transactions()
                
                for tx in transactions:
                    self._publish_transaction(tx)

                self.producer.poll(0)
                time.sleep(0.5) 

        except KeyboardInterrupt:
            print("\nStopping data stream gracefully...")
        finally:
            self.producer.flush()
            print("Producer flushed and closed.")

if __name__ == "__main__":
    app = StreamProducer()
    app.run()