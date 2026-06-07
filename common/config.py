import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRANSACTIONS = "payment_transactions"
TOPIC_ALERTS = "transaction_alerts"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = "fraud_detection"
MONGO_COLLECTION_ALERTS = "alerts"