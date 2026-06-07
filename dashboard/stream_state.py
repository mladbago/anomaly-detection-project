# dashboard/stream_state.py
"""Background Kafka tap + thread-safe in-memory state for the Streamlit UI.

Streamlit re-runs the whole script on every refresh, which is hostile to a
long-lived Kafka consumer. So we keep exactly one :class:`StreamState` per
server process (Streamlit caches it with ``@st.cache_resource``); it owns a
daemon thread that tails both Kafka topics and folds every message into compact,
lock-guarded aggregates. The UI just reads cheap snapshots of those aggregates.

The state also queries MongoDB for the *persisted* alert totals (everything the
alert-manager has ever stored), so the dashboard shows both the live session and
the durable history.
"""
import json
import threading
import time
from collections import Counter, deque
from typing import Dict, List

from confluent_kafka import Consumer
from pymongo import MongoClient

from common import config

# How much rolling history to keep for the live view.
RECENT_TX = 400          # points feeding the live map
RECENT_FEED = 15         # rows in the transaction / alert feeds
TIMESERIES_SECONDS = 120  # width of the volume-over-time charts


class StreamState:
    """Owns the consumer thread and all shared aggregates behind one lock."""

    def __init__(self):
        self._lock = threading.Lock()
        self._started = False

        # --- live aggregates (all guarded by self._lock) ---
        self.total_tx = 0
        self.total_alerts = 0
        self.injected_tx = 0          # ground-truth anomalies seen on the wire
        self.unique_cards = set()
        self.alert_counts = Counter()
        self.recent_tx = deque(maxlen=RECENT_FEED)
        self.recent_alerts = deque(maxlen=RECENT_FEED)
        self.geo_points = deque(maxlen=RECENT_TX)   # {lat, lon, kind}
        self.tx_per_sec = Counter()                  # epoch_second -> count
        self.alerts_per_sec = Counter()              # epoch_second -> count
        self.connected = False
        self.last_error = ""

    # ------------------------------------------------------------------ start
    def start(self):
        """Idempotently launch the background consumer thread."""
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(target=self._consume_loop, name="kafka-tap", daemon=True).start()

    # ------------------------------------------------------------- consumer
    def _consume_loop(self):
        """Tail both topics forever, reconnecting on failure."""
        while True:
            try:
                consumer = Consumer({
                    "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                    "group.id": f"streamlit-dashboard-{int(time.time())}",
                    "auto.offset.reset": "latest",
                })
                consumer.subscribe([config.TOPIC_TRANSACTIONS, config.TOPIC_ALERTS])
                with self._lock:
                    self.connected = True
                    self.last_error = ""
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None or msg.error():
                        continue
                    self._ingest(msg.topic(), json.loads(msg.value().decode("utf-8")))
            except Exception as exc:  # noqa: BLE001 - surface, then retry
                with self._lock:
                    self.connected = False
                    self.last_error = str(exc)
                time.sleep(3)

    def _ingest(self, topic: str, payload: dict):
        """Fold one Kafka record into the aggregates."""
        now = int(time.time())
        with self._lock:
            if topic == config.TOPIC_TRANSACTIONS:
                self.total_tx += 1
                self.tx_per_sec[now] += 1
                self.unique_cards.add(payload.get("card_id"))
                if payload.get("is_injected_anomaly"):
                    self.injected_tx += 1
                self.recent_tx.appendleft(payload)
                loc = payload.get("location", {})
                if "lat" in loc and "lon" in loc:
                    self.geo_points.append({"lat": loc["lat"], "lon": loc["lon"], "kind": "transaction"})
            else:  # transaction_alerts
                self.total_alerts += 1
                self.alerts_per_sec[now] += 1
                atype = payload.get("alert_type", "UNKNOWN")
                self.alert_counts[atype] += 1
                self.recent_alerts.appendleft(payload)
                loc = payload.get("transaction", {}).get("location", {})
                if atype == "IMPOSSIBLE_TRAVEL" and "lat" in loc and "lon" in loc:
                    self.geo_points.append({"lat": loc["lat"], "lon": loc["lon"], "kind": "gps_jump"})
            self._prune(now)

    def _prune(self, now: int):
        """Drop time-series buckets older than the chart window (lock held)."""
        cutoff = now - TIMESERIES_SECONDS
        for bucket in (self.tx_per_sec, self.alerts_per_sec):
            for sec in [s for s in bucket if s < cutoff]:
                del bucket[sec]

    # -------------------------------------------------------------- snapshot
    def snapshot(self) -> Dict:
        """Cheap, consistent copy of everything the UI needs for one render."""
        now = int(time.time())
        with self._lock:
            series = []
            for sec in range(now - TIMESERIES_SECONDS + 1, now + 1):
                series.append({
                    "time": sec,
                    "transactions": self.tx_per_sec.get(sec, 0),
                    "alerts": self.alerts_per_sec.get(sec, 0),
                })
            return {
                "total_tx": self.total_tx,
                "total_alerts": self.total_alerts,
                "injected_tx": self.injected_tx,
                "unique_cards": len(self.unique_cards),
                "alert_counts": dict(self.alert_counts),
                "recent_tx": list(self.recent_tx),
                "recent_alerts": list(self.recent_alerts),
                "geo_points": list(self.geo_points),
                "series": series,
                "connected": self.connected,
                "last_error": self.last_error,
            }

    # ---------------------------------------------------------------- mongo
    @staticmethod
    def persisted_alert_counts() -> List[dict]:
        """Durable alert totals by type, straight from MongoDB (best-effort)."""
        try:
            client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=1500)
            coll = client[config.MONGO_DB_NAME][config.MONGO_COLLECTION_ALERTS]
            pipeline = [{"$group": {"_id": "$alert_type", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}]
            return [{"alert_type": d["_id"], "count": d["count"]} for d in coll.aggregate(pipeline)]
        except Exception:  # noqa: BLE001 - dashboard must render without Mongo
            return []
