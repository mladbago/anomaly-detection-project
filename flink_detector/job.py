# flink_detector/job.py
"""Real-time payment fraud detector (PyFlink 1.18+).

Reads `payment_transactions`, runs six stateful anomaly checks keyed by
`card_id`, and emits alerts to `transaction_alerts`.

All the actual maths lives in the PyFlink-free :mod:`flink_detector.rules`
module; this file owns only the *stateful orchestration* (keyed ValueState,
ordering, the Welford accumulator) and the Kafka source/sink wiring.
"""
import glob
import json
import os
import sys
import time
from typing import Iterable, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from common import config, constants
from flink_detector import rules


class FraudDetector(KeyedProcessFunction):
    """One KeyedProcessFunction per card_id holding all anomaly state."""

    def open(self, ctx: RuntimeContext):
        """SRP: wire up the ValueState handles for this key."""
        self.last_tx = ctx.get_state(ValueStateDescriptor("last_tx", Types.STRING()))
        self.count = ctx.get_state(ValueStateDescriptor("freq_count", Types.INT()))
        self.window_start = ctx.get_state(ValueStateDescriptor("window_start", Types.LONG()))
        # Welford running statistics for the rolling Z-score amount profile.
        self.amt_count = ctx.get_state(ValueStateDescriptor("amt_count", Types.INT()))
        self.amt_mean = ctx.get_state(ValueStateDescriptor("amt_mean", Types.DOUBLE()))
        self.amt_m2 = ctx.get_state(ValueStateDescriptor("amt_m2", Types.DOUBLE()))

    def process_element(self, value: str, ctx) -> Iterable[str]:
        """SRP: orchestrate detection for one transaction, then persist it."""
        tx = json.loads(value)
        last = self._read_last()
        anomalies = self._run_detectors(tx, last)
        self.last_tx.update(value)
        for anomaly in anomalies:
            yield self._build_alert(anomaly, tx)

    def _read_last(self) -> Optional[dict]:
        """SRP: deserialize the previously seen transaction, if any."""
        raw = self.last_tx.value()
        return json.loads(raw) if raw is not None else None

    def _run_detectors(self, tx: dict, last: Optional[dict]) -> list:
        """SRP: collect the names of every anomaly this transaction triggers."""
        results = [
            self._check_max_out(tx),
            self._check_night_owl(tx),
            self._check_impossible_travel(tx, last),
            self._check_card_testing(tx, last),
            self._check_frequency_burst(tx),
            self._check_amount_spike(tx),
        ]
        return [name for name in results if name is not None]

    def _check_max_out(self, tx: dict) -> Optional[str]:
        """MAX_OUT: a single charge consumes >= 95% of the available limit."""
        if rules.exceeds_max_out(tx["transaction_value"], tx["available_limit"]):
            return constants.ANOMALY_MAX_OUT
        return None

    def _check_night_owl(self, tx: dict) -> Optional[str]:
        """NIGHT_OWL: high-value charge landing in the 3 AM local-time window."""
        if rules.is_night_owl(tx["transaction_value"], self._local_hour(tx["timestamp"])):
            return constants.ANOMALY_NIGHT_OWL
        return None

    def _check_impossible_travel(self, tx: dict, last: Optional[dict]) -> Optional[str]:
        """IMPOSSIBLE_TRAVEL: a large jump covered faster than physically possible."""
        if last is None:
            return None
        distance, speed = rules.required_speed_kmh(
            last["location"]["lat"], last["location"]["lon"], last["timestamp"],
            tx["location"]["lat"], tx["location"]["lon"], tx["timestamp"],
        )
        if rules.is_impossible_travel(distance, speed):
            return constants.ANOMALY_IMPOSSIBLE_TRAVEL
        return None

    def _check_card_testing(self, tx: dict, last: Optional[dict]) -> Optional[str]:
        """CARD_TESTING: a micro-charge immediately followed by a big spike."""
        if last is None:
            return None
        if rules.is_card_testing(last["transaction_value"], tx["transaction_value"]):
            return constants.ANOMALY_CARD_TESTING
        return None

    def _check_frequency_burst(self, tx: dict) -> Optional[str]:
        """FREQUENCY_BURST: 5+ charges inside one 2s tumbling window."""
        self._roll_window(tx["timestamp"])
        if rules.is_frequency_burst(self._increment_count()):
            return constants.ANOMALY_FREQUENCY_BURST
        return None

    def _check_amount_spike(self, tx: dict) -> Optional[str]:
        """AMOUNT_SPIKE: charge sits >= 3.5 rolling std-devs above the card mean.

        Uses Welford's online algorithm so we never store the full history:
        only (count, mean, M2) live in keyed state. The current charge is
        scored against the *prior* distribution, then folded into it.
        """
        n = self.amt_count.value() or 0
        mean = self.amt_mean.value() or 0.0
        m2 = self.amt_m2.value() or 0.0
        amount = float(tx["transaction_value"])

        anomaly = constants.ANOMALY_AMOUNT_SPIKE if rules.is_amount_spike(amount, mean, m2, n) else None

        # Fold the new observation into the running statistics.
        n, mean, m2 = rules.welford_update(n, mean, m2, amount)
        self.amt_count.update(n)
        self.amt_mean.update(mean)
        self.amt_m2.update(m2)
        return anomaly

    def _roll_window(self, ts: int):
        """SRP: reset the counter when the current 2s window has elapsed."""
        if rules.window_should_reset(self.window_start.value(), ts):
            self.window_start.update(ts)
            self.count.update(0)

    def _increment_count(self) -> int:
        """SRP: bump and persist the per-window transaction counter."""
        updated = (self.count.value() or 0) + 1
        self.count.update(updated)
        return updated

    @staticmethod
    def _local_hour(ts_ms: int) -> int:
        """SRP: extract the local-time hour-of-day from an epoch-ms timestamp."""
        return time.localtime(ts_ms / 1000.0).tm_hour

    @staticmethod
    def _build_alert(anomaly: str, tx: dict) -> str:
        """SRP: render the canonical alert envelope as a JSON string."""
        return json.dumps({
            "alert_type": anomaly,
            "card_id": tx["card_id"],
            "timestamp": tx["timestamp"],
            "transaction": tx,
        })


def card_id_of(value: str) -> str:
    """SRP: key selector extracting card_id from a raw transaction string."""
    return json.loads(value)["card_id"]


def build_source() -> KafkaSource:
    """SRP: configure the modern KafkaSource for incoming transactions."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(config.TOPIC_TRANSACTIONS)
        .set_group_id("flink-fraud-detector")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_sink() -> KafkaSink:
    """SRP: configure the modern KafkaSink for outgoing alerts."""
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(config.TOPIC_ALERTS)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(config.KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def _locate_connector_jar() -> Optional[str]:
    """SRP: resolve a Kafka connector jar, or None if it ships with the cluster.

    Inside Docker the fat-jar is baked into ``/opt/flink/lib`` by the image,
    so it is already on the classpath and we return None. For a bare local
    run, set ``FLINK_KAFKA_JAR`` or drop the jar in the project root.
    """
    explicit = os.getenv("FLINK_KAFKA_JAR")
    if explicit:
        return os.path.abspath(explicit)
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    matches = glob.glob(os.path.join(root, "flink-sql-connector-kafka-*.jar"))
    return matches[0] if matches else None


def register_connector_jar(env: StreamExecutionEnvironment):
    """SRP: attach the Kafka connector jar to the classpath when one is local."""
    jar = _locate_connector_jar()
    if jar:
        env.add_jars(f"file://{jar}")


def build_pipeline(env: StreamExecutionEnvironment):
    """SRP: wire source -> keyed detection -> sink."""
    stream = env.from_source(build_source(), WatermarkStrategy.no_watermarks(), "transactions")
    alerts = stream.key_by(card_id_of).process(FraudDetector(), Types.STRING())
    alerts.sink_to(build_sink())


def main():
    """SRP: assemble and launch the streaming job."""
    env = StreamExecutionEnvironment.get_execution_environment()
    register_connector_jar(env)
    build_pipeline(env)
    env.execute("payment-fraud-detector")


if __name__ == "__main__":
    main()
