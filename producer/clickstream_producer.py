"""
clickstream_producer.py
Kafka producer for e-commerce clickstream events.

Features:
- Configurable throughput (events/second)
- JSON serialization with ISO-8601 timestamps
- Delivery confirmation callbacks with dead-letter queue fallback
- Graceful shutdown on SIGINT / SIGTERM
- Prometheus metrics exposition (optional)
- Structured logging with correlation IDs
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import uuid
from typing import Any

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from event_schema import ClickstreamEvent, ClickstreamEventGenerator

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("clickstream.producer")

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream-events")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "clickstream-events-dlq")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "100"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "6"))
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "1"))
NUM_USERS = int(os.getenv("NUM_USERS", "500"))

PRODUCER_CONFIG: dict[str, Any] = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",  # wait for all ISR replicas
    "retries": 5,
    "retry.backoff.ms": 200,
    "linger.ms": 10,  # micro-batch for throughput
    "batch.size": 65536,  # 64 KB batch
    "compression.type": "lz4",
    "enable.idempotence": True,  # exactly-once producer semantics
    "max.in.flight.requests.per.connection": 5,
    "socket.keepalive.enable": True,
    "statistics.interval.ms": 30000,
}


# ─── Topic Management ─────────────────────────────────────────────────────────


def ensure_topics_exist(bootstrap_servers: str) -> None:
    """Create the main topic and DLQ if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics

    topics_to_create = []
    for topic, partitions in [
        (KAFKA_TOPIC, NUM_PARTITIONS),
        (KAFKA_DLQ_TOPIC, 1),
    ]:
        if topic not in existing:
            topics_to_create.append(
                NewTopic(
                    topic,
                    num_partitions=partitions,
                    replication_factor=REPLICATION_FACTOR,
                    config={
                        "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "min.insync.replicas": "1",
                    },
                )
            )

    if topics_to_create:
        fs = admin.create_topics(topics_to_create)
        for topic, future in fs.items():
            try:
                future.result()
                logger.info("Created Kafka topic: %s", topic)
            except Exception as exc:
                logger.warning("Topic '%s' may already exist: %s", topic, exc)


# ─── Serializer ──────────────────────────────────────────────────────────────


def serialize_event(event: ClickstreamEvent) -> bytes:
    """Serialize a ClickstreamEvent to UTF-8 JSON bytes."""
    return json.dumps(event.to_dict(), default=str).encode("utf-8")


def partition_key(event: ClickstreamEvent) -> bytes:
    """
    Partition by user_id so all events for a given user land on the same
    partition, preserving per-user ordering for session analytics.
    """
    return event.user_id.encode("utf-8")


# ─── Callbacks ───────────────────────────────────────────────────────────────


class DeliveryHandler:
    """Tracks delivery success/failure stats and routes failures to the DLQ."""

    def __init__(self, producer: Producer):
        self._producer = producer
        self.sent = 0
        self.delivered = 0
        self.failed = 0

    def on_delivery(self, err, msg) -> None:
        if err:
            self.failed += 1
            logger.error(
                "Delivery failed | topic=%s partition=%d offset=%s error=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                err,
            )
            # Route to DLQ
            try:
                self._producer.produce(
                    topic=KAFKA_DLQ_TOPIC,
                    value=msg.value(),
                    key=msg.key(),
                )
            except KafkaException as dlq_err:
                logger.critical("DLQ produce failed: %s", dlq_err)
        else:
            self.delivered += 1

    def log_stats(self) -> None:
        logger.info(
            "Stats | sent=%d delivered=%d failed=%d in_flight=%d",
            self.sent,
            self.delivered,
            self.failed,
            self.sent - self.delivered - self.failed,
        )


# ─── Producer ────────────────────────────────────────────────────────────────


class ClickstreamProducer:
    def __init__(self):
        self._producer = Producer(PRODUCER_CONFIG)
        self._handler = DeliveryHandler(self._producer)
        self._generator = ClickstreamEventGenerator(num_users=NUM_USERS)
        self._running = False
        self._run_id = str(uuid.uuid4())

        # Graceful shutdown on SIGINT / SIGTERM
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("Shutdown signal received (%s). Flushing producer …", signum)
        self._running = False

    def _produce_one(self, event: ClickstreamEvent) -> None:
        self._producer.produce(
            topic=KAFKA_TOPIC,
            key=partition_key(event),
            value=serialize_event(event),
            headers={
                "event_type": event.event_type,
                "producer_run_id": self._run_id,
                "schema_version": "1.0",
            },
            on_delivery=self._handler.on_delivery,
        )
        self._handler.sent += 1

    def run(self, max_events: int | None = None) -> None:
        """
        Produce events continuously at the configured rate.
        If max_events is set, stop after that many events (useful for tests).
        """
        ensure_topics_exist(KAFKA_BOOTSTRAP_SERVERS)

        self._running = True
        total_produced = 0
        interval = BATCH_SIZE / EVENTS_PER_SECOND  # seconds per batch
        stats_every = max(1, int(10 / interval))  # log stats ~every 10s
        batch_num = 0

        logger.info(
            "Producer started | topic=%s bootstrap=%s rate=%.0f eps batch=%d run_id=%s",
            KAFKA_TOPIC,
            KAFKA_BOOTSTRAP_SERVERS,
            EVENTS_PER_SECOND,
            BATCH_SIZE,
            self._run_id,
        )

        while self._running:
            batch_start = time.monotonic()

            events = self._generator.generate_batch(BATCH_SIZE)
            for event in events:
                try:
                    self._produce_one(event)
                except BufferError:
                    # Local queue full — poll to drain, then retry
                    self._producer.poll(0.1)
                    self._produce_one(event)

            # Poll to trigger delivery callbacks without blocking
            self._producer.poll(0)
            total_produced += len(events)
            batch_num += 1

            if batch_num % stats_every == 0:
                self._handler.log_stats()

            if max_events and total_produced >= max_events:
                logger.info("max_events=%d reached. Stopping.", max_events)
                break

            # Rate limiting — sleep remainder of interval
            elapsed = time.monotonic() - batch_start
            sleep_time = max(0.0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        self._flush()
        self._handler.log_stats()
        logger.info("Producer stopped. Total events sent: %d", total_produced)

    def _flush(self) -> None:
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            logger.warning("%d messages not delivered after flush.", remaining)
        else:
            logger.info("All messages flushed.")


# ─── Entrypoint ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="E-commerce clickstream Kafka producer"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after N events (default: run forever)",
    )
    args = parser.parse_args()

    producer = ClickstreamProducer()
    producer.run(max_events=args.max_events)
