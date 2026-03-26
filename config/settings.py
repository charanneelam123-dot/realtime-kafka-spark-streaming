"""
settings.py
Central configuration with environment-variable overrides and validation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "clickstream-events")
    dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "clickstream-events-dlq")
    consumer_group: str = os.getenv(
        "KAFKA_CONSUMER_GROUP", "spark-clickstream-consumer"
    )
    num_partitions: int = int(os.getenv("NUM_PARTITIONS", "6"))
    replication_factor: int = int(os.getenv("REPLICATION_FACTOR", "1"))
    starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    max_offsets_per_trigger: int = int(
        os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50000")
    )
    retention_ms: int = 7 * 24 * 60 * 60 * 1000  # 7 days

    def validate(self) -> None:
        assert self.bootstrap_servers, "KAFKA_BOOTSTRAP_SERVERS must be set"
        assert self.num_partitions > 0
        assert self.replication_factor > 0


@dataclass(frozen=True)
class ProducerSettings:
    events_per_second: float = float(os.getenv("EVENTS_PER_SECOND", "100"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "50"))
    num_users: int = int(os.getenv("NUM_USERS", "500"))
    acks: str = "all"
    retries: int = 5
    linger_ms: int = 10
    compression_type: str = "lz4"
    enable_idempotence: bool = True


@dataclass(frozen=True)
class ConsumerSettings:
    delta_base_path: str = os.getenv(
        "DELTA_BASE_PATH", "/tmp/delta/clickstream"
    )  # nosec B108
    checkpoint_base: str = os.getenv(
        "CHECKPOINT_BASE", "/tmp/checkpoints/clickstream"
    )  # nosec B108
    trigger_interval_secs: int = int(os.getenv("TRIGGER_INTERVAL_SECS", "30"))
    watermark_minutes: int = int(os.getenv("WATERMARK_MINUTES", "5"))
    session_timeout_minutes: int = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))
    alert_purchase_threshold: float = float(
        os.getenv("ALERT_PURCHASE_THRESHOLD", "500.0")
    )

    @property
    def bronze_path(self) -> str:
        return f"{self.delta_base_path}/bronze"

    @property
    def silver_agg_path(self) -> str:
        return f"{self.delta_base_path}/silver_agg"

    @property
    def corrupt_path(self) -> str:
        return f"{self.delta_base_path}/corrupt_records"


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    producer: ProducerSettings = field(default_factory=ProducerSettings)
    consumer: ConsumerSettings = field(default_factory=ConsumerSettings)
    env: str = os.getenv("ENV", "local")  # local | dev | prod

    def validate(self) -> None:
        self.kafka.validate()
        assert self.env in ("local", "dev", "prod"), f"Unknown ENV: {self.env}"


# Singleton
settings = Settings()
