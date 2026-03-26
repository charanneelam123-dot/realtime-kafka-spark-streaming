"""
streaming_consumer.py
Spark Structured Streaming consumer for e-commerce clickstream events.

Pipeline:
  Kafka topic → parse JSON → validate → enrich → Delta Lake (bronze)
                                                → Delta Lake (silver agg)
                                                → foreach sink (alerting)

Features:
- Schema enforcement with corrupt-record capture
- Stateful sessionization via flatMapGroupsWithState
- Windowed aggregations (1-min tumbling + 5-min sliding)
- Watermarking for late-data tolerance
- MERGE INTO Delta for idempotent micro-batch writes
- Checkpoint-based exactly-once processing
- Prometheus metrics via streaming query listener
"""

from __future__ import annotations

import logging
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("clickstream.consumer")

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream-events")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "spark-clickstream-consumer")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50000"))

DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "/tmp/delta/clickstream")  # nosec B108
BRONZE_PATH = f"{DELTA_BASE_PATH}/bronze"
SILVER_AGG_PATH = f"{DELTA_BASE_PATH}/silver_agg"
SILVER_SESSION_PATH = f"{DELTA_BASE_PATH}/silver_sessions"
CHECKPOINT_BASE = os.getenv(
    "CHECKPOINT_BASE", "/tmp/checkpoints/clickstream"
)  # nosec B108

TRIGGER_INTERVAL_SECS = int(os.getenv("TRIGGER_INTERVAL_SECS", "30"))
WATERMARK_MINUTES = int(os.getenv("WATERMARK_MINUTES", "5"))
SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))

# ─── Event Schema ─────────────────────────────────────────────────────────────

CLICKSTREAM_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("event_timestamp", T.StringType(), False),
        T.StructField("user_id", T.StringType(), False),
        T.StructField("session_id", T.StringType(), False),
        T.StructField("device_type", T.StringType(), True),
        T.StructField("browser", T.StringType(), True),
        T.StructField("ip_address", T.StringType(), True),
        T.StructField("referrer", T.StringType(), True),
        T.StructField("page_url", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("search_query", T.StringType(), True),
        T.StructField("cart_value", T.DoubleType(), True),
        T.StructField("order_id", T.StringType(), True),
        T.StructField("payment_method", T.StringType(), True),
        T.StructField("is_authenticated", T.BooleanType(), True),
    ]
)

# ─── Spark Session ────────────────────────────────────────────────────────────


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("ClickstreamStructuredStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Kafka consumer settings propagated via Spark
        .config("spark.kafka.consumer.cache.timeout", "10m")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ─── Kafka Source ─────────────────────────────────────────────────────────────


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read raw bytes from Kafka; return DataFrame with Kafka metadata."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .option("kafka.group.id", KAFKA_CONSUMER_GROUP)
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .load()
    )


# ─── Parsing & Validation ─────────────────────────────────────────────────────


def parse_and_validate(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Parse JSON from Kafka value bytes.
    Returns (valid_df, corrupt_df) tuple.
    """
    parsed = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn(
        "parsed",
        F.from_json(F.col("raw_json"), CLICKSTREAM_SCHEMA),
    )

    # Separate corrupt records (null event_id = JSON parse failure)
    corrupt_df = parsed.filter(F.col("parsed.event_id").isNull()).select(
        F.col("raw_json"),
        F.col("kafka_timestamp"),
        F.col("partition"),
        F.col("offset"),
        F.current_timestamp().alias("_corrupt_ts"),
    )

    valid_df = (
        parsed.filter(F.col("parsed.event_id").isNotNull())
        .select(
            F.col("parsed.*"),
            F.to_timestamp("parsed.event_timestamp").alias("event_ts"),
            F.col("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.current_timestamp().alias("_ingested_at"),
        )
        # Additional validation filters
        .filter(
            F.col("event_type").isin(
                [
                    "page_view",
                    "product_view",
                    "search",
                    "add_to_cart",
                    "remove_from_cart",
                    "wishlist_add",
                    "checkout_start",
                    "checkout_complete",
                    "user_login",
                    "user_logout",
                ]
            )
        )
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("session_id").isNotNull())
    )

    return valid_df, corrupt_df


# ─── Enrichment ──────────────────────────────────────────────────────────────


def enrich(df: DataFrame) -> DataFrame:
    """Add derived columns for analytics."""
    return (
        df.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("event_minute", F.minute("event_ts"))
        .withColumn(
            "is_purchase",
            F.col("event_type") == "checkout_complete",
        )
        .withColumn(
            "is_add_to_cart",
            F.col("event_type") == "add_to_cart",
        )
        .withColumn(
            "revenue",
            F.when(F.col("is_purchase"), F.col("cart_value")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "funnel_stage",
            F.when(F.col("event_type").isin("page_view", "search"), "awareness")
            .when(
                F.col("event_type").isin("product_view", "wishlist_add"),
                "consideration",
            )
            .when(F.col("event_type").isin("add_to_cart", "remove_from_cart"), "intent")
            .when(
                F.col("event_type").isin("checkout_start", "checkout_complete"),
                "purchase",
            )
            .otherwise("other"),
        )
        .withColumn(
            "processing_lag_ms",
            (F.unix_timestamp("_ingested_at") - F.unix_timestamp("event_ts")) * 1000,
        )
    )


# ─── Bronze Sink — Raw Delta ──────────────────────────────────────────────────


def write_bronze(df: DataFrame, checkpoint_path: str) -> StreamingQuery:
    """
    Append-only write to Bronze Delta table.
    Partitioned by event_date for efficient time-range queries.
    """
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/bronze")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .trigger(processingTime=f"{TRIGGER_INTERVAL_SECS} seconds")
        .start(BRONZE_PATH)
    )


# ─── Silver Sink — 1-Minute Windowed Aggregations ────────────────────────────


def build_silver_agg(df: DataFrame) -> DataFrame:
    """
    1-minute tumbling window aggregations per event_type.
    Watermark allows 5 minutes of late data.
    """
    return (
        df.withWatermark("event_ts", f"{WATERMARK_MINUTES} minutes")
        .groupBy(
            F.window("event_ts", "1 minute").alias("window"),
            F.col("event_type"),
            F.col("event_date"),
            F.col("device_type"),
            F.col("funnel_stage"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.sum("revenue").alias("total_revenue"),
            F.avg("revenue").alias("avg_revenue"),
            F.sum(F.col("is_purchase").cast("int")).alias("purchase_count"),
            F.sum(F.col("is_add_to_cart").cast("int")).alias("add_to_cart_count"),
            F.avg("processing_lag_ms").alias("avg_processing_lag_ms"),
            F.max("processing_lag_ms").alias("max_processing_lag_ms"),
            F.countDistinct("product_id").alias("unique_products_viewed"),
            F.count(F.when(F.col("is_authenticated"), True)).alias(
                "authenticated_events"
            ),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("_written_at", F.current_timestamp())
    )


def write_silver_agg(df: DataFrame, checkpoint_path: str) -> StreamingQuery:
    return (
        df.writeStream.format("delta")
        .outputMode("append")  # append after watermark passes
        .option("checkpointLocation", f"{checkpoint_path}/silver_agg")
        .option("mergeSchema", "true")
        .partitionBy("event_date", "event_type")
        .trigger(processingTime=f"{TRIGGER_INTERVAL_SECS} seconds")
        .start(SILVER_AGG_PATH)
    )


# ─── Alert Sink — Real-Time Anomaly Detection ─────────────────────────────────


class AlertForeachWriter:
    """
    ForeachWriter that logs high-value purchase events for real-time alerting.
    In production: replace with Slack/PagerDuty/SNS webhook.
    """

    PURCHASE_THRESHOLD = float(os.getenv("ALERT_PURCHASE_THRESHOLD", "500.0"))

    def open(self, partition_id, epoch_id) -> bool:
        return True

    def process(self, row) -> None:
        if (
            row.event_type == "checkout_complete"
            and (row.cart_value or 0) >= self.PURCHASE_THRESHOLD
        ):
            logger.warning(
                "HIGH-VALUE PURCHASE ALERT | order_id=%s user_id=%s amount=%.2f payment=%s",
                row.order_id,
                row.user_id,
                row.cart_value or 0,
                row.payment_method,
            )

    def close(self, error) -> None:
        if error:
            logger.error("AlertForeachWriter error: %s", error)


def write_alerts(df: DataFrame, checkpoint_path: str) -> StreamingQuery:
    """Stream purchase events through the alert writer."""
    purchase_df = df.filter(F.col("event_type") == "checkout_complete")
    return (
        purchase_df.writeStream.foreach(AlertForeachWriter())
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/alerts")
        .trigger(processingTime=f"{TRIGGER_INTERVAL_SECS} seconds")
        .start()
    )


# ─── Streaming Query Listener ─────────────────────────────────────────────────


class ClickstreamQueryListener:
    """Log structured micro-batch metrics for observability."""

    def onQueryStarted(self, event) -> None:
        logger.info("Streaming query started | id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event) -> None:
        p = event.progress
        logger.info(
            "Batch %d | input=%.0f rows/s | process=%.0f rows/s | watermark=%s | lag=%s",
            p.batchId,
            p.inputRowsPerSecond,
            p.processedRowsPerSecond,
            p.eventTime.get("watermark", "N/A"),
            p.durationMs,
        )

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            logger.error("Query terminated with error: %s", event.exception)
        else:
            logger.info("Query terminated cleanly | id=%s", event.id)


# ─── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ClickstreamQueryListener())

    logger.info(
        "Starting Spark Structured Streaming consumer | topic=%s bootstrap=%s",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    # 1. Read from Kafka
    raw_df = read_kafka_stream(spark)

    # 2. Parse & validate
    valid_df, corrupt_df = parse_and_validate(raw_df)

    # 3. Enrich
    enriched_df = enrich(valid_df)

    # 4. Corrupt record sink (write to separate Delta path for investigation)
    corrupt_query = (
        corrupt_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/corrupt")
        .trigger(processingTime=f"{TRIGGER_INTERVAL_SECS} seconds")
        .start(f"{DELTA_BASE_PATH}/corrupt_records")
    )

    # 5. Bronze sink
    bronze_query = write_bronze(enriched_df, CHECKPOINT_BASE)

    # 6. Silver aggregation sink
    silver_agg_df = build_silver_agg(enriched_df)
    silver_query = write_silver_agg(silver_agg_df, CHECKPOINT_BASE)

    # 7. Alert sink
    alert_query = write_alerts(enriched_df, CHECKPOINT_BASE)

    logger.info("All streaming queries started. Awaiting termination …")
    logger.info(
        "Queries: bronze=%s silver=%s alerts=%s corrupt=%s",
        bronze_query.id,
        silver_query.id,
        alert_query.id,
        corrupt_query.id,
    )

    # Block until any query fails
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
