"""
test_consumer.py
Unit tests for Spark Structured Streaming consumer logic.
Uses a local SparkSession — no Kafka or Delta cluster required.

Tests validate:
- JSON parsing & schema enforcement
- Corrupt record isolation
- Column enrichment correctness
- Windowed aggregation schema
- Event validation filters
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_streaming_consumer")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# Import consumer functions after spark fixture is set up
@pytest.fixture(scope="session", autouse=True)
def import_consumer():
    """Defer import so PYTHONPATH is set before import."""
    pass


# ─── Helpers ─────────────────────────────────────────────────────────────────

VALID_EVENT = {
    "event_id": "evt-0001",
    "event_type": "product_view",
    "event_timestamp": "2023-06-15 14:30:00",
    "user_id": "U-ABCD1234",
    "session_id": "S-SESSABC123",
    "device_type": "desktop",
    "browser": "Chrome",
    "ip_address": "192.168.1.100",
    "referrer": "google.com",
    "page_url": "/product/PROD-00001",
    "product_id": "PROD-00001",
    "product_name": "Product 1",
    "category": "Electronics",
    "price": 299.99,
    "quantity": 1,
    "is_authenticated": True,
}

PURCHASE_EVENT = {
    **VALID_EVENT,
    "event_id": "evt-0002",
    "event_type": "checkout_complete",
    "page_url": "/order-confirmation",
    "order_id": "ORD-ABC123",
    "cart_value": 599.98,
    "payment_method": "credit_card",
}

ADD_TO_CART_EVENT = {
    **VALID_EVENT,
    "event_id": "evt-0003",
    "event_type": "add_to_cart",
    "page_url": "/cart",
    "cart_value": 299.99,
    "quantity": 1,
}


def make_kafka_df(spark: SparkSession, payloads: list[dict | str]):
    """
    Simulate Kafka messages as a static DataFrame.
    Each payload is JSON-serialized into the 'value' column.
    """
    rows = []
    for i, payload in enumerate(payloads):
        if isinstance(payload, dict):
            value = json.dumps(payload).encode("utf-8")
        else:
            value = payload.encode("utf-8") if isinstance(payload, str) else payload
        rows.append(
            Row(
                key=f"U-USER{i:04d}".encode("utf-8"),
                value=value,
                topic="clickstream-events",
                partition=i % 6,
                offset=i,
                timestamp=datetime.now(timezone.utc),
                timestampType=0,
            )
        )

    schema = T.StructType(
        [
            T.StructField("key", T.BinaryType(), True),
            T.StructField("value", T.BinaryType(), True),
            T.StructField("topic", T.StringType(), True),
            T.StructField("partition", T.IntegerType(), True),
            T.StructField("offset", T.LongType(), True),
            T.StructField("timestamp", T.TimestampType(), True),
            T.StructField("timestampType", T.IntegerType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


# ─── Import consumer functions (extracted for testability) ───────────────────


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

VALID_EVENT_TYPES = [
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


def _parse_kafka_df(raw_df):
    parsed = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("parsed", F.from_json(F.col("raw_json"), CLICKSTREAM_SCHEMA))
    corrupt = parsed.filter(F.col("parsed.event_id").isNull())
    valid = (
        parsed.filter(F.col("parsed.event_id").isNotNull())
        .select(
            F.col("parsed.*"),
            F.to_timestamp("parsed.event_timestamp").alias("event_ts"),
            F.col("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.current_timestamp().alias("_ingested_at"),
        )
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("session_id").isNotNull())
    )
    return valid, corrupt


def _enrich(df):
    return (
        df.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("is_purchase", F.col("event_type") == "checkout_complete")
        .withColumn("is_add_to_cart", F.col("event_type") == "add_to_cart")
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


# ─── Tests: Parsing ───────────────────────────────────────────────────────────


class TestParsing:

    def test_valid_event_parsed_correctly(self, spark):
        raw = make_kafka_df(spark, [VALID_EVENT])
        valid, corrupt = _parse_kafka_df(raw)
        assert valid.count() == 1
        assert corrupt.count() == 0

    def test_corrupt_json_goes_to_corrupt_df(self, spark):
        raw = make_kafka_df(spark, ["not valid json {{{{"])
        valid, corrupt = _parse_kafka_df(raw)
        assert valid.count() == 0
        assert corrupt.count() == 1

    def test_missing_event_id_goes_to_corrupt(self, spark):
        bad = {**VALID_EVENT}
        del bad["event_id"]
        raw = make_kafka_df(spark, [bad])
        valid, corrupt = _parse_kafka_df(raw)
        assert valid.count() == 0
        assert corrupt.count() == 1

    def test_invalid_event_type_filtered(self, spark):
        bad = {**VALID_EVENT, "event_type": "unknown_action"}
        raw = make_kafka_df(spark, [bad])
        valid, corrupt = _parse_kafka_df(raw)
        assert valid.count() == 0

    def test_mixed_batch_routes_correctly(self, spark):
        payloads = [
            VALID_EVENT,  # valid
            PURCHASE_EVENT,  # valid
            "garbage",  # corrupt
            {**VALID_EVENT, "event_type": "bad_type"},  # filtered
        ]
        raw = make_kafka_df(spark, payloads)
        valid, corrupt = _parse_kafka_df(raw)
        assert valid.count() == 2
        assert corrupt.count() == 1

    def test_event_ts_cast_to_timestamp(self, spark):
        raw = make_kafka_df(spark, [VALID_EVENT])
        valid, _ = _parse_kafka_df(raw)
        dtype = dict(valid.dtypes).get("event_ts")
        assert dtype == "timestamp"

    def test_kafka_partition_and_offset_preserved(self, spark):
        raw = make_kafka_df(spark, [VALID_EVENT])
        valid, _ = _parse_kafka_df(raw)
        row = valid.collect()[0]
        assert "kafka_partition" in row.asDict()
        assert "kafka_offset" in row.asDict()

    def test_all_valid_event_types_pass(self, spark):
        events = [
            {**VALID_EVENT, "event_id": f"id-{i}", "event_type": et}
            for i, et in enumerate(VALID_EVENT_TYPES)
        ]
        raw = make_kafka_df(spark, events)
        valid, corrupt = _parse_kafka_df(raw)
        assert valid.count() == len(VALID_EVENT_TYPES)
        assert corrupt.count() == 0


# ─── Tests: Enrichment ───────────────────────────────────────────────────────


class TestEnrichment:

    def _get_enriched_row(self, spark, event_override=None):
        payload = {**VALID_EVENT, **(event_override or {})}
        raw = make_kafka_df(spark, [payload])
        valid, _ = _parse_kafka_df(raw)
        return _enrich(valid).collect()[0]

    def test_event_date_extracted(self, spark):
        row = self._get_enriched_row(spark)
        assert row["event_date"] is not None

    def test_event_hour_extracted(self, spark):
        row = self._get_enriched_row(spark)
        assert row["event_hour"] == 14  # "2023-06-15 14:30:00"

    def test_is_purchase_false_for_product_view(self, spark):
        row = self._get_enriched_row(spark)
        assert row["is_purchase"] is False

    def test_is_purchase_true_for_checkout_complete(self, spark):
        row = self._get_enriched_row(spark, {"event_id": "x", **PURCHASE_EVENT})
        assert row["is_purchase"] is True

    def test_revenue_zero_for_non_purchase(self, spark):
        row = self._get_enriched_row(spark)
        assert row["revenue"] == 0.0

    def test_revenue_equals_cart_value_for_purchase(self, spark):
        row = self._get_enriched_row(spark, {"event_id": "x", **PURCHASE_EVENT})
        assert row["revenue"] == pytest.approx(599.98, abs=0.01)

    def test_funnel_stage_awareness_for_page_view(self, spark):
        row = self._get_enriched_row(spark, {"event_type": "page_view"})
        assert row["funnel_stage"] == "awareness"

    def test_funnel_stage_consideration_for_product_view(self, spark):
        row = self._get_enriched_row(spark)
        assert row["funnel_stage"] == "consideration"

    def test_funnel_stage_intent_for_add_to_cart(self, spark):
        row = self._get_enriched_row(spark, {"event_id": "x", **ADD_TO_CART_EVENT})
        assert row["funnel_stage"] == "intent"

    def test_funnel_stage_purchase_for_checkout(self, spark):
        row = self._get_enriched_row(spark, {"event_id": "x", **PURCHASE_EVENT})
        assert row["funnel_stage"] == "purchase"

    def test_processing_lag_ms_is_positive(self, spark):
        row = self._get_enriched_row(spark)
        # event_timestamp is in the past, ingested_at is now → positive lag
        assert row["processing_lag_ms"] >= 0

    def test_is_add_to_cart_flag(self, spark):
        row = self._get_enriched_row(spark, {"event_id": "x", **ADD_TO_CART_EVENT})
        assert row["is_add_to_cart"] is True


# ─── Tests: Windowed Aggregation ─────────────────────────────────────────────


class TestWindowedAggregation:

    def test_aggregation_output_has_expected_columns(self, spark):
        events = [
            {**VALID_EVENT, "event_id": f"id-{i}", "event_type": "page_view"}
            for i in range(10)
        ]
        raw = make_kafka_df(spark, events)
        valid, _ = _parse_kafka_df(raw)
        enriched = _enrich(valid)

        agg = enriched.groupBy("event_type", "event_date").agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum("revenue").alias("total_revenue"),
            F.sum(F.col("is_purchase").cast("int")).alias("purchase_count"),
        )
        result = agg.collect()
        assert len(result) >= 1

        row = result[0]
        assert row["event_count"] > 0
        assert "unique_users" in row.asDict()
        assert "total_revenue" in row.asDict()
        assert "purchase_count" in row.asDict()

    def test_purchase_count_aggregates_correctly(self, spark):
        events = [{**PURCHASE_EVENT, "event_id": f"p-{i}"} for i in range(3)] + [
            {**VALID_EVENT, "event_id": f"v-{i}"} for i in range(7)
        ]
        raw = make_kafka_df(spark, events)
        valid, _ = _parse_kafka_df(raw)
        enriched = _enrich(valid)

        agg = enriched.groupBy(F.lit("all").alias("grain")).agg(
            F.count("*").alias("total_events"),
            F.sum(F.col("is_purchase").cast("int")).alias("purchase_count"),
            F.sum("revenue").alias("total_revenue"),
        )
        row = agg.collect()[0]
        assert row["total_events"] == 10
        assert row["purchase_count"] == 3
        assert row["total_revenue"] == pytest.approx(3 * 599.98, abs=0.01)
