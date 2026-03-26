# realtime-kafka-spark-streaming

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.6-231F20?logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1.0-00ADD8)
![Docker](https://img.shields.io/badge/Docker_Compose-7.6-2496ED?logo=docker&logoColor=white)
![CI](https://img.shields.io/github/actions/workflow/status/your-org/realtime-kafka-spark-streaming/ci.yml?label=CI)

Production-grade **real-time e-commerce clickstream pipeline** using Apache Kafka + Spark Structured Streaming + Delta Lake. Fully runnable locally via Docker Compose.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PRODUCER  (producer/clickstream_producer.py)                           │
│                                                                         │
│  ClickstreamEventGenerator                                              │
│  ├── 500 virtual users, stateful sessions & cart tracking               │
│  ├── 10 event types with weighted funnel distribution                   │
│  │   page_view(30%) → product_view(25%) → search(15%)                  │
│  │   → add_to_cart(12%) → checkout_start(5%) → checkout_complete(3%)   │
│  ├── Keyed by user_id (per-user partition ordering)                     │
│  ├── exactly-once idempotent producer (enable.idempotence=true)         │
│  ├── lz4 compression, linger.ms=10 for micro-batching                  │
│  └── Dead-letter queue (DLQ) on delivery failure                        │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  JSON over Kafka (6 partitions, lz4)
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  KAFKA  (Confluent CP 7.6 — Docker Compose)                             │
│                                                                         │
│  clickstream-events        (6 partitions, 7-day retention)              │
│  clickstream-events-dlq    (1 partition,  30-day retention)             │
│                                                                         │
│  Supporting services:                                                   │
│  ├── ZooKeeper        :2181                                             │
│  ├── Schema Registry  :8081                                             │
│  └── Kafka UI         :8080  (browse topics, consumer groups, offsets)  │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  Spark readStream (maxOffsetsPerTrigger=50k)
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SPARK STRUCTURED STREAMING  (consumer/streaming_consumer.py)           │
│                                                                         │
│  ┌─────────────┐   parse_and_validate()                                 │
│  │  Raw Kafka  │──▶ from_json() + schema enforcement                   │
│  │  DataFrame  │──▶ valid ──────────────────────────┐                  │
│  └─────────────┘──▶ corrupt ──▶ Delta corrupt_records│                 │
│                                                      │                  │
│                     enrich()                         │                  │
│                     ├── event_date / event_hour      │                  │
│                     ├── is_purchase / is_add_to_cart │                  │
│                     ├── revenue (purchase events)    │                  │
│                     ├── funnel_stage (4 stages)      │                  │
│                     └── processing_lag_ms            │                  │
│                                          ◄───────────┘                 │
│  ┌──────────────────────────────────────────────────────┐              │
│  │  Sink 1 — Bronze Delta (append, partitioned by date) │              │
│  │  Sink 2 — Silver Agg  (1-min tumbling window + wm)  │              │
│  │  Sink 3 — Alert       (ForeachWriter, purchases>$500)│              │
│  │  Sink 4 — Corrupt     (Delta, for investigation)     │              │
│  └──────────────────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  DELTA LAKE  (local /tmp/delta or cloud object store)                   │
│                                                                         │
│  bronze/     — raw enriched events (append-only, event_date partition)  │
│  silver_agg/ — 1-min windowed KPIs per event_type × device × funnel    │
│  corrupt_records/ — parse failures for investigation                    │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  CI / CD  (GitHub Actions)                                              │
│                                                                         │
│  lint ──▶ test-producer ──▶ test-consumer ──▶ integration-test ──┐     │
│       └──▶ docker-validate ────────────────────────────────────┐ │     │
│       └──▶ security ───────────────────────────────────────────┴─┴──▶ ci-gate
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Event Schema

```json
{
  "event_id":        "3f4a2b1c-...",
  "event_type":      "add_to_cart",
  "event_timestamp": "2024-01-15T14:30:00.123456+00:00",
  "user_id":         "U-ABCD1234",
  "session_id":      "S-SESSABC123",
  "device_type":     "mobile",
  "browser":         "Chrome",
  "ip_address":      "203.0.113.42",
  "referrer":        "google.com",
  "page_url":        "/product/PROD-00042",
  "product_id":      "PROD-00042",
  "product_name":    "Product 42",
  "category":        "Electronics",
  "price":           299.99,
  "quantity":        1,
  "cart_value":      299.99,
  "is_authenticated": true
}
```

## Project Structure

```
realtime-kafka-spark-streaming/
├── producer/
│   ├── event_schema.py          # Event dataclass + stateful generator
│   └── clickstream_producer.py  # Confluent Kafka producer, DLQ, rate control
├── consumer/
│   └── streaming_consumer.py    # Spark Structured Streaming (4 sinks)
├── config/
│   └── settings.py              # Centralized env-var config
├── docker/
│   ├── docker-compose.yml       # ZK + Kafka + Schema Registry + UI
│   ├── Dockerfile.producer
│   └── Dockerfile.consumer
├── tests/
│   ├── test_producer.py         # 25+ tests (schema, state, distribution)
│   └── test_consumer.py         # 25+ tests (parsing, enrichment, agg)
├── scripts/
│   └── reset_local.sh           # Tear-down & restart helper
├── .github/workflows/
│   └── ci.yml                   # 6-job CI with Kafka integration test
└── requirements.txt
```

---

## Quick Start (Local)

### Prerequisites
- Docker Desktop
- Python 3.11+
- Java 11+ (for local PySpark tests)

### 1. Start the Kafka stack

```bash
cd realtime-kafka-spark-streaming
docker compose -f docker/docker-compose.yml up -d zookeeper kafka-1 schema-registry topic-init kafka-ui
```

Open **Kafka UI** at http://localhost:8080 to monitor topics and consumer groups.

### 2. Start the producer

```bash
# In Docker
docker compose -f docker/docker-compose.yml up producer

# Or locally
pip install confluent-kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
EVENTS_PER_SECOND=200 \
python producer/clickstream_producer.py
```

### 3. Start the Spark Structured Streaming consumer

```bash
# In Docker (includes spark-submit with Kafka + Delta packages)
docker compose -f docker/docker-compose.yml up consumer

# Or locally
pip install pyspark==3.5.0 delta-spark==3.1.0
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  consumer/streaming_consumer.py
```

### 4. Query Delta tables

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

spark = configure_spark_with_delta_pip(SparkSession.builder).getOrCreate()

# Browse Bronze
spark.read.format("delta").load("/tmp/delta/clickstream/bronze").show()

# Browse Silver aggregations
spark.read.format("delta").load("/tmp/delta/clickstream/silver_agg").show()
```

### 5. Reset local environment

```bash
bash scripts/reset_local.sh
```

---

## Running Tests

```bash
pip install -r requirements.txt

# Producer tests (no Kafka required)
pytest tests/test_producer.py -v

# Consumer tests (local PySpark, no cluster required)
PYSPARK_PYTHON=$(which python) \
SPARK_LOCAL_IP=127.0.0.1 \
pytest tests/test_consumer.py -v

# All tests with coverage
pytest tests/ -v --cov=. --cov-report=term-missing
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker(s) |
| `KAFKA_TOPIC` | `clickstream-events` | Main topic |
| `KAFKA_DLQ_TOPIC` | `clickstream-events-dlq` | Dead-letter queue topic |
| `EVENTS_PER_SECOND` | `100` | Producer throughput |
| `BATCH_SIZE` | `50` | Events per producer batch |
| `NUM_USERS` | `500` | Simulated user pool size |
| `DELTA_BASE_PATH` | `/tmp/delta/clickstream` | Delta Lake root path |
| `CHECKPOINT_BASE` | `/tmp/checkpoints/clickstream` | Spark checkpoint root |
| `TRIGGER_INTERVAL_SECS` | `30` | Micro-batch trigger interval |
| `WATERMARK_MINUTES` | `5` | Late data tolerance |
| `ALERT_PURCHASE_THRESHOLD` | `500.0` | Alert on purchases above this value |

---

## Tech Stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka (Confluent CP 7.6) |
| Stream processing | Spark Structured Streaming 3.5 |
| Storage format | Delta Lake 3.1 |
| Producer SDK | confluent-kafka-python 2.3 |
| Local infra | Docker Compose |
| Testing | pytest + local PySpark |
| CI/CD | GitHub Actions (6-job pipeline with Kafka integration test) |

