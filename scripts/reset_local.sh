#!/usr/bin/env bash
# reset_local.sh — Tear down and restart the local Kafka + Spark stack cleanly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "==> Stopping all containers and removing volumes..."
docker compose -f "$PROJECT_ROOT/docker/docker-compose.yml" down -v --remove-orphans

echo "==> Removing local Delta and checkpoint data..."
rm -rf /tmp/delta/clickstream /tmp/checkpoints/clickstream

echo "==> Starting fresh Kafka stack..."
docker compose -f "$PROJECT_ROOT/docker/docker-compose.yml" up -d zookeeper kafka-1 schema-registry topic-init kafka-ui

echo "==> Waiting for Kafka to be healthy..."
timeout 120 bash -c '
  until docker compose -f '"$PROJECT_ROOT"'/docker/docker-compose.yml exec -T kafka-1 \
    kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; do
    sleep 3
    echo "  ... waiting for Kafka ..."
  done
'

echo ""
echo "==> Stack is ready!"
echo "   Kafka UI   : http://localhost:8080"
echo "   Schema Reg : http://localhost:8081"
echo "   Kafka      : localhost:9092"
echo ""
echo "==> To start the producer:"
echo "   docker compose -f docker/docker-compose.yml up producer"
echo ""
echo "==> To start the consumer (Spark):"
echo "   docker compose -f docker/docker-compose.yml up consumer"
