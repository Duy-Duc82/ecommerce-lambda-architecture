#!/bin/bash
# ============================================================
# Tao Kafka Topics cho he thong TMDT
# Chay trong Kafka container:
#   docker exec kafka /opt/kafka/bin/kafka-topics.sh ...
# ============================================================

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="localhost:9092"

echo "=== Tao Kafka Topics ==="

# Topic chinh cho events (page_view, add_to_cart, review, search)
$KAFKA_BIN/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic ecommerce_events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# Topic cho don hang (purchase events)
$KAFKA_BIN/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic ecommerce_orders \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# Topic cho thay doi gia (price_change events)
$KAFKA_BIN/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic ecommerce_prices \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo ""
echo "=== Danh sach Topics ==="
$KAFKA_BIN/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP

echo ""
echo "Kafka topics da duoc tao thanh cong!"
