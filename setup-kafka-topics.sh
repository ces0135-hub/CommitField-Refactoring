#!/bin/bash

echo "🚀 Kafka 토픽 생성 시작..."

# Kafka 컨테이너 이름
KAFKA_CONTAINER="commitfield-kafka"

# 1. 알림 이벤트 토픽 생성
echo "📢 알림 이벤트 토픽 생성 중..."
docker exec -it $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic notification-events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# 2. 커밋 업데이트 토픽 생성
echo "📈 커밋 업데이트 토픽 생성 중..."
docker exec -it $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic commit-update-events \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# 3. 사용자별 알림 토픽 (패턴 토픽은 자동 생성됨)
echo "👤 사용자별 알림 토픽 예시 생성 중..."
docker exec -it $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic user-notifications-test \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

# 토픽 목록 확인
echo "📋 생성된 토픽 목록 확인:"
docker exec -it $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

echo "✅ Kafka 토픽 생성 완료!"
