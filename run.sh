#!/bin/bash

# Kafka Spark Streaming Application Runner
# This script builds and runs the Kafka Spark Streaming application

set -e

echo "Building the application..."
mvn clean package -DskipTests

echo "Setting up environment variables..."
export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-messages}"
export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
export S3_BUCKET="${S3_BUCKET:-your-bucket}"

echo "Configuration:"
echo "  Kafka Brokers: $KAFKA_BROKERS"
echo "  Kafka Topic: $KAFKA_TOPIC"
echo "  S3 Endpoint: $S3_ENDPOINT"
echo "  S3 Bucket: $S3_BUCKET"

echo "Running Spark application..."
spark-submit \
  --class com.example.streaming.KafkaSparkStreamingApp \
  --master local[*] \
  --packages io.delta:delta-core_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
  --conf spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT \
  --conf spark.hadoop.fs.s3a.access.key=$S3_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=$S3_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.databricks.delta.optimizeWrite.enabled=true \
  --conf spark.databricks.delta.autoCompact.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  target/kafka-spark-streaming-1.0-SNAPSHOT.jar