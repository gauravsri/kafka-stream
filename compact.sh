#!/bin/bash

# Delta Lake Compaction Job Runner
# This script runs the Delta Lake compaction job to optimize small files

set -e

echo "🔧 Delta Lake Compaction Job"
echo "============================"
echo

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Run Delta Lake compaction job to optimize small files"
    echo
    echo "Options:"
    echo "  --days N               Compact partitions from last N days (default: 7)"
    echo "  --partitions 'LIST'    Compact specific partitions (comma-separated)"
    echo "  --no-vacuum            Skip VACUUM operation"
    echo "  --no-stats             Skip statistics generation"
    echo "  --retention-hours N    VACUUM retention period in hours (default: 168)"
    echo "  --table-path PATH      Delta table path (default: from env)"
    echo "  --help                 Show this help message"
    echo
    echo "Environment Variables:"
    echo "  S3_ENDPOINT            MinIO/S3 endpoint (default: http://localhost:9000)"
    echo "  S3_ACCESS_KEY          S3 access key (default: minioadmin)"
    echo "  S3_SECRET_KEY          S3 secret key (default: minioadmin)"
    echo "  S3_BUCKET              S3 bucket name (default: your-bucket)"
    echo "  DELTA_TABLE_PATH       Delta table path (default: s3a://\$S3_BUCKET/messages/)"
    echo
    echo "Examples:"
    echo "  $0                                    # Compact last 7 days"
    echo "  $0 --days 3                          # Compact last 3 days"
    echo "  $0 --partitions 'date=2024-01-15'    # Compact specific partition"
    echo "  $0 --no-vacuum --days 1              # Compact without VACUUM"
}

# Parse command line arguments
DAYS_TO_COMPACT=""
SPECIFIC_PARTITIONS=""
VACUUM_ENABLED="true"
GENERATE_STATISTICS="true"
VACUUM_RETENTION_HOURS=""
DELTA_TABLE_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --days)
            DAYS_TO_COMPACT="$2"
            shift 2
            ;;
        --partitions)
            SPECIFIC_PARTITIONS="$2"
            shift 2
            ;;
        --no-vacuum)
            VACUUM_ENABLED="false"
            shift
            ;;
        --no-stats)
            GENERATE_STATISTICS="false"
            shift
            ;;
        --retention-hours)
            VACUUM_RETENTION_HOURS="$2"
            shift 2
            ;;
        --table-path)
            DELTA_TABLE_PATH="$2"
            shift 2
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            echo
            show_usage
            exit 1
            ;;
    esac
done

# Set environment variables for the job
if [ -n "$DAYS_TO_COMPACT" ]; then
    export DAYS_TO_COMPACT="$DAYS_TO_COMPACT"
fi

if [ -n "$SPECIFIC_PARTITIONS" ]; then
    export SPECIFIC_PARTITIONS="$SPECIFIC_PARTITIONS"
fi

if [ -n "$VACUUM_ENABLED" ]; then
    export VACUUM_ENABLED="$VACUUM_ENABLED"
fi

if [ -n "$GENERATE_STATISTICS" ]; then
    export GENERATE_STATISTICS="$GENERATE_STATISTICS"
fi

if [ -n "$VACUUM_RETENTION_HOURS" ]; then
    export VACUUM_RETENTION_HOURS="$VACUUM_RETENTION_HOURS"
fi

if [ -n "$DELTA_TABLE_PATH" ]; then
    export DELTA_TABLE_PATH="$DELTA_TABLE_PATH"
fi

# Set default values if not provided
export DAYS_TO_COMPACT="${DAYS_TO_COMPACT:-7}"
export VACUUM_ENABLED="${VACUUM_ENABLED:-true}"
export GENERATE_STATISTICS="${GENERATE_STATISTICS:-true}"
export VACUUM_RETENTION_HOURS="${VACUUM_RETENTION_HOURS:-168}"

# Display configuration
echo "📋 Compaction Configuration:"
echo "  Days to compact: $DAYS_TO_COMPACT"
echo "  Specific partitions: ${SPECIFIC_PARTITIONS:-'None (auto-detect)'}"
echo "  VACUUM enabled: $VACUUM_ENABLED"
echo "  Generate statistics: $GENERATE_STATISTICS"
echo "  VACUUM retention hours: $VACUUM_RETENTION_HOURS"
echo "  Table path: ${DELTA_TABLE_PATH:-'From environment'}"
echo

# Check if JAR exists
JAR_FILE="target/kafka-spark-streaming-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "📦 Building project (JAR not found)..."
    mvn clean package -DskipTests -q
    echo "✅ Project built successfully"
    echo
fi

# Run the compaction job
echo "🚀 Starting Delta Lake compaction job..."
echo

spark-submit \
  --class com.example.streaming.DeltaLakeCompactionJob \
  --master local[*] \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-avro_2.12:3.5.0 \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT:-http://localhost:9000}" \
  --conf spark.hadoop.fs.s3a.access.key="${S3_ACCESS_KEY:-minioadmin}" \
  --conf spark.hadoop.fs.s3a.secret.key="${S3_SECRET_KEY:-minioadmin}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.databricks.delta.optimizeWrite.enabled=true \
  --conf spark.databricks.delta.autoCompact.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  "$JAR_FILE"

echo
echo "✅ Delta Lake compaction job completed!"
echo

# Show post-compaction information
if [ "$GENERATE_STATISTICS" = "true" ]; then
    echo "📊 Check application logs above for table statistics"
    echo
fi

echo "💡 Tips:"
echo "  • Run this job periodically (daily/weekly) via cron"
echo "  • Monitor Delta Lake logs for optimization metrics"
echo "  • Adjust --days parameter based on your data volume"
echo "  • Use --partitions for targeted compaction of specific data"