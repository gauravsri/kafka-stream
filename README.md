# Kafka Spark Streaming with Delta Lake

A Java application that reads Avro messages from Apache Kafka and writes them to Delta Lake format on MinIO/S3 storage, partitioned by message type and date.

## Features

### Streaming Job
- Reads from configurable Kafka topic
- Deserializes Avro messages using Spark's `from_avro` function
- Writes to Delta Lake format with partitioning
- Supports MinIO/S3 storage
- Auto-compact and optimize-write enabled
- Environment-based configuration

### Compaction Job
- Periodic Delta Lake table optimization
- Small file compaction and coalescing
- Partition-based or full table optimization
- VACUUM operation for old file cleanup
- Automated table statistics generation
- Configurable retention policies

## Prerequisites

- Java 11+
- Apache Spark 3.5.0+
- Apache Kafka
- MinIO or S3-compatible storage

## Building the Project

```bash
mvn clean compile
mvn package
```

This creates a fat JAR: `target/kafka-spark-streaming-1.0-SNAPSHOT.jar`

## Configuration

Set these environment variables:

```bash
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="messages"
export S3_ENDPOINT="http://localhost:9000"
export S3_ACCESS_KEY="minioadmin"
export S3_SECRET_KEY="minioadmin"
export S3_BUCKET="your-bucket"
export CHECKPOINT_LOCATION="s3a://your-bucket/checkpoints/"

# Compaction job configuration
export DELTA_TABLE_PATH="s3a://your-bucket/messages/"
export DAYS_TO_COMPACT="7"
export VACUUM_RETENTION_HOURS="168"
```

## Running the Applications

### Streaming Job

Run the streaming application with spark-submit:

### Basic Command

```bash
spark-submit \
  --class com.example.streaming.KafkaSparkStreamingApp \
  --master local[*] \
  --packages io.delta:delta-core_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
  target/kafka-spark-streaming-1.0-SNAPSHOT.jar
```

### With MinIO/S3 Configuration

```bash
spark-submit \
  --class com.example.streaming.KafkaSparkStreamingApp \
  --master local[*] \
  --packages io.delta:delta-core_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.databricks.delta.optimizeWrite.enabled=true \
  --conf spark.databricks.delta.autoCompact.enabled=true \
  target/kafka-spark-streaming-1.0-SNAPSHOT.jar
```

### For Production (Cluster Mode)

```bash
spark-submit \
  --class com.example.streaming.KafkaSparkStreamingApp \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 4g \
  --executor-cores 2 \
  --driver-memory 2g \
  --packages io.delta:delta-core_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
  --conf spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT \
  --conf spark.hadoop.fs.s3a.access.key=$S3_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=$S3_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.databricks.delta.optimizeWrite.enabled=true \
  --conf spark.databricks.delta.autoCompact.enabled=true \
  target/kafka-spark-streaming-1.0-SNAPSHOT.jar
```

### Compaction Job

Run the periodic compaction job to optimize Delta Lake files:

#### Quick Start
```bash
./compact.sh
```

#### Basic Usage
```bash
# Compact last 7 days (default)
./compact.sh

# Compact last 3 days only
./compact.sh --days 3

# Compact specific partitions
./compact.sh --partitions "partition_date='2024-01-15',partition_date='2024-01-16'"

# Skip VACUUM operation
./compact.sh --no-vacuum --days 1

# Custom retention period
./compact.sh --retention-hours 48
```

#### Spark Submit Command
```bash
spark-submit \
  --class com.example.streaming.DeltaLakeCompactionJob \
  --master local[*] \
  --packages io.delta:delta-core_2.12:2.4.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT \
  --conf spark.hadoop.fs.s3a.access.key=$S3_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=$S3_SECRET_KEY \
  target/kafka-spark-streaming-1.0-SNAPSHOT.jar
```

#### Scheduling with Cron
```bash
# Run compaction daily at 2 AM
0 2 * * * /path/to/kafka-stream/compact.sh --days 1

# Run full compaction weekly on Sundays at 3 AM
0 3 * * 0 /path/to/kafka-stream/compact.sh --days 7
```

## Output Structure

Data is written to: `s3a://your-bucket/messages/`

Partitioned by:
- `partition_message_type=<type>/`
- `partition_date=<date>/`

Example: `s3a://your-bucket/messages/partition_message_type=user_event/partition_date=2024-01-15/`

## Message Schema

The application expects Avro messages with this schema:

```json
{
  "type": "record",
  "name": "Message",
  "fields": [
    { "name": "message_type",  "type": "string" },
    { "name": "date",          "type": "string" },
    { "name": "message_id",    "type": "string" },
    { "name": "creation_time", "type": "long" }
  ]
}
```

## Monitoring

### Streaming Job
- SLF4J logging
- Processing timestamps
- Spark UI for monitoring streaming metrics
- Delta Lake transaction logs for data lineage

### Compaction Job
- Table statistics and row counts
- Partition-level optimization metrics
- VACUUM operation status
- File count reduction reporting
- Delta Lake history tracking

## Testing

### Running Tests

The project includes comprehensive unit and integration tests with code coverage reporting.

#### Unit Tests
```bash
mvn test
```

#### Integration Tests (requires Docker)
```bash
mvn verify
```

#### Code Coverage Report
```bash
mvn test jacoco:report
```

View coverage report: `target/site/jacoco/index.html`

### Test Structure

- **Unit Tests** (`src/test/java`):
  - `StreamingConfigTest` - Configuration management tests
  - `TestDataGeneratorTest` - Test utility validation
  - `KafkaSparkStreamingAppTest` - Core application logic tests

- **Integration Tests**:
  - `KafkaSparkStreamingAppIntegrationTest` - End-to-end testing with Testcontainers

- **Test Utilities**:
  - `TestSparkSession` - Managed Spark session for tests
  - `TestConfiguration` - Test configuration utilities
  - `TestDataGenerator` - Avro message generation for tests

### Coverage Requirements

- **Instruction Coverage**: 80% minimum
- **Branch Coverage**: 70% minimum

### Test Dependencies

- JUnit 5 for test framework
- Mockito for mocking
- AssertJ for fluent assertions
- Testcontainers for integration testing (Kafka + MinIO)
- Awaitility for async test assertions

## Troubleshooting

1. **S3/MinIO Connection Issues**: Verify endpoint, credentials, and network connectivity
2. **Kafka Connection Issues**: Check broker addresses and topic existence
3. **Avro Deserialization**: Ensure Kafka messages match the expected schema
4. **Permission Issues**: Verify S3/MinIO bucket permissions for read/write access
5. **Test Failures**: Ensure Docker is running for integration tests
6. **Compaction Issues**: Verify Delta table exists before running compaction
7. **Performance**: Adjust compaction frequency based on data volume and query patterns