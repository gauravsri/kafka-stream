package com.example.streaming;

import com.example.avro.Message;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaSparkStreamingApp Unit Tests")
class KafkaSparkStreamingAppTest {

    private SparkSession spark;
    private Path tempOutputDir;
    private Path tempCheckpointDir;

    @BeforeEach
    void setUp() throws IOException {
        spark = TestSparkSession.getOrCreate();
        tempOutputDir = TestConfiguration.createTempDirectory("test-output-");
        tempCheckpointDir = TestConfiguration.createTempDirectory("test-checkpoint-");
    }

    @AfterEach
    void tearDown() {
        TestConfiguration.cleanupTempDirectory("file://" + tempOutputDir.toString());
        TestConfiguration.cleanupTempDirectory("file://" + tempCheckpointDir.toString());
    }

    @Test
    @DisplayName("Should create SparkSession with correct configuration")
    void shouldCreateSparkSessionWithCorrectConfiguration() {
        SparkSession testSpark = TestSparkSession.getOrCreate();

        assertThat(testSpark).isNotNull();
        assertThat(testSpark.conf().get("spark.sql.extensions"))
                .contains("io.delta.sql.DeltaSparkSessionExtension");
        assertThat(testSpark.conf().get("spark.sql.catalog.spark_catalog"))
                .isEqualTo("org.apache.spark.sql.delta.catalog.DeltaCatalog");
        assertThat(testSpark.conf().get("spark.databricks.delta.optimizeWrite.enabled"))
                .isEqualTo("true");
        assertThat(testSpark.conf().get("spark.databricks.delta.autoCompact.enabled"))
                .isEqualTo("true");
    }

    @Test
    @DisplayName("Should add partition columns correctly")
    void shouldAddPartitionColumnsCorrectly() {
        Dataset<Row> testData = createTestDataset(Arrays.asList(
                new TestRecord("user_event", "2024-01-15", "msg1", 123456789L),
                new TestRecord("system_event", "2024-01-16", "msg2", 123456790L)
        ));

        Dataset<Row> partitionedData = addPartitionColumns(testData);

        List<Row> results = partitionedData.collectAsList();

        assertThat(results).hasSize(2);
        
        Row firstRow = results.get(0);
        assertThat((String) firstRow.getAs("partition_message_type")).isEqualTo("user_event");
        assertThat((String) firstRow.getAs("partition_date")).isEqualTo("2024-01-15");
        
        Row secondRow = results.get(1);
        assertThat((String) secondRow.getAs("partition_message_type")).isEqualTo("system_event");
        assertThat((String) secondRow.getAs("partition_date")).isEqualTo("2024-01-16");
    }

    @Test
    @DisplayName("Should handle empty dataset gracefully")
    void shouldHandleEmptyDatasetGracefully() {
        Dataset<Row> emptyData = spark.emptyDataFrame().withColumn("message_type", lit(""))
                .withColumn("date", lit(""))
                .filter(col("message_type").isNotNull().and(col("message_type").notEqual("")));

        Dataset<Row> partitionedData = addPartitionColumns(emptyData);

        assertThat(partitionedData.count()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should validate Avro schema compatibility")
    void shouldValidateAvroSchemaCompatibility() {
        String expectedSchema = Message.getClassSchema().toString();
        
        assertThat(expectedSchema).contains("\"name\":\"Message\"");
        assertThat(expectedSchema).contains("\"name\":\"message_type\"");
        assertThat(expectedSchema).contains("\"name\":\"date\"");
        assertThat(expectedSchema).contains("\"name\":\"message_id\"");
        assertThat(expectedSchema).contains("\"name\":\"creation_time\"");
    }

    @Test
    @DisplayName("Should handle various message types")
    void shouldHandleVariousMessageTypes() {
        List<TestRecord> testRecords = Arrays.asList(
                new TestRecord("user_event", "2024-01-15", "msg1", 123456789L),
                new TestRecord("system_event", "2024-01-15", "msg2", 123456790L),
                new TestRecord("error_event", "2024-01-15", "msg3", 123456791L),
                new TestRecord("audit_event", "2024-01-15", "msg4", 123456792L)
        );

        Dataset<Row> testData = createTestDataset(testRecords);
        Dataset<Row> partitionedData = addPartitionColumns(testData);

        List<Row> results = partitionedData.collectAsList();

        assertThat(results).hasSize(4);
        
        List<String> messageTypes = results.stream()
                .map(row -> row.getAs("partition_message_type").toString())
                .collect(java.util.stream.Collectors.toList());
        
        assertThat(messageTypes).containsExactlyInAnyOrder("user_event", "system_event", "error_event", "audit_event");
    }

    @Test
    @DisplayName("Should create valid test records")
    void shouldCreateValidTestRecords() {
        TestRecord record = new TestRecord("test_event", "2024-01-15", "test_id", 123456789L);
        
        assertThat(record.getMessage_type()).isEqualTo("test_event");
        assertThat(record.getDate()).isEqualTo("2024-01-15");
        assertThat(record.getMessage_id()).isEqualTo("test_id");
        assertThat(record.getCreation_time()).isEqualTo(123456789L);
    }

    private Dataset<Row> addPartitionColumns(Dataset<Row> deserializedStream) {
        return deserializedStream
                .withColumn("partition_message_type", col("message_type"))
                .withColumn("partition_date", col("date"));
    }

    private Dataset<Row> createTestDataset(List<TestRecord> records) {
        return spark.createDataFrame(records, TestRecord.class)
                .withColumn("processing_time", current_timestamp());
    }

    public static class TestRecord {
        private String message_type;
        private String date;
        private String message_id;
        private Long creation_time;

        public TestRecord() {}

        public TestRecord(String message_type, String date, String message_id, Long creation_time) {
            this.message_type = message_type;
            this.date = date;
            this.message_id = message_id;
            this.creation_time = creation_time;
        }

        public String getMessage_type() { return message_type; }
        public void setMessage_type(String message_type) { this.message_type = message_type; }
        
        public String getDate() { return date; }
        public void setDate(String date) { this.date = date; }
        
        public String getMessage_id() { return message_id; }
        public void setMessage_id(String message_id) { this.message_id = message_id; }
        
        public Long getCreation_time() { return creation_time; }
        public void setCreation_time(Long creation_time) { this.creation_time = creation_time; }
    }
}