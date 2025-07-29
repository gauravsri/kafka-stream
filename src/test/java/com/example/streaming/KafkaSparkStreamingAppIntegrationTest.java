package com.example.streaming;

import com.example.avro.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@Testcontainers
@DisplayName("KafkaSparkStreamingApp Integration Tests")
class KafkaSparkStreamingAppIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withEmbeddedZookeeper();

    @Container
    static final MinIOContainer minio = new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2023-09-04T19-57-37Z"))
            .withUserName(TestConfiguration.TEST_S3_ACCESS_KEY)
            .withPassword(TestConfiguration.TEST_S3_SECRET_KEY);

    private SparkSession spark;
    private StreamingConfig config;
    private KafkaProducer<String, byte[]> producer;
    private Path tempLocalOutput;

    @BeforeEach
    void setUp() throws IOException {
        spark = TestSparkSession.getOrCreate();
        tempLocalOutput = TestConfiguration.createTempDirectory("integration-test-");
        
        configureSparkForContainers();
        
        config = TestConfiguration.createTestConfig(
                kafka.getBootstrapServers(),
                minio.getS3URL()
        );
        
        System.setProperty("S3_BUCKET", TestConfiguration.TEST_S3_BUCKET);
        System.setProperty("CHECKPOINT_LOCATION", "file://" + tempLocalOutput.resolve("checkpoints"));
        
        setupKafkaProducer();
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        TestConfiguration.cleanupTempDirectory("file://" + tempLocalOutput.toString());
        TestConfiguration.clearSystemProperties();
    }

    @Test
    @DisplayName("Should process messages end-to-end with local file system")
    void shouldProcessMessagesEndToEndWithLocalFileSystem() throws Exception {
        String localOutputPath = "file://" + tempLocalOutput.resolve("output").toString();
        
        List<Message> testMessages = TestDataGenerator.createTestMessagesWithTypes(
                "user_event", "system_event", "user_event"
        );

        sendMessagesToKafka(testMessages);

        Dataset<Row> kafkaStream = readFromKafkaForTest();
        Dataset<Row> deserializedStream = deserializeAvroMessages(kafkaStream);
        Dataset<Row> partitionedData = addPartitionColumns(deserializedStream);

        partitionedData.write()
                .format("parquet")
                .mode("overwrite")
                .partitionBy("partition_message_type", "partition_date")
                .save(localOutputPath);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Dataset<Row> result = spark.read().parquet(localOutputPath);
            assertThat(result.count()).isEqualTo(3);
            
            long userEvents = result.filter("partition_message_type = 'user_event'").count();
            long systemEvents = result.filter("partition_message_type = 'system_event'").count();
            
            assertThat(userEvents).isEqualTo(2);
            assertThat(systemEvents).isEqualTo(1);
        });
    }

    @Test
    @DisplayName("Should validate Kafka connectivity")
    void shouldValidateKafkaConnectivity() {
        assertThat(kafka.isRunning()).isTrue();
        assertThat(kafka.getBootstrapServers()).isNotEmpty();
    }

    @Test
    @DisplayName("Should validate MinIO connectivity")
    void shouldValidateMinIOConnectivity() {
        assertThat(minio.isRunning()).isTrue();
        assertThat(minio.getS3URL()).isNotEmpty();
    }

    @Test
    @DisplayName("Should validate configuration creation")
    void shouldValidateConfigurationCreation() {
        assertThat(config).isNotNull();
        assertThat(config.getKafkaBrokers()).isEqualTo(kafka.getBootstrapServers());
        assertThat(config.getS3Endpoint()).isEqualTo(minio.getS3URL());
        assertThat(config.getKafkaTopic()).isEqualTo(TestConfiguration.TEST_KAFKA_TOPIC);
    }

    private void configureSparkForContainers() {
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", minio.getS3URL());
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", TestConfiguration.TEST_S3_ACCESS_KEY);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", TestConfiguration.TEST_S3_SECRET_KEY);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
    }

    private void setupKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        producer = new KafkaProducer<>(props);
    }

    private void sendMessagesToKafka(List<Message> messages) throws Exception {
        for (Message message : messages) {
            byte[] serializedMessage = TestDataGenerator.serializeMessage(message);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    TestConfiguration.TEST_KAFKA_TOPIC,
                    message.getMessageId().toString(),
                    serializedMessage
            );
            producer.send(record).get();
        }
        producer.flush();
    }

    private Dataset<Row> readFromKafkaForTest() {
        return spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka.getBootstrapServers())
                .option("subscribe", TestConfiguration.TEST_KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load();
    }

    private Dataset<Row> deserializeAvroMessages(Dataset<Row> kafkaStream) {
        String avroSchema = Message.getClassSchema().toString();
        
        return kafkaStream
                .select(org.apache.spark.sql.avro.functions.from_avro(
                        org.apache.spark.sql.functions.col("value"), avroSchema).as("message"))
                .select("message.*")
                .withColumn("processing_time", org.apache.spark.sql.functions.current_timestamp());
    }

    private Dataset<Row> addPartitionColumns(Dataset<Row> deserializedStream) {
        return deserializedStream
                .withColumn("partition_message_type", org.apache.spark.sql.functions.col("message_type"))
                .withColumn("partition_date", org.apache.spark.sql.functions.col("date"));
    }
}