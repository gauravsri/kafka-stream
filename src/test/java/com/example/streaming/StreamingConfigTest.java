package com.example.streaming;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("StreamingConfig Tests")
class StreamingConfigTest {

    @BeforeEach
    void setUp() {
        TestConfiguration.clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        TestConfiguration.clearSystemProperties();
    }

    @Test
    @DisplayName("Should use default values when environment variables are not set")
    void shouldUseDefaultValues() {
        StreamingConfig config = new StreamingConfig();

        assertThat(config.getKafkaBrokers()).isEqualTo("localhost:9092");
        assertThat(config.getKafkaTopic()).isEqualTo("messages");
        assertThat(config.getS3Endpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getS3AccessKey()).isEqualTo("minioadmin");
        assertThat(config.getS3SecretKey()).isEqualTo("minioadmin");
        assertThat(config.getS3Bucket()).isEqualTo("your-bucket");
        assertThat(config.getS3OutputPath()).isEqualTo("s3a://your-bucket/messages/");
        assertThat(config.getCheckpointLocation()).isEqualTo("s3a://your-bucket/checkpoints/");
    }

    @Test
    @DisplayName("Should use environment variables when they are set")
    void shouldUseEnvironmentVariables() {
        System.setProperty("KAFKA_BROKERS", "kafka1:9092,kafka2:9092");
        System.setProperty("KAFKA_TOPIC", "custom-topic");
        System.setProperty("S3_ENDPOINT", "https://s3.amazonaws.com");
        System.setProperty("S3_ACCESS_KEY", "custom-access-key");
        System.setProperty("S3_SECRET_KEY", "custom-secret-key");
        System.setProperty("S3_BUCKET", "custom-bucket");
        System.setProperty("CHECKPOINT_LOCATION", "s3a://custom-bucket/custom-checkpoints/");

        StreamingConfig config = new StreamingConfig();

        assertThat(config.getKafkaBrokers()).isEqualTo("kafka1:9092,kafka2:9092");
        assertThat(config.getKafkaTopic()).isEqualTo("custom-topic");
        assertThat(config.getS3Endpoint()).isEqualTo("https://s3.amazonaws.com");
        assertThat(config.getS3AccessKey()).isEqualTo("custom-access-key");
        assertThat(config.getS3SecretKey()).isEqualTo("custom-secret-key");
        assertThat(config.getS3Bucket()).isEqualTo("custom-bucket");
        assertThat(config.getS3OutputPath()).isEqualTo("s3a://custom-bucket/messages/");
        assertThat(config.getCheckpointLocation()).isEqualTo("s3a://custom-bucket/custom-checkpoints/");
    }

    @Test
    @DisplayName("Should generate correct S3 output path based on bucket name")
    void shouldGenerateCorrectS3OutputPath() {
        System.setProperty("S3_BUCKET", "test-data-bucket");

        StreamingConfig config = new StreamingConfig();

        assertThat(config.getS3OutputPath()).isEqualTo("s3a://test-data-bucket/messages/");
    }

    @Test
    @DisplayName("Should handle null environment variables gracefully")
    void shouldHandleNullEnvironmentVariables() {
        System.clearProperty("KAFKA_BROKERS");
        System.clearProperty("KAFKA_TOPIC");
        System.clearProperty("S3_ENDPOINT");

        StreamingConfig config = new StreamingConfig();

        assertThat(config.getKafkaBrokers()).isEqualTo("localhost:9092");
        assertThat(config.getKafkaTopic()).isEqualTo("messages");
        assertThat(config.getS3Endpoint()).isEqualTo("http://localhost:9000");
    }

    @Test
    @DisplayName("Should handle empty environment variables")
    void shouldHandleEmptyEnvironmentVariables() {
        System.setProperty("KAFKA_BROKERS", "");
        System.setProperty("KAFKA_TOPIC", "");
        System.setProperty("S3_ENDPOINT", "");

        StreamingConfig config = new StreamingConfig();

        assertThat(config.getKafkaBrokers()).isEqualTo("localhost:9092");
        assertThat(config.getKafkaTopic()).isEqualTo("messages");
        assertThat(config.getS3Endpoint()).isEqualTo("http://localhost:9000");
    }

    @Test
    @DisplayName("Should have proper toString representation")
    void shouldHaveProperToStringRepresentation() {
        StreamingConfig config = new StreamingConfig();

        String configString = config.toString();

        assertThat(configString).contains("StreamingConfig{");
        assertThat(configString).contains("kafkaBrokers='localhost:9092'");
        assertThat(configString).contains("kafkaTopic='messages'");
        assertThat(configString).contains("s3Endpoint='http://localhost:9000'");
        assertThat(configString).contains("s3Bucket='your-bucket'");
        assertThat(configString).contains("s3OutputPath='s3a://your-bucket/messages/'");
        assertThat(configString).contains("checkpointLocation='s3a://your-bucket/checkpoints/'");
        assertThat(configString).doesNotContain("s3AccessKey");
        assertThat(configString).doesNotContain("s3SecretKey");
    }

    @Test
    @DisplayName("Should maintain immutability of configuration values")
    void shouldMaintainImmutabilityOfConfigurationValues() {
        StreamingConfig config1 = new StreamingConfig();
        String originalBrokers = config1.getKafkaBrokers();
        String originalTopic = config1.getKafkaTopic();

        System.setProperty("KAFKA_BROKERS", "new-brokers:9092");
        System.setProperty("KAFKA_TOPIC", "new-topic");

        StreamingConfig config2 = new StreamingConfig();

        assertThat(config1.getKafkaBrokers()).isEqualTo(originalBrokers);
        assertThat(config1.getKafkaTopic()).isEqualTo(originalTopic);
        assertThat(config2.getKafkaBrokers()).isEqualTo("new-brokers:9092");
        assertThat(config2.getKafkaTopic()).isEqualTo("new-topic");
    }
}