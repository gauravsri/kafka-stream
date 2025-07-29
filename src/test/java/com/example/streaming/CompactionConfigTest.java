package com.example.streaming;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("CompactionConfig Tests")
class CompactionConfigTest {

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
        CompactionConfig config = new CompactionConfig();

        assertThat(config.getS3Endpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getS3AccessKey()).isEqualTo("minioadmin");
        assertThat(config.getS3SecretKey()).isEqualTo("minioadmin");
        assertThat(config.getS3Bucket()).isEqualTo("your-bucket");
        assertThat(config.getDeltaTablePath()).isEqualTo("s3a://your-bucket/messages/");
        assertThat(config.shouldCompactByPartition()).isTrue();
        assertThat(config.getDaysToCompact()).isEqualTo(7);
        assertThat(config.getSpecificPartitions()).isEmpty();
        assertThat(config.shouldVacuum()).isTrue();
        assertThat(config.getVacuumRetentionHours()).isEqualTo(168);
        assertThat(config.shouldGenerateStatistics()).isTrue();
    }

    @Test
    @DisplayName("Should use custom values when system properties are set")
    void shouldUseCustomValuesWhenSystemPropertiesAreSet() {
        System.setProperty("S3_ENDPOINT", "https://s3.amazonaws.com");
        System.setProperty("S3_ACCESS_KEY", "custom-access-key");
        System.setProperty("S3_SECRET_KEY", "custom-secret-key");
        System.setProperty("S3_BUCKET", "custom-bucket");
        System.setProperty("DELTA_TABLE_PATH", "s3a://custom-bucket/delta/");
        System.setProperty("COMPACT_BY_PARTITION", "false");
        System.setProperty("DAYS_TO_COMPACT", "3");
        System.setProperty("SPECIFIC_PARTITIONS", "partition_date='2024-01-15',partition_date='2024-01-16'");
        System.setProperty("VACUUM_ENABLED", "false");
        System.setProperty("VACUUM_RETENTION_HOURS", "48");
        System.setProperty("GENERATE_STATISTICS", "false");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getS3Endpoint()).isEqualTo("https://s3.amazonaws.com");
        assertThat(config.getS3AccessKey()).isEqualTo("custom-access-key");
        assertThat(config.getS3SecretKey()).isEqualTo("custom-secret-key");
        assertThat(config.getS3Bucket()).isEqualTo("custom-bucket");
        assertThat(config.getDeltaTablePath()).isEqualTo("s3a://custom-bucket/delta/");
        assertThat(config.shouldCompactByPartition()).isFalse();
        assertThat(config.getDaysToCompact()).isEqualTo(3);
        assertThat(config.getSpecificPartitions()).containsExactly(
            "partition_date='2024-01-15'", "partition_date='2024-01-16'");
        assertThat(config.shouldVacuum()).isFalse();
        assertThat(config.getVacuumRetentionHours()).isEqualTo(48);
        assertThat(config.shouldGenerateStatistics()).isFalse();
    }

    @Test
    @DisplayName("Should generate correct Delta table path based on bucket name")
    void shouldGenerateCorrectDeltaTablePathBasedOnBucketName() {
        System.setProperty("S3_BUCKET", "analytics-data");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getDeltaTablePath()).isEqualTo("s3a://analytics-data/messages/");
    }

    @Test
    @DisplayName("Should handle empty specific partitions gracefully")
    void shouldHandleEmptySpecificPartitionsGracefully() {
        System.setProperty("SPECIFIC_PARTITIONS", "");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getSpecificPartitions()).isEmpty();
    }

    @Test
    @DisplayName("Should handle boolean parsing correctly")
    void shouldHandleBooleanParsingCorrectly() {
        System.setProperty("COMPACT_BY_PARTITION", "TRUE");
        System.setProperty("VACUUM_ENABLED", "False");
        System.setProperty("GENERATE_STATISTICS", "yes"); // Invalid, should default to false

        CompactionConfig config = new CompactionConfig();

        assertThat(config.shouldCompactByPartition()).isTrue();
        assertThat(config.shouldVacuum()).isFalse();
        assertThat(config.shouldGenerateStatistics()).isFalse(); // "yes" is not "true"
    }

    @Test
    @DisplayName("Should handle integer parsing correctly")
    void shouldHandleIntegerParsingCorrectly() {
        System.setProperty("DAYS_TO_COMPACT", "14");
        System.setProperty("VACUUM_RETENTION_HOURS", "72");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getDaysToCompact()).isEqualTo(14);
        assertThat(config.getVacuumRetentionHours()).isEqualTo(72);
    }

    @Test
    @DisplayName("Should have proper toString representation")
    void shouldHaveProperToStringRepresentation() {
        CompactionConfig config = new CompactionConfig();

        String configString = config.toString();

        assertThat(configString).contains("CompactionConfig{");
        assertThat(configString).contains("s3Endpoint='http://localhost:9000'");
        assertThat(configString).contains("s3Bucket='your-bucket'");
        assertThat(configString).contains("deltaTablePath='s3a://your-bucket/messages/'");
        assertThat(configString).contains("compactByPartition=true");
        assertThat(configString).contains("daysToCompact=7");
        assertThat(configString).contains("vacuum=true");
        assertThat(configString).contains("vacuumRetentionHours=168");
        assertThat(configString).contains("generateStatistics=true");
    }

    @Test
    @DisplayName("Should parse specific partitions correctly")
    void shouldParseSpecificPartitionsCorrectly() {
        System.setProperty("SPECIFIC_PARTITIONS", "date='2024-01-15',type='user_event',date='2024-01-16'");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getSpecificPartitions()).hasSize(3);
        assertThat(config.getSpecificPartitions()).containsExactly(
            "date='2024-01-15'", "type='user_event'", "date='2024-01-16'");
    }
}