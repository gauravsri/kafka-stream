package com.example.streaming;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("DeltaLakeCompactionJob Unit Tests")
class DeltaLakeCompactionJobTest {

    private Path tempOutputDir;

    @BeforeEach
    void setUp() throws IOException {
        tempOutputDir = TestConfiguration.createTempDirectory("compaction-test-");
        TestConfiguration.clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        TestConfiguration.cleanupTempDirectory("file://" + tempOutputDir.toString());
        TestConfiguration.clearSystemProperties();
    }

    @Test
    @DisplayName("Should create CompactionConfig with default values")
    void shouldCreateCompactionConfigWithDefaultValues() {
        CompactionConfig config = new CompactionConfig();

        assertThat(config).isNotNull();
        assertThat(config.getS3Endpoint()).isEqualTo("http://localhost:9000");
        assertThat(config.getDaysToCompact()).isEqualTo(7);
        assertThat(config.shouldCompactByPartition()).isTrue();
        assertThat(config.shouldVacuum()).isTrue();
        assertThat(config.shouldGenerateStatistics()).isTrue();
    }

    @Test
    @DisplayName("Should create CompactionConfig with custom values")
    void shouldCreateCompactionConfigWithCustomValues() {
        System.setProperty("S3_ENDPOINT", "https://custom-endpoint.com");
        System.setProperty("DAYS_TO_COMPACT", "3");
        System.setProperty("COMPACT_BY_PARTITION", "false");
        System.setProperty("VACUUM_ENABLED", "false");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getS3Endpoint()).isEqualTo("https://custom-endpoint.com");
        assertThat(config.getDaysToCompact()).isEqualTo(3);
        assertThat(config.shouldCompactByPartition()).isFalse();
        assertThat(config.shouldVacuum()).isFalse();
    }

    @Test
    @DisplayName("Should validate configuration parsing")
    void shouldValidateConfigurationParsing() {
        System.setProperty("S3_BUCKET", "test-compaction-bucket");
        System.setProperty("VACUUM_RETENTION_HOURS", "48");
        System.setProperty("SPECIFIC_PARTITIONS", "date='2024-01-15',type='user_event'");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getS3Bucket()).isEqualTo("test-compaction-bucket");
        assertThat(config.getVacuumRetentionHours()).isEqualTo(48);
        assertThat(config.getSpecificPartitions()).hasSize(2);
        assertThat(config.getSpecificPartitions()).containsExactly("date='2024-01-15'", "type='user_event'");
    }

    @Test
    @DisplayName("Should handle empty specific partitions")
    void shouldHandleEmptySpecificPartitions() {
        System.setProperty("SPECIFIC_PARTITIONS", "");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getSpecificPartitions()).isEmpty();
    }

    @Test
    @DisplayName("Should generate correct Delta table path")
    void shouldGenerateCorrectDeltaTablePath() {
        System.setProperty("S3_BUCKET", "analytics-data");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getDeltaTablePath()).isEqualTo("s3a://analytics-data/messages/");
    }

    @Test
    @DisplayName("Should validate boolean configuration parsing")
    void shouldValidateBooleanConfigurationParsing() {
        System.setProperty("COMPACT_BY_PARTITION", "false");
        System.setProperty("VACUUM_ENABLED", "true");
        System.setProperty("GENERATE_STATISTICS", "false");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.shouldCompactByPartition()).isFalse();
        assertThat(config.shouldVacuum()).isTrue();
        assertThat(config.shouldGenerateStatistics()).isFalse();
    }

    @Test
    @DisplayName("Should provide comprehensive configuration toString")
    void shouldProvideComprehensiveConfigurationToString() {
        CompactionConfig config = new CompactionConfig();

        String configStr = config.toString();

        assertThat(configStr).contains("CompactionConfig{");
        assertThat(configStr).contains("deltaTablePath=");
        assertThat(configStr).contains("compactByPartition=");
        assertThat(configStr).contains("daysToCompact=");
        assertThat(configStr).contains("vacuum=");
        assertThat(configStr).contains("vacuumRetentionHours=");
    }

    @Test
    @DisplayName("Should handle configuration with local file paths for testing")
    void shouldHandleConfigurationWithLocalFilePathsForTesting() {
        String localPath = "file://" + tempOutputDir.resolve("delta-table").toString();
        System.setProperty("DELTA_TABLE_PATH", localPath);

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getDeltaTablePath()).isEqualTo(localPath);
    }

    @Test
    @DisplayName("Should validate integer parsing with edge cases")
    void shouldValidateIntegerParsingWithEdgeCases() {
        System.setProperty("DAYS_TO_COMPACT", "1");
        System.setProperty("VACUUM_RETENTION_HOURS", "24");

        CompactionConfig config = new CompactionConfig();

        assertThat(config.getDaysToCompact()).isEqualTo(1);
        assertThat(config.getVacuumRetentionHours()).isEqualTo(24);
    }
}