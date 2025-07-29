package com.example.streaming;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestConfiguration {
    
    public static final String TEST_KAFKA_TOPIC = "test-messages";
    public static final String TEST_S3_BUCKET = "test-bucket";
    public static final String TEST_S3_ACCESS_KEY = "testkey";
    public static final String TEST_S3_SECRET_KEY = "testsecret";
    
    public static StreamingConfig createTestConfig() {
        return createTestConfig("localhost:9092", "http://localhost:9000");
    }
    
    public static StreamingConfig createTestConfig(String kafkaBrokers, String s3Endpoint) {
        System.setProperty("KAFKA_BROKERS", kafkaBrokers);
        System.setProperty("KAFKA_TOPIC", TEST_KAFKA_TOPIC);
        System.setProperty("S3_ENDPOINT", s3Endpoint);
        System.setProperty("S3_ACCESS_KEY", TEST_S3_ACCESS_KEY);
        System.setProperty("S3_SECRET_KEY", TEST_S3_SECRET_KEY);
        System.setProperty("S3_BUCKET", TEST_S3_BUCKET);
        
        return new StreamingConfig();
    }
    
    public static Path createTempDirectory(String prefix) throws IOException {
        return Files.createTempDirectory(prefix);
    }
    
    public static String createTempS3Path() throws IOException {
        Path tempDir = createTempDirectory("test-s3-");
        return "file://" + tempDir.toAbsolutePath().toString();
    }
    
    public static String createTempCheckpointPath() throws IOException {
        Path tempDir = createTempDirectory("test-checkpoint-");
        return "file://" + tempDir.toAbsolutePath().toString();
    }
    
    public static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
    
    public static void cleanupTempDirectory(String path) {
        try {
            if (path.startsWith("file://")) {
                Path dir = Paths.get(path.substring(7));
                if (Files.exists(dir)) {
                    Files.walk(dir)
                            .sorted((a, b) -> b.compareTo(a))
                            .forEach(p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (IOException e) {
                                    // Ignore cleanup errors in tests
                                }
                            });
                }
            }
        } catch (IOException e) {
            // Ignore cleanup errors in tests
        }
    }
    
    public static void clearSystemProperties() {
        // Streaming config properties
        System.clearProperty("KAFKA_BROKERS");
        System.clearProperty("KAFKA_TOPIC");
        System.clearProperty("S3_ENDPOINT");
        System.clearProperty("S3_ACCESS_KEY");
        System.clearProperty("S3_SECRET_KEY");
        System.clearProperty("S3_BUCKET");
        System.clearProperty("CHECKPOINT_LOCATION");
        
        // Compaction config properties
        System.clearProperty("DELTA_TABLE_PATH");
        System.clearProperty("COMPACT_BY_PARTITION");
        System.clearProperty("DAYS_TO_COMPACT");
        System.clearProperty("SPECIFIC_PARTITIONS");
        System.clearProperty("VACUUM_ENABLED");
        System.clearProperty("VACUUM_RETENTION_HOURS");
        System.clearProperty("GENERATE_STATISTICS");
    }
}