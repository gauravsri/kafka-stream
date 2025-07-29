package com.example.streaming;

import org.springframework.stereotype.Component;

@Component
public class StreamingConfig {
    
    private final String kafkaBrokers;
    private final String kafkaTopic;
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3Bucket;
    private final String s3OutputPath;
    private final String checkpointLocation;
    
    public StreamingConfig() {
        this.kafkaBrokers = getEnvOrDefault("KAFKA_BROKERS", "localhost:9092");
        this.kafkaTopic = getEnvOrDefault("KAFKA_TOPIC", "messages");
        this.s3Endpoint = getEnvOrDefault("S3_ENDPOINT", "http://localhost:9000");
        this.s3AccessKey = getEnvOrDefault("S3_ACCESS_KEY", "minioadmin");
        this.s3SecretKey = getEnvOrDefault("S3_SECRET_KEY", "minioadmin");
        this.s3Bucket = getEnvOrDefault("S3_BUCKET", "your-bucket");
        this.s3OutputPath = String.format("s3a://%s/messages/", s3Bucket);
        this.checkpointLocation = getEnvOrDefault("CHECKPOINT_LOCATION", 
            String.format("s3a://%s/checkpoints/", s3Bucket));
    }
    
    private String getEnvOrDefault(String envVar, String defaultValue) {
        String value = System.getenv(envVar);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(envVar);
        }
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
    
    public String getKafkaBrokers() {
        return kafkaBrokers;
    }
    
    public String getKafkaTopic() {
        return kafkaTopic;
    }
    
    public String getS3Endpoint() {
        return s3Endpoint;
    }
    
    public String getS3AccessKey() {
        return s3AccessKey;
    }
    
    public String getS3SecretKey() {
        return s3SecretKey;
    }
    
    public String getS3Bucket() {
        return s3Bucket;
    }
    
    public String getS3OutputPath() {
        return s3OutputPath;
    }
    
    public String getCheckpointLocation() {
        return checkpointLocation;
    }
    
    @Override
    public String toString() {
        return "StreamingConfig{" +
                "kafkaBrokers='" + kafkaBrokers + '\'' +
                ", kafkaTopic='" + kafkaTopic + '\'' +
                ", s3Endpoint='" + s3Endpoint + '\'' +
                ", s3Bucket='" + s3Bucket + '\'' +
                ", s3OutputPath='" + s3OutputPath + '\'' +
                ", checkpointLocation='" + checkpointLocation + '\'' +
                '}';
    }
}