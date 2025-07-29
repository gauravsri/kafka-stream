package com.example.streaming;

import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class CompactionConfig {
    
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3Bucket;
    private final String deltaTablePath;
    private final boolean compactByPartition;
    private final int daysToCompact;
    private final List<String> specificPartitions;
    private final boolean vacuum;
    private final int vacuumRetentionHours;
    private final boolean generateStatistics;
    
    public CompactionConfig() {
        this.s3Endpoint = getEnvOrDefault("S3_ENDPOINT", "http://localhost:9000");
        this.s3AccessKey = getEnvOrDefault("S3_ACCESS_KEY", "minioadmin");
        this.s3SecretKey = getEnvOrDefault("S3_SECRET_KEY", "minioadmin");
        this.s3Bucket = getEnvOrDefault("S3_BUCKET", "your-bucket");
        this.deltaTablePath = getEnvOrDefault("DELTA_TABLE_PATH", 
            String.format("s3a://%s/messages/", s3Bucket));
        
        this.compactByPartition = Boolean.parseBoolean(
            getEnvOrDefault("COMPACT_BY_PARTITION", "true"));
        this.daysToCompact = Integer.parseInt(
            getEnvOrDefault("DAYS_TO_COMPACT", "7"));
        
        String partitionsStr = getEnvOrDefault("SPECIFIC_PARTITIONS", "");
        this.specificPartitions = partitionsStr.isEmpty() ? 
            List.of() : Arrays.asList(partitionsStr.split(","));
        
        this.vacuum = Boolean.parseBoolean(
            getEnvOrDefault("VACUUM_ENABLED", "true"));
        this.vacuumRetentionHours = Integer.parseInt(
            getEnvOrDefault("VACUUM_RETENTION_HOURS", "168")); // 7 days default
        
        this.generateStatistics = Boolean.parseBoolean(
            getEnvOrDefault("GENERATE_STATISTICS", "true"));
    }
    
    private String getEnvOrDefault(String envVar, String defaultValue) {
        String value = System.getenv(envVar);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(envVar);
        }
        return (value != null && !value.isEmpty()) ? value : defaultValue;
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
    
    public String getDeltaTablePath() {
        return deltaTablePath;
    }
    
    public boolean shouldCompactByPartition() {
        return compactByPartition;
    }
    
    public int getDaysToCompact() {
        return daysToCompact;
    }
    
    public List<String> getSpecificPartitions() {
        return specificPartitions;
    }
    
    public boolean shouldVacuum() {
        return vacuum;
    }
    
    public int getVacuumRetentionHours() {
        return vacuumRetentionHours;
    }
    
    public boolean shouldGenerateStatistics() {
        return generateStatistics;
    }
    
    @Override
    public String toString() {
        return "CompactionConfig{" +
                "s3Endpoint='" + s3Endpoint + '\'' +
                ", s3Bucket='" + s3Bucket + '\'' +
                ", deltaTablePath='" + deltaTablePath + '\'' +
                ", compactByPartition=" + compactByPartition +
                ", daysToCompact=" + daysToCompact +
                ", specificPartitions=" + specificPartitions +
                ", vacuum=" + vacuum +
                ", vacuumRetentionHours=" + vacuumRetentionHours +
                ", generateStatistics=" + generateStatistics +
                '}';
    }
}