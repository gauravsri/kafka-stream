package com.example.streaming;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class DeltaLakeCompactionJob {
    
    private static final Logger logger = LoggerFactory.getLogger(DeltaLakeCompactionJob.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    public static void main(String[] args) {
        SparkSession spark = createSparkSession();
        
        try {
            CompactionConfig config = new CompactionConfig();
            
            configureS3Properties(spark, config);
            
            runCompactionJob(spark, config);
            
            logger.info("Delta Lake compaction job completed successfully");
            
        } catch (Exception e) {
            logger.error("Error in Delta Lake compaction job", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
    
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("DeltaLakeCompactionJob")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.optimizeWrite.enabled", "true")
                .config("spark.databricks.delta.autoCompact.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();
    }
    
    private static void configureS3Properties(SparkSession spark, CompactionConfig config) {
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", config.getS3Endpoint());
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", config.getS3AccessKey());
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", config.getS3SecretKey());
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
        
        logger.info("S3/MinIO configuration applied for compaction job");
    }
    
    private static void runCompactionJob(SparkSession spark, CompactionConfig config) {
        String tablePath = config.getDeltaTablePath();
        
        logger.info("Starting compaction for Delta table at: {}", tablePath);
        
        if (!DeltaTable.isDeltaTable(spark, tablePath)) {
            logger.warn("No Delta table found at path: {}. Skipping compaction.", tablePath);
            return;
        }
        
        DeltaTable deltaTable = DeltaTable.forPath(spark, tablePath);
        
        if (config.shouldCompactByPartition()) {
            compactByPartitions(spark, deltaTable, config);
        } else {
            compactEntireTable(deltaTable);
        }
        
        if (config.shouldVacuum()) {
            runVacuum(deltaTable, config);
        }
        
        if (config.shouldGenerateStatistics()) {
            generateTableStatistics(spark, tablePath);
        }
    }
    
    private static void compactByPartitions(SparkSession spark, DeltaTable deltaTable, CompactionConfig config) {
        List<String> partitionsToCompact = getPartitionsToCompact(spark, deltaTable, config);
        
        logger.info("Compacting {} partitions", partitionsToCompact.size());
        
        for (String partition : partitionsToCompact) {
            try {
                logger.info("Optimizing partition: {}", partition);
                
                deltaTable.optimize()
                        .where(partition)
                        .executeCompaction();
                
                logger.info("Successfully optimized partition: {}", partition);
                
            } catch (Exception e) {
                logger.error("Failed to optimize partition: {}", partition, e);
            }
        }
    }
    
    private static void compactEntireTable(DeltaTable deltaTable) {
        logger.info("Optimizing entire Delta table");
        
        deltaTable.optimize()
                .executeCompaction();
        
        logger.info("Successfully optimized entire Delta table");
    }
    
    private static List<String> getPartitionsToCompact(SparkSession spark, DeltaTable deltaTable, CompactionConfig config) {
        if (config.getSpecificPartitions() != null && !config.getSpecificPartitions().isEmpty()) {
            return config.getSpecificPartitions();
        }
        
        return getRecentPartitions(spark, deltaTable, config);
    }
    
    private static List<String> getRecentPartitions(SparkSession spark, DeltaTable deltaTable, CompactionConfig config) {
        int daysBack = config.getDaysToCompact();
        
        logger.info("Finding partitions from last {} days for compaction", daysBack);
        
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(daysBack);
        
        Dataset<Row> partitions = spark.sql(String.format(
            "SELECT DISTINCT partition_date FROM delta.`%s` " +
            "WHERE partition_date >= '%s' AND partition_date <= '%s'",
            deltaTable.toString(),
            startDate.format(DATE_FORMATTER),
            endDate.format(DATE_FORMATTER)
        ));
        
        return partitions.collectAsList().stream()
                .map(row -> String.format("partition_date = '%s'", row.getString(0)))
                .collect(java.util.stream.Collectors.toList());
    }
    
    private static void runVacuum(DeltaTable deltaTable, CompactionConfig config) {
        int retentionHours = config.getVacuumRetentionHours();
        
        logger.info("Running VACUUM with retention period of {} hours", retentionHours);
        
        try {
            deltaTable.vacuum(retentionHours);
            logger.info("VACUUM completed successfully");
        } catch (Exception e) {
            logger.error("VACUUM operation failed", e);
        }
    }
    
    private static void generateTableStatistics(SparkSession spark, String tablePath) {
        logger.info("Generating table statistics");
        
        try {            
            long totalRows = spark.sql(String.format(
                "SELECT COUNT(*) as row_count FROM delta.`%s`", tablePath
            )).first().getLong(0);
            
            Dataset<Row> partitionStats = spark.sql(String.format(
                "SELECT partition_message_type, partition_date, COUNT(*) as row_count " +
                "FROM delta.`%s` GROUP BY partition_message_type, partition_date " +
                "ORDER BY partition_date DESC, partition_message_type", tablePath
            ));
            
            logger.info("Table statistics generated:");
            logger.info("Total rows: {}", totalRows);
            logger.info("Partition breakdown:");
            
            partitionStats.collectAsList().forEach(row -> 
                logger.info("  {}/{}: {} rows", 
                    row.getString(0), row.getString(1), row.getLong(2))
            );
            
        } catch (Exception e) {
            logger.error("Failed to generate table statistics", e);
        }
    }
    
    public static void compactTable(SparkSession spark, String tablePath, CompactionConfig config) {
        configureS3Properties(spark, config);
        
        if (!DeltaTable.isDeltaTable(spark, tablePath)) {
            logger.warn("No Delta table found at path: {}. Skipping compaction.", tablePath);
            return;
        }
        
        DeltaTable deltaTable = DeltaTable.forPath(spark, tablePath);
        
        if (config.shouldCompactByPartition()) {
            compactByPartitions(spark, deltaTable, config);
        } else {
            compactEntireTable(deltaTable);
        }
    }
}