package com.example.streaming;

import com.example.avro.Message;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.avro.functions.from_avro;

public class KafkaSparkStreamingApp {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaSparkStreamingApp.class);
    
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = createSparkSession();
        
        try {
            StreamingConfig config = new StreamingConfig();
            
            configureS3Properties(spark, config);
            
            Dataset<Row> kafkaStream = readFromKafka(spark, config);
            
            Dataset<Row> deserializedStream = deserializeAvroMessages(kafkaStream);
            
            Dataset<Row> partitionedData = addPartitionColumns(deserializedStream);
            
            StreamingQuery query = writeToDeltaLake(partitionedData, config);
            
            logger.info("Streaming query started. Waiting for termination...");
            query.awaitTermination();
            
        } catch (Exception e) {
            logger.error("Error in streaming application", e);
            throw e;
        } finally {
            spark.stop();
        }
    }
    
    private static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("KafkaSparkStreamingApp")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.optimizeWrite.enabled", "true")
                .config("spark.databricks.delta.autoCompact.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
    }
    
    private static void configureS3Properties(SparkSession spark, StreamingConfig config) {
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", config.getS3Endpoint());
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", config.getS3AccessKey());
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", config.getS3SecretKey());
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
        
        logger.info("S3/MinIO configuration applied");
    }
    
    private static Dataset<Row> readFromKafka(SparkSession spark, StreamingConfig config) {
        logger.info("Reading from Kafka topic: {} at brokers: {}", config.getKafkaTopic(), config.getKafkaBrokers());
        
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getKafkaBrokers())
                .option("subscribe", config.getKafkaTopic())
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();
    }
    
    private static Dataset<Row> deserializeAvroMessages(Dataset<Row> kafkaStream) {
        String avroSchema = Message.getClassSchema().toString();
        
        logger.info("Deserializing Avro messages with schema: {}", avroSchema);
        
        return kafkaStream
                .select(from_avro(col("value"), avroSchema).as("message"))
                .select("message.*")
                .withColumn("processing_time", current_timestamp());
    }
    
    private static Dataset<Row> addPartitionColumns(Dataset<Row> deserializedStream) {
        return deserializedStream
                .withColumn("partition_message_type", col("message_type"))
                .withColumn("partition_date", col("date"));
    }
    
    private static StreamingQuery writeToDeltaLake(Dataset<Row> partitionedData, StreamingConfig config) {
        String outputPath = config.getS3OutputPath();
        
        logger.info("Writing to Delta Lake at path: {}", outputPath);
        
        return partitionedData.writeStream()
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", config.getCheckpointLocation())
                .partitionBy("partition_message_type", "partition_date")
                .start(outputPath);
    }
}