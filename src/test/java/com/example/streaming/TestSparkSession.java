package com.example.streaming;

import org.apache.spark.sql.SparkSession;

public class TestSparkSession {
    
    private static SparkSession sparkSession;
    
    public static SparkSession getOrCreate() {
        if (sparkSession == null) {
            sparkSession = SparkSession.builder()
                    .appName("TestSparkApp")
                    .master("local[2]")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
                    .config("spark.databricks.delta.autoCompact.enabled", "true")
                    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
                    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                    .getOrCreate();
            
            sparkSession.sparkContext().setLogLevel("WARN");
        }
        return sparkSession;
    }
    
    public static void stop() {
        if (sparkSession != null) {
            sparkSession.stop();
            sparkSession = null;
        }
    }
    
    public static void reset() {
        stop();
        sparkSession = getOrCreate();
    }
}