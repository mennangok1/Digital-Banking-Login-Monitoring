from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
import time
import os
import shutil

class SparkProcessor:
    # Adjusted timing and batch parameters
    TRIGGER_INTERVAL = "10 seconds"
    BATCH_SIZE = 5000
    DEFAULT_PARTITIONS = 4

    def __init__(self, kafka_bootstrap_servers: str, kafka_topic: str):
        # Create checkpoint directory
        os.makedirs("/tmp/checkpoints", exist_ok=True)

        self.spark = (
            SparkSession.builder
                .appName("BankLoginProcessor")
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
                .config("spark.sql.shuffle.partitions", self.DEFAULT_PARTITIONS)
                .config("spark.streaming.kafka.maxRatePerPartition", "5000")
                .config("spark.default.parallelism", "8")
                .config("spark.streaming.backpressure.enabled", "true")
                .config("spark.streaming.kafka.consumer.cache.enabled", "true")
                .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
                .config("spark.memory.offHeap.enabled", "true")
                .config("spark.memory.offHeap.size", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.memory", "2g")
                .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

        self.mongo = MongoClient("mongodb://admin:password@localhost:27017/")
        self.db = self.mongo["bank_login_metrics"]

        self.login_schema = StructType([
            StructField("event_type", StringType()),
            StructField("user_id", StringType()),
            StructField("timestamp", LongType()),
            StructField("device_info", StructType([
                StructField("device_id", StringType()),
                StructField("device_type", StringType()),
                StructField("os_type", StringType()),
            ])),
            StructField("location", StructType([
                StructField("city", StringType()),
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType()),
            ])),
            StructField("authentication_method", StringType()),
            StructField("success", BooleanType()),
            StructField("failure_reason", StringType()),
        ])

    def _save_df(self, collection: str, df, epoch_id):
        """Optimized batch save to MongoDB with bulk operations"""
        try:
            docs = df.coalesce(1).collect()
            
            if docs:
                coll = self.db[collection]
                with coll.bulk_write() as bulk_ops:
                    coll.delete_many({})
                    coll.insert_many([row.asDict() for row in docs])
                print(f"✅ Saved {len(docs)} documents to {collection}")
        except Exception as e:
            print(f"❌ Error saving to MongoDB: {e}")

    def _sink(self, collection, order_cols, num_rows=50):
        def _fn(df, epoch_id):
            try:
                start_time = time.time()
                
                if order_cols:
                    present = [c for c in order_cols if c in df.columns]
                    if present:
                        df = df.orderBy(*present)
                
                df.show(num_rows, truncate=False)
                self._save_df(collection, df, epoch_id)
                
                processing_time = time.time() - start_time
                print(f"⏱️  Processing time for {collection}: {processing_time:.2f} seconds")
                
            except Exception as e:
                print(f"❌ Error in sink function: {e}")
        return _fn

    def _create_metrics_table(self, df, group_cols):
        """Common metrics calculation logic"""
        is_success = expr("CASE WHEN success THEN 1 ELSE 0 END")
        return (
            df.groupBy(*group_cols)
            .agg(
                count("*").alias("total_attempts"),
                sum(is_success).alias("successful_attempts")
            )
            .withColumn("failed_attempts", col("total_attempts") - col("successful_attempts"))
            .withColumn("success_rate", 
                round(col("successful_attempts") * 100.0 / col("total_attempts"), 2))
        )
    def start_processing(self):
        try:
            # Base DataFrame with optimized reading
            raw_df = (
                self.spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                    .option("subscribe", self.kafka_topic)
                    .option("startingOffsets", "earliest")
                    .option("maxOffsetsPerTrigger", self.BATCH_SIZE)
                    .option("failOnDataLoss", "false")
                    .load()
                    .selectExpr("CAST(value AS STRING) AS json_str")
                    .select(from_json(col("json_str"), self.login_schema).alias("data"))
                    .select("data.*")
                    .withColumn("event_time", from_unixtime(col("timestamp")/1000).cast("timestamp"))
                    .withWatermark("event_time", "1 minute")
                    .filter(col("event_type") == "login")
            )

            # Store all queries
            queries = []

            # Combo metrics (city + device + method)
            combo_tbl = self._create_metrics_table(
                raw_df,
                [
                    col("location.city").alias("city"),
                    col("device_info.device_type").alias("device_type"),
                    col("authentication_method").alias("login_method")
                ]
            )
            queries.append(
                combo_tbl.writeStream
                    .outputMode("complete")
                    .trigger(processingTime=self.TRIGGER_INTERVAL)
                    .option("checkpointLocation", "/tmp/checkpoints/combo")
                    .option("spark.sql.shuffle.partitions", self.DEFAULT_PARTITIONS)
                    .option("maxBatchesToRetain", "2")
                    .foreachBatch(self._sink("combo_metrics", ["city", "device_type", "login_method"], 100))
                    .start()
            )

            # City metrics
            city_tbl = self._create_metrics_table(
                raw_df,
                [col("location.city").alias("city")]
            )
            queries.append(
                city_tbl.writeStream
                    .outputMode("complete")
                    .trigger(processingTime=self.TRIGGER_INTERVAL)
                    .option("checkpointLocation", "/tmp/checkpoints/city")
                    .option("spark.sql.shuffle.partitions", self.DEFAULT_PARTITIONS)
                    .option("maxBatchesToRetain", "2")
                    .foreachBatch(self._sink("city_metrics", ["city"], 50))
                    .start()
            )

            # Failure metrics
            failure_tbl = (
                raw_df.filter(~col("success"))
                    .groupBy(col("failure_reason").alias("reason"))
                    .count()
            )
            queries.append(
                failure_tbl.writeStream
                    .outputMode("complete")
                    .trigger(processingTime=self.TRIGGER_INTERVAL)
                    .option("checkpointLocation", "/tmp/checkpoints/failure")
                    .option("spark.sql.shuffle.partitions", self.DEFAULT_PARTITIONS)
                    .option("maxBatchesToRetain", "2")
                    .foreachBatch(self._sink("failure_metrics", ["reason"], 20))
                    .start()
            )

            # Authentication method metrics
            method_tbl = self._create_metrics_table(
                raw_df,
                [col("authentication_method").alias("login_method")]
            )
            queries.append(
                method_tbl.writeStream
                    .outputMode("complete")
                    .trigger(processingTime=self.TRIGGER_INTERVAL)
                    .option("checkpointLocation", "/tmp/checkpoints/method")
                    .option("spark.sql.shuffle.partitions", self.DEFAULT_PARTITIONS)
                    .option("maxBatchesToRetain", "2")
                    .foreachBatch(self._sink("method_metrics", ["login_method"], 20))
                    .start()
            )

            # Device metrics
            device_tbl = self._create_metrics_table(
                raw_df,
                [col("device_info.device_type").alias("device_type")]
            )
            queries.append(
                device_tbl.writeStream
                    .outputMode("complete")
                    .trigger(processingTime=self.TRIGGER_INTERVAL)
                    .option("checkpointLocation", "/tmp/checkpoints/device")
                    .option("spark.sql.shuffle.partitions", self.DEFAULT_PARTITIONS)
                    .option("maxBatchesToRetain", "2")
                    .foreachBatch(self._sink("device_metrics", ["device_type"], 20))
                    .start()
            )

            print("\n✨ Processing started! Streaming metrics:")
            print("- Combo metrics (city + device + method)")
            print("- City-based metrics")
            print("- Failure analysis")
            print("- Authentication method metrics")
            print("- Device type metrics\n")

            # Wait for all queries
            for query in queries:
                query.awaitTermination()

        except Exception as e:
            print(f"\n❌ Error: {e}")
            raise e

    def cleanup(self):
        """Cleanup resources and checkpoints"""
        try:
            if self.spark:
                self.spark.stop()
            if self.mongo:
                self.mongo.close()
            # Clean checkpoints
            shutil.rmtree("/tmp/checkpoints", ignore_errors=True)
            print("\n✅ Resources and checkpoints cleaned up successfully")
        except Exception as e:
            print(f"\n❌ Error during cleanup: {e}")

def main():
    processor = None
    try:
        processor = SparkProcessor("localhost:9092", "bank-login-monitoring-final")
        processor.start_processing()
    except KeyboardInterrupt:
        print("\n🛑 Stopping processor...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if processor:
            processor.cleanup()

if __name__ == "__main__":
    main()
