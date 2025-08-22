from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
import os

#   combo_metrics   (city+device_type+login_method)
#   city_metrics
#   failure_metrics
#   method_metrics
#   device_metrics

class SparkProcessor:
    def __init__(self, kafka_bootstrap_servers: str, kafka_topic: str):
        self.spark = (
            SparkSession.builder
                .appName("BankLoginProcessor")
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
                .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

        self.kafka_bootstrap_servers = kafka_bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_topic = kafka_topic or os.getenv("LOGIN_TOPIC", "login-events")

        self.mongo = MongoClient("mongodb://admin:password@localhost:27017/")
        self.db    = self.mongo["bank_login_metrics"]

        self.login_schema = StructType([
            StructField("event_type",      StringType()),
            StructField("user_id",         StringType()),
            StructField("timestamp",       LongType()),
            StructField("device_info",     StructType([
                StructField("device_id",   StringType()),
                StructField("device_type", StringType()),
                StructField("os_type",     StringType()),
            ])),
            StructField("location",        StructType([
                StructField("city",       StringType()),
                StructField("latitude",   DoubleType()),
                StructField("longitude",  DoubleType()),
            ])),
            StructField("authentication_method", StringType()),
            StructField("success",         BooleanType()),
            StructField("failure_reason",  StringType()),
        ])

    def _save_df(self, collection: str, df, epoch_id):
        """Upsert one micro‑batch into Mongo (replace on collection)"""
        docs = [row.asDict() for row in df.collect()]
        coll = self.db[collection]
        coll.delete_many({})          
        if docs:
            coll.insert_many(docs)

    def _sink(self, collection, order_cols, num_rows=50):
        def _fn(df, epoch_id):
            if order_cols:
                present = [c for c in order_cols if c in df.columns]
                if present:
                    df = df.orderBy(*present)
            df.show(num_rows, truncate=False)
            self._save_df(collection, df, epoch_id)
        return _fn

    def start_processing(self):
        raw_df = (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", self.kafka_topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) AS json_str")
                .select(from_json(col("json_str"), self.login_schema).alias("data"))
                .select("data.*")
                .withColumn("event_time", from_unixtime(col("timestamp")/1000).cast("timestamp"))
                .filter(col("event_type") == "login")
        )
        is_success = expr("CASE WHEN success THEN 1 ELSE 0 END")

        combo_tbl = (
            raw_df.groupBy(
                col("location.city").alias("city"),
                col("device_info.device_type").alias("device_type"),
                col("authentication_method").alias("login_method"))
            .agg(count("*").alias("total_attempts"),
                 sum(is_success).alias("successful_attempts"))
            .withColumn("failed_attempts", col("total_attempts")-col("successful_attempts"))
            .withColumn("success_rate", round(col("successful_attempts")*100.0/col("total_attempts"),2))
        )
        (combo_tbl.writeStream
            .outputMode("complete")
             .trigger(processingTime="2 minutes")
             .foreachBatch(self._sink("combo_metrics", ["city","device_type","login_method"], 100))
             .start())

        city_tbl = (
            raw_df.groupBy(col("location.city").alias("city"))
            .agg(count("*").alias("total_attempts"),
                 sum(is_success).alias("successful_attempts"))
            .withColumn("failed_attempts", col("total_attempts")-col("successful_attempts"))
            .withColumn("success_rate", round(col("successful_attempts")*100.0/col("total_attempts"),2))
        )
        (city_tbl.writeStream.outputMode("complete")
             .trigger(processingTime="4 minutes")
             .foreachBatch(self._sink("city_metrics", ["city"], 50))
             .start())

        failure_tbl = (
            raw_df.filter(~col("success"))
                  .groupBy(col("failure_reason").alias("reason"))
                  .count())
        (failure_tbl.writeStream.outputMode("complete")
             .trigger(processingTime="4 minutes")
             .foreachBatch(self._sink("failure_metrics", ["reason"], 20))
             .start())

        # 4️⃣ method_metrics ----------------------------------------------
        method_tbl = (
            raw_df.groupBy(col("authentication_method").alias("login_method"))
            .agg(count("*").alias("total_attempts"),
                 sum(is_success).alias("successful_attempts"))
            .withColumn("failed_attempts", col("total_attempts")-col("successful_attempts"))
            .withColumn("success_rate", round(col("successful_attempts")*100.0/col("total_attempts"),2))
        )
        (method_tbl.writeStream.outputMode("complete")
             .trigger(processingTime="4 minutes")
             .foreachBatch(self._sink("method_metrics", ["login_method"], 20))
             .start())

        device_tbl = (
            raw_df.groupBy(col("device_info.device_type").alias("device_type"))
            .agg(count("*").alias("total_attempts"),
                 sum(is_success).alias("successful_attempts"))
            .withColumn("failed_attempts", col("total_attempts")-col("successful_attempts"))
            .withColumn("success_rate", round(col("successful_attempts")*100.0/col("total_attempts"),2))
        )
        (device_tbl.writeStream.outputMode("complete")
             .trigger(processingTime="4 minutes")
             .foreachBatch(self._sink("device_metrics", ["device_type"], 20))
             .start())

        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    try:
        SparkProcessor("localhost:9092", "bank-login-monitoring-final").start_processing()
    except KeyboardInterrupt:
        print("\nStopping …")
