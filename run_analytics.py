from spark_processor import SparkProcessor

def main():
    spark_processor = SparkProcessor(
        kafka_bootstrap_servers='localhost:9092',
        kafka_topic='bank-login-monitoring-final'
    )
    
    try:
        spark_processor.start_processing()
    except KeyboardInterrupt:
        spark_processor.cleanup()

if __name__ == "__main__":
    main()