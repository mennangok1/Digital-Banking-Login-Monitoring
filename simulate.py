from producer import Producer
from consumer import Consumer
from mongo_class import MongoDBHandler
from prometheus_handler import PrometheusHandler
from spark_processor import SparkProcessor
import threading
import time

class BankLoginSimulation:
    def __init__(self):
        # Existing initialization
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'topic': 'bank-login-monitoring'
        }
        self.producer = Producer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            topic=self.kafka_config['topic']
        )
        self.mongo_handler = MongoDBHandler()
        self.prometheus_handler = PrometheusHandler(port=8000)
        self.consumer = Consumer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            topic=self.kafka_config['topic'],
            group_id='bank_login_group',
            mongo=self.mongo_handler
        )
        
        # Add Spark processor
        self.spark_processor = SparkProcessor(
            kafka_bootstrap_servers=self.kafka_config['bootstrap_servers'],
            kafka_topic=self.kafka_config['topic']
        )

    def run_spark_analytics(self):
        """Run Spark analytics in separate thread"""
        try:
            self.spark_processor.start_processing()
        except Exception as e:
            print(f"Error in Spark processing: {e}")

    def run_simulation(self):
        try:
            # Start consumer thread (existing)
            consumer_thread = threading.Thread(target=self.consumer.start_reading, daemon=True)
            consumer_thread.start()

            # Start metrics thread (existing)
            metrics_thread = threading.Thread(target=self.prometheus_handler.update_metrics, 
                                           args=(self.mongo_handler,), daemon=True)
            metrics_thread.start()

            # Start Spark analytics thread (new)
            spark_thread = threading.Thread(target=self.run_spark_analytics, daemon=True)
            spark_thread.start()

            # Run producer (existing login event generation)
            print("Starting bank login simulation...")
            while True:
                try:
                    # Your existing event generation code
                    pass
                except Exception as e:
                    print(f"Error in simulation loop: {e}")
                    time.sleep(1)

        except KeyboardInterrupt:
            print("\nStopping simulation...")
        finally:
            self.cleanup()

    def cleanup(self):
        """Enhanced cleanup including Spark"""
        self.running = False
        self.producer.close()
        self.consumer.close()
        self.prometheus_handler.cleanup()
        self.spark_processor.cleanup()
        print("Simulation stopped and resources cleaned up")