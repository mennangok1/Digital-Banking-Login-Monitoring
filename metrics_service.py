from prometheus_handler import PrometheusHandler
from mongo_class import MongoDBHandler
import time

class MetricsService:
    def __init__(self):
        self.mongo_handler = MongoDBHandler()
        self.prometheus_handler = PrometheusHandler(port=8000)
        self.running = True

    def run(self):
        try:
            print("Starting metrics service...")
            while self.running:
                try:
                    self.prometheus_handler.update_metrics(self.mongo_handler)
                    time.sleep(2)
                except Exception as e:
                    print(f"Error updating metrics: {e}")
                    time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping metrics service...")
            self.running = False
            self.prometheus_handler.cleanup()

if __name__ == "__main__":
    metrics = MetricsService()
    metrics.run()