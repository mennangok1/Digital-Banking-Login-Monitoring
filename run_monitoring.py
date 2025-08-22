from consumer import Consumer
from mongo_class import MongoDBHandler
from prometheus_handler import PrometheusHandler

def main():
    mongo_handler = MongoDBHandler()
    prometheus_handler = PrometheusHandler(port=8000)
    
    consumer = Consumer(
        bootstrap_servers=['localhost:9092'],
        topic='bank-login-monitoring-final',
        group_id='monitoring_group',
        mongo=mongo_handler
    )
    
    try:
        consumer.start_reading()
    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()