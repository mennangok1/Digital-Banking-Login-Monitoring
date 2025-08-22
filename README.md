# Bank Login Monitoring System

Real-time data pipeline for monitoring digital banking login/logout events using **Apache Kafka, MongoDB, Spark, Prometheus, and Grafana**. The system leverages **Dockerized microservices** for event streaming, storage, large-scale processing, and visualization, providing real-time observability of user activity.

## 🚀 Features
- **Event Streaming**: Kafka producer/consumer architecture for login/logout events  
- **Data Storage**: MongoDB integration for event persistence  
- **Stream Processing**: Apache Spark for scalable, real-time data processing  
- **Monitoring & Metrics**: Prometheus for metric collection and alerting  
- **Visualization**: Grafana dashboards for real-time system and user activity monitoring  
- **Dockerized Services**: Portable, reproducible deployment with Docker Compose  

## 🏗️ Architecture

🛠️ Technologies

Streaming: Apache Kafka

Processing: Apache Spark

Storage: MongoDB

Monitoring: Prometheus

Visualization: Grafana

Deployment: Docker, Docker Compose

![System Architecture](images/data_flow)


## 📂 Repository Structure
├── producer.py # Kafka producer for login/logout events
├── consumer.py # Kafka consumer service
├── producer_service.py # Producer microservice wrapper
├── consumer_service.py # Consumer microservice wrapper
├── mongo_class.py # MongoDB integration
├── spark_processor.py # Spark streaming processor
├── metrics_service.py # Custom Prometheus metrics service
├── prometheus_handler.py # Prometheus exporter configuration
├── simulate.py # Event simulation script
├── run_monitoring.py # Monitoring entrypoint
├── docker-compose.yml # Multi-service deployment
└── prometheus.yml # Prometheus scraping configuration

## ⚡ Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/<your-username>/bank-login-monitoring.git
cd bank-login-monitoring
```

### 2. Start Services
```bash
docker-compose up --build
```

### 3. Access Services
Kafka → localhost:9092

MongoDB → localhost:27017

Prometheus → http://localhost:9090

Grafana → http://localhost:3000 (default login: admin/admin)


## 📊 Grafana Dashboards

* Real-time active session monitoring

* Login/logout activity trends

* System reliability metrics

![Grafana Dashboard](images/grafana_result)

## ⚡ Apache Spark Processing

* Real-time streaming of login/logout events from Kafka
  
* Batch aggregation of user activity for trend analysis
  
* Fault-tolerant, scalable processing across multiple nodes

![Spark Result](images/spark_result)






## 📌 Use Cases

* Real-time monitoring of user logins/logouts

* Fraud detection and anomaly detection extensions

* System reliability and performance observability



👨‍💻 Author

**Mennan Gök**

[Contact me via email](mennan.gok@ug.bilkent.edu.tr)
