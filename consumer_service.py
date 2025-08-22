from kafka import KafkaConsumer
from prometheus_client import start_http_server, Gauge, Counter
import json
import time

# --- Prometheus Metric Definitions ---

# Toplam login sayısı (hep artar)
login_counter = Counter('login_events_total', 'Total number of login events')

# Toplam logout sayısı (hep artar)
logout_counter = Counter('logout_events_total', 'Total number of logout events')

# Aktif oturum sayısı (artabilir ve azalabilir)
active_sessions = Gauge('active_sessions_total', 'Number of active sessions')

# Cihaz türüne göre aktif oturumlar
active_sessions_by_device = Gauge('active_sessions_by_device',
                                  'Number of active sessions by device type',
                                  ['device_type'])


# --- Kafka Consumer Initialization ---
consumer = KafkaConsumer(
    'login_events',                 # Kafka topic adı
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-metrics-group'
)


# --- Event Processing Function ---
def process_event(event):
    """
    Kafka'dan gelen her event'i işler.
    Event tipi login ise oturum artırılır, logout ise azaltılır.
    """
    event_type = event.get("event_type")  # login veya logout
    user_id = event.get("user_id")
    device_type = event.get("device_type", "unknown")

    if event_type == "login":
        login_counter.inc()
        active_sessions.inc()
        active_sessions_by_device.labels(device_type=device_type).inc()
        print(f"[LOGIN] User {user_id} ({device_type}) logged in. Active sessions: {active_sessions._value.get()}")

    elif event_type == "logout":
        logout_counter.inc()
        active_sessions.dec()
        active_sessions_by_device.labels(device_type=device_type).dec()
        print(f"[LOGOUT] User {user_id} ({device_type}) logged out. Active sessions: {active_sessions._value.get()}")

    else:
        print(f"[UNKNOWN EVENT] {event}")


# --- Main Execution ---
if __name__ == "__main__":
    # Prometheus HTTP endpoint başlat (8000 portu)
    start_http_server(8000)
    print("Prometheus metrics available at http://localhost:8000/metrics")

    # Kafka'dan gelen mesajları sürekli tüket
    for message in consumer:
        try:
            event_data = message.value
            process_event(event_data)
        except Exception as e:
            print(f"Error processing message: {e}")
