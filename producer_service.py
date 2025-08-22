from producer import Producer
import random
import time
from datetime import datetime

class LoginEventProducer:
    def __init__(self):
        # Existing configuration
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'topic': 'bank-spark'
        }
        self.producer = Producer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            topic=self.kafka_config['topic']
        )
        self.user_ids = [f"user_{i}" for i in range(1, 11)]
        self.auth_methods = ["password", "biometric", "2fa"]
        self.device_types = ["mobile", "web"]
        self.failure_reasons = ["wrong_password", "expired_session", "suspicious_location"]

        self.turkish_cities = [
            {"city": "Istanbul", "lat": 41.0082, "lon": 28.9784},
            {"city": "Ankara", "lat": 39.9334, "lon": 32.8597},
            {"city": "Izmir", "lat": 38.4237, "lon": 27.1428},
            {"city": "Bursa", "lat": 40.1885, "lon": 29.0610},
            {"city": "Antalya", "lat": 36.8969, "lon": 30.7133},
            {"city": "Adana", "lat": 37.0000, "lon": 35.3213},
            {"city": "Gaziantep", "lat": 37.0662, "lon": 37.3833},
            {"city": "Konya", "lat": 37.8667, "lon": 32.4833},
            {"city": "Mersin", "lat": 36.8000, "lon": 34.6333},
            {"city": "Diyarbakir", "lat": 37.9144, "lon": 40.2306}
        ]
         
        # New configuration
        self.os_types = {
            "mobile": ["iOS", "Android"],
            "web": ["Windows", "macOS", "Linux"]
        }
        self.suspicious_activities = [
            "multiple_failed_attempts",
            "unusual_location",
            "rapid_device_switch"
        ]
        self.risk_levels = ["low", "medium", "high"]
        self.session_durations = {}  # Track active sessions
        
    def generate_login_event(self):
        """Generate a login event"""
        # Existing logic
        success = random.random() < 0.8  # 80% success rate
        device_type = random.choice(self.device_types)
        
        # New: Add OS type based on device type
        os_type = random.choice(self.os_types[device_type])
        
        # Enhanced event structure (existing + new fields)
        location = random.choice(self.turkish_cities)
        
        event = {
            "event_type": "login",
            "user_id": random.choice(self.user_ids),
            "device_info": {
                "device_id": f"device_{random.randint(1, 5)}",
                "device_type": random.choice(self.device_types),
                "os_type": os_type,
                "ip_address": f"192.168.1.{random.randint(1, 255)}"
            },
            "location": {
                "city": location["city"],
                "latitude": location["lat"],
                "longitude": location["lon"],
                "country": "Turkey"
            },
            "authentication_method": random.choice(self.auth_methods),
            "success": success,
            "timestamp": datetime.now().isoformat()
        }
        
        # Existing failure handling
        if not success:
            event["failure_reason"] = random.choice(self.failure_reasons)

        # New: Add suspicious activity detection    
        if random.random() < 0.1:  # 10% chance of suspicious activity
            event["suspicious_activity"] = {
                "type": random.choice(self.suspicious_activities),
                "risk_level": random.choice(self.risk_levels)
            }
            
        return event
    
    def generate_logout_event(self, user_id):
        """Generate a logout event"""
        device_type = random.choice(self.device_types)
        
        # Enhanced event structure (existing + new fields)
        event = {
            "event_type": "logout",
            "user_id": user_id,
            "device_info": {
                "device_id": f"device_{random.randint(1, 5)}",
                "device_type": device_type,
                "os_type": random.choice(self.os_types[device_type])  # New field
            },
            "timestamp": datetime.now().isoformat()
        }

        # New: Add session duration if available
        if user_id in self.session_durations:
            login_time = self.session_durations[user_id]
            duration = (datetime.now() - login_time).total_seconds()
            event["session_duration"] = duration
            del self.session_durations[user_id]

        return event

    def run(self):
        try:
            print("Starting login event producer...")
            while True:
                try:
                    if random.random() < 0.7:  # 70% chance of login event
                        event = self.generate_login_event()
                        # New: Track session start time for successful logins
                        if event["success"]:
                            self.session_durations[event["user_id"]] = datetime.now()
                        
                        # Fixed string formatting
                        failure_msg = f"Failed ({event.get('failure_reason')})" if not event["success"] else "Success"
                        print(f"Login attempt: User {event['user_id']} via {event['authentication_method']} "
                            f"on {event['device_info']['os_type']} - {failure_msg}")
                        
                        # New: Log suspicious activity
                        if "suspicious_activity" in event:
                            print(f"⚠️ Suspicious activity detected: {event['suspicious_activity']['type']}")
                    else:
                        # Modified logout logic to handle session duration
                        active_users = list(self.session_durations.keys())
                        if active_users:
                            user_id = random.choice(active_users)
                        else:
                            user_id = random.choice(self.user_ids)
                            
                        event = self.generate_logout_event(user_id)
                        duration_msg = f"{event.get('session_duration', 'N/A')}s"
                        print(f"Logout: User {event['user_id']} - Session duration: {duration_msg}")
                    
                    self.producer.send_message(event)
                    time.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    print(f"Error generating event: {e}")
                    time.sleep(1)   
        except KeyboardInterrupt:
            print("\nStopping producer...")
            self.producer.close()
            
            
            
if __name__ == "__main__":
    producer = LoginEventProducer()
    producer.run()