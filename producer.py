from kafka import KafkaProducer
import json
from datetime import datetime
import time

class Producer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    
    def send_message(self, message):
        try:
            # Add timestamp if not present
            if 'timestamp' not in message:
                message['timestamp'] = datetime.now().isoformat()
            
            # Send message
            future = self.producer.send(self.topic, value=message)
            result = future.get(timeout=60)
            
            # Print appropriate message based on event type
            if message.get('event_type') == 'login':
                print(f"Login event sent: User {message['user_id']} via {message['authentication_method']} - {'Success' if message['success'] else 'Failed'}")
            elif message.get('event_type') == 'logout':
                print(f"Logout event sent: User {message['user_id']} from device {message['device_info']['device_id']}")
            else:
                print(f"Message sent successfully to partition {result.partition}")
            
            return True
            
        except Exception as e:
            print(f"Error sending message: {e}")
            return False
    
    def send_login_event(self, user_id, device_info, auth_method, success=True, failure_reason=None):
        """Helper method to send login events"""
        message = {
            "event_type": "login",
            "user_id": user_id,
            "device_info": device_info,
            "authentication_method": auth_method,
            "success": success,
            "failure_reason": failure_reason,
            "timestamp": datetime.now().isoformat()
        }
        return self.send_message(message)
    
    def send_logout_event(self, user_id, device_info):
        """Helper method to send logout events"""
        message = {
            "event_type": "logout",
            "user_id": user_id,
            "device_info": device_info,
            "timestamp": datetime.now().isoformat()
        }
        return self.send_message(message)
    
    def send_suspicious_activity_event(self, user_id, device_info, activity_type, risk_level):
        """Helper method to send suspicious activity events"""
        message = {
            "event_type": "suspicious_activity",
            "user_id": user_id,
            "device_info": device_info,
            "activity_type": activity_type,
            "risk_level": risk_level,
            "timestamp": datetime.now().isoformat()
        }
        return self.send_message(message)
    
    def close(self):
        try:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()
            print("Producer closed successfully")
        except Exception as e:
            print(f"Error closing producer: {e}")