from kafka import KafkaConsumer, TopicPartition
import json
from datetime import datetime

class Consumer:
    def __init__(self, bootstrap_servers, topic, group_id, mongo=None, prometheus_handler=None):
        self.topic = topic
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x:json.loads(x.decode('utf-8'))
        )
        self.consumer.subscribe([topic])
        self.mongo = mongo
        self.prometheus_handler = prometheus_handler
        
    def process_message(self, message):
        print(f"""
Topic: {message.topic}
Partition: {message.partition}
Offset: {message.offset}
Timestamp: {datetime.fromtimestamp(message.timestamp/1000)}
Value: {message.value}
              """)
        
    def start_reading(self):
        if self.mongo:
            for message in self.consumer:
                self.write_mongo_db(message)
        else:    
            for message in self.consumer:
                self.process_message(message)
        self.close()
        
    def close(self):
        self.consumer.close()

    def write_mongo_db(self, message):
        """
        Write Kafka message to MongoDB based on the event type
        """
        try:
            event = message.value
            self.process_message(message)
            
            if event.get('event_type') == 'login':
                # Extract location data
                location = event.get('location', {})
                device_info = event.get('device_info', {})
                
                # Record metrics if prometheus_handler is available
                if self.prometheus_handler:
                    # Record login location
                    self.prometheus_handler.record_login_location(
                        city=location.get('city', 'unknown'),
                        latitude=location.get('latitude', 0),
                        longitude=location.get('longitude', 0),
                        success=event.get('success', False)
                    )
                    
                    # Record device connection
                    self.prometheus_handler.record_device_connection(
                        device_type=device_info.get('device_type', 'unknown'),
                        os_type=device_info.get('os_type', 'unknown')
                    )
                    
                    # Record authentication metrics
                    self.prometheus_handler.record_login_attempt(
                        auth_method=event.get('authentication_method'),
                        success=event.get('success', False),
                        failure_reason=event.get('failure_reason')
                    )

                # Write to MongoDB
                success = self.mongo.record_login_attempt(
                user_id=event['user_id'],
                device_info=event['device_info'],
                auth_method=event['authentication_method'],
                success=event['success'],
                failure_reason=event.get('failure_reason'),
                location=event.get('location')  # Pass location data
            )
                
                if success:
                    print(f"Login event written to MongoDB: User {event['user_id']} "
                          f"via {event['authentication_method']} from {location.get('city', 'unknown')}")
                else:
                    print(f"Failed to write login event to MongoDB for user {event['user_id']}")

            elif event.get('event_type') == 'logout':
                if self.prometheus_handler:
                    # Update active sessions metric
                    self.prometheus_handler.record_logout(
                        device_type=event['device_info'].get('device_type', 'unknown'),
                        city=event.get('location', {}).get('city', 'unknown')
                    )

                success = self.mongo.record_logout(
                user_id=event['user_id'],
                device_info=event['device_info'],
                location=event.get('location')  # Pass location data
            )
                
                if success:
                    print(f"Logout event written to MongoDB: User {event['user_id']}")
                else:
                    print(f"Failed to write logout event to MongoDB for user {event['user_id']}")

            elif event.get('event_type') == 'suspicious_activity':
                if self.prometheus_handler:
                    self.prometheus_handler.record_suspicious_activity(
                        activity_type=event['activity_type'],
                        risk_level=event['risk_level'],
                        city=event.get('location', {}).get('city', 'unknown')
                    )

                success = self.mongo.record_suspicious_activity(
                    user_id=event['user_id'],
                    device_info=event['device_info'],
                    activity_type=event['activity_type'],
                    risk_level=event['risk_level'],
                    location=event.get('location')  # Add location data
                )
                
                if success:
                    print(f"Suspicious activity recorded for user {event['user_id']}: "
                          f"{event['activity_type']} in {event.get('location', {}).get('city', 'unknown')}")
                else:
                    print(f"Failed to record suspicious activity for user {event['user_id']}")

            elif 'user_id' in event and 'registration' in event:
                user_data = {
                    "user_id": event["user_id"],
                    "username": event["username"],
                    "account_type": event["account_type"],
                    "device_ids": [event["device_info"]["device_id"]],
                    "account_status": "active",
                    "created_at": datetime.now(),
                    "last_login": None,
                    "failed_attempts": 0,
                    "registered_location": event.get("location")  # Add location data
                }
                self.mongo.create_user(user_data)
                print(f"New user registered in MongoDB: {event['username']} "
                      f"from {event.get('location', {}).get('city', 'unknown')}")

        except Exception as e:
            print(f"Error writing to MongoDB: {e}")
            if self.mongo:
                self.mongo.log_error({
                    "error_type": "db_write_error",
                    "timestamp": datetime.now(),
                    "details": str(e),
                    "event_data": event,
                    "location": event.get('location')  # Add location to error logs
                })