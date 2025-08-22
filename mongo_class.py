from pymongo import MongoClient
from datetime import datetime, timedelta

class MongoDBHandler:
    def __init__(self):
        self.client = MongoClient('mongodb://admin:password@localhost:27017/')
        self.db = self.client['bank_login_metrics']

    def record_login_attempt(self, user_id, device_info, auth_method, success=True, failure_reason=None, location=None):
        """Record a login attempt with location data"""
        try:
            login_event = {
                'user_id': user_id,
                'device_info': device_info,
                'authentication_method': auth_method,
                'success': success,
                'timestamp': datetime.now()
            }

            # Add optional fields
            if failure_reason:
                login_event['failure_reason'] = failure_reason
            if location:
                login_event['location'] = location

            result = self.db.login_events.insert_one(login_event)
            
            # Update user's last login info
            self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "last_login": datetime.now(),
                        "last_device": device_info,
                        "last_location": location
                    },
                    "$inc": {"failed_attempts": 0 if success else 1}
                },
                upsert=True
            )
            
            return True
            
        except Exception as e:
            print(f"Error processing login: {e}")
            return False

    def record_logout(self, user_id, device_info, location=None):
        """Record a logout event with location data"""
        try:
            logout_event = {
                'user_id': user_id,
                'device_info': device_info,
                'event_type': 'logout',
                'timestamp': datetime.now()
            }
            
            if location:
                logout_event['location'] = location

            result = self.db.login_events.insert_one(logout_event)
            return True
            
        except Exception as e:
            print(f"Error processing logout: {e}")
            return False

    def record_suspicious_activity(self, user_id, device_info, activity_type, risk_level):
        """Record suspicious activity"""
        try:
            suspicious_event = {
                'user_id': user_id,
                'event_type': 'suspicious_activity',
                'device_info': device_info,
                'activity_type': activity_type,
                'risk_level': risk_level,
                'timestamp': datetime.now()
            }
            self.db.security_events.insert_one(suspicious_event)
            return True
        except Exception as e:
            print(f"Error logging suspicious activity: {e}")
            return False

    def get_login_metrics(self):
        """Get login-related metrics including location data"""
        try:
            return {
                'login_attempts_by_method': list(self.db.login_events.aggregate([
                    {
                        "$group": {
                            "_id": "$authentication_method",
                            "total_attempts": {"$sum": 1},
                            "successful_attempts": {
                                "$sum": {"$cond": ["$success", 1, 0]}
                            }
                        }
                    }
                ])),
                'device_distribution': list(self.db.login_events.aggregate([
                    {"$group": {"_id": "$device_info.device_type", "count": {"$sum": 1}}}
                ])),
                'location_distribution': list(self.db.login_events.aggregate([
                    {"$match": {"location": {"$exists": True}}},
                    {"$group": {"_id": "$location.city", "count": {"$sum": 1}}}
                ]))
            }
        except Exception as e:
            print(f"Error getting metrics: {e}")
            return {}

    def get_security_metrics(self):
        """Get security-related metrics including location data"""
        try:
            current_time = datetime.now()
            return list(self.db.login_events.aggregate([
                {
                    "$match": {
                        "success": False,
                        "timestamp": {"$gte": current_time - timedelta(hours=24)}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "city": "$location.city",
                            "reason": "$failure_reason"
                        },
                        "count": {"$sum": 1}
                    }
                }
            ]))
        except Exception as e:
            print(f"Error getting security metrics: {e}")
            return []

    def get_login_history(self, user_id, start_time=None, end_time=None):
        """Get login history for a specific user"""
        query = {"user_id": user_id}
        if start_time:
            query['timestamp'] = {'$gte': start_time}
        if end_time:
            query['timestamp'] = query.get('timestamp', {})
            query['timestamp']['$lte'] = end_time
            
        return list(self.db.login_events.find(query).sort('timestamp', -1))

    def get_active_sessions(self):
        """Get currently active sessions"""
        return list(self.db.login_events.aggregate([
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": "$user_id",
                "last_event": {"$first": "$$ROOT"}
            }},
            {"$match": {"last_event.event_type": "login"}}
        ]))

    def log_error(self, error_data):
        """Log system errors"""
        error_data['timestamp'] = datetime.now()
        self.db.system_errors.insert_one(error_data)