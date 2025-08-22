from prometheus_client import start_http_server, Gauge, Counter, Summary, Histogram
import time

class PrometheusHandler:
    def __init__(self, port=8000):
        
        # Login metrics
        self.LOGIN_ATTEMPTS = Counter('login_attempts_total', 
            'Total number of login attempts', ['auth_method', 'status'])
        self.ACTIVE_SESSIONS = Gauge('active_sessions_current', 
            'Current number of active sessions', ['device_type'])
        self.LOGIN_DURATION = Histogram('session_duration_seconds', 
            'Duration of user sessions', ['device_type'])
        
        # Authentication metrics
        self.AUTH_FAILURES = Counter('authentication_failures_total', 
            'Number of failed authentication attempts', ['auth_method', 'failure_reason'])
        self.AUTH_LATENCY = Summary('authentication_latency_seconds', 
            'Time taken for authentication', ['auth_method'])
        
        # Device metrics
        self.DEVICE_CONNECTIONS = Counter('device_connections_total', 
            'Number of connections by device type', ['device_type', 'os_type'])
        self.UNIQUE_DEVICES = Gauge('unique_devices_active', 
            'Number of unique devices currently active', ['device_type'])
        
        # Security metrics
        self.SUSPICIOUS_ACTIVITIES = Counter('suspicious_activities_total', 
            'Number of suspicious activities detected', ['activity_type', 'risk_level'])
        self.BLOCKED_ATTEMPTS = Counter('blocked_attempts_total', 
            'Number of blocked login attempts', ['reason'])
        self.SECURITY_ALERTS = Gauge('security_alerts_current', 
            'Current number of active security alerts', ['severity'])
        
        # User metrics
        self.USER_STATUS = Gauge('user_status_current', 
            'Current user status counts', ['status'])
        self.CONCURRENT_USERS = Gauge('concurrent_users_current', 
            'Number of concurrent users', ['user_type'])
        
        # Performance metrics
        self.REQUEST_LATENCY = Histogram('request_latency_seconds', 
            'Request latency in seconds', ['endpoint'])
        self.ERROR_RATE = Counter('errors_total', 
            'Number of errors encountered', ['error_type'])
        
        
        # geomap
        self.LOGIN_LOCATIONS = Counter('login_locations_total', 
            'Number of logins by location', ['city', 'result'])
        
        self.LOGIN_LOCATIONS_ACTIVE = Gauge('login_locations_active', 
            'Current active sessions by location', ['city', 'latitude', 'longitude'])
        
        self.last_metrics_update = 0
        self.metrics_update_interval = 2
        
        # Start the Prometheus server
        start_http_server(port, '0.0.0.0')
        print(f"Prometheus metrics server started on port {port}")

    def record_login_attempt(self, auth_method, success, device_type=None, failure_reason=None):
        """Record a login attempt"""
        status = 'success' if success else 'failure'
        self.LOGIN_ATTEMPTS.labels(auth_method=auth_method, status=status).inc()
        
        if not success and failure_reason:
            self.AUTH_FAILURES.labels(
                auth_method=auth_method,
                failure_reason=failure_reason
            ).inc()
        
        if device_type:
            self.DEVICE_CONNECTIONS.labels(
                device_type=device_type,
                os_type=device_type.get('os', 'unknown')
            ).inc()

    def record_suspicious_activity(self, activity_type, risk_level):
        """Record suspicious activity"""
        self.SUSPICIOUS_ACTIVITIES.labels(
            activity_type=activity_type,
            risk_level=risk_level
        ).inc()

    def update_metrics(self, mongo_handler):
        """Update all Prometheus metrics"""
        current_time = time.time()
        
        if current_time - self.last_metrics_update < self.metrics_update_interval:
            return

        try:
            # Get current metrics from MongoDB
            metrics = mongo_handler.get_login_metrics()
            
            # Update authentication method metrics
            for item in metrics.get('login_attempts_by_method', []):
                auth_method = item['_id']
                self.LOGIN_ATTEMPTS.labels(
                    auth_method=auth_method,
                    status='success'
                ).inc(item['successful_attempts'])
                
                failed_attempts = item['total_attempts'] - item['successful_attempts']
                self.LOGIN_ATTEMPTS.labels(
                    auth_method=auth_method,
                    status='failure'
                ).inc(failed_attempts)

            # Update device distribution metrics
            for item in metrics.get('device_distribution', []):
                if item['_id']:
                    self.ACTIVE_SESSIONS.labels(
                        device_type=item['_id']
                    ).set(item['count'])

            # Update security metrics
            security_metrics = mongo_handler.get_security_metrics()
            for metric in security_metrics:
                self.SECURITY_ALERTS.labels(
                    severity=metric['_id']
                ).set(metric['count'])

            # Update active sessions
            active_sessions = mongo_handler.get_active_sessions()
            self.CONCURRENT_USERS.labels(
                user_type='total'
            ).set(len(active_sessions))

            self.last_metrics_update = current_time

        except Exception as e:
            print(f"Error updating Prometheus metrics: {e}")
            self.ERROR_RATE.labels(error_type='metrics_update').inc()

    def record_latency(self, endpoint, duration):
        """Record API endpoint latency"""
        self.REQUEST_LATENCY.labels(endpoint=endpoint).observe(duration)

    def record_error(self, error_type):
        """Record system error"""
        self.ERROR_RATE.labels(error_type=error_type).inc()

    def update_user_status(self, status_counts):
        """Update user status metrics"""
        for status, count in status_counts.items():
            self.USER_STATUS.labels(status=status).set(count)
            
            
    def record_login_location(self, city, latitude, longitude, success):
        """Record a login attempt location"""
        self.LOGIN_LOCATIONS.labels(
            city=city,
            result='success' if success else 'failure'
        ).inc()
        
        if success:
            self.LOGIN_LOCATIONS_ACTIVE.labels(
                city=city,
                latitude=str(latitude),
                longitude=str(longitude)
            ).inc()