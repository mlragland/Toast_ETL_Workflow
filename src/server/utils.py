"""
Toast ETL Pipeline - Server Utilities
Utility functions for logging, error handling, and monitoring.
"""

import os
import logging
from google.cloud import logging as cloud_logging


def setup_logging(app):
    """Setup structured logging for the Flask application."""
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, app.config.get('LOG_LEVEL', 'INFO')),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup Google Cloud Logging if in production
    if app.config.get('ENVIRONMENT') == 'production':
        try:
            client = cloud_logging.Client()
            client.setup_logging()
            app.logger.info("Google Cloud Logging configured")
        except Exception as e:
            app.logger.error(f"Failed to setup Google Cloud Logging: {str(e)}")
    
    # Set log level for Flask app
    app.logger.setLevel(getattr(logging, app.config.get('LOG_LEVEL', 'INFO')))
    
    app.logger.info(f"Logging configured for environment: {app.config.get('ENVIRONMENT')}")


def handle_errors(app):
    """Setup error handling for the Flask application."""
    
    @app.errorhandler(Exception)
    def handle_exception(e):
        """Handle unexpected exceptions."""
        app.logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        return {
            'error': 'Internal server error',
            'message': str(e)
        }, 500


def publish_notification(topic_name, message_type, data):
    """Publish notification to Pub/Sub topic."""
    if not topic_name:
        return
    
    try:
        from google.cloud import pubsub_v1
        import json
        from datetime import datetime
        
        publisher = pubsub_v1.PublisherClient()
        project_id = os.getenv('PROJECT_ID')
        
        if not project_id:
            return
        
        topic_path = publisher.topic_path(project_id, topic_name)
        
        message_data = {
            'type': message_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': data
        }
        
        future = publisher.publish(
            topic_path,
            data=json.dumps(message_data).encode('utf-8'),
            source='toast-etl-server'
        )
        
        logging.info(f"Published notification: {future.result()}")
        
    except Exception as e:
        logging.error(f"Failed to publish notification: {str(e)}")


def track_execution_metrics(endpoint, execution_time, status, error=None):
    """Track execution metrics for monitoring."""
    try:
        # Log metrics
        logging.info(
            f"Execution metrics - Endpoint: {endpoint}, Time: {execution_time:.2f}s, Status: {status}",
            extra={
                'endpoint': endpoint,
                'execution_time': execution_time,
                'status': status,
                'error': error
            }
        )
        
        # In production, this would send metrics to Google Cloud Monitoring
        
    except Exception as e:
        logging.error(f"Failed to track metrics: {str(e)}") 