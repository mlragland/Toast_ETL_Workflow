"""
Toast ETL Pipeline - Flask Application
Main web server for Cloud Run deployment with comprehensive monitoring and error handling.
"""

import os
import logging
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging
import json
from datetime import datetime

from .routes import register_routes
from .utils import setup_logging, handle_errors
from .monitoring import setup_monitoring


def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Configuration
    app.config.update(
        PROJECT_ID=os.getenv('PROJECT_ID'),
        DATASET_ID=os.getenv('DATASET_ID'),
        GCS_BUCKET=os.getenv('GCS_BUCKET'),
        ENVIRONMENT=os.getenv('ENVIRONMENT', 'production'),
        LOG_LEVEL=os.getenv('LOG_LEVEL', 'INFO'),
        PUBSUB_TOPIC=os.getenv('PUBSUB_TOPIC'),
        ENABLE_MONITORING=os.getenv('ENABLE_MONITORING', 'true').lower() == 'true'
    )
    
    # Setup logging
    setup_logging(app)
    
    # Setup monitoring
    if app.config['ENABLE_MONITORING']:
        setup_monitoring(app)
    
    # Register routes
    register_routes(app)
    
    # Error handlers
    register_error_handlers(app)
    
    # Request logging middleware
    @app.before_request
    def log_request():
        """Log incoming requests with relevant metadata."""
        app.logger.info(
            f"Request: {request.method} {request.path}",
            extra={
                'method': request.method,
                'path': request.path,
                'remote_addr': request.remote_addr,
                'user_agent': str(request.user_agent),
                'scheduler_source': request.headers.get('X-Scheduler-Source'),
                'request_id': request.headers.get('X-Request-ID')
            }
        )
    
    @app.after_request
    def log_response(response):
        """Log response status and execution time."""
        app.logger.info(
            f"Response: {response.status_code}",
            extra={
                'status_code': response.status_code,
                'content_length': response.content_length,
                'method': request.method,
                'path': request.path
            }
        )
        return response
    
    return app


def register_error_handlers(app):
    """Register comprehensive error handlers."""
    
    @app.errorhandler(400)
    def bad_request(error):
        """Handle bad request errors."""
        app.logger.error(f"Bad request: {error.description}")
        return jsonify({
            'error': 'Bad Request',
            'message': error.description,
            'status': 400
        }), 400
    
    @app.errorhandler(401)
    def unauthorized(error):
        """Handle unauthorized access."""
        app.logger.error("Unauthorized access attempt")
        return jsonify({
            'error': 'Unauthorized',
            'message': 'Authentication required',
            'status': 401
        }), 401
    
    @app.errorhandler(403)
    def forbidden(error):
        """Handle forbidden access."""
        app.logger.error("Forbidden access attempt")
        return jsonify({
            'error': 'Forbidden',
            'message': 'Access denied',
            'status': 403
        }), 403
    
    @app.errorhandler(404)
    def not_found(error):
        """Handle not found errors."""
        app.logger.warning(f"Not found: {request.path}")
        return jsonify({
            'error': 'Not Found',
            'message': 'Endpoint not found',
            'status': 404
        }), 404
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        """Handle method not allowed errors."""
        app.logger.warning(f"Method not allowed: {request.method} {request.path}")
        return jsonify({
            'error': 'Method Not Allowed',
            'message': f'Method {request.method} not allowed for this endpoint',
            'status': 405
        }), 405
    
    @app.errorhandler(500)
    def internal_error(error):
        """Handle internal server errors."""
        app.logger.error(f"Internal server error: {str(error)}", exc_info=True)
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'An unexpected error occurred',
            'status': 500
        }), 500
    
    @app.errorhandler(Exception)
    def handle_unexpected_error(error):
        """Handle any unexpected errors."""
        app.logger.error(f"Unexpected error: {str(error)}", exc_info=True)
        
        # Publish error notification
        try:
            publish_error_notification(app, error)
        except Exception as pub_error:
            app.logger.error(f"Failed to publish error notification: {str(pub_error)}")
        
        return jsonify({
            'error': 'Unexpected Error',
            'message': 'An unexpected error occurred',
            'status': 500
        }), 500


def publish_error_notification(app, error):
    """Publish error notification to Pub/Sub."""
    if not app.config.get('PUBSUB_TOPIC'):
        return
    
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            app.config['PROJECT_ID'], 
            app.config['PUBSUB_TOPIC']
        )
        
        message_data = {
            'type': 'error',
            'service': 'toast-etl-pipeline',
            'error': str(error),
            'environment': app.config['ENVIRONMENT'],
            'timestamp': str(datetime.utcnow()),
            'severity': 'HIGH'
        }
        
        future = publisher.publish(
            topic_path, 
            data=json.dumps(message_data).encode('utf-8'),
            source='toast-etl-server',
            type='error'
        )
        
        app.logger.info(f"Error notification published with message ID: {future.result()}")
        
    except Exception as e:
        app.logger.error(f"Failed to publish error notification: {str(e)}")


# Create the application instance
app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False) 