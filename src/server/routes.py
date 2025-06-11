"""
Toast ETL Pipeline - HTTP Routes
Defines endpoints for Cloud Run deployment and Cloud Scheduler integration.
"""

import os
import json
import time
import asyncio
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, current_app
from functools import wraps
import traceback

# Import ETL components
# Note: These imports are commented out temporarily for server startup
# from ..transformers.toast_transformer import ToastDataTransformer
# from ..validators.data_validator import DataValidator
# from ..utils.monitoring import publish_notification, track_execution_metrics

# Import dashboard routes
from .dashboard_routes import dashboard_bp


def register_routes(app):
    """Register all routes with the Flask application."""
    
    # Register dashboard blueprint
    app.register_blueprint(dashboard_bp)
    
    # Health check endpoint
    @app.route('/health', methods=['GET'])
    def health():
        """Health check endpoint for Cloud Run probes."""
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'service': 'toast-etl-pipeline',
            'version': '1.0.0'
        }), 200
    
    # ETL execution endpoint
    @app.route('/execute', methods=['POST'])
    @require_authentication
    @track_execution_time
    def execute():
        """Execute the ETL pipeline."""
        execution_id = f"exec_{int(time.time())}"
        
        try:
            request_data = request.get_json() or {}
            execution_date = request_data.get('execution_date', datetime.utcnow().strftime('%Y-%m-%d'))
            
            current_app.logger.info(f"Starting ETL execution {execution_id}")
            
            # Mock execution for now - will be replaced with actual pipeline
            result = {
                'files_processed': 7,
                'records_processed': 1000,
                'validation_passed': True,
                'execution_time': 45.5
            }
            
            return jsonify({
                'status': 'success',
                'execution_id': execution_id,
                'execution_date': execution_date,
                'result': result,
                'timestamp': datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            current_app.logger.error(f"ETL execution failed: {str(e)}")
            return jsonify({
                'status': 'error',
                'execution_id': execution_id,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }), 500
    
    # Weekly validation endpoint
    @app.route('/validate-weekly', methods=['POST'])
    @require_authentication
    @track_execution_time
    def validate_weekly():
        """Perform comprehensive weekly validation."""
        validation_id = f"val_{int(time.time())}"
        
        try:
            request_data = request.get_json() or {}
            
            current_app.logger.info(f"Starting weekly validation {validation_id}")
            
            # Mock validation for now
            result = {
                'validation_type': 'comprehensive',
                'quality_score': 95.5,
                'issues_found': 2,
                'recommendations': ['Fix duplicate records', 'Update schema validation']
            }
            
            return jsonify({
                'status': 'success',
                'validation_id': validation_id,
                'result': result,
                'timestamp': datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            current_app.logger.error(f"Weekly validation failed: {str(e)}")
            return jsonify({
                'status': 'error',
                'validation_id': validation_id,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }), 500
    
    # Status endpoint
    @app.route('/status', methods=['GET'])
    def status():
        """Get service status."""
        return jsonify({
            'service': 'toast-etl-pipeline',
            'status': 'running',
            'environment': current_app.config.get('ENVIRONMENT', 'production'),
            'timestamp': datetime.utcnow().isoformat()
        }), 200


def require_authentication(f):
    """Decorator to require proper authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # In production, this would validate the OIDC token
        # For now, we'll check for the presence of required headers
        scheduler_source = request.headers.get('X-Scheduler-Source')
        if not scheduler_source:
            current_app.logger.warning("Request missing scheduler source header")
            return jsonify({'error': 'Authentication required'}), 401
        
        return f(*args, **kwargs)
    return decorated_function


def track_execution_time(f):
    """Decorator to track execution time and publish metrics."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        start_time = time.time()
        
        try:
            result = f(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Track success metrics
            # track_execution_metrics(
            #     endpoint=f.__name__,
            #     execution_time=execution_time,
            #     status='success'
            # )
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            # Track failure metrics
            # track_execution_metrics(
            #     endpoint=f.__name__,
            #     execution_time=execution_time,
            #     status='failure',
            #     error=str(e)
            # )
            
            raise
    
    return decorated_function


def execute_with_retry(pipeline, execution_date, enable_validation, quality_report, execution_id):
    """Execute pipeline with exponential backoff retry logic."""
    max_retries = 3
    base_delay = 30  # seconds
    max_delay = 600  # 10 minutes
    
    for attempt in range(max_retries + 1):
        try:
            current_app.logger.info(f"ETL execution attempt {attempt + 1}/{max_retries + 1}")
            
            # Execute the main ETL pipeline
            result = pipeline.run(
                execution_date=execution_date,
                enable_validation=enable_validation,
                quality_report=quality_report
            )
            
            return result
            
        except Exception as e:
            if attempt == max_retries:
                # Final attempt failed
                current_app.logger.error(f"ETL execution failed after {max_retries + 1} attempts")
                raise
            
            # Calculate delay with exponential backoff
            delay = min(base_delay * (2 ** attempt), max_delay)
            
            current_app.logger.warning(
                f"ETL execution attempt {attempt + 1} failed, retrying in {delay} seconds: {str(e)}"
            )
            
            time.sleep(delay)


def perform_weekly_validation(quality_checker, validation_type, date_range_days, deep_analysis, validation_id):
    """Perform comprehensive weekly validation with retry logic."""
    try:
        # Calculate date range
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=date_range_days)
        
        current_app.logger.info(
            f"Performing {validation_type} validation from {start_date} to {end_date}"
        )
        
        # Run validation based on type
        if validation_type == 'comprehensive':
            result = quality_checker.comprehensive_quality_check(
                start_date=start_date,
                end_date=end_date,
                deep_analysis=deep_analysis
            )
        else:
            result = quality_checker.basic_quality_check(
                start_date=start_date,
                end_date=end_date
            )
        
        return result
        
    except Exception as e:
        current_app.logger.error(f"Weekly validation failed: {str(e)}", exc_info=True)
        raise 