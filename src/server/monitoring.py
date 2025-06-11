"""
Toast ETL Pipeline - Monitoring Utilities
Integration with Google Cloud Monitoring for metrics and alerting.
"""

import os
import time
from datetime import datetime
from google.cloud import monitoring_v3


def setup_monitoring(app):
    """Setup Google Cloud Monitoring for the application."""
    
    try:
        if app.config.get('ENVIRONMENT') == 'production':
            # Initialize monitoring client
            client = monitoring_v3.MetricServiceClient()
            project_name = f"projects/{app.config.get('PROJECT_ID')}"
            
            app.logger.info("Google Cloud Monitoring configured")
        else:
            app.logger.info("Monitoring disabled for non-production environment")
            
    except Exception as e:
        app.logger.error(f"Failed to setup monitoring: {str(e)}")


def create_custom_metric(project_id, metric_type, display_name, description):
    """Create a custom metric in Google Cloud Monitoring."""
    
    try:
        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{project_id}"
        
        descriptor = monitoring_v3.MetricDescriptor()
        descriptor.type = f"custom.googleapis.com/{metric_type}"
        descriptor.metric_kind = monitoring_v3.MetricDescriptor.MetricKind.GAUGE
        descriptor.value_type = monitoring_v3.MetricDescriptor.ValueType.DOUBLE
        descriptor.display_name = display_name
        descriptor.description = description
        
        descriptor = client.create_metric_descriptor(
            name=project_name, metric_descriptor=descriptor
        )
        
        return descriptor
        
    except Exception as e:
        print(f"Failed to create custom metric: {str(e)}")
        return None


def write_time_series_data(project_id, metric_type, value, labels=None):
    """Write time series data to Google Cloud Monitoring."""
    
    try:
        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{project_id}"
        
        series = monitoring_v3.TimeSeries()
        series.metric.type = f"custom.googleapis.com/{metric_type}"
        
        # Add labels if provided
        if labels:
            for key, value in labels.items():
                series.metric.labels[key] = str(value)
        
        series.resource.type = "gce_instance"
        series.resource.labels["instance_id"] = "1234567890123456789"
        series.resource.labels["zone"] = "us-central1-a"
        
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": seconds, "nanos": nanos}}
        )
        point = monitoring_v3.Point(
            {"interval": interval, "value": {"double_value": value}}
        )
        series.points = [point]
        
        client.create_time_series(name=project_name, time_series=[series])
        
    except Exception as e:
        print(f"Failed to write time series data: {str(e)}")


class ETLMetrics:
    """Class to manage ETL pipeline metrics."""
    
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = monitoring_v3.MetricServiceClient() if project_id else None
    
    def record_execution_time(self, execution_time, status='success'):
        """Record ETL execution time."""
        if not self.client:
            return
        
        write_time_series_data(
            self.project_id,
            'etl_execution_time',
            execution_time,
            {'status': status}
        )
    
    def record_file_processing_count(self, file_count):
        """Record number of files processed."""
        if not self.client:
            return
        
        write_time_series_data(
            self.project_id,
            'etl_files_processed',
            file_count
        )
    
    def record_record_count(self, record_count, file_type):
        """Record number of records processed by file type."""
        if not self.client:
            return
        
        write_time_series_data(
            self.project_id,
            'etl_records_processed',
            record_count,
            {'file_type': file_type}
        )
    
    def record_validation_score(self, score, validation_type):
        """Record data validation score."""
        if not self.client:
            return
        
        write_time_series_data(
            self.project_id,
            'etl_validation_score',
            score,
            {'validation_type': validation_type}
        ) 