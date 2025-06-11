"""
Dashboard API Routes - Toast ETL Pipeline
RESTful endpoints for dashboard functionality and data visualization.
"""

import os
import json
import time
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, current_app
from functools import wraps
from google.cloud import bigquery
import uuid

# Create dashboard blueprint
dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

def get_bigquery_client():
    """Get BigQuery client instance."""
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    return bigquery.Client(project=project_id)

def get_dataset_info():
    """Get project and dataset information."""
    return {
        'project_id': os.getenv('PROJECT_ID', 'toast-analytics-444116'),
        'dataset_id': os.getenv('DATASET_ID', 'toast_analytics')
    }

@dashboard_bp.route('/overview', methods=['GET'])
def get_overview():
    """Get dashboard overview with key metrics and recent runs."""
    try:
        client = get_bigquery_client()
        info = get_dataset_info()
        
        # Get current data stats from order_details
        data_stats_query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT PARSE_DATE('%Y-%m-%d', processing_date)) as unique_days,
            MIN(PARSE_DATE('%Y-%m-%d', processing_date)) as earliest_date,
            MAX(PARSE_DATE('%Y-%m-%d', processing_date)) as latest_date,
            SUM(total) as total_sales_in_db
        FROM `{info['project_id']}.{info['dataset_id']}.order_details`
        """
        
        # Execute queries
        data_stats_result = list(client.query(data_stats_query).result())
        
        # Process results
        data_stats = data_stats_result[0] if data_stats_result else {
            'total_records': 0,
            'unique_days': 0,
            'earliest_date': None,
            'latest_date': None,
            'total_sales_in_db': 0
        }
        
        return jsonify({
            'status': 'success',
            'data': {
                'summary': {
                    'total_runs': 0,  # Will be populated when ETL runs exist
                    'success_rate': 100.0,
                    'failed_runs': 0,
                    'avg_execution_time': 0,
                    'last_run_time': None
                },
                'database_stats': {
                    'total_records': data_stats['total_records'],
                    'unique_days': data_stats['unique_days'],
                    'date_range': {
                        'start': data_stats['earliest_date'].isoformat() if data_stats['earliest_date'] else None,
                        'end': data_stats['latest_date'].isoformat() if data_stats['latest_date'] else None
                    },
                    'total_sales': round(data_stats['total_sales_in_db'] or 0, 2)
                },
                'latest_run': {
                    'run_id': None,
                    'date': None,
                    'status': 'no_runs',
                    'records': 0,
                    'sales': 0,
                    'completed_at': None,
                    'error': None
                }
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Dashboard overview failed: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@dashboard_bp.route('/runs', methods=['GET'])
def get_runs():
    """Get paginated ETL run history with filtering."""
    try:
        client = get_bigquery_client()
        info = get_dataset_info()
        
        # Get query parameters
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 20)), 100)  # Cap at 100
        status_filter = request.args.get('status')  # 'success', 'failed', 'running'
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        
        # Build query conditions
        conditions = []
        if status_filter:
            conditions.append(f"status = '{status_filter}'")
        if date_from:
            conditions.append(f"execution_date >= '{date_from}'")
        if date_to:
            conditions.append(f"execution_date <= '{date_to}'")
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        # Get total count
        count_query = f"""
        SELECT COUNT(*) as total
        FROM `{info['project_id']}.{info['dataset_id']}.etl_runs`
        {where_clause}
        """
        
        # Get paginated runs
        offset = (page - 1) * per_page
        runs_query = f"""
        SELECT 
            run_id,
            execution_date,
            started_at,
            completed_at,
            status,
            files_processed,
            records_processed,
            total_sales,
            execution_time_seconds,
            source_type,
            error_message,
            DATETIME_DIFF(completed_at, started_at, SECOND) as duration_seconds
        FROM `{info['project_id']}.{info['dataset_id']}.etl_runs`
        {where_clause}
        ORDER BY started_at DESC
        LIMIT {per_page} OFFSET {offset}
        """
        
        try:
            total_result = list(client.query(count_query).result())
            runs_result = list(client.query(runs_query).result())
            
            total_count = total_result[0]['total'] if total_result else 0
            
            runs = []
            for row in runs_result:
                runs.append({
                    'run_id': row['run_id'],
                    'execution_date': row['execution_date'].isoformat(),
                    'started_at': row['started_at'].isoformat(),
                    'completed_at': row['completed_at'].isoformat() if row['completed_at'] else None,
                    'status': row['status'],
                    'files_processed': row['files_processed'],
                    'records_processed': row['records_processed'],
                    'total_sales': row['total_sales'],
                    'execution_time': row['execution_time_seconds'],
                    'source_type': row['source_type'],
                    'error_message': row['error_message'],
                    'duration_seconds': row['duration_seconds']
                })
            
        except Exception as e:
            current_app.logger.warning(f"Runs query failed, returning empty: {str(e)}")
            total_count = 0
            runs = []
        
        return jsonify({
            'status': 'success',
            'data': {
                'runs': runs,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total_count,
                    'pages': (total_count + per_page - 1) // per_page
                }
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Dashboard runs failed: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@dashboard_bp.route('/metrics', methods=['GET'])
def get_metrics():
    """Get aggregated metrics for charts and analytics."""
    try:
        client = get_bigquery_client()
        info = get_dataset_info()
        
        # Get daily trends (last 30 days)
        daily_trends_query = f"""
        SELECT 
            execution_date,
            runs_count,
            successful_runs,
            failed_runs,
            avg_execution_time,
            total_records,
            total_sales
        FROM `{info['project_id']}.{info['dataset_id']}.daily_summary`
        WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ORDER BY execution_date ASC
        """
        
        # Get business metrics from order_details
        business_metrics_query = f"""
        SELECT 
            DATE(created_date) as order_date,
            COUNT(*) as order_count,
            SUM(CAST(REPLACE(total_price, '$', '') AS FLOAT64)) as daily_sales,
            AVG(CAST(REPLACE(total_price, '$', '') AS FLOAT64)) as avg_order_value
        FROM `{info['project_id']}.{info['dataset_id']}.order_details`
        WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY DATE(created_date)
        ORDER BY order_date ASC
        """
        
        try:
            daily_trends_result = list(client.query(daily_trends_query).result())
            business_metrics_result = list(client.query(business_metrics_query).result())
            
            # Process daily trends
            daily_trends = []
            for row in daily_trends_result:
                daily_trends.append({
                    'date': row['execution_date'].isoformat(),
                    'runs_count': row['runs_count'],
                    'successful_runs': row['successful_runs'],
                    'failed_runs': row['failed_runs'],
                    'avg_execution_time': row['avg_execution_time'],
                    'total_records': row['total_records'],
                    'total_sales': row['total_sales']
                })
            
            # Process business metrics
            business_metrics = []
            for row in business_metrics_result:
                business_metrics.append({
                    'date': row['order_date'].isoformat(),
                    'order_count': row['order_count'],
                    'daily_sales': round(row['daily_sales'], 2),
                    'avg_order_value': round(row['avg_order_value'], 2)
                })
            
        except Exception as e:
            current_app.logger.warning(f"Metrics queries failed, using defaults: {str(e)}")
            daily_trends = []
            business_metrics = []
        
        return jsonify({
            'status': 'success',
            'data': {
                'daily_trends': daily_trends,
                'business_metrics': business_metrics
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Dashboard metrics failed: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@dashboard_bp.route('/backfill', methods=['POST'])
def trigger_backfill():
    """Trigger a backfill job for historical data."""
    try:
        data = request.get_json()
        date_start = data.get('date_start')
        date_end = data.get('date_end')
        requested_by = data.get('requested_by', 'dashboard')
        
        if not date_start or not date_end:
            return jsonify({
                'status': 'error',
                'error': 'date_start and date_end are required'
            }), 400
        
        # Generate job ID
        job_id = f"backfill_{int(time.time())}_{str(uuid.uuid4())[:8]}"
        
        # Insert backfill job record
        client = get_bigquery_client()
        info = get_dataset_info()
        
        insert_query = f"""
        INSERT INTO `{info['project_id']}.{info['dataset_id']}.backfill_jobs`
        (job_id, requested_at, date_range_start, date_range_end, status, progress_percentage, dates_processed, total_dates, requested_by)
        VALUES 
        ('{job_id}', CURRENT_TIMESTAMP(), '{date_start}', '{date_end}', 'queued', 0.0, 0, 0, '{requested_by}')
        """
        
        client.query(insert_query).result()
        
        # TODO: Trigger actual backfill process (integrate with existing backfill system)
        current_app.logger.info(f"Backfill job {job_id} created for {date_start} to {date_end}")
        
        return jsonify({
            'status': 'success',
            'data': {
                'job_id': job_id,
                'date_range': {
                    'start': date_start,
                    'end': date_end
                },
                'status': 'queued'
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 201
        
    except Exception as e:
        current_app.logger.error(f"Backfill trigger failed: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@dashboard_bp.route('/backfill/status', methods=['GET'])
def get_backfill_status():
    """Get current backfill job status and progress."""
    try:
        client = get_bigquery_client()
        info = get_dataset_info()
        
        # Get recent backfill jobs
        backfill_query = f"""
        SELECT 
            job_id,
            requested_at,
            date_range_start,
            date_range_end,
            status,
            progress_percentage,
            dates_processed,
            total_dates,
            completed_at,
            error_message,
            records_added,
            requested_by
        FROM `{info['project_id']}.{info['dataset_id']}.backfill_jobs`
        ORDER BY requested_at DESC
        LIMIT 10
        """
        
        try:
            backfill_result = list(client.query(backfill_query).result())
            
            jobs = []
            for row in backfill_result:
                jobs.append({
                    'job_id': row['job_id'],
                    'requested_at': row['requested_at'].isoformat(),
                    'date_range': {
                        'start': row['date_range_start'].isoformat(),
                        'end': row['date_range_end'].isoformat()
                    },
                    'status': row['status'],
                    'progress_percentage': row['progress_percentage'],
                    'dates_processed': row['dates_processed'],
                    'total_dates': row['total_dates'],
                    'completed_at': row['completed_at'].isoformat() if row['completed_at'] else None,
                    'error_message': row['error_message'],
                    'records_added': row['records_added'],
                    'requested_by': row['requested_by']
                })
            
        except Exception as e:
            current_app.logger.warning(f"Backfill status query failed: {str(e)}")
            jobs = []
        
        return jsonify({
            'status': 'success',
            'data': {
                'jobs': jobs
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Backfill status failed: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@dashboard_bp.route('/data/summary', methods=['GET'])
def get_data_summary():
    """Get business data summary and insights."""
    try:
        client = get_bigquery_client()
        info = get_dataset_info()
        
        # Get comprehensive data summary
        summary_query = f"""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT order_id) as unique_orders,
            COUNT(DISTINCT PARSE_DATE('%Y-%m-%d', processing_date)) as unique_days,
            SUM(total) as total_revenue,
            AVG(total) as avg_order_value,
            MIN(PARSE_DATE('%Y-%m-%d', processing_date)) as earliest_order,
            MAX(PARSE_DATE('%Y-%m-%d', processing_date)) as latest_order
        FROM `{info['project_id']}.{info['dataset_id']}.order_details`
        """
        
        summary_result = list(client.query(summary_query).result())
        summary = summary_result[0] if summary_result else {}
        
        return jsonify({
            'status': 'success',
            'data': {
                'summary': {
                    'total_orders': summary.get('total_orders', 0),
                    'unique_orders': summary.get('unique_orders', 0),
                    'unique_days': summary.get('unique_days', 0),
                    'total_revenue': round(summary.get('total_revenue', 0), 2),
                    'avg_order_value': round(summary.get('avg_order_value', 0), 2),
                    'date_range': {
                        'start': summary['earliest_order'].isoformat() if summary.get('earliest_order') else None,
                        'end': summary['latest_order'].isoformat() if summary.get('latest_order') else None
                    }
                }
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Data summary failed: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500 