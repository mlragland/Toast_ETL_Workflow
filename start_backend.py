#!/usr/bin/env python3
"""
Simple Flask backend for Toast ETL Dashboard
Phase 6 Development Server
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
from google.cloud import bigquery

app = Flask(__name__)
CORS(app)  # Enable CORS for React development

# Configuration
PROJECT_ID = os.getenv('PROJECT_ID', 'toast-analytics-444116')
DATASET_ID = os.getenv('DATASET_ID', 'toast_analytics')

# Initialize BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'toast-etl-dashboard-api',
        'version': '1.0.0',
        'database': f'{PROJECT_ID}.{DATASET_ID}'
    }), 200

@app.route('/api/dashboard/summary', methods=['GET'])
def dashboard_summary():
    """Get dashboard summary statistics excluding closure records."""
    try:
        # Get table row counts
        tables = ['order_details', 'all_items_report', 'check_details', 
                 'cash_entries', 'item_selection_details', 'kitchen_timings', 'payment_details']
        
        table_stats = {}
        total_rows = 0
        
        for table_name in tables:
            try:
                table = bq_client.get_table(f'{PROJECT_ID}.{DATASET_ID}.{table_name}')
                rows = table.num_rows
                table_stats[table_name] = {
                    'rows': rows,
                    'size_mb': round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0
                }
                total_rows += rows
            except Exception:
                table_stats[table_name] = {'rows': 0, 'size_mb': 0}
        
        # Get business metrics if we have order data (excluding closure records)
        business_metrics = {}
        if table_stats['order_details']['rows'] > 0:
            query = f"""
            SELECT 
                COUNT(*) as total_orders,
                ROUND(SUM(total), 2) as total_sales,
                ROUND(AVG(total), 2) as avg_order_value,
                MIN(DATE(opened)) as earliest_date,
                MAX(DATE(opened)) as latest_date,
                COUNT(DISTINCT DATE(opened)) as unique_dates,
                COUNT(CASE WHEN closure_indicator = TRUE THEN 1 END) as closure_days
            FROM `{PROJECT_ID}.{DATASET_ID}.order_details`
            WHERE total IS NOT NULL 
            AND (closure_indicator IS NULL OR closure_indicator = FALSE)
            """
            
            result = bq_client.query(query).result()
            for row in result:
                business_metrics = {
                    'total_orders': row.total_orders,
                    'total_sales': row.total_sales,
                    'avg_order_value': row.avg_order_value,
                    'earliest_date': str(row.earliest_date) if row.earliest_date else None,
                    'latest_date': str(row.latest_date) if row.latest_date else None,
                    'unique_dates': row.unique_dates,
                    'closure_days': row.closure_days
                }
        
        return jsonify({
            'status': 'success',
            'data': {
                'total_rows': total_rows,
                'table_stats': table_stats,
                'business_metrics': business_metrics,
                'last_updated': datetime.utcnow().isoformat()
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get dashboard summary: {str(e)}'
        }), 500

@app.route('/api/orders/recent', methods=['GET'])
def recent_orders():
    """Get recent orders data excluding closure records."""
    try:
        limit = request.args.get('limit', 10, type=int)
        
        query = f"""
        SELECT 
            order_id,
            order_number,
            opened,
            server,
            total,
            revenue_center,
            service,
            guest_count
        FROM `{PROJECT_ID}.{DATASET_ID}.order_details`
        WHERE total > 0 
        AND (closure_indicator IS NULL OR closure_indicator = FALSE)
        ORDER BY opened DESC
        LIMIT {limit}
        """
        
        result = bq_client.query(query).result()
        orders = []
        
        for row in result:
            orders.append({
                'order_id': row.order_id,
                'order_number': row.order_number,
                'opened': str(row.opened),
                'server': row.server,
                'total': float(row.total) if row.total else 0,
                'revenue_center': row.revenue_center,
                'service': row.service,
                'guest_count': row.guest_count
            })
        
        return jsonify({
            'status': 'success',
            'data': orders
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get recent orders: {str(e)}'
        }), 500

@app.route('/api/analytics/sales-by-service', methods=['GET'])
def sales_by_service():
    """Get sales breakdown by service type excluding closure records."""
    try:
        query = f"""
        SELECT 
            service,
            COUNT(*) as order_count,
            ROUND(SUM(total), 2) as total_sales,
            ROUND(AVG(total), 2) as avg_order_value
        FROM `{PROJECT_ID}.{DATASET_ID}.order_details`
        WHERE service IS NOT NULL AND service != '' AND total > 0
        AND (closure_indicator IS NULL OR closure_indicator = FALSE)
        GROUP BY service
        ORDER BY total_sales DESC
        """
        
        result = bq_client.query(query).result()
        services = []
        
        for row in result:
            services.append({
                'service': row.service,
                'order_count': row.order_count,
                'total_sales': float(row.total_sales),
                'avg_order_value': float(row.avg_order_value)
            })
        
        return jsonify({
            'status': 'success',
            'data': services
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get sales by service: {str(e)}'
        }), 500

@app.route('/api/analytics/top-servers', methods=['GET'])
def top_servers():
    """Get top servers by sales excluding closure records."""
    try:
        limit = request.args.get('limit', 10, type=int)
        
        query = f"""
        SELECT 
            server,
            COUNT(*) as order_count,
            ROUND(SUM(total), 2) as total_sales,
            ROUND(AVG(total), 2) as avg_order_value
        FROM `{PROJECT_ID}.{DATASET_ID}.order_details`
        WHERE server IS NOT NULL AND server != '' AND total > 0
        AND (closure_indicator IS NULL OR closure_indicator = FALSE)
        GROUP BY server
        ORDER BY total_sales DESC
        LIMIT {limit}
        """
        
        result = bq_client.query(query).result()
        servers = []
        
        for row in result:
            servers.append({
                'server': row.server,
                'order_count': row.order_count,
                'total_sales': float(row.total_sales),
                'avg_order_value': float(row.avg_order_value)
            })
        
        return jsonify({
            'status': 'success',
            'data': servers
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get top servers: {str(e)}'
        }), 500

@app.route('/api/analytics/closure-summary', methods=['GET'])
def closure_summary():
    """Get business closure summary for operational insights."""
    try:
        query = f"""
        SELECT 
            processing_date,
            closure_reason,
            COUNT(*) as closure_records,
            COUNT(DISTINCT closure_reason) as unique_reasons
        FROM `{PROJECT_ID}.{DATASET_ID}.order_details`
        WHERE closure_indicator = TRUE
        GROUP BY processing_date, closure_reason
        ORDER BY processing_date DESC
        LIMIT 50
        """
        
        result = bq_client.query(query).result()
        closures = []
        
        for row in result:
            closures.append({
                'date': str(row.processing_date),
                'reason': row.closure_reason,
                'records': row.closure_records
            })
        
        # Get closure pattern analysis
        pattern_query = f"""
        SELECT 
            closure_reason,
            COUNT(DISTINCT processing_date) as closure_days,
            COUNT(*) as total_records
        FROM `{PROJECT_ID}.{DATASET_ID}.order_details`
        WHERE closure_indicator = TRUE
        GROUP BY closure_reason
        ORDER BY closure_days DESC
        """
        
        pattern_result = bq_client.query(pattern_query).result()
        patterns = []
        
        for row in pattern_result:
            patterns.append({
                'reason': row.closure_reason,
                'closure_days': row.closure_days,
                'total_records': row.total_records
            })
        
        return jsonify({
            'status': 'success',
            'data': {
                'recent_closures': closures,
                'closure_patterns': patterns
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get closure summary: {str(e)}'
        }), 500

@app.route('/api/analytics/daily-trends', methods=['GET'])
def daily_trends():
    """Get daily business trends excluding closure days."""
    try:
        days = request.args.get('days', 30, type=int)
        
        query = f"""
        SELECT 
            DATE(opened) as business_date,
            COUNT(*) as order_count,
            ROUND(SUM(total), 2) as daily_sales,
            ROUND(AVG(total), 2) as avg_order_value,
            COUNT(DISTINCT server) as active_servers,
            MIN(opened) as first_order,
            MAX(opened) as last_order
        FROM `{PROJECT_ID}.{DATASET_ID}.order_details`
        WHERE DATE(opened) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        AND total > 0
        AND (closure_indicator IS NULL OR closure_indicator = FALSE)
        GROUP BY DATE(opened)
        ORDER BY business_date DESC
        """
        
        result = bq_client.query(query).result()
        trends = []
        
        for row in result:
            trends.append({
                'date': str(row.business_date),
                'order_count': row.order_count,
                'daily_sales': float(row.daily_sales),
                'avg_order_value': float(row.avg_order_value),
                'active_servers': row.active_servers,
                'first_order': str(row.first_order),
                'last_order': str(row.last_order)
            })
        
        return jsonify({
            'status': 'success',
            'data': trends
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get daily trends: {str(e)}'
        }), 500

@app.route('/api/runs', methods=['GET'])
def etl_runs():
    """Get recent ETL run metadata."""
    try:
        # For now, return mock data since we don't have an ETL runs table yet
        # In a full implementation, this would query an etl_runs_log table
        runs = [
            {
                'run_id': 'etl_2024_06_11_04_30',
                'start_time': '2024-06-11T04:30:00Z',
                'end_time': '2024-06-11T04:45:00Z',
                'status': 'completed',
                'records_processed': 1234,
                'files_processed': ['order_details.csv', 'all_items_report.csv'],
                'duration_minutes': 15
            },
            {
                'run_id': 'etl_2024_06_10_04_30',
                'start_time': '2024-06-10T04:30:00Z',
                'end_time': '2024-06-10T04:42:00Z',
                'status': 'completed',
                'records_processed': 1187,
                'files_processed': ['order_details.csv', 'all_items_report.csv'],
                'duration_minutes': 12
            }
        ]
        
        return jsonify({
            'status': 'success',
            'data': runs
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get ETL runs: {str(e)}'
        }), 500

@app.route('/api/metrics', methods=['GET'])
def file_metrics():
    """Get file-level processing metrics."""
    try:
        # Get table statistics as file metrics
        tables = ['order_details', 'all_items_report', 'check_details', 
                 'cash_entries', 'item_selection_details', 'kitchen_timings', 'payment_details']
        
        metrics = []
        for table_name in tables:
            try:
                table = bq_client.get_table(f'{PROJECT_ID}.{DATASET_ID}.{table_name}')
                metrics.append({
                    'file_type': table_name,
                    'total_records': table.num_rows,
                    'size_mb': round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0,
                    'last_updated': str(table.modified) if table.modified else None,
                    'status': 'active' if table.num_rows > 0 else 'empty'
                })
            except Exception:
                metrics.append({
                    'file_type': table_name,
                    'total_records': 0,
                    'size_mb': 0,
                    'last_updated': None,
                    'status': 'not_found'
                })
        
        return jsonify({
            'status': 'success',
            'data': metrics
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get file metrics: {str(e)}'
        }), 500

@app.route('/api/backfill', methods=['POST'])
def trigger_backfill():
    """Trigger bulk re-ingestion for specified date range."""
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({
                'status': 'error',
                'message': 'start_date and end_date are required'
            }), 400
        
        # Create job ID
        job_id = f'backfill_{start_date}_{end_date}_{int(time.time())}'
        
        # TODO: In a production environment, this would be queued to a job processor
        # For now, we return a mock response that simulates job creation
        backfill_job = {
            'job_id': job_id,
            'status': 'queued',
            'start_date': start_date,
            'end_date': end_date,
            'created_at': datetime.utcnow().isoformat(),
            'message': f'Backfill job {job_id} queued for {start_date} to {end_date}. Use CLI for actual processing.'
        }
        
        logger.info(f"Backfill job created: {job_id} for {start_date} to {end_date}")
        
        return jsonify({
            'status': 'success',
            'data': backfill_job
        }), 202
        
    except Exception as e:
        logger.error(f"Backfill trigger failed: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to trigger backfill: {str(e)}'
        }), 500

@app.route('/api/backfill', methods=['GET'])
def get_backfill_jobs():
    """Get list of recent backfill jobs."""
    try:
        # Mock backfill jobs data
        # In a production environment, this would query a job tracking database
        mock_jobs = [
            {
                'job_id': 'backfill_20240607_20240609_1234567890',
                'status': 'completed',
                'start_date': '20240607',
                'end_date': '20240609',
                'created_at': '2024-06-10T10:30:00Z',
                'message': 'Successfully processed 3 dates with 152 records'
            },
            {
                'job_id': 'backfill_20240601_20240605_1234567891',
                'status': 'failed',
                'start_date': '20240601',
                'end_date': '20240605',
                'created_at': '2024-06-10T09:15:00Z',
                'message': 'Failed: SFTP connection timeout'
            },
            {
                'job_id': 'backfill_20240610_20240612_1234567892',
                'status': 'running',
                'start_date': '20240610',
                'end_date': '20240612',
                'created_at': '2024-06-10T11:00:00Z',
                'message': 'Processing... 2/3 dates completed'
            }
        ]
        
        return jsonify({
            'status': 'success',
            'data': mock_jobs
        }), 200
        
    except Exception as e:
        logger.error(f"Failed to get backfill jobs: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to get backfill jobs: {str(e)}'
        }), 500

@app.route('/', methods=['GET'])
def index():
    """Simple index page showing available endpoints."""
    html = """
    <h1>🍴 Toast ETL Dashboard API</h1>
    <h2>Available Endpoints:</h2>
    <ul>
        <li><a href="/health">/health</a> - Health check</li>
        <li><a href="/api/dashboard/summary">/api/dashboard/summary</a> - Dashboard summary</li>
        <li><a href="/api/orders/recent">/api/orders/recent</a> - Recent orders</li>
        <li><a href="/api/analytics/sales-by-service">/api/analytics/sales-by-service</a> - Sales by service</li>
        <li><a href="/api/analytics/top-servers">/api/analytics/top-servers</a> - Top servers</li>
        <li><a href="/api/runs">/api/runs</a> - ETL run metadata</li>
        <li><a href="/api/metrics">/api/metrics</a> - File processing metrics</li>
        <li>POST /api/backfill - Trigger bulk re-ingestion</li>
    </ul>
    <p><strong>React Frontend:</strong> <a href="http://localhost:3000">http://localhost:3000</a></p>
    """
    return render_template_string(html)

if __name__ == '__main__':
    print("🍴 Starting Toast ETL Dashboard API Server...")
    print(f"📊 Database: {PROJECT_ID}.{DATASET_ID}")
    print("🚀 Server starting on http://localhost:8080")
    print("⚛️  React frontend should be running on http://localhost:3000")
    print("-" * 60)
    
    app.run(host='0.0.0.0', port=8080, debug=True) 