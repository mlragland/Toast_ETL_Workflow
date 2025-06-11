#!/usr/bin/env python3
"""
Toast ETL Database Summary
Comprehensive analysis of current database state with correct column names.
"""

import os
from datetime import datetime
from google.cloud import bigquery

def get_database_summary():
    """Get comprehensive database summary with business metrics."""
    
    # Configuration
    project_id = os.getenv('PROJECT_ID', 'toast-analytics-444116')
    dataset_id = os.getenv('DATASET_ID', 'toast_analytics')
    
    print("🍴 Toast ETL Database Summary")
    print("=" * 70)
    print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🗄️  Database: {project_id}.{dataset_id}")
    print("=" * 70)
    
    client = bigquery.Client(project=project_id)
    
    # Table row counts
    tables = [
        'all_items_report', 'check_details', 'cash_entries',
        'item_selection_details', 'kitchen_timings', 
        'order_details', 'payment_details'
    ]
    
    print("\n📊 TABLE ROW COUNTS:")
    print("-" * 40)
    total_rows = 0
    
    for table_name in tables:
        try:
            table = client.get_table(f'{project_id}.{dataset_id}.{table_name}')
            rows = table.num_rows
            size_mb = round(table.num_bytes / (1024 * 1024), 2) if table.num_bytes else 0
            total_rows += rows
            
            if rows > 0:
                print(f"✅ {table_name:<20} {rows:>8,} rows ({size_mb} MB)")
            else:
                print(f"⭕ {table_name:<20} {rows:>8,} rows (empty)")
                
        except Exception as e:
            print(f"❌ {table_name:<20} Error: {str(e)}")
    
    print("-" * 40)
    print(f"📈 TOTAL ROWS: {total_rows:,}")
    
    # Detailed order analysis
    if total_rows > 0:
        print("\n💰 BUSINESS METRICS (Order Details):")
        print("-" * 50)
        
        try:
            # Date range analysis
            date_query = f"""
            SELECT 
                MIN(DATE(opened)) as earliest_date,
                MAX(DATE(opened)) as latest_date,
                COUNT(DISTINCT DATE(opened)) as unique_dates,
                COUNT(*) as total_orders
            FROM `{project_id}.{dataset_id}.order_details`
            WHERE opened IS NOT NULL AND opened != ''
            """
            
            result = client.query(date_query).result()
            for row in result:
                print(f"📅 Date Range: {row.earliest_date} to {row.latest_date}")
                print(f"📊 Unique Dates: {row.unique_dates}")
                print(f"🛒 Total Orders: {row.total_orders:,}")
                
                if row.latest_date:
                    days_ago = (datetime.now().date() - row.latest_date).days
                    print(f"⏰ Latest Data: {days_ago} days ago")
            
            # Sales metrics
            sales_query = f"""
            SELECT 
                COUNT(*) as order_count,
                ROUND(SUM(total), 2) as total_sales,
                ROUND(AVG(total), 2) as avg_order_value,
                ROUND(MIN(total), 2) as min_order,
                ROUND(MAX(total), 2) as max_order,
                COUNT(CASE WHEN voided = true THEN 1 END) as voided_orders,
                COUNT(DISTINCT server) as unique_servers,
                COUNT(DISTINCT DATE(opened)) as business_days
            FROM `{project_id}.{dataset_id}.order_details`
            WHERE total IS NOT NULL
            """
            
            result = client.query(sales_query).result()
            for row in result:
                print(f"💵 Total Sales: ${row.total_sales:,.2f}")
                print(f"📊 Avg Order Value: ${row.avg_order_value:.2f}")
                print(f"📉 Min Order: ${row.min_order:.2f}")
                print(f"📈 Max Order: ${row.max_order:.2f}")
                print(f"❌ Voided Orders: {row.voided_orders:,}")
                print(f"👨‍💼 Unique Servers: {row.unique_servers}")
                print(f"📅 Business Days: {row.business_days}")
                
                if row.business_days > 0:
                    daily_avg = row.total_sales / row.business_days
                    print(f"📈 Daily Avg Sales: ${daily_avg:,.2f}")
            
            # Service type breakdown
            print(f"\n🍽️  SERVICE TYPE BREAKDOWN:")
            print("-" * 30)
            
            service_query = f"""
            SELECT 
                service,
                COUNT(*) as order_count,
                ROUND(SUM(total), 2) as total_sales,
                ROUND(AVG(total), 2) as avg_order_value
            FROM `{project_id}.{dataset_id}.order_details`
            WHERE service IS NOT NULL AND service != ''
            GROUP BY service
            ORDER BY total_sales DESC
            """
            
            result = client.query(service_query).result()
            for row in result:
                print(f"{row.service:<12} {row.order_count:>4} orders | ${row.total_sales:>8,.2f} | Avg: ${row.avg_order_value:.2f}")
            
            # Revenue center breakdown
            print(f"\n🏢 REVENUE CENTER BREAKDOWN:")
            print("-" * 35)
            
            revenue_query = f"""
            SELECT 
                revenue_center,
                COUNT(*) as order_count,
                ROUND(SUM(total), 2) as total_sales
            FROM `{project_id}.{dataset_id}.order_details`
            WHERE revenue_center IS NOT NULL AND revenue_center != ''
            GROUP BY revenue_center
            ORDER BY total_sales DESC
            """
            
            result = client.query(revenue_query).result()
            for row in result:
                print(f"{row.revenue_center:<20} {row.order_count:>4} orders | ${row.total_sales:>8,.2f}")
            
            # Top servers
            print(f"\n👨‍💼 TOP SERVERS BY SALES:")
            print("-" * 30)
            
            server_query = f"""
            SELECT 
                server,
                COUNT(*) as order_count,
                ROUND(SUM(total), 2) as total_sales,
                ROUND(AVG(total), 2) as avg_order_value
            FROM `{project_id}.{dataset_id}.order_details`
            WHERE server IS NOT NULL AND server != '' AND total > 0
            GROUP BY server
            ORDER BY total_sales DESC
            LIMIT 5
            """
            
            result = client.query(server_query).result()
            for row in result:
                print(f"{row.server:<15} {row.order_count:>3} orders | ${row.total_sales:>8,.2f} | Avg: ${row.avg_order_value:.2f}")
                
        except Exception as e:
            print(f"❌ Error analyzing business metrics: {str(e)}")
    
    # Data quality assessment
    print(f"\n🎯 DATA QUALITY ASSESSMENT:")
    print("-" * 40)
    
    tables_with_data = 0
    for table_name in tables:
        try:
            table = client.get_table(f'{project_id}.{dataset_id}.{table_name}')
            if table.num_rows > 0:
                tables_with_data += 1
        except:
            pass
    
    coverage = (tables_with_data / len(tables)) * 100
    print(f"📋 Tables with Data: {tables_with_data}/{len(tables)} ({coverage:.0f}%)")
    
    if total_rows == 0:
        print("🚨 Status: Database is empty")
        print("💡 Action: Run ETL pipeline to load data")
    elif total_rows < 500:
        print("⚠️  Status: Very limited data")
        print("💡 Action: Consider historical backfill")
    elif total_rows < 2000:
        print("✅ Status: Good development dataset")
        print("💡 Action: Ready for dashboard development")
    else:
        print("🎉 Status: Substantial production dataset")
        print("💡 Action: Ready for full analytics")
    
    print("\n" + "=" * 70)
    print(f"📊 SUMMARY: {total_rows:,} total rows across {tables_with_data} active tables")
    print("🚀 Database ready for Toast ETL Dashboard development!")
    print("=" * 70)

if __name__ == "__main__":
    get_database_summary() 