from google.cloud import bigquery

def run_query():
    client = bigquery.Client()
    query = """
    SELECT 
        server,
        COUNT(*) as order_count,
        ROUND(SUM(total), 2) as total_sales,
        ROUND(AVG(total), 2) as avg_order_value
    FROM `toast-analytics-444116.toast_analytics.order_details`
    WHERE server IS NOT NULL 
    AND server != '' 
    AND total > 0
    GROUP BY server
    ORDER BY total_sales DESC
    LIMIT 20
    """
    
    results = client.query(query).result()
    
    print('\n👨‍💼 TOP 20 SERVERS BY SALES:')
    print('-' * 80)
    print(f'{"Server":<20} {"Orders":>8} {"Total Sales":>15} {"Avg Order":>15}')
    print('-' * 80)
    
    for row in results:
        print(f'{row.server:<20} {row.order_count:>8,} ${row.total_sales:>13,.2f} ${row.avg_order_value:>13,.2f}')

if __name__ == '__main__':
    run_query() 