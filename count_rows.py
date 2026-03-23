from google.cloud import bigquery

def count_rows():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*) as total_rows
    FROM `toast-analytics-444116.toast_analytics.order_details`
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        print(f"Total rows in order_details table: {row.total_rows:,}")

if __name__ == "__main__":
    count_rows() 