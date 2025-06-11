
# Mock API Responses

## GET /runs
```json
[
  {
    "date": "2025-06-09",
    "status": "success",
    "total_rows": 23456,
    "total_sales": 11234.55,
    "filename": "AllItemsReport.csv"
  },
  {
    "date": "2025-06-08",
    "status": "error",
    "error_message": "File not found",
    "filename": "CheckDetails.csv"
  }
]
```

## POST /backfill
### Request
```json
{
  "date": "2025-05-01"
}
```

### Response
```json
{
  "status": "submitted",
  "message": "Backfill job triggered for 2025-05-01"
}
```

## GET /metrics?date=2025-06-09
```json
{
  "total_files": 7,
  "total_rows": 23456,
  "total_sales": 11234.55,
  "expected_sales": 11200.00,
  "sales_variance": 34.55
}
```
