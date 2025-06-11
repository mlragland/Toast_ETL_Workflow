
# API Contract: ETL Dashboard Backend

## Endpoint: /runs
- **Method**: GET
- **Description**: Fetch list of ETL runs
- **Response**:
  ```json
  [
    {
      "date": "2025-06-08",
      "status": "success",
      "total_rows": 32112,
      "total_sales": 13123.55
    }
  ]
  ```

## Endpoint: /backfill
- **Method**: POST
- **Body**:
  ```json
  { "date": "2025-05-01" }
  ```
- **Response**: `200 OK` or `400 Bad Request`
