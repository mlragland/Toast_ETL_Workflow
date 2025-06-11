
# QA Strategy

## Scope
This strategy covers unit, integration, and system testing for the Toast ETL pipeline.

## Tools
- Pytest for unit testing
- Postman for API testing
- BigQuery SQL validation queries

## QA Scenarios
- [ ] SFTP file not found
- [ ] CSV with invalid headers
- [ ] Data duplication scenario
- [ ] Total sales mismatch
- [ ] API returns all recent runs

## Regression Criteria
- All ETL stages complete within SLA
- No duplicate rows in target tables
- No schema drift
