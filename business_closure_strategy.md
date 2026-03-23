# 🍴 Toast ETL Business Closure Detection Strategy

## **Record-Count Based Closure Detection**

### **📊 Core Strategy**

Instead of maintaining holiday calendars, detect business closures based on **actual data volume**:

- **Low Record Count**: < 15 total records across all files
- **Missing Files**: < 5 of 7 expected CSV files  
- **Low Sales**: < $100 total sales (when available)
- **No Data**: Zero files or records found

### **✅ Benefits Over Holiday-Based Detection**

1. **Automatic Adaptation**: Works for any business without configuration
2. **Catches All Closures**: Holidays, emergencies, power outages, maintenance
3. **No Calendar Maintenance**: No need to update holiday lists
4. **Business-Specific**: Adapts to actual operating patterns
5. **Technical Issue Detection**: Distinguishes closure from data problems

---

## **🔧 Implementation Steps**

### **1. Configure Thresholds**

```python
# Recommended starting thresholds for medium restaurant
CLOSURE_THRESHOLDS = {
    'min_records_threshold': 15,    # At least 15 total records
    'min_files_threshold': 5,       # At least 5 of 7 files
    'min_sales_threshold': 100.0    # At least $100 in sales
}
```

### **2. Detection Logic**

```python
def detect_business_closure(date: str, file_analysis: dict) -> tuple[bool, str]:
    """
    Detect if date represents business closure.
    
    Returns: (is_closure, reason)
    """
    total_records = file_analysis.get('total_records', 0)
    files_found = file_analysis.get('files_found', 0)
    total_sales = file_analysis.get('total_sales', 0.0)
    
    # Check closure conditions
    if files_found == 0:
        return True, 'no_files'
    
    if total_records < CLOSURE_THRESHOLDS['min_records_threshold']:
        return True, 'low_activity'
    
    if files_found < CLOSURE_THRESHOLDS['min_files_threshold']:
        return True, 'insufficient_files'
    
    if total_sales > 0 and total_sales < CLOSURE_THRESHOLDS['min_sales_threshold']:
        return True, 'low_sales'
    
    return False, 'normal_operations'
```

### **3. Zero Record Generation**

For detected closures, generate one record per table:

```python
closure_record = {
    'order_id': 'CLOSURE_RECORD',
    'location': 'Business Closed',
    'total': 0.0,
    'processing_date': processing_date,
    'closure_indicator': True,
    'closure_reason': reason  # 'low_activity', 'no_files', etc.
}
```

---

## **📈 Backfill Integration**

### **Modified Backfill Process**

```python
def process_date_with_closure_detection(date: str):
    """Process a single date with closure detection."""
    
    # Step 1: Download and analyze files
    file_analysis = analyze_sftp_files(date)
    
    # Step 2: Check for business closure
    is_closure, reason = detect_business_closure(date, file_analysis)
    
    if is_closure:
        # Generate and load zero records
        closure_records = generate_closure_records(date, reason)
        load_closure_records_to_bigquery(closure_records)
        log_closure_detection(date, reason, file_analysis)
    else:
        # Process normal data
        process_normal_business_day(date, file_analysis)
```

### **Backfill Strategy Benefits**

1. **Complete Date Coverage**: Every date gets processed
2. **Clear Audit Trail**: Know exactly what happened each day
3. **Consistent Reporting**: No gaps in time series data
4. **Operational Insights**: Track closure patterns and reasons

---

## **🎯 Recommended Thresholds by Business Size**

### **Small Cafe (< 50 orders/day)**
```python
SMALL_CAFE_THRESHOLDS = {
    'min_records_threshold': 5,
    'min_files_threshold': 3,
    'min_sales_threshold': 25.0
}
```

### **Medium Restaurant (50-200 orders/day)**
```python
MEDIUM_RESTAURANT_THRESHOLDS = {
    'min_records_threshold': 15,
    'min_files_threshold': 5,
    'min_sales_threshold': 100.0
}
```

### **Large Restaurant (200+ orders/day)**
```python
LARGE_RESTAURANT_THRESHOLDS = {
    'min_records_threshold': 50,
    'min_files_threshold': 6,
    'min_sales_threshold': 300.0
}
```

---

## **📊 Dashboard Integration**

### **Business Metrics (Exclude Closures)**

```sql
-- Daily sales excluding closure days
SELECT 
    DATE(processing_date) as business_date,
    SUM(total) as daily_sales,
    COUNT(*) as order_count
FROM order_details 
WHERE (closure_indicator IS NULL OR closure_indicator = FALSE)
GROUP BY DATE(processing_date)
ORDER BY business_date
```

### **Closure Calendar View**

```sql
-- Show closure dates and reasons
SELECT 
    processing_date,
    closure_reason,
    COUNT(*) as tables_affected
FROM order_details 
WHERE closure_indicator = TRUE
GROUP BY processing_date, closure_reason
ORDER BY processing_date DESC
```

### **Operational Insights**

```sql
-- Closure pattern analysis
SELECT 
    EXTRACT(DAYOFWEEK FROM DATE(processing_date)) as day_of_week,
    closure_reason,
    COUNT(*) as closure_count
FROM order_details 
WHERE closure_indicator = TRUE
GROUP BY day_of_week, closure_reason
ORDER BY day_of_week, closure_count DESC
```

---

## **🔄 Threshold Tuning Process**

### **1. Start Conservative**
Begin with lower thresholds to avoid missing actual closures:
```python
INITIAL_THRESHOLDS = {
    'min_records_threshold': 10,
    'min_files_threshold': 4,
    'min_sales_threshold': 50.0
}
```

### **2. Monitor Detection Accuracy**
Track false positives and negatives:
```sql
-- Review detected closures for validation
SELECT 
    processing_date,
    closure_reason,
    -- Add manual validation column
    actual_closure_status
FROM closure_detection_log
WHERE detection_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
```

### **3. Adjust Based on Patterns**
- **Too many false positives**: Increase thresholds
- **Missing actual closures**: Decrease thresholds
- **Seasonal variations**: Consider time-based adjustments

---

## **🚀 Implementation Checklist**

- [ ] **Configure thresholds** based on business size
- [ ] **Update BigQuery schemas** to include `closure_indicator` and `closure_reason`
- [ ] **Modify backfill logic** to detect and handle closures
- [ ] **Update dashboard queries** to exclude closure records from metrics
- [ ] **Add closure reporting** for operational insights
- [ ] **Test with historical data** to validate detection accuracy
- [ ] **Monitor and tune** thresholds based on results
- [ ] **Document closure reasons** for business stakeholders

---

## **📝 Example Scenarios**

| Date | Records | Files | Sales | Detection | Reason |
|------|---------|-------|-------|-----------|---------|
| 2024-12-25 | 0 | 0 | $0 | 🔴 Closure | no_files |
| 2024-12-24 | 8 | 6 | $45 | 🔴 Closure | low_activity |
| 2024-12-23 | 245 | 7 | $1,250 | 🟢 Normal | normal_operations |
| 2024-01-01 | 3 | 2 | $15 | 🔴 Closure | low_activity |
| 2024-07-04 | 187 | 7 | $980 | 🟢 Normal | normal_operations |

**Key Insight**: The business operates on July 4th (Independence Day) but closes on Christmas and New Year's Day. The system automatically detects this without manual configuration.

---

This strategy provides **robust, data-driven closure detection** that adapts to your actual business patterns while ensuring **complete reporting consistency** and **operational insights**. 