# 🍴 Toast ETL Transformation Requirements Analysis

Based on examination of legacy scripts in `/legacy_scripts` folder, this document outlines the comprehensive transformation requirements and schema mappings for each Toast CSV file.

## Overview of Transformation Requirements

The Toast ETL pipeline processes 7 CSV files exported from Toast POS system. Each file requires specific column mapping, data type conversions, and cleaning operations.

### Key Transformation Patterns

1. **Column Name Sanitization**: Original Toast CSV headers contain special characters (parentheses, slashes, spaces) that are invalid in BigQuery
2. **Date/Time Processing**: Multiple date/time formats need standardization
3. **String Type Preservation**: Certain ID columns need explicit string handling to prevent scientific notation
4. **Special Value Conversions**: Kitchen timing data requires custom parsing
5. **Missing Value Handling**: All null values filled with empty strings

---

## 📊 File-by-File Transformation Analysis

### 1. AllItemsReport.csv

**Purpose**: Menu item sales analysis and performance metrics

**Original → Cleaned Column Mappings**:
```
"Master ID" → "master_id"                           # ⚠️ PROBLEMATIC: Contains spaces and parentheses
"Item ID" → "item_id"
"Parent ID" → "parent_id"
"Menu Name" → "menu_name"
"Menu Group" → "menu_group"
"Subgroup" → "subgroup"
"Menu Item" → "menu_item"
"Tags" → "tags"
"Avg Price" → "avg_price"
"Item Qty (incl voids)" → "item_qty_incl_voids"    # ⚠️ PROBLEMATIC: Contains parentheses
"% of Ttl Qty (incl voids)" → "percent_ttl_qty_incl_voids"
"Gross Amount (incl voids)" → "gross_amount_incl_voids"
"% of Ttl Amt (incl voids)" → "percent_ttl_amt_incl_voids"
"Item Qty" → "item_qty"
"Gross Amount" → "gross_amount"
"Void Qty" → "void_qty"
"Void Amount" → "void_amount"
"Discount Amount" → "discount_amount"
"Net Amount" → "net_amount"
"# Orders" → "num_orders"
"% of Ttl # Orders" → "percent_ttl_num_orders"
"% Qty (Group)" → "percent_qty_group"
"% Qty (Menu)" → "percent_qty_menu"
"% Qty (All)" → "percent_qty_all"
"% Net Amt (Group)" → "percent_net_amt_group"
"% Net Amt (Menu)" → "percent_net_amt_menu"
"% Net Amt (All)" → "percent_net_amt_all"
```

**Special Processing**:
- Force `master_id`, `item_id`, `parent_id` to string type to prevent scientific notation
- Add `processing_date` column (DATE)

**BigQuery Schema**:
- 27 columns total (26 data + 1 processing_date)
- Mix of STRING, FLOAT, INTEGER, DATE types
- All columns NULLABLE

---

### 2. CheckDetails.csv

**Purpose**: Individual check/receipt details and customer information

**Original → Cleaned Column Mappings**:
```
"Customer Id" → "customer_id"
"Customer" → "customer"
"Customer Phone" → "customer_phone"
"Customer Email" → "customer_email"
"Location Code" → "location_code"
"Opened Date" → "opened_date"
"Opened Time" → "opened_time"
"Item Description" → "item_description"
"Server" → "server"
"Tax" → "tax"
"Tender" → "tender"
"Check Id" → "check_id"
"Check #" → "check_number"
"Total" → "total"
"Customer Family" → "customer_family"
"Table Size" → "table_size"
"Discount" → "discount"
"Reason of Discount" → "reason_of_discount"
"Link" → "link"
```

**Special Processing**:
- `opened_date` → DATE format (YYYY-MM-DD)
- `opened_time` → TIME format (HH:MM:SS)
- Add `processing_date` column (DATE)

**BigQuery Schema**:
- 20 columns total (19 data + 1 processing_date)
- Mix of STRING, FLOAT, INTEGER, DATE, TIME types

---

### 3. CashEntries.csv

**Purpose**: Cash drawer transactions and employee actions

**Original → Cleaned Column Mappings**:
```
"Location" → "location"
"Entry Id" → "entry_id"                             # REQUIRED field
"Created Date" → "created_date"
"Action" → "action"
"Amount" → "amount"
"Cash Drawer" → "cash_drawer"
"Payout Reason" → "payout_reason"
"No Sale Reason" → "no_sale_reason"
"Comment" → "comment"
"Employee" → "employee"
"Employee 2" → "employee_2"
```

**Special Processing**:
- `created_date` → DATETIME format (YYYY-MM-DD HH:MM:SS)
- `entry_id` is REQUIRED (only non-nullable field)
- Add `processing_date` column (DATE)

**BigQuery Schema**:
- 12 columns total (11 data + 1 processing_date)
- Mix of STRING, FLOAT, DATETIME, DATE types

---

### 4. ItemSelectionDetails.csv

**Purpose**: Individual item selections within orders

**Original → Cleaned Column Mappings**:
```
"Location" → "location"
"Order Id" → "order_id"
"Order #" → "order_number"
"Sent Date" → "sent_date"
"Order Date" → "order_date"
"Check Id" → "check_id"
"Server" → "server"
"Table" → "table"
"Dining Area" → "dining_area"
"Service" → "service"
"Dining Option" → "dining_option"
"Item Selection Id" → "item_selection_id"
"Item Id" → "item_id"
"Master Id" → "master_id"
"SKU" → "sku"
"PLU" → "plu"
"Menu Item" → "menu_item"
"Menu Subgroup(s)" → "menu_subgroup"                # ⚠️ PROBLEMATIC: Contains parentheses
"Menu Group" → "menu_group"
"Menu" → "menu"
"Sales Category" → "sales_category"
"Gross Price" → "gross_price"
"Discount" → "discount"
"Net Price" → "net_price"
"Qty" → "quantity"
"Tax" → "tax"
"Void?" → "void"
"Deferred" → "deferred"
"Tax Exempt" → "tax_exempt"
"Tax Inclusion Option" → "tax_inclusion_option"
"Dining Option Tax" → "dining_option_tax"
"Tab Name" → "tab_name"
```

**Special Processing**:
- `sent_date`, `order_date` → DATETIME format (YYYY-MM-DD HH:MM:SS)
- Boolean fields: `void`, `deferred`, `tax_exempt`
- Add `processing_date` column (DATE)

**BigQuery Schema**:
- 33 columns total (32 data + 1 processing_date)
- Mix of STRING, FLOAT, INTEGER, DATETIME, BOOLEAN, DATE types

---

### 5. KitchenTimings.csv

**Purpose**: Kitchen order fulfillment timing metrics

**Original → Cleaned Column Mappings**:
```
"Location" → "location"
"ID" → "id"
"Server" → "server"
"Check #" → "check_number"
"Table" → "table"
"Check Opened" → "check_opened"
"Station" → "station"
"Expediter Level" → "expediter_level"
"Fired Date" → "fired_date"
"Fulfilled Date" → "fulfilled_date"
"Fulfillment Time" → "fulfillment_time"             # Special parsing required
"Fulfilled By" → "fulfilled_by"
```

**Special Processing**:
- `check_opened`, `fired_date`, `fulfilled_date` → DATETIME format
- **Critical**: `fulfillment_time` requires custom parsing from "X hours, Y minutes, Z seconds" → total minutes (FLOAT)
- Add `processing_date` column (DATE)

**Fulfillment Time Conversion Logic**:
```python
def convert_to_minutes(time_str):
    # Parses "2 hours, 15 minutes, 30 seconds" → "135.5"
    # Handles missing components gracefully
    # Returns string representation of float
```

**BigQuery Schema**:
- 13 columns total (12 data + 1 processing_date)
- Mix of STRING, INTEGER, DATETIME, FLOAT, DATE types

---

### 6. OrderDetails.csv

**Purpose**: High-level order information and totals

**Original → Cleaned Column Mappings**:
```
"Location" → "location"
"Order Id" → "order_id"
"Order #" → "order_number"
"Checks" → "checks"
"Opened" → "opened"
"# of Guests" → "guest_count"
"Tab Names" → "tab_names"
"Server" → "server"
"Table" → "table"
"Revenue Center" → "revenue_center"
"Dining Area" → "dining_area"
"Service" → "service"
"Dining Options" → "dining_options"
"Discount Amount" → "discount_amount"
"Amount" → "amount"
"Tax" → "tax"
"Tip" → "tip"
"Gratuity" → "gratuity"
"Total" → "total"
"Voided" → "voided"
"Paid" → "paid"
"Closed" → "closed"
"Duration (Opened to Paid)" → "duration_opened_to_paid"  # ⚠️ PROBLEMATIC: Contains parentheses
"Order Source" → "order_source"
```

**Special Processing**:
- `opened`, `paid`, `closed` → DATETIME format
- `duration_opened_to_paid` → TIME format (HH:MM:SS)
- `voided` → BOOLEAN
- Add `processing_date` column (DATE)

**BigQuery Schema**:
- 25 columns total (24 data + 1 processing_date)
- Mix of STRING, INTEGER, FLOAT, DATETIME, BOOLEAN, TIME, DATE types

---

### 7. PaymentDetails.csv

**Purpose**: Payment transaction details and methods

**Original → Cleaned Column Mappings**:
```
"Location" → "location"
"Payment Id" → "payment_id"
"Order Id" → "order_id"
"Order #" → "order_number"
"Paid Date" → "paid_date"
"Order Date" → "order_date"
"Check Id" → "check_id"
"Check #" → "check_number"
"Tab Name" → "tab_name"
"Server" → "server"
"Table" → "table"
"Dining Area" → "dining_area"
"Service" → "service"
"Dining Option" → "dining_option"
"House Acct #" → "house_account_number"
"Amount" → "amount"
"Tip" → "tip"
"Gratuity" → "gratuity"
"Total" → "total"
"Swiped Card Amount" → "swiped_card_amount"
"Keyed Card Amount" → "keyed_card_amount"
"Amount Tendered" → "amount_tendered"
"Refunded" → "refunded"
"Refund Date" → "refund_date"
"Refund Amount" → "refund_amount"
"Refund Tip Amount" → "refund_tip_amount"
"Void User" → "void_user"
"Void Approver" → "void_approver"
"Void Date" → "void_date"
"Status" → "status"
"Type" → "type"
"Cash Drawer" → "cash_drawer"
"Card Type" → "card_type"
"Other Type" → "other_type"
"Email" → "email"
"Phone" → "phone"
"Last 4 Card Digits" → "last_4_card_digits"
"V/MC/D Fees" → "vmcd_fees"                         # ⚠️ PROBLEMATIC: Contains slashes
"Room Info" → "room_info"
"Receipt" → "receipt"
"Source" → "source"
"Last 4 Gift Card Digits" → "last_4_gift_card_digits"
"First 5 Gift Card Digits" → "first_5_gift_card_digits"
```

**Special Processing**:
- `paid_date`, `refund_date`, `order_date`, `void_date` → DATETIME format
- Add `processing_date` column (DATE)

**BigQuery Schema**:
- 44 columns total (43 data + 1 processing_date)
- Mix of STRING, FLOAT, DATETIME, DATE types

---

## ⚠️ Critical Column Name Issues Identified

Based on Phase 2 testing, these original column names cause BigQuery failures:

### **Files with Problematic Column Names**:

1. **AllItemsReport.csv**:
   - `"Item Qty (incl voids)"` - parentheses not allowed

2. **PaymentDetails.csv**:
   - `"V/MC/D Fees"` - slashes not allowed

3. **OrderDetails.csv**:
   - `"Duration (Opened to Paid)"` - parentheses not allowed

4. **ItemSelectionDetails.csv**:
   - `"Menu Subgroup(s)"` - parentheses not allowed

### **Solution Required for Phase 3**:
Implement column name sanitization that:
- Removes or replaces parentheses: `()` → `_` or remove
- Removes or replaces slashes: `/` → `_` or remove
- Handles other special characters consistently

---

## 🔄 Transformation Processing Flow

### Current Legacy Process:
1. **Extract**: SFTP download of 7 CSV files
2. **Transform**: Column renaming + data type conversion
3. **Load**: Upload to GCS → BigQuery load with JSON schemas

### Data Type Conversion Rules:
- **DATE**: `YYYY-MM-DD` format
- **DATETIME**: `YYYY-MM-DD HH:MM:SS` format  
- **TIME**: `HH:MM:SS` format
- **FLOAT**: Kitchen timing special conversion
- **STRING**: Default for text, explicit for IDs
- **BOOLEAN**: True/False values
- **INTEGER**: Numeric counters

### Missing Value Handling:
- All null/empty values → empty string `""`
- Ensures consistent BigQuery loading

---

## 📈 Schema Summary by File

| File | Columns | Key Data Types | Special Processing |
|------|---------|---------------|-------------------|
| AllItemsReport | 28 | STRING, FLOAT, INTEGER, DATE | String type preservation |
| CheckDetails | 20 | STRING, FLOAT, INTEGER, DATE, TIME | Date/time separation |
| CashEntries | 12 | STRING, FLOAT, DATETIME, DATE | Required entry_id |
| ItemSelectionDetails | 33 | STRING, FLOAT, INTEGER, DATETIME, BOOLEAN, DATE | Boolean conversions |
| KitchenTimings | 13 | STRING, INTEGER, DATETIME, FLOAT, DATE | Time parsing to minutes |
| OrderDetails | 25 | STRING, INTEGER, FLOAT, DATETIME, BOOLEAN, TIME, DATE | Duration as TIME |
| PaymentDetails | 44 | STRING, FLOAT, DATETIME, DATE | Most complex schema |

**Total**: 175 columns across 7 tables, requiring 23 datetime conversions and 4 special transformations.

---

## 🎯 Phase 3 Implementation Requirements

Based on this analysis, Phase 3 (Data Transformation Layer) must implement:

1. **Column Name Sanitization**: Handle special characters in original headers
2. **Robust Date/Time Parsing**: 23 datetime field conversions
3. **Special Value Processing**: Kitchen timing minutes conversion
4. **Data Type Enforcement**: Prevent scientific notation in ID fields
5. **Validation Logic**: Ensure all transformations complete successfully
6. **Error Handling**: Graceful handling of malformed data

This comprehensive transformation layer will bridge the gap between raw Toast CSV exports and BigQuery-ready data.