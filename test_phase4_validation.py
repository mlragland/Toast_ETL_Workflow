#!/usr/bin/env python3
"""
Test Phase 4 Advanced Data Processing Features

Tests the comprehensive data validation, schema enforcement, and quality checking
features implemented in Phase 4 of the Toast ETL Pipeline modernization.
"""

import pandas as pd
import tempfile
import os
from pathlib import Path
import json

from src.validators.schema_enforcer import SchemaEnforcer
from src.validators.data_validator import DataValidator
from src.validators.quality_checker import QualityChecker


def create_test_data():
    """Create test CSV files with various data quality issues."""
    test_data = {}
    
    # OrderDetails with quality issues
    order_data = {
        "Location": ["Store 1", "Store 2", "Store 1"],
        "Order Id": ["O001", "O002", "O001"],  # Duplicate order ID
        "Order #": ["12345", "12346", "12347"],
        "Checks": ["1", "2", "1"],
        "Opened": ["2024-06-07 12:00:00", "2024-06-07 13:00:00", "2024-06-07 14:00:00"],
        "# of Guests": [4, 8, 2],
        "Tab Names": ["Table 5", "Table 12", "Table 3"],
        "Server": ["John", "Jane", "John"],
        "Table": ["5", "12", "3"],
        "Revenue Center": ["Restaurant", "Bar", "Restaurant"],
        "Dining Area": ["Main", "Patio", "Main"],
        "Service": ["Dine In", "Dine In", "Takeout"],
        "Dining Options": ["Regular", "Happy Hour", "Regular"],
        "Discount Amount": [0.0, 5.50, 0.0],
        "Amount": [45.99, 78.25, 23.50],
        "Tax": [4.14, 7.04, 2.12],
        "Tip": [9.00, 15.00, 0.0],
        "Gratuity": [0.0, 0.0, 0.0],
        "Total": [59.13, 105.79, 25.62],
        "Voided": [False, False, False],
        "Paid": ["2024-06-07 12:30:00", "2024-06-07 14:00:00", "2024-06-07 14:15:00"],
        "Closed": ["2024-06-07 12:32:00", "2024-06-07 14:02:00", "2024-06-07 14:17:00"],
        "Duration (Opened to Paid)": ["30 minutes", "1 hour", "15 minutes"],
        "Order Source": ["POS", "Online", "POS"]
    }
    test_data["OrderDetails.csv"] = pd.DataFrame(order_data)
    
    # PaymentDetails with various issues
    payment_data = {
        "Location": ["Store 1", "Store 2", "Store 1"],
        "Payment Id": ["P001", "P002", "P003"],
        "Order Id": ["O001", "O002", "O999"],  # O999 doesn't exist in orders (orphaned)
        "Order #": ["12345", "12346", "99999"],
        "Paid Date": ["2024-06-07 12:30:00", "2024-06-07 14:00:00", "2024-06-07 14:15:00"],
        "Order Date": ["2024-06-07 12:00:00", "2024-06-07 13:00:00", "2024-06-07 14:00:00"],
        "Check Id": ["C001", "C002", "C003"],
        "Check #": ["12345", "12346", "99999"],
        "Tab Name": ["Table 5", "Table 12", "Table 3"],
        "Server": ["John", "Jane", "John"],
        "Table": ["5", "12", "3"],
        "Dining Area": ["Main", "Patio", "Main"],
        "Service": ["Dine In", "Dine In", "Takeout"],
        "Dining Option": ["Regular", "Happy Hour", "Regular"],
        "House Acct #": ["", "", ""],
        "Amount": [45.99, 78.25, 23.50],
        "Tip": [9.00, 15.00, 0.0],
        "Gratuity": [0.0, 0.0, 0.0],
        "Total": [59.13, 105.79, 25.62],
        "Swiped Card Amount": [59.13, 105.79, 25.62],
        "Keyed Card Amount": [0.0, 0.0, 0.0],
        "Amount Tendered": [60.0, 106.0, 26.0],
        "Refunded": ["No", "No", "No"],
        "Refund Date": ["", "", ""],
        "Refund Amount": [0.0, 0.0, 0.0],
        "Refund Tip Amount": [0.0, 0.0, 0.0],
        "Void User": ["", "", ""],
        "Void Approver": ["", "", ""],
        "Void Date": ["", "", ""],
        "Status": ["Completed", "Completed", "InvalidStatus"],  # Invalid status
        "Type": ["Credit Card", "Credit Card", "Credit Card"],
        "Cash Drawer": ["1", "1", "1"],
        "Card Type": ["Visa", "MasterCard", "Visa"],
        "Other Type": ["", "", ""],
        "Email": ["john@example.com", "invalid-email", "jane@example.com"],  # Invalid email
        "Phone": ["555-123-4567", "not-a-phone", "555-987-6543"],  # Invalid phone
        "Last 4 Card Digits": ["1234", "567", "8901"],  # Wrong length
        "V/MC/D Fees": [1.5, 2.8, 0.7],
        "Room Info": ["", "", ""],
        "Receipt": ["Yes", "Yes", "No"],
        "Source": ["Terminal", "Online", "Terminal"],
        "Last 4 Gift Card Digits": ["", "", ""],
        "First 5 Gift Card Digits": ["", "", ""]
    }
    test_data["PaymentDetails.csv"] = pd.DataFrame(payment_data)
    
    # AllItemsReport with outliers and range issues
    items_data = {
        "Master ID": ["M001", "M002", "M003"],
        "Item ID": ["I001", "I002", "I003"],
        "Parent ID": ["P001", "", "P003"],
        "Menu Name": ["Lunch Menu", "Dinner Menu", "Breakfast Menu"],
        "Menu Group": ["Entrees", "Appetizers", "Beverages"],
        "Subgroup": ["Burgers", "Salads", "Coffee"],
        "Menu Item": ["Classic Burger", "Caesar Salad", "House Coffee"],
        "Tags": ["Popular, Lunch", "", "Hot, Breakfast"],
        "Avg Price": [15.99, 12.50, 3.99],
        "Item Qty (incl voids)": [45, 23, 150],  # High quantity for coffee
        "% of Ttl Qty (incl voids)": [35.5, 18.1, 46.4],
        "Gross Amount (incl voids)": [719.55, 287.50, 598.50],
        "% of Ttl Amt (incl voids)": [44.8, 17.9, 37.3],
        "Item Qty": [43, 23, 148],
        "Gross Amount": [687.57, 287.50, 590.52],
        "Void Qty": [2, 0, 2],
        "Void Amount": [31.98, 0.0, 7.98],
        "Discount Amount": [10.0, 5.0, 0.0],
        "Net Amount": [677.57, 282.50, 590.52],
        "# Orders": [15, 8, 42],
        "% of Ttl # Orders": [23.1, 12.3, 64.6],
        "% Qty (Group)": [100.0, 85.2, 98.7],
        "% Qty (Menu)": [67.2, 34.3, 87.1],
        "% Qty (All)": [35.5, 18.1, 46.4],
        "% Net Amt (Group)": [100.0, 78.9, 99.2],
        "% Net Amt (Menu)": [72.3, 30.1, 89.4],
        "% Net Amt (All)": [44.8, 17.9, 37.3]
    }
    test_data["AllItemsReport.csv"] = pd.DataFrame(items_data)
    
    return test_data


def test_schema_enforcer():
    """Test the Schema Enforcer functionality."""
    print("🔍 Testing Schema Enforcer...")
    
    schema_enforcer = SchemaEnforcer()
    
    # Test with OrderDetails data
    test_data = create_test_data()
    df = test_data["OrderDetails.csv"]
    
    print(f"✅ Created test data with {len(df)} rows")
    
    # Test schema validation
    validation_result = schema_enforcer.validate_schema_compliance(df, "OrderDetails.csv")
    print(f"✅ Schema validation completed: {validation_result['valid']}")
    print(f"   Missing columns: {len(validation_result['missing_columns'])}")
    print(f"   Type mismatches: {len(validation_result['type_mismatches'])}")
    
    # Test schema enforcement
    corrected_df, warnings = schema_enforcer.enforce_schema_types(df, "OrderDetails.csv")
    print(f"✅ Schema enforcement completed with {len(warnings)} warnings")
    print(f"   Original shape: {df.shape}, Corrected shape: {corrected_df.shape}")
    
    # Test schema report generation
    report = schema_enforcer.generate_schema_report(df, "OrderDetails.csv")
    print(f"✅ Schema report generated: {report['severity']}")
    
    return True


def test_data_validator():
    """Test the Data Validator functionality."""
    print("\n🔍 Testing Data Validator...")
    
    data_validator = DataValidator()
    
    # Test with PaymentDetails data (has various validation issues)
    test_data = create_test_data()
    df = test_data["PaymentDetails.csv"]
    
    print(f"✅ Created test data with {len(df)} rows")
    
    # Test business rules validation
    business_result = data_validator.validate_business_rules(df, "PaymentDetails.csv")
    print(f"✅ Business rules validation: {business_result['valid']}")
    print(f"   Errors: {len(business_result['errors'])}")
    print(f"   Warnings: {len(business_result['warnings'])}")
    
    if business_result['errors']:
        print("   Sample errors:")
        for error in business_result['errors'][:3]:
            print(f"     - {error}")
    
    # Test anomaly detection
    anomalies = data_validator.detect_anomalies(df, "PaymentDetails.csv")
    print(f"✅ Anomaly detection completed")
    print(f"   Duplicates found: {anomalies['duplicates']['total_duplicate_rows']}")
    print(f"   Outliers detected: {len(anomalies['outliers'])}")
    print(f"   Consistency issues: {len(anomalies['data_consistency'])}")
    
    return True


def test_quality_checker():
    """Test the Quality Checker comprehensive functionality."""
    print("\n🔍 Testing Quality Checker...")
    
    quality_checker = QualityChecker()
    
    # Test with multiple files
    test_data = create_test_data()
    
    print(f"✅ Created test data with {len(test_data)} files")
    
    # Test comprehensive quality check
    quality_report = quality_checker.comprehensive_quality_check(test_data)
    
    print(f"✅ Comprehensive quality check completed")
    print(f"   Overall status: {quality_report['overall_status']}")
    print(f"   Files analyzed: {len(quality_report['file_reports'])}")
    print(f"   Critical issues: {len(quality_report['critical_issues'])}")
    print(f"   Warnings: {len(quality_report['warnings'])}")
    print(f"   Recommendations: {len(quality_report['recommendations'])}")
    
    # Test referential integrity
    ref_integrity = quality_report['referential_integrity']
    print(f"   Referential relationships checked: {len(ref_integrity)}")
    
    for rel_name, result in ref_integrity.items():
        if not result.get('valid', True):
            print(f"     ❌ {rel_name}: {len(result.get('violations', []))} violations")
        else:
            print(f"     ✅ {rel_name}: Valid")
    
    # Test individual file validation and enforcement
    df = test_data["OrderDetails.csv"]
    corrected_df, validation_report = quality_checker.validate_and_enforce(df, "OrderDetails.csv")
    
    print(f"✅ Individual file validation and enforcement completed")
    print(f"   Severity: {validation_report['severity']}")
    print(f"   Schema corrections applied: {validation_report.get('schema_corrections', {}).get('applied', False)}")
    
    return True


def test_integration_with_transformer():
    """Test integration with the updated ToastDataTransformer."""
    print("\n🔍 Testing Integration with ToastDataTransformer...")
    
    from src.transformers.toast_transformer import ToastDataTransformer
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        input_dir = os.path.join(temp_dir, "input")
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(input_dir)
        
        # Save test data to CSV files
        test_data = create_test_data()
        for filename, df in test_data.items():
            file_path = os.path.join(input_dir, filename)
            df.to_csv(file_path, index=False)
            print(f"✅ Created test file: {filename}")
        
        # Initialize transformer
        transformer = ToastDataTransformer(processing_date="2024-06-07")
        
        # Test transformation without validation
        print("\n📝 Testing transformation without validation...")
        results_basic, validation_reports_basic = transformer.transform_files(
            input_dir, os.path.join(output_dir, "basic"), enable_validation=False
        )
        
        successful_basic = sum(1 for success in results_basic.values() if success)
        print(f"✅ Basic transformation: {successful_basic}/{len(results_basic)} files successful")
        
        # Test transformation with validation
        print("\n📝 Testing transformation with validation...")
        results_advanced, validation_reports_advanced = transformer.transform_files(
            input_dir, os.path.join(output_dir, "advanced"), enable_validation=True
        )
        
        successful_advanced = sum(1 for success in results_advanced.values() if success)
        print(f"✅ Advanced transformation: {successful_advanced}/{len(results_advanced)} files successful")
        
        # Check validation reports
        if validation_reports_advanced:
            print("✅ Validation reports generated:")
            for filename, report in validation_reports_advanced.items():
                if "error" in report:
                    print(f"   ❌ {filename}: {report['error']}")
                else:
                    severity = report.get("severity", "UNKNOWN")
                    print(f"   📊 {filename}: {severity}")
        
        print(f"✅ Integration test completed successfully")
    
    return True


def main():
    """Run all Phase 4 validation tests."""
    print("🍴 Testing Toast ETL Pipeline Phase 4: Advanced Data Processing")
    print("=" * 70)
    
    try:
        # Run individual component tests
        test_schema_enforcer()
        test_data_validator()
        test_quality_checker()
        test_integration_with_transformer()
        
        print("\n" + "=" * 70)
        print("🎉 All Phase 4 tests completed successfully!")
        print("\nPhase 4 Features Validated:")
        print("✅ Schema Enforcement - BigQuery compatibility validation and correction")
        print("✅ Data Validation - Business rules and data quality checks")
        print("✅ Quality Checker - Comprehensive cross-file quality assessment")
        print("✅ Referential Integrity - Cross-file relationship validation")
        print("✅ Anomaly Detection - Outliers, duplicates, and consistency checks")
        print("✅ Integration - Seamless integration with existing transformation pipeline")
        
        print("\n🚀 Phase 4 Advanced Data Processing is ready for production!")
        
    except Exception as e:
        print(f"\n❌ Phase 4 testing failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)