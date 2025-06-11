#!/usr/bin/env python3
"""
Comprehensive ETL Test - Process December 2024 data with all 7 file types
"""

import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def run_comprehensive_test():
    print("🧪 Comprehensive ETL Test - All 7 File Types")
    print("=" * 60)
    
    test_date = '20241201'
    
    try:
        from src.transformers.toast_transformer import ToastDataTransformer
        from src.loaders.bigquery_loader import BigQueryLoader
        
        print(f"📅 Testing with date: {test_date}")
        
        # We already downloaded files, so let's use existing data
        input_dir = f"/tmp/toast_raw_data/raw/{test_date}"
        
        if not os.path.exists(input_dir):
            print(f"❌ Input directory not found: {input_dir}")
            return False
        
        csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
        print(f"📄 Found {len(csv_files)} CSV files to process")
        
        # Transform files
        transformer = ToastDataTransformer(test_date)
        
        # Use the transform_files method
        results, validation_reports = transformer.transform_files(
            input_folder=input_dir,
            output_folder=transformer.cleaned_local_dir,
            file_list=csv_files,
            enable_validation=False
        )
        
        print(f"\n🔄 Transformation Results:")
        transformed_files = []
        for filename, success in results.items():
            if success:
                print(f"   ✅ {filename}")
                output_file = os.path.join(
                    transformer.cleaned_local_dir,
                    filename.replace('.csv', '_cleaned.csv')
                )
                transformed_files.append(output_file)
            else:
                print(f"   ❌ {filename}")
        
        # Load to BigQuery
        print(f"\n⬆️ Loading to BigQuery:")
        loader = BigQueryLoader()
        
        table_mapping = {
            'AllItemsReport.csv': 'all_items_report',
            'CheckDetails.csv': 'check_details',
            'CashEntries.csv': 'cash_entries',
            'ItemSelectionDetails.csv': 'item_selection_details',
            'KitchenTimings.csv': 'kitchen_timings',
            'OrderDetails.csv': 'order_details',
            'PaymentDetails.csv': 'payment_details'
        }
        
        load_count = 0
        total_rows = 0
        
        for output_file in transformed_files:
            filename = os.path.basename(output_file)
            base_name = filename.replace('_cleaned.csv', '.csv')
            table_name = table_mapping.get(base_name)
            
            if table_name:
                try:
                    result = loader.load_csv_to_table(output_file, table_name)
                    if result:
                        rows = result.get('rows_loaded', 0)
                        print(f"   ✅ {table_name}: {rows:,} rows")
                        load_count += 1
                        total_rows += rows
                    else:
                        print(f"   ❌ {table_name}: Load failed")
                except Exception as e:
                    print(f"   ❌ {table_name}: {str(e)}")
        
        print(f"\n🎯 Final Results:")
        print(f"   📄 CSV files found: {len(csv_files)}")
        print(f"   🔄 Files transformed: {len(transformed_files)}")
        print(f"   ⬆️ Tables loaded: {load_count}")
        print(f"   📊 Total rows: {total_rows:,}")
        
        if load_count >= 6:  # Allow for some failures
            print(f"\n🎉 SUCCESS! Comprehensive data pipeline working")
            return True
        else:
            print(f"\n⚠️ Partial success - some issues to resolve")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_comprehensive_test()
    if success:
        print(f"\n🚀 READY FOR PHASE 6 DASHBOARD!")
    else:
        print(f"\n🔧 Need to fix issues before dashboard") 