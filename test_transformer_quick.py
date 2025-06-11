#!/usr/bin/env python3
"""Quick test script for ToastDataTransformer."""

import sys
sys.path.append('.')

from src.transformers.toast_transformer import ToastDataTransformer
import tempfile
import pandas as pd
import os

def test_basic_functionality():
    """Test basic transformer functionality."""
    print('Testing ToastDataTransformer...')

    transformer = ToastDataTransformer(processing_date='2024-12-18')
    print(f'✅ Transformer initialized with date: {transformer.processing_date}')

    # Test column sanitization
    test_cases = [
        ('Item Qty (incl voids)', 'item_qty_incl_voids'),
        ('V/MC/D Fees', 'v_mc_d_fees'),
        ('Duration (Opened to Paid)', 'duration_opened_to_paid'),
        ('Menu Subgroup(s)', 'menu_subgroup_s'),
    ]
    
    for original, expected in test_cases:
        result = transformer.sanitize_column_name(original)
        assert result == expected, f"Column sanitization failed: {original} → {result}, expected {expected}"
        print(f'✅ Column sanitization: "{original}" → "{result}"')

    # Test time conversion
    time_cases = [
        ('10 minutes', '10.0'),
        ('1 hour, 30 minutes', '90.0'),
        ('2 hours, 15 minutes, 30 seconds', '135.5'),
    ]
    
    for time_str, expected in time_cases:
        result = transformer.convert_to_minutes(time_str)
        assert result == expected, f"Time conversion failed: {time_str} → {result}, expected {expected}"
        print(f'✅ Time conversion: "{time_str}" → {result} minutes')

    print('🎉 All basic transformer functionality tests passed!')

def test_file_transformation():
    """Test actual file transformation."""
    print('\nTesting file transformation...')
    
    transformer = ToastDataTransformer(processing_date='2024-12-18')
    
    # Create test directory
    test_dir = tempfile.mkdtemp()
    
    try:
        # Create sample AllItemsReport.csv
        sample_data = {
            "Master ID": ["12345", "67890"],
            "Item ID": ["1001", "1002"],
            "Menu Item": ["Burger", "Pizza"],
            "Item Qty (incl voids)": [10.0, 5.0],
            "Net Amount": [159.90, 112.50]
        }
        
        df = pd.DataFrame(sample_data)
        input_file = os.path.join(test_dir, "AllItemsReport.csv")
        output_file = os.path.join(test_dir, "AllItemsReport_cleaned.csv")
        
        df.to_csv(input_file, index=False)
        print(f'✅ Created test file: {input_file}')
        
        # Transform the file
        result = transformer.transform_csv(input_file, output_file)
        
        if result:
            print(f'✅ File transformation successful')
            
            # Check output
            cleaned_df = pd.read_csv(output_file)
            print(f'✅ Transformed file has {len(cleaned_df)} rows and {len(cleaned_df.columns)} columns')
            
            # Check for processing_date column
            assert 'processing_date' in cleaned_df.columns
            print(f'✅ Processing date column added: {cleaned_df["processing_date"].iloc[0]}')
            
            # Check column sanitization
            assert 'item_qty_incl_voids' in cleaned_df.columns
            print(f'✅ Column names sanitized properly')
            
        else:
            print('❌ File transformation failed')
            return False
    
    finally:
        # Cleanup
        import shutil
        shutil.rmtree(test_dir, ignore_errors=True)
    
    print('🎉 File transformation test passed!')
    return True

if __name__ == "__main__":
    try:
        test_basic_functionality()
        test_file_transformation()
        print('\n🎉 All tests passed! Phase 3 transformation layer is working correctly.')
    except Exception as e:
        print(f'\n❌ Test failed: {e}')
        sys.exit(1) 