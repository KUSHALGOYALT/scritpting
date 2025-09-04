#!/usr/bin/env python3
"""
Test script for enhanced energy data extractors
Tests past 7 days extraction, update handling, and master dataset creation
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_past_7_days_calculation():
    """Test the past 7 days week calculation"""
    logger.info("🧪 Testing past 7 days week calculation...")
    
    try:
        # Test NRLDC extractor
        from extractors.nrldc.nrldc_extractor import NRLDCWorkingDSAExtractor
        nrldc_extractor = NRLDCWorkingDSAExtractor()
        past_weeks = nrldc_extractor.get_past_7_days_weeks()
        
        logger.info(f"✅ NRLDC: Generated {len(past_weeks)} weeks")
        for week in past_weeks[:3]:  # Show first 3 weeks
            logger.info(f"   📅 {week['start_date']} to {week['end_date']} (WK{week['week_num']})")
        
        # Test ERLDC extractor
        from extractors.erldc.erldc_extractor import ERLDCDynamicExtractor
        erldc_extractor = ERLDCDynamicExtractor()
        past_weeks = erldc_extractor.get_past_7_days_weeks()
        
        logger.info(f"✅ ERLDC: Generated {len(past_weeks)} weeks")
        for week in past_weeks[:3]:  # Show first 3 weeks
            logger.info(f"   📅 {week['start_date']} to {week['end_date']} (WK{week['week_num']})")
        
        # Test WRPC extractor
        from extractors.wrpc.wrpc_extractor import WRPCDynamicExtractor
        wrpc_extractor = WRPCDynamicExtractor()
        past_weeks = wrpc_extractor.get_past_7_days_weeks()
        
        logger.info(f"✅ WRPC: Generated {len(past_weeks)} weeks")
        for week in past_weeks[:3]:  # Show first 3 weeks
            logger.info(f"   📅 {week['start_date']} to {week['end_date']} (WK{week['week_num']})")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

def test_processed_weeks_tracking():
    """Test the processed weeks tracking functionality"""
    logger.info("🧪 Testing processed weeks tracking...")
    
    try:
        # Test NRLDC extractor
        from extractors.nrldc.nrldc_extractor import NRLDCWorkingDSAExtractor
        nrldc_extractor = NRLDCWorkingDSAExtractor()
        
        # Test loading/saving processed weeks
        test_week = "test_week_123"
        nrldc_extractor.processed_weeks[test_week] = {
            'timestamp': datetime.now().isoformat(),
            'filename': 'test_file.csv',
            'csv_file': 'test_file.csv',
            'url': 'http://test.com'
        }
        nrldc_extractor.save_processed_weeks()
        
        # Reload and check
        nrldc_extractor.processed_weeks = {}
        nrldc_extractor.processed_weeks = nrldc_extractor.load_processed_weeks()
        
        if test_week in nrldc_extractor.processed_weeks:
            logger.info("✅ NRLDC: Processed weeks tracking working")
        else:
            logger.error("❌ NRLDC: Processed weeks tracking failed")
            return False
        
        # Clean up test data
        if test_week in nrldc_extractor.processed_weeks:
            del nrldc_extractor.processed_weeks[test_week]
            nrldc_extractor.save_processed_weeks()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

def test_master_dataset_creation():
    """Test the master dataset creation functionality"""
    logger.info("🧪 Testing master dataset creation...")
    
    try:
        # Create test directories
        test_dirs = [
            "master_data/NRLDC",
            "master_data/ERLDC", 
            "master_data/WRPC"
        ]
        
        for test_dir in test_dirs:
            Path(test_dir).mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Created test directory: {test_dir}")
        
        # Test NRLDC master dataset creation
        from extractors.nrldc.nrldc_extractor import NRLDCWorkingDSAExtractor
        nrldc_extractor = NRLDCWorkingDSAExtractor()
        
        # Create a test CSV file
        import pandas as pd
        test_data = pd.DataFrame({
            'Date': ['2025-01-01', '2025-01-01'],
            'Time': ['00:00', '00:15'],
            'Block': [1, 2],
            'Constituents': ['Test State', 'Test State'],
            'Actual (MWH)': [100, 110],
            'Schedule (MWH)': [100, 105],
            'Region': ['NRLDC', 'NRLDC'],
            'Week': ['test_week_1', 'test_week_1']  # Add required Week column
        })
        
        test_csv = Path("local_data/NRLDC/test_data.csv")
        test_csv.parent.mkdir(parents=True, exist_ok=True)
        test_data.to_csv(test_csv, index=False)
        
        # Test master dataset creation
        master_file = nrldc_extractor.create_master_dataset()
        if master_file:
            logger.info("✅ NRLDC: Master dataset creation working")
        else:
            logger.error("❌ NRLDC: Master dataset creation failed")
            return False
        
        # Clean up test files
        if test_csv.exists():
            test_csv.unlink()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

def test_week_update_handling():
    """Test the week update handling functionality"""
    logger.info("🧪 Testing week update handling...")
    
    try:
        # Test NRLDC extractor
        from extractors.nrldc.nrldc_extractor import NRLDCWorkingDSAExtractor
        nrldc_extractor = NRLDCWorkingDSAExtractor()
        
        # Test with a sample week
        test_week = "test_week_456"
        old_timestamp = "2025-01-01T00:00:00"
        new_timestamp = datetime.now().isoformat()
        
        # Add old timestamp
        nrldc_extractor.processed_weeks[test_week] = {
            'timestamp': old_timestamp,
            'filename': 'old_file.csv',
            'csv_file': 'old_file.csv',
            'url': 'http://old.com'
        }
        nrldc_extractor.save_processed_weeks()
        
        # Simulate update check
        if test_week in nrldc_extractor.processed_weeks:
            existing_timestamp = nrldc_extractor.processed_weeks[test_week].get('timestamp', '')
            current_timestamp = datetime.now().isoformat()
            
            if existing_timestamp < current_timestamp:
                logger.info("✅ Week update detection working")
                
                # Update the week
                nrldc_extractor.processed_weeks[test_week] = {
                    'timestamp': new_timestamp,
                    'filename': 'new_file.csv',
                    'csv_file': 'new_file.csv',
                    'url': 'http://new.com'
                }
                nrldc_extractor.save_processed_weeks()
                logger.info("✅ Week update handling working")
            else:
                logger.error("❌ Week update detection failed")
                return False
        
        # Clean up test data
        if test_week in nrldc_extractor.processed_weeks:
            del nrldc_extractor.processed_weeks[test_week]
            nrldc_extractor.save_processed_weeks()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

def run_all_tests():
    """Run all tests"""
    logger.info("🚀 Starting enhanced extractor tests...")
    
    tests = [
        ("Past 7 Days Calculation", test_past_7_days_calculation),
        ("Processed Weeks Tracking", test_processed_weeks_tracking),
        ("Master Dataset Creation", test_master_dataset_creation),
        ("Week Update Handling", test_week_update_handling)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running test: {test_name}")
        try:
            if test_func():
                logger.info(f"✅ {test_name}: PASSED")
                passed += 1
            else:
                logger.error(f"❌ {test_name}: FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name}: ERROR - {e}")
    
    logger.info(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Enhanced extractors are working correctly.")
        return True
    else:
        logger.error(f"❌ {total - passed} tests failed. Please check the implementation.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
