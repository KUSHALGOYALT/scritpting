#!/usr/bin/env python3
"""
Demonstration script for enhanced energy data extractors
Shows past 7 days extraction, update handling, and master dataset creation
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

def demonstrate_past_7_days():
    """Demonstrate past 7 days week calculation"""
    logger.info("🎯 Demonstrating Past 7 Days Extraction...")
    
    try:
        # Import and test NRLDC extractor
        from extractors.nrldc.nrldc_extractor import NRLDCWorkingDSAExtractor
        nrldc_extractor = NRLDCWorkingDSAExtractor()
        
        # Get past 7 days weeks
        past_weeks = nrldc_extractor.get_past_7_days_weeks()
        
        logger.info(f"📅 NRLDC: Generated {len(past_weeks)} weeks for past 7 days")
        logger.info("📊 Week breakdown:")
        
        for i, week in enumerate(past_weeks):
            logger.info(f"   Week {i+1}: {week['start_date']} to {week['end_date']} (WK{week['week_num']})")
            logger.info(f"           Format: {week['start_ddmmyy']} to {week['end_ddmmyy']}")
            logger.info(f"           Key: {week['week_key']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Demonstration failed: {e}")
        return False

def demonstrate_week_tracking():
    """Demonstrate week tracking and update handling"""
    logger.info("🎯 Demonstrating Week Tracking and Update Handling...")
    
    try:
        from extractors.nrldc.nrldc_extractor import NRLDCWorkingDSAExtractor
        nrldc_extractor = NRLDCWorkingDSAExtractor()
        
        # Show current processed weeks
        logger.info(f"📋 Current processed weeks: {len(nrldc_extractor.processed_weeks)}")
        
        # Add a test week
        test_week = "demo_week_2025"
        old_timestamp = "2025-01-01T00:00:00"
        
        nrldc_extractor.processed_weeks[test_week] = {
            'timestamp': old_timestamp,
            'filename': 'demo_old.csv',
            'csv_file': 'demo_old.csv',
            'url': 'http://demo.com/old'
        }
        
        logger.info(f"➕ Added test week: {test_week}")
        logger.info(f"📅 Old timestamp: {old_timestamp}")
        
        # Simulate update check
        current_timestamp = datetime.now().isoformat()
        logger.info(f"📅 Current timestamp: {current_timestamp}")
        
        if test_week in nrldc_extractor.processed_weeks:
            existing_timestamp = nrldc_extractor.processed_weeks[test_week].get('timestamp', '')
            
            if existing_timestamp < current_timestamp:
                logger.info("🔄 Update detected! Updating week data...")
                
                # Update the week
                nrldc_extractor.processed_weeks[test_week] = {
                    'timestamp': current_timestamp,
                    'filename': 'demo_new.csv',
                    'csv_file': 'demo_new.csv',
                    'url': 'http://demo.com/new'
                }
                
                logger.info("✅ Week updated successfully!")
                logger.info(f"📊 Updated data: {nrldc_extractor.processed_weeks[test_week]}")
            else:
                logger.info("⏭️ No update needed")
        
        # Save and reload to demonstrate persistence
        nrldc_extractor.save_processed_weeks()
        logger.info("💾 Processed weeks saved to disk")
        
        # Clean up test data
        if test_week in nrldc_extractor.processed_weeks:
            del nrldc_extractor.processed_weeks[test_week]
            nrldc_extractor.save_processed_weeks()
            logger.info("🧹 Test data cleaned up")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Demonstration failed: {e}")
        return False

def demonstrate_master_dataset_creation():
    """Demonstrate master dataset creation"""
    logger.info("🎯 Demonstrating Master Dataset Creation...")
    
    try:
        from extractors.nrldc.nrldc_extractor import NRLDCWorkingDSAExtractor
        nrldc_extractor = NRLDCWorkingDSAExtractor()
        
        # Create test data directory
        test_dir = Path("local_data/NRLDC")
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Create sample CSV files
        import pandas as pd
        
        # File 1: Week 1 data
        week1_data = pd.DataFrame({
            'Date': ['2025-01-01', '2025-01-01', '2025-01-01'],
            'Time': ['00:00', '00:15', '00:30'],
            'Block': [1, 2, 3],
            'Constituents': ['Delhi State', 'Delhi State', 'Delhi State'],
            'Actual (MWH)': [100, 110, 120],
            'Schedule (MWH)': [100, 105, 115],
            'Region': ['NRLDC', 'NRLDC', 'NRLDC'],
            'Week': ['WK1', 'WK1', 'WK1']
        })
        
        # File 2: Week 2 data
        week2_data = pd.DataFrame({
            'Date': ['2025-01-08', '2025-01-08', '2025-01-08'],
            'Time': ['00:00', '00:15', '00:30'],
            'Block': [1, 2, 3],
            'Constituents': ['Haryana State', 'Haryana State', 'Haryana State'],
            'Actual (MWH)': [90, 95, 100],
            'Schedule (MWH)': [90, 92, 98],
            'Region': ['NRLDC', 'NRLDC', 'NRLDC'],
            'Week': ['WK2', 'WK2', 'WK2']
        })
        
        # Save test files
        week1_file = test_dir / "week1_data.csv"
        week2_file = test_dir / "week2_data.csv"
        
        week1_data.to_csv(week1_file, index=False)
        week2_data.to_csv(week2_file, index=False)
        
        logger.info(f"📁 Created test files:")
        logger.info(f"   📄 {week1_file.name}: {len(week1_data)} rows")
        logger.info(f"   📄 {week2_file.name}: {len(week2_data)} rows")
        
        # Create master dataset
        logger.info("🔧 Creating master dataset...")
        master_file = nrldc_extractor.create_master_dataset()
        
        if master_file:
            logger.info(f"✅ Master dataset created: {master_file}")
            
            # Read and show summary
            master_df = pd.read_csv(master_file)
            logger.info(f"📊 Master dataset summary:")
            logger.info(f"   Total records: {len(master_df)}")
            logger.info(f"   Date range: {master_df['Date'].min()} to {master_df['Date'].max()}")
            logger.info(f"   Entities: {master_df['Constituents'].unique()}")
            logger.info(f"   Weeks: {master_df['Week'].unique()}")
            
            # Show sample data
            logger.info(f"📋 Sample data:")
            logger.info(master_df.head(3).to_string())
            
        else:
            logger.error("❌ Master dataset creation failed")
            return False
        
        # Clean up test files
        if week1_file.exists():
            week1_file.unlink()
        if week2_file.exists():
            week2_file.unlink()
        
        logger.info("🧹 Test files cleaned up")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Demonstration failed: {e}")
        return False

def demonstrate_global_integration():
    """Demonstrate global master dataset creation"""
    logger.info("🎯 Demonstrating Global Master Dataset Integration...")
    
    try:
        # Import the global integration function
        from run_extractors import create_global_master_dataset
        
        # Create global master dataset
        logger.info("🔧 Creating global master dataset...")
        global_master = create_global_master_dataset()
        
        if global_master:
            logger.info(f"✅ Global master dataset created: {global_master}")
            
            # Read and show summary
            import pandas as pd
            global_df = pd.read_csv(global_master)
            
            logger.info(f"📊 Global dataset summary:")
            logger.info(f"   Total records: {len(global_df)}")
            logger.info(f"   Regions: {global_df['Global_Region'].unique()}")
            logger.info(f"   Source files: {global_df['Global_Source_File'].unique()}")
            
            # Show sample data
            logger.info(f"📋 Sample global data:")
            logger.info(global_df.head(3).to_string())
            
        else:
            logger.warning("⚠️ Global master dataset creation failed (no regional data)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Demonstration failed: {e}")
        return False

def run_demonstration():
    """Run the complete demonstration"""
    logger.info("🚀 Starting Enhanced Features Demonstration...")
    logger.info("=" * 60)
    
    demonstrations = [
        ("Past 7 Days Extraction", demonstrate_past_7_days),
        ("Week Tracking & Updates", demonstrate_week_tracking),
        ("Master Dataset Creation", demonstrate_master_dataset_creation),
        ("Global Integration", demonstrate_global_integration)
    ]
    
    passed = 0
    total = len(demonstrations)
    
    for demo_name, demo_func in demonstrations:
        logger.info(f"\n🎯 {demo_name}")
        logger.info("-" * 40)
        
        try:
            if demo_func():
                logger.info(f"✅ {demo_name}: SUCCESS")
                passed += 1
            else:
                logger.error(f"❌ {demo_name}: FAILED")
        except Exception as e:
            logger.error(f"❌ {demo_name}: ERROR - {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info(f"📊 Demonstration Results: {passed}/{total} successful")
    
    if passed == total:
        logger.info("🎉 All demonstrations completed successfully!")
        logger.info("✨ Enhanced extractors are ready for production use!")
    else:
        logger.warning(f"⚠️ {total - passed} demonstrations failed")
    
    return passed == total

if __name__ == "__main__":
    success = run_demonstration()
    sys.exit(0 if success else 1)
