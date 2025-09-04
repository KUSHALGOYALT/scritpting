#!/usr/bin/env python3
"""
Main runner script for all energy data extractors
Enhanced with past 7 days extraction, update handling, and master dataset creation
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_global_master_dataset():
    """Create a global master dataset combining all regions"""
    try:
        logger.info("🔧 Creating global master dataset...")
        
        # Check if we have master data from each region
        regions = ['NRLDC', 'ERLDC', 'WRPC']
        all_data = []
        
        for region in regions:
            master_dir = Path(f"master_data/{region}")
            if master_dir.exists():
                # Look for the most recent master dataset
                master_files = list(master_dir.glob(f"{region}_Master_Dataset_*.csv"))
                if master_files:
                    # Get the most recent file
                    latest_file = max(master_files, key=lambda x: x.stat().st_mtime)
                    try:
                        import pandas as pd
                        df = pd.read_csv(latest_file)
                        df['Global_Region'] = region
                        df['Global_Source_File'] = latest_file.name
                        all_data.append(df)
                        logger.info(f"📊 Added {region}: {len(df)} rows from {latest_file.name}")
                    except Exception as e:
                        logger.warning(f"⚠️ Could not read {region} master dataset: {e}")
                else:
                    logger.warning(f"⚠️ No master dataset found for {region}")
            else:
                logger.warning(f"⚠️ Master data directory not found for {region}")
        
        if not all_data:
            logger.error("❌ No data to combine for global master dataset")
            return None
        
        # Combine all data
        global_df = pd.concat(all_data, ignore_index=True)
        
        # Add global metadata
        global_df['Global_Master_Dataset_Created'] = datetime.now().isoformat()
        global_df['Total_Global_Records'] = len(global_df)
        
        # Create global master directory
        global_master_dir = Path("master_data/GLOBAL")
        global_master_dir.mkdir(parents=True, exist_ok=True)
        
        # Save global master dataset
        global_file = global_master_dir / f"GLOBAL_Master_Dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        global_df.to_csv(global_file, index=False)
        
        # Create global summary
        global_summary = {
            'total_regions': len(regions),
            'total_records': len(global_df),
            'regions_processed': [region for region in regions if any(region in str(f) for f in all_data)],
            'date_range': {
                'start': global_df['Date'].min() if 'Date' in global_df.columns else 'Unknown',
                'end': global_df['Date'].min() if 'Date' in global_df.columns else 'Unknown'
            },
            'created_at': datetime.now().isoformat()
        }
        
        summary_file = global_master_dir / "GLOBAL_Summary.json"
        with open(summary_file, 'w') as f:
            import json
            json.dump(global_summary, f, indent=2)
        
        logger.info(f"✅ Global master dataset created: {global_file} ({len(global_df)} total rows)")
        logger.info(f"📋 Global summary saved: {summary_file}")
        
        return str(global_file)
        
    except Exception as e:
        logger.error(f"❌ Error creating global master dataset: {e}")
        return None

def run_all_extractors():
    """Run all energy data extractors"""
    try:
        logger.info("🚀 Starting all energy data extractors...")
        
        all_results = []
        
        # Run NRLDC extractor
        logger.info("📊 Running NRLDC extractor...")
        try:
            os.system("python extractors/nrldc/nrldc_extractor.py")
            logger.info("✅ NRLDC extractor completed")
        except Exception as e:
            logger.error(f"❌ NRLDC extractor failed: {e}")
        
        # Run ERLDC extractor
        logger.info("📊 Running ERLDC extractor...")
        try:
            os.system("python extractors/erldc/erldc_extractor.py")
            logger.info("✅ ERLDC extractor completed")
        except Exception as e:
            logger.error(f"❌ ERLDC extractor failed: {e}")
        
        # Run WRPC extractor
        logger.info("📊 Running WRPC extractor...")
        try:
            os.system("python extractors/wrpc/wrpc_extractor.py")
            logger.info("✅ WRPC extractor completed")
        except Exception as e:
            logger.error(f"❌ WRPC extractor failed: {e}")
        
        # Create global master dataset
        logger.info("🔧 Creating global master dataset...")
        global_master = create_global_master_dataset()
        if global_master:
            all_results.append(global_master)
        
        logger.info("✅ All extractors completed successfully!")
        logger.info(f"📊 Results: {all_results}")
        
        return all_results
        
    except Exception as e:
        logger.error(f"❌ Error running extractors: {e}")
        sys.exit(1)

def run_single_extractor(region):
    """Run a single extractor for a specific region"""
    try:
        logger.info(f"🚀 Running {region} extractor...")
        
        if region.upper() == 'NRLDC':
            os.system("python extractors/nrldc/nrldc_extractor.py")
        elif region.upper() == 'ERLDC':
            os.system("python extractors/erldc/erldc_extractor.py")
        elif region.upper() == 'WRPC':
            os.system("python extractors/wrpc/wrpc_extractor.py")
        else:
            logger.error(f"❌ Unknown region: {region}")
            return False
        
        logger.info(f"✅ {region} extractor completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error running {region} extractor: {e}")
        return False

def show_status():
    """Show the status of all regions"""
    try:
        logger.info("📊 Checking status of all regions...")
        
        regions = ['NRLDC', 'ERLDC', 'WRPC']
        
        for region in regions:
            master_dir = Path(f"master_data/{region}")
            if master_dir.exists():
                # Look for master datasets
                master_files = list(master_dir.glob(f"{region}_Master_Dataset_*.csv"))
                summary_files = list(master_dir.glob(f"{region}_Summary.json"))
                
                if master_files:
                    latest_file = max(master_files, key=lambda x: x.stat().st_mtime)
                    file_size = latest_file.stat().st_size / (1024 * 1024)  # MB
                    logger.info(f"✅ {region}: Latest dataset {latest_file.name} ({file_size:.2f} MB)")
                else:
                    logger.warning(f"⚠️ {region}: No master datasets found")
                
                if summary_files:
                    logger.info(f"   📋 Summary files: {len(summary_files)}")
                else:
                    logger.warning(f"   ⚠️ No summary files found")
            else:
                logger.warning(f"⚠️ {region}: Master data directory not found")
        
        # Check global master dataset
        global_master_dir = Path("master_data/GLOBAL")
        if global_master_dir.exists():
            global_files = list(global_master_dir.glob("GLOBAL_Master_Dataset_*.csv"))
            if global_files:
                latest_global = max(global_files, key=lambda x: x.stat().st_mtime)
                file_size = latest_global.stat().st_size / (1024 * 1024)  # MB
                logger.info(f"✅ GLOBAL: Latest dataset {latest_global.name} ({file_size:.2f} MB)")
            else:
                logger.warning(f"⚠️ GLOBAL: No master datasets found")
        else:
            logger.warning(f"⚠️ GLOBAL: Master data directory not found")
            
    except Exception as e:
        logger.error(f"❌ Error checking status: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Energy Data Extractors Runner')
    parser.add_argument('--region', '-r', choices=['NRLDC', 'ERLDC', 'WRPC'], 
                       help='Run extractor for specific region only')
    parser.add_argument('--status', '-s', action='store_true',
                       help='Show status of all regions')
    parser.add_argument('--all', '-a', action='store_true',
                       help='Run all extractors (default)')
    
    args = parser.parse_args()
    
    if args.status:
        show_status()
    elif args.region:
        run_single_extractor(args.region)
    else:
        # Default: run all extractors
        run_all_extractors()
