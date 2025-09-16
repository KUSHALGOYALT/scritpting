#!/usr/bin/env python3
"""
Reupload All Data with Correct Structure
Reuploads all data with the correct S3 path structure:
- Raw: dsm_data/raw/REGION/year/filename (exact filename from web)
- Parquet: dsm_data/parquet/REGION/station/year/month/filename
"""

import os
import sys
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_nerpc():
    """Run NERPC extractor with corrected paths"""
    try:
        logger.info("🚀 Starting NERPC reupload with corrected paths...")
        os.chdir("extractors/nerpc")
        
        from nerpc_extractor import NERPCDynamicExtractor
        extractor = NERPCDynamicExtractor()
        
        # Run extraction
        result = extractor.run_extraction()
        logger.info(f"✅ NERPC reupload completed: {result}")
        
        os.chdir("../..")
        return True
    except Exception as e:
        logger.error(f"❌ NERPC reupload failed: {e}")
        os.chdir("../..")
        return False

def run_wrpc():
    """Run WRPC extractor with corrected paths"""
    try:
        logger.info("🚀 Starting WRPC reupload with corrected paths...")
        os.chdir("extractors/wrpc")
        
        from wrpc_extractor import WRPCDynamicExtractor
        extractor = WRPCDynamicExtractor()
        
        # Run extraction
        result = extractor.run_extraction()
        logger.info(f"✅ WRPC reupload completed: {result}")
        
        os.chdir("../..")
        return True
    except Exception as e:
        logger.error(f"❌ WRPC reupload failed: {e}")
        os.chdir("../..")
        return False

def run_erldc():
    """Run ERLDC extractor with corrected paths"""
    try:
        logger.info("🚀 Starting ERLDC reupload with corrected paths...")
        os.chdir("extractors/erldc")
        
        from erldc_extractor import ERLDCDynamicExtractor
        extractor = ERLDCDynamicExtractor()
        
        # Run extraction
        result = extractor.run_extraction()
        logger.info(f"✅ ERLDC reupload completed: {result}")
        
        os.chdir("../..")
        return True
    except Exception as e:
        logger.error(f"❌ ERLDC reupload failed: {e}")
        os.chdir("../..")
        return False

def run_srpc():
    """Run SRPC extractor with corrected paths"""
    try:
        logger.info("🚀 Starting SRPC reupload with corrected paths...")
        os.chdir("extractors/srpc")
        
        from srpc_extractor import SRPCExtractor
        extractor = SRPCExtractor()
        
        # Run extraction for last 7 days
        result = extractor.discover_last_7_days()
        logger.info(f"✅ SRPC reupload completed: {result}")
        
        os.chdir("../..")
        return True
    except Exception as e:
        logger.error(f"❌ SRPC reupload failed: {e}")
        os.chdir("../..")
        return False

def run_nrldc():
    """Run NRLDC extractor with corrected paths"""
    try:
        logger.info("🚀 Starting NRLDC reupload with corrected paths...")
        os.chdir("extractors/nrldc")
        
        from nrldc_extractor import NRLDCWorkingDSAExtractor
        extractor = NRLDCWorkingDSAExtractor()
        
        # Run extraction
        result = extractor.run_extraction()
        logger.info(f"✅ NRLDC reupload completed: {result}")
        
        os.chdir("../..")
        return True
    except Exception as e:
        logger.error(f"❌ NRLDC reupload failed: {e}")
        os.chdir("../..")
        return False

def main():
    """Main function to reupload all data with correct structure"""
    print("🔄 Reuploading All Data with Correct S3 Structure")
    print("=" * 60)
    print("📁 Corrected S3 Paths:")
    print("   Raw: dsm_data/raw/REGION/year/filename")
    print("   Parquet: dsm_data/parquet/REGION/station/year/month/filename")
    print()
    
    start_time = datetime.now()
    results = {}
    
    # Run all extractors
    extractors = [
        ("NERPC", run_nerpc),
        ("WRPC", run_wrpc), 
        ("ERLDC", run_erldc),
        ("SRPC", run_srpc),
        ("NRLDC", run_nrldc)
    ]
    
    for region, func in extractors:
        print(f"\n{'='*20} {region} {'='*20}")
        try:
            success = func()
            results[region] = "✅ SUCCESS" if success else "❌ FAILED"
        except Exception as e:
            logger.error(f"❌ {region} failed with exception: {e}")
            results[region] = "❌ EXCEPTION"
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*60}")
    print("📊 REUPLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"⏱️ Total Duration: {duration}")
    print()
    
    for region, status in results.items():
        print(f"   {region}: {status}")
    
    success_count = sum(1 for status in results.values() if "✅" in status)
    total_count = len(results)
    
    print(f"\n🎯 Overall Result: {success_count}/{total_count} regions successful")
    
    if success_count == total_count:
        print("🎉 ALL REGIONS REUPLOADED SUCCESSFULLY!")
        print("\n📁 Files are now stored with correct S3 structure:")
        print("   • Raw files: dsm_data/raw/REGION/year/filename")
        print("   • Parquet files: dsm_data/parquet/REGION/station/year/month/filename")
    else:
        print("⚠️ Some regions failed. Check logs for details.")
    
    return success_count == total_count

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
