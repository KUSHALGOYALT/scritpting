#!/usr/bin/env python3
"""
Run all energy data extractors to post data to S3
"""

import sys
import os
import logging
from datetime import datetime

# Add the extractors directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'extractors'))

# Import all extractors
from srpc.srpc_extractor import SRPCExtractor
from nerpc.nerpc_extractor import NERPCExtractor
from wrpc.wrpc_extractor import WRPCExtractor
from erldc.erldc_extractor import ERLDCExtractor
from nrldc.nrldc_extractor import NRLDCExtractor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_all_extractors():
    """Run all energy data extractors"""
    logger.info("🚀 Starting all energy data extractors")
    
    extractors = [
        ("SRPC", SRPCExtractor),
        ("NERPC", NERPCExtractor),
        ("WRPC", WRPCExtractor),
        ("ERLDC", ERLDCExtractor),
        ("NRLDC", NRLDCExtractor)
    ]
    
    results = {}
    
    for name, extractor_class in extractors:
        try:
            logger.info(f"🔄 Running {name} extractor...")
            extractor = extractor_class()
            
            # Run the extraction
            result = extractor.run_extraction()
            results[name] = {
                'status': 'success',
                'result': result
            }
            logger.info(f"✅ {name} extractor completed successfully")
            
        except Exception as e:
            logger.error(f"❌ {name} extractor failed: {e}")
            results[name] = {
                'status': 'error',
                'error': str(e)
            }
    
    # Summary
    logger.info("📊 Extraction Summary:")
    successful = 0
    failed = 0
    
    for name, result in results.items():
        if result['status'] == 'success':
            logger.info(f"  ✅ {name}: SUCCESS")
            successful += 1
        else:
            logger.info(f"  ❌ {name}: FAILED - {result['error']}")
            failed += 1
    
    logger.info(f"🎯 Final Results: {successful} successful, {failed} failed")
    
    return results

if __name__ == "__main__":
    results = run_all_extractors()
    if all(result['status'] == 'success' for result in results.values()):
        logger.info("🎉 All extractors completed successfully!")
        sys.exit(0)
    else:
        logger.error("⚠️ Some extractors failed. Check logs above.")
        sys.exit(1)
