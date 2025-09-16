#!/usr/bin/env python3
"""
Run only the NERPC extractor for testing
"""

import sys
import os
from pathlib import Path

# Add the extractors directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'extractors', 'nerpc'))

from nerpc_extractor import NERPCDynamicExtractor

def main():
    """Run the NERPC extractor"""
    print("🚀 Running NERPC Extractor...")
    
    try:
        # Initialize extractor
        extractor = NERPCDynamicExtractor()
        
        # Run extraction
        result = extractor.run_extraction()
        
        if result['status'] == 'success':
            print(f"✅ Successfully processed {result['files_processed']} files")
            if result.get('master_dataset'):
                print(f"📊 Master dataset created: {result['master_dataset']}")
        else:
            print(f"❌ Extraction failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
