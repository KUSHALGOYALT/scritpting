#!/usr/bin/env python3
"""
Test script to verify cleanup scripts work without AWS credentials
"""

import os
import sys
from dotenv import load_dotenv

def test_script_imports():
    """Test that cleanup scripts can be imported"""
    print("🧪 Testing cleanup script imports...")
    
    try:
        # Test cleanup_s3_data.py
        sys.path.insert(0, '.')
        from cleanup_s3_data import S3DataCleaner
        print("✅ cleanup_s3_data.py imports successfully")
        
        # Test quick_cleanup.py
        from quick_cleanup import delete_s3_data
        print("✅ quick_cleanup.py imports successfully")
        
        # Test cleanup_master_data.py
        from cleanup_master_data import MasterDataCleaner
        print("✅ cleanup_master_data.py imports successfully")
        
        # Test quick_cleanup_master.py
        from quick_cleanup_master import delete_master_data
        print("✅ quick_cleanup_master.py imports successfully")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_env_setup():
    """Test environment variable setup"""
    print("\n🔧 Testing environment setup...")
    
    # Load .env if it exists
    if os.path.exists('.env'):
        load_dotenv()
        print("✅ .env file found and loaded")
    else:
        print("⚠️ .env file not found - you'll need to create one")
    
    # Check required environment variables
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY', 
        'AWS_S3_BUCKET'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"⚠️ Missing environment variables: {missing_vars}")
        print("📋 Use the template in aws_config_template.txt to create .env file")
        return False
    else:
        print("✅ All required environment variables are set")
        return True

def show_usage():
    """Show usage instructions"""
    print("\n📖 Usage Instructions:")
    print("=" * 50)
    print("1. Create .env file with your AWS credentials:")
    print("   cp aws_config_template.txt .env")
    print("   # Then edit .env with your actual AWS credentials")
    print()
    print("2. Run the cleanup scripts:")
    print("   # General data cleanup:")
    print("   python quick_cleanup.py          # Quick deletion of raw/parquet")
    print("   python cleanup_s3_data.py        # Interactive menu for raw/parquet")
    print("   # Master data cleanup:")
    print("   python quick_cleanup_master.py   # Quick deletion of master data")
    print("   python cleanup_master_data.py    # Interactive menu for master data")
    print()
    print("3. Safety features:")
    print("   - All scripts require confirmation before deletion")
    print("   - quick_cleanup.py: Type 'DELETE ALL'")
    print("   - cleanup_s3_data.py: Interactive menu with options")
    print("   - quick_cleanup_master.py: Type 'DELETE ALL MASTER DATA'")
    print("   - cleanup_master_data.py: Interactive menu with options")
    print("=" * 50)

def main():
    """Main test function"""
    print("🧹 S3 Cleanup Scripts Test")
    print("=" * 40)
    
    # Test imports
    imports_ok = test_script_imports()
    
    # Test environment
    env_ok = test_env_setup()
    
    # Show results
    print(f"\n📊 Test Results:")
    print(f"  Scripts import: {'✅' if imports_ok else '❌'}")
    print(f"  Environment setup: {'✅' if env_ok else '⚠️'}")
    
    if imports_ok and env_ok:
        print("\n🎉 All tests passed! Scripts are ready to use.")
    elif imports_ok:
        print("\n⚠️ Scripts work but need AWS credentials setup.")
    else:
        print("\n❌ Scripts have issues that need to be fixed.")
    
    # Show usage instructions
    show_usage()

if __name__ == "__main__":
    main()
