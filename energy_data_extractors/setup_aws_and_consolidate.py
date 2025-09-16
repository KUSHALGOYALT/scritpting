#!/usr/bin/env python3
"""
Setup AWS credentials and create consolidated dataset
This script helps you set up AWS credentials and download all data from S3
"""

import os
import sys
import getpass
from pathlib import Path

def setup_aws_credentials():
    """Interactive setup of AWS credentials"""
    print("🔐 AWS Credentials Setup")
    print("=" * 30)
    print("You need AWS credentials to download data from S3.")
    print("If you don't have AWS credentials, please:")
    print("1. Go to AWS Console -> IAM -> Users -> Create User")
    print("2. Attach policy: AmazonS3ReadOnlyAccess")
    print("3. Create Access Key")
    print()
    
    # Get credentials from user
    access_key = getpass.getpass("Enter AWS Access Key ID: ").strip()
    secret_key = getpass.getpass("Enter AWS Secret Access Key: ").strip()
    region = input("Enter AWS Region (default: us-east-1): ").strip() or "us-east-1"
    bucket = input("Enter S3 Bucket Name (default: dsm_data): ").strip() or "dsm_data"
    
    # Create .env file content
    env_content = f"""# AWS Configuration
AWS_ACCESS_KEY_ID={access_key}
AWS_SECRET_ACCESS_KEY={secret_key}
AWS_REGION={region}
AWS_S3_BUCKET={bucket}

# Optional: Other settings
DEBUG=false
LOG_LEVEL=INFO
"""
    
    # Write to .env file
    env_file = Path(".env")
    try:
        with open(env_file, 'w') as f:
            f.write(env_content)
        print(f"✅ AWS credentials saved to {env_file.absolute()}")
        return True
    except Exception as e:
        print(f"❌ Error saving credentials: {e}")
        return False

def create_consolidated_dataset():
    """Run the consolidated dataset creation"""
    print("\n🚀 Creating consolidated dataset...")
    
    try:
        # Import and run the consolidation
        from create_consolidated_dataset import ConsolidatedDatasetCreator
        
        creator = ConsolidatedDatasetCreator()
        
        # Test S3 connection
        print("🔍 Testing S3 connection...")
        creator.s3_client.head_bucket(Bucket=creator.bucket_name)
        print("✅ S3 connection successful!")
        
        # Run consolidation
        output_files = creator.consolidate_all_data(output_format='both')
        
        if output_files:
            print("\n🎉 Consolidation completed successfully!")
            print("\n📁 Generated files:")
            for format_type, filepath in output_files.items():
                print(f"  {format_type.upper()}: {filepath}")
            return True
        else:
            print("❌ No data was consolidated!")
            return False
            
    except Exception as e:
        print(f"❌ Error during consolidation: {e}")
        return False

def main():
    """Main function"""
    print("🔧 AWS Setup and Data Consolidation Tool")
    print("=" * 50)
    
    # Check if .env file exists
    env_file = Path(".env")
    if env_file.exists():
        print("✅ Found existing .env file")
        choice = input("Do you want to use existing credentials? (y/n): ").strip().lower()
        if choice != 'y':
            setup_aws_credentials()
    else:
        print("❌ No .env file found")
        setup_aws_credentials()
    
    # Try to create consolidated dataset
    if create_consolidated_dataset():
        print("\n🎉 SUCCESS! Consolidated dataset created!")
        print("\n📋 What was created:")
        print("  - CSV file with all station data")
        print("  - Parquet file with all station data")
        print("  - Summary report with statistics")
        print("\n💡 You can now use these files for analysis!")
    else:
        print("\n❌ Failed to create consolidated dataset")
        print("\n🔍 Troubleshooting:")
        print("  1. Check your AWS credentials")
        print("  2. Verify S3 bucket access permissions")
        print("  3. Ensure the bucket contains data")

if __name__ == "__main__":
    main()
