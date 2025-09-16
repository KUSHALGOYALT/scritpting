#!/usr/bin/env python3
"""
Quick Master Data Cleanup Script
Simple script to delete all master data from S3
"""

import boto3
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_master_data():
    """Delete all master data from S3"""
    
    try:
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        bucket_name = os.getenv('AWS_S3_BUCKET')
        if not bucket_name:
            raise ValueError("AWS_S3_BUCKET environment variable not set")
        
        logger.info(f"✅ Connected to S3 bucket: {bucket_name}")
        
        # Define master data patterns to search for
        master_data_patterns = [
            'master_data/',
            'MASTER_',
            '_MASTER_',
            '_Master_',
            'master_dataset',
            'Master_Dataset',
            'master_summary',
            'Master_Summary'
        ]
        
        logger.info("🔍 Searching for master data objects...")
        
        # List all objects and filter for master data
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        master_data_objects = []
        total_objects_checked = 0
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_objects_checked += 1
                    key = obj['Key']
                    
                    # Check if object matches any master data pattern
                    if any(pattern.lower() in key.lower() for pattern in master_data_patterns):
                        master_data_objects.append(obj)
                        logger.info(f"📄 Found master data: {key}")
        
        logger.info(f"📊 Search complete: {total_objects_checked} objects checked")
        logger.info(f"🎯 Found {len(master_data_objects)} master data objects")
        
        if not master_data_objects:
            logger.info("ℹ️ No master data objects found to delete")
            return True
        
        # Delete master data objects
        logger.info("🗑️ Starting deletion of master data objects...")
        
        delete_objects = [{'Key': obj['Key']} for obj in master_data_objects]
        deleted_count = 0
        
        # Delete objects in batches of 1000
        batch_size = 1000
        for i in range(0, len(delete_objects), batch_size):
            batch = delete_objects[i:i + batch_size]
            
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': batch,
                    'Quiet': False
                }
            )
            
            batch_deleted = len(response.get('Deleted', []))
            deleted_count += batch_deleted
            
            # Log any errors
            if 'Errors' in response:
                for error in response['Errors']:
                    logger.error(f"❌ Failed to delete {error['Key']}: {error['Message']}")
            
            logger.info(f"✅ Deleted batch: {batch_deleted} objects")
        
        logger.info(f"🎉 Master data cleanup complete! {deleted_count} files deleted")
        return True
        
    except Exception as e:
        logger.error(f"❌ Master data cleanup failed: {e}")
        return False

def main():
    """Main function"""
    print("🧹 Quick Master Data Cleanup")
    print("=" * 40)
    print("This will delete ALL master data from S3 including:")
    print("  - master_data/ directory")
    print("  - Files with 'MASTER_' prefix")
    print("  - Files with '_MASTER_' in name")
    print("  - Files with 'Master_' prefix")
    print("  - Files with 'master_dataset' in name")
    print("  - Files with 'Master_Dataset' in name")
    print("  - Files with 'master_summary' in name")
    print("  - Files with 'Master_Summary' in name")
    print("=" * 40)
    
    confirm = input("\n⚠️ Type 'DELETE ALL MASTER DATA' to confirm: ").strip()
    
    if confirm == 'DELETE ALL MASTER DATA':
        success = delete_master_data()
        if success:
            print("\n✅ Master data cleanup completed successfully!")
        else:
            print("\n❌ Master data cleanup failed!")
    else:
        print("❌ Master data cleanup cancelled")

if __name__ == "__main__":
    main()
