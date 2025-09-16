#!/usr/bin/env python3
"""
Quick S3 Data Cleanup Script
Simple script to delete all raw and parquet data
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

def delete_s3_data():
    """Delete all raw and parquet data from S3"""
    
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
        
        # Define prefixes to delete
        prefixes_to_delete = [
            'raw/',
            'parquet/'
        ]
        
        total_deleted = 0
        
        for prefix in prefixes_to_delete:
            logger.info(f"🗑️ Deleting all objects with prefix: {prefix}")
            
            # List all objects with this prefix
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            objects_to_delete = []
            for page in pages:
                if 'Contents' in page:
                    objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
            
            if objects_to_delete:
                logger.info(f"📋 Found {len(objects_to_delete)} objects to delete")
                
                # Delete objects in batches of 1000
                batch_size = 1000
                for i in range(0, len(objects_to_delete), batch_size):
                    batch = objects_to_delete[i:i + batch_size]
                    
                    response = s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={
                            'Objects': batch,
                            'Quiet': False
                        }
                    )
                    
                    deleted_count = len(response.get('Deleted', []))
                    total_deleted += deleted_count
                    
                    # Log any errors
                    if 'Errors' in response:
                        for error in response['Errors']:
                            logger.error(f"❌ Failed to delete {error['Key']}: {error['Message']}")
                    
                    logger.info(f"✅ Deleted batch: {deleted_count} objects")
            else:
                logger.info(f"ℹ️ No objects found with prefix: {prefix}")
        
        logger.info(f"🎉 Cleanup complete! Total files deleted: {total_deleted}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return False

def main():
    """Main function"""
    print("🧹 Quick S3 Data Cleanup")
    print("=" * 40)
    print("This will delete ALL data from:")
    print("  - raw/ directory")
    print("  - parquet/ directory")
    print("=" * 40)
    
    confirm = input("\n⚠️ Type 'DELETE ALL' to confirm: ").strip()
    
    if confirm == 'DELETE ALL':
        success = delete_s3_data()
        if success:
            print("\n✅ Cleanup completed successfully!")
        else:
            print("\n❌ Cleanup failed!")
    else:
        print("❌ Cleanup cancelled")

if __name__ == "__main__":
    main()
