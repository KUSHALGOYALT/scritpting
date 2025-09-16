#!/usr/bin/env python3
"""
Master Data Cleanup Script
Deletes all master data files from S3 for all regions
"""

import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
import logging
from typing import List, Dict

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MasterDataCleaner:
    """Clean up master data from S3 for all regions"""
    
    def __init__(self):
        """Initialize S3 client"""
        try:
            # Initialize S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_REGION', 'us-east-1')
            )
            
            self.bucket_name = os.getenv('AWS_S3_BUCKET')
            if not self.bucket_name:
                raise ValueError("AWS_S3_BUCKET environment variable not set")
            
            logger.info(f"✅ S3 client initialized for bucket: {self.bucket_name}")
            
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found. Please check your .env file")
            raise
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3 client: {e}")
            raise
    
    def list_master_data_objects(self) -> List[Dict]:
        """List all master data objects in S3"""
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
        
        all_objects = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Check if object matches any master data pattern
                        if any(pattern.lower() in key.lower() for pattern in master_data_patterns):
                            all_objects.append(obj)
                            logger.info(f"📄 Found master data: {key}")
            
            logger.info(f"📋 Found {len(all_objects)} master data objects")
            return all_objects
            
        except ClientError as e:
            logger.error(f"❌ Error listing objects: {e}")
            return []
    
    def list_region_master_data(self, region: str) -> List[Dict]:
        """List master data objects for a specific region"""
        region_patterns = [
            f'master_data/{region}/',
            f'{region}_MASTER_',
            f'{region}_Master_',
            f'{region}_master_',
            f'MASTER_{region}',
            f'Master_{region}',
            f'master_{region}'
        ]
        
        region_objects = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Check if object matches any region master data pattern
                        if any(pattern.lower() in key.lower() for pattern in region_patterns):
                            region_objects.append(obj)
                            logger.info(f"📄 Found {region} master data: {key}")
            
            logger.info(f"📋 Found {len(region_objects)} {region} master data objects")
            return region_objects
            
        except ClientError as e:
            logger.error(f"❌ Error listing {region} objects: {e}")
            return []
    
    def delete_objects(self, objects: List[Dict]) -> int:
        """Delete multiple objects from S3"""
        if not objects:
            return 0
        
        try:
            # Prepare delete request
            delete_objects = [{'Key': obj['Key']} for obj in objects]
            
            # S3 allows up to 1000 objects per delete request
            deleted_count = 0
            batch_size = 1000
            
            for i in range(0, len(delete_objects), batch_size):
                batch = delete_objects[i:i + batch_size]
                
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        'Objects': batch,
                        'Quiet': False
                    }
                )
                
                # Count successful deletions
                if 'Deleted' in response:
                    deleted_count += len(response['Deleted'])
                
                # Log any errors
                if 'Errors' in response:
                    for error in response['Errors']:
                        logger.error(f"❌ Failed to delete {error['Key']}: {error['Message']}")
                
                logger.info(f"🗑️ Deleted batch {i//batch_size + 1}: {len(batch)} objects")
            
            return deleted_count
            
        except ClientError as e:
            logger.error(f"❌ Error deleting objects: {e}")
            return 0
    
    def cleanup_all_master_data(self) -> int:
        """Clean up all master data from S3"""
        logger.info("🧹 Starting cleanup of all master data")
        
        objects = self.list_master_data_objects()
        if not objects:
            logger.info("ℹ️ No master data objects found")
            return 0
        
        return self.delete_objects(objects)
    
    def cleanup_region_master_data(self, region: str) -> int:
        """Clean up master data for a specific region"""
        logger.info(f"🧹 Starting cleanup of {region} master data")
        
        objects = self.list_region_master_data(region)
        if not objects:
            logger.info(f"ℹ️ No {region} master data objects found")
            return 0
        
        return self.delete_objects(objects)
    
    def list_all_master_data(self) -> None:
        """List all master data objects (for verification)"""
        logger.info("📋 Listing all master data objects")
        
        objects = self.list_master_data_objects()
        
        if not objects:
            logger.info("ℹ️ No master data objects found")
            return
        
        total_size = 0
        regions = {}
        
        for obj in objects:
            key = obj['Key']
            size = obj['Size']
            total_size += size
            
            # Try to determine region from key
            region = "Unknown"
            for r in ['SRPC', 'NERPC', 'WRPC', 'ERLDC', 'NRLDC', 'GLOBAL']:
                if r.lower() in key.lower():
                    region = r
                    break
            
            if region not in regions:
                regions[region] = 0
            regions[region] += 1
            
            logger.info(f"📄 {key} ({size} bytes)")
        
        logger.info(f"📊 Summary:")
        logger.info(f"  Total objects: {len(objects)}")
        logger.info(f"  Total size: {total_size / (1024**3):.2f} GB")
        logger.info(f"  By region: {regions}")


def main():
    """Main function to run the master data cleanup"""
    try:
        cleaner = MasterDataCleaner()
        
        print("🧹 Master Data Cleanup Tool")
        print("=" * 50)
        print("1. List all master data (no deletion)")
        print("2. Clean all master data")
        print("3. Clean specific region master data")
        print("4. Exit")
        print("=" * 50)
        
        while True:
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == '1':
                cleaner.list_all_master_data()
            
            elif choice == '2':
                print("\n⚠️ WARNING: This will delete ALL master data from S3!")
                confirm = input("Type 'DELETE ALL MASTER DATA' to confirm: ").strip()
                if confirm == 'DELETE ALL MASTER DATA':
                    deleted = cleaner.cleanup_all_master_data()
                    print(f"\n📊 Cleanup Results: {deleted} master data files deleted")
                else:
                    print("❌ Cleanup cancelled")
            
            elif choice == '3':
                region = input("Enter region name (SRPC/NERPC/WRPC/ERLDC/NRLDC): ").strip().upper()
                if region in ['SRPC', 'NERPC', 'WRPC', 'ERLDC', 'NRLDC']:
                    print(f"\n⚠️ WARNING: This will delete ALL {region} master data!")
                    confirm = input(f"Type 'DELETE {region} MASTER DATA' to confirm: ").strip()
                    if confirm == f'DELETE {region} MASTER DATA':
                        deleted = cleaner.cleanup_region_master_data(region)
                        print(f"\n📊 {region} Cleanup Results: {deleted} master data files deleted")
                    else:
                        print("❌ Cleanup cancelled")
                else:
                    print("❌ Invalid region name")
            
            elif choice == '4':
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice. Please enter 1-4.")
    
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
