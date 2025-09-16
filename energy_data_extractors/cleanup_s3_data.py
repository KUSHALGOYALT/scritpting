#!/usr/bin/env python3
"""
S3 Data Cleanup Script
Deletes all data from S3 raw and parquet directories for all regions
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

class S3DataCleaner:
    """Clean up S3 data from raw and parquet directories"""
    
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
    
    def list_objects_with_prefix(self, prefix: str) -> List[Dict]:
        """List all objects with a given prefix"""
        objects = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    objects.extend(page['Contents'])
            
            logger.info(f"📋 Found {len(objects)} objects with prefix: {prefix}")
            return objects
            
        except ClientError as e:
            logger.error(f"❌ Error listing objects with prefix {prefix}: {e}")
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
    
    def cleanup_region_data(self, region: str) -> Dict[str, int]:
        """Clean up all data for a specific region"""
        logger.info(f"🧹 Starting cleanup for region: {region}")
        
        results = {
            'raw_deleted': 0,
            'parquet_deleted': 0,
            'total_deleted': 0
        }
        
        # Clean up raw data
        raw_prefix = f"raw/{region}/"
        raw_objects = self.list_objects_with_prefix(raw_prefix)
        if raw_objects:
            results['raw_deleted'] = self.delete_objects(raw_objects)
            logger.info(f"✅ Deleted {results['raw_deleted']} raw files for {region}")
        
        # Clean up parquet data
        parquet_prefix = f"parquet/{region}/"
        parquet_objects = self.list_objects_with_prefix(parquet_prefix)
        if parquet_objects:
            results['parquet_deleted'] = self.delete_objects(parquet_objects)
            logger.info(f"✅ Deleted {results['parquet_deleted']} parquet files for {region}")
        
        results['total_deleted'] = results['raw_deleted'] + results['parquet_deleted']
        
        return results
    
    def cleanup_all_regions(self) -> Dict[str, Dict[str, int]]:
        """Clean up data for all regions"""
        regions = ['SRPC', 'NERPC', 'WRPC', 'ERLDC', 'NRLDC']
        
        logger.info(f"🚀 Starting cleanup for all regions: {regions}")
        
        total_results = {}
        grand_total = 0
        
        for region in regions:
            try:
                region_results = self.cleanup_region_data(region)
                total_results[region] = region_results
                grand_total += region_results['total_deleted']
                
                logger.info(f"✅ {region} cleanup complete: {region_results['total_deleted']} files deleted")
                
            except Exception as e:
                logger.error(f"❌ Error cleaning up {region}: {e}")
                total_results[region] = {'raw_deleted': 0, 'parquet_deleted': 0, 'total_deleted': 0}
        
        logger.info(f"🎉 Total cleanup complete: {grand_total} files deleted across all regions")
        return total_results
    
    def cleanup_specific_paths(self, paths: List[str]) -> int:
        """Clean up specific S3 paths"""
        logger.info(f"🎯 Cleaning up specific paths: {paths}")
        
        total_deleted = 0
        
        for path in paths:
            objects = self.list_objects_with_prefix(path)
            if objects:
                deleted = self.delete_objects(objects)
                total_deleted += deleted
                logger.info(f"✅ Deleted {deleted} files from {path}")
            else:
                logger.info(f"ℹ️ No files found in {path}")
        
        return total_deleted
    
    def list_bucket_contents(self, prefix: str = "") -> None:
        """List all contents in the bucket (for verification)"""
        logger.info(f"📋 Listing bucket contents with prefix: {prefix}")
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            total_objects = 0
            total_size = 0
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_objects += 1
                        total_size += obj['Size']
                        logger.info(f"📄 {obj['Key']} ({obj['Size']} bytes)")
            
            logger.info(f"📊 Total: {total_objects} objects, {total_size / (1024**3):.2f} GB")
            
        except ClientError as e:
            logger.error(f"❌ Error listing bucket contents: {e}")


def main():
    """Main function to run the cleanup"""
    try:
        cleaner = S3DataCleaner()
        
        print("🧹 S3 Data Cleanup Tool")
        print("=" * 50)
        print("1. Clean all regions (SRPC, NERPC, WRPC, ERLDC, NRLDC)")
        print("2. Clean specific region")
        print("3. Clean specific paths")
        print("4. List bucket contents")
        print("5. Exit")
        print("=" * 50)
        
        while True:
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                print("\n⚠️ WARNING: This will delete ALL data from raw and parquet directories!")
                confirm = input("Type 'DELETE ALL' to confirm: ").strip()
                if confirm == 'DELETE ALL':
                    results = cleaner.cleanup_all_regions()
                    print("\n📊 Cleanup Results:")
                    for region, stats in results.items():
                        print(f"  {region}: {stats['total_deleted']} files deleted")
                else:
                    print("❌ Cleanup cancelled")
            
            elif choice == '2':
                region = input("Enter region name (SRPC/NERPC/WRPC/ERLDC/NRLDC): ").strip().upper()
                if region in ['SRPC', 'NERPC', 'WRPC', 'ERLDC', 'NRLDC']:
                    print(f"\n⚠️ WARNING: This will delete ALL data for {region}!")
                    confirm = input("Type 'DELETE' to confirm: ").strip()
                    if confirm == 'DELETE':
                        results = cleaner.cleanup_region_data(region)
                        print(f"\n📊 {region} Cleanup Results:")
                        print(f"  Raw files deleted: {results['raw_deleted']}")
                        print(f"  Parquet files deleted: {results['parquet_deleted']}")
                        print(f"  Total files deleted: {results['total_deleted']}")
                    else:
                        print("❌ Cleanup cancelled")
                else:
                    print("❌ Invalid region name")
            
            elif choice == '3':
                print("Enter S3 paths to clean (one per line, empty line to finish):")
                paths = []
                while True:
                    path = input("Path: ").strip()
                    if not path:
                        break
                    paths.append(path)
                
                if paths:
                    print(f"\n⚠️ WARNING: This will delete data from {len(paths)} paths!")
                    confirm = input("Type 'DELETE' to confirm: ").strip()
                    if confirm == 'DELETE':
                        deleted = cleaner.cleanup_specific_paths(paths)
                        print(f"\n📊 Cleanup Results: {deleted} files deleted")
                    else:
                        print("❌ Cleanup cancelled")
                else:
                    print("❌ No paths provided")
            
            elif choice == '4':
                prefix = input("Enter prefix to filter (or press Enter for all): ").strip()
                cleaner.list_bucket_contents(prefix)
            
            elif choice == '5':
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice. Please enter 1-5.")
    
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
