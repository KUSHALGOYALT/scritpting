#!/usr/bin/env python3
"""Auto S3 Upload"""

import boto3
import os
import logging
from datetime import datetime
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class AutoS3Uploader:
    def __init__(self):
        # Load AWS credentials from environment variables
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME', 'hexa-energy-data-repository')
        
        if not self.aws_access_key or not self.aws_secret_key:
            logger.error("❌ AWS credentials not found in environment variables")
            logger.error("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file")
            self.enabled = False
            return
        
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.aws_region
            )
            self.enabled = True
            logger.info("✅ Auto S3 upload enabled")
        except Exception as e:
            logger.error(f"❌ S3 upload disabled: {e}")
            self.enabled = False

    def extract_date_from_filename(self, filename):
        patterns = [
            (r'(\d{6})-\d{6}', '%d%m%y'),
            (r'^(\d{8})', '%d%m%Y'),
            (r'(\d{6})', '%d%m%y'),
        ]
        
        for pattern, date_format in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    date_obj = datetime.strptime(match.group(1), date_format)
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        
        return datetime.now().strftime('%Y-%m-%d')

    def determine_region(self, file_path):
        """Determine region from file path or filename"""
        path_str = str(file_path).lower()
        filename = os.path.basename(file_path).lower()
        
        # Check path first
        if 'nrldc' in path_str:
            return 'NRLDC'
        elif 'wrpc' in path_str:
            return 'WRPC'
        elif 'erldc' in path_str:
            return 'ERLDC'
        
        # Check filename patterns
        if any(pattern in filename for pattern in ['nrldc', 'nrpc', 'supporting_files']):
            return 'NRLDC'
        elif any(pattern in filename for pattern in ['wrpc', 'western', 'sum1', 'sum2']):
            return 'WRPC'
        elif any(pattern in filename for pattern in ['erldc', 'erpc', 'eastern']):
            return 'ERLDC'
        
        # Check for specific file patterns
        if filename.startswith('supporting_files_'):
            return 'NRLDC'
        elif filename.endswith('sum1.zip') or filename.endswith('sum2.zip') or 'sum1a.zip' in filename:
            return 'WRPC'
        elif 'dsm_data' in filename and any(region in filename for region in ['bseb', 'dvc', 'gridco', 'jbvnl', 'sikkim', 'wbseb']):
            return 'ERLDC'
        
        return 'UNKNOWN'

    def generate_readable_filename(self, original_filename, region, date_str):
        """Generate a readable filename for S3 storage"""
        # Extract base name without extension
        base_name = os.path.splitext(original_filename)[0]
        
        # Create readable filename
        if region == 'NRLDC':
            if 'supporting_files' in base_name.lower():
                # Extract week info from NRLDC files
                week_match = re.search(r'(\d{6}-\d{6}.*?)(?:\(WK-\d+\))?', base_name)
                if week_match:
                    week_info = week_match.group(1)
                    return f"NRLDC_Supporting_Files_{week_info}_{date_str}.{os.path.splitext(original_filename)[1]}"
                else:
                    return f"NRLDC_Supporting_Files_{date_str}.{os.path.splitext(original_filename)[1]}"
            else:
                return f"NRLDC_{base_name}_{date_str}.{os.path.splitext(original_filename)[1]}"
        
        elif region == 'WRPC':
            if original_filename.endswith('.zip'):
                # Extract date from WRPC zip files
                date_match = re.search(r'(\d{8})', original_filename)
                if date_match:
                    file_date = date_match.group(1)
                    return f"WRPC_DSM_Data_{file_date}.zip"
                else:
                    return f"WRPC_DSM_Data_{date_str}.zip"
            else:
                return f"WRPC_{base_name}_{date_str}.{os.path.splitext(original_filename)[1]}"
        
        elif region == 'ERLDC':
            if 'dsm_data' in base_name.lower():
                # Extract entity info from ERLDC files
                entity_match = re.search(r'ERLDC_(\w+)_DSM_Data', base_name)
                if entity_match:
                    entity = entity_match.group(1)
                    return f"ERLDC_{entity}_DSM_Data_{date_str}.{os.path.splitext(original_filename)[1]}"
                else:
                    return f"ERLDC_DSM_Data_{date_str}.{os.path.splitext(original_filename)[1]}"
            else:
                return f"ERLDC_{base_name}_{date_str}.{os.path.splitext(original_filename)[1]}"
        
        else:
            return f"{region}_{base_name}_{date_str}.{os.path.splitext(original_filename)[1]}"

    def auto_upload_file(self, local_path, original_filename=None):
        if not self.enabled:
            return False
        
        try:
            if original_filename is None:
                original_filename = os.path.basename(local_path)
            
            region = self.determine_region(original_filename)  # Use original filename for region detection
            date_str = self.extract_date_from_filename(original_filename)
            
            # Generate readable filename
            readable_filename = self.generate_readable_filename(original_filename, region, date_str)
            s3_key = f"dsm_data/raw/{region}/{date_str}/{readable_filename}"
            
            # Upload raw file
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"📤 Auto-uploaded: {readable_filename} to raw/{region}/")
            
            # Auto-convert to parquet
            try:
                from parquet_processor import parquet_processor
                success = parquet_processor.process_raw_file_to_parquet(s3_key)
                if success:
                    logger.info(f"🔄 Auto-converted to parquet: {readable_filename}")
                else:
                    logger.warning(f"⚠️ Parquet conversion failed: {readable_filename}")
            except ImportError:
                logger.warning("⚠️ Parquet processor not available")
            except Exception as e:
                logger.error(f"❌ Parquet conversion error: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Auto-upload failed for {original_filename}: {e}")
            return False

auto_uploader = AutoS3Uploader()
