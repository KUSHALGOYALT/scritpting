#!/usr/bin/env python3
"""Auto S3 Upload"""

import boto3
import os
import logging
from datetime import datetime
import re
from dotenv import load_dotenv, find_dotenv

# Load environment variables from nearest .env file (search upward)
try:
    dotenv_path = find_dotenv()
    if dotenv_path:
        load_dotenv(dotenv_path, override=True)
    else:
        load_dotenv()
except Exception:
    # Safe fallback
    load_dotenv()

logger = logging.getLogger(__name__)

class AutoS3Uploader:
    def __init__(self):
        # Load AWS credentials from environment variables (support alternate keys)
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('HEXAA_AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('HEXAA_AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME', 'hexa-energy-data-repository')
        self.aws_profile = os.getenv('AWS_PROFILE')
        
        try:
            # Prefer explicit keys if provided; otherwise use default credential chain (env, shared config, IAM, etc.)
            if self.aws_profile:
                session = boto3.Session(profile_name=self.aws_profile, region_name=self.aws_region)
                self.s3_client = session.client('s3')
            elif self.aws_access_key and self.aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key,
                    region_name=self.aws_region
                )
            else:
                self.s3_client = boto3.client('s3', region_name=self.aws_region)

            # Optional: validate access by a lightweight call
            try:
                # Head on bucket to verify access if bucket exists
                if self.bucket_name:
                    self.s3_client.head_bucket(Bucket=self.bucket_name)
            except Exception:
                # Don't disable; bucket might not exist yet or permissions limited
                pass

            self.enabled = True
            logger.info("✅ Auto S3 upload enabled (credentials resolved)")
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
        elif 'srpc' in path_str:
            return 'SRPC'
        elif 'nerpc' in path_str:
            return 'NERPC'
        
        # Check filename patterns
        if any(pattern in filename for pattern in ['nrldc', 'supporting_files']):
            return 'NRLDC'
        elif any(pattern in filename for pattern in ['wrpc', 'western', 'sum1', 'sum2']):
            return 'WRPC'
        elif any(pattern in filename for pattern in ['erldc', 'erpc', 'eastern']):
            return 'ERLDC'
        elif any(pattern in filename for pattern in ['srpc', 'southern']):
            return 'SRPC'
        elif any(pattern in filename for pattern in ['nerpc', 'north_eastern', 'northeastern']):
            return 'NERPC'
        
        # Check for specific file patterns
        if filename.startswith('supporting_files_'):
            return 'NRLDC'
        elif filename.endswith('sum1.zip') or filename.endswith('sum2.zip') or 'sum1a.zip' in filename:
            return 'WRPC'
        elif 'dsm_data' in filename and any(region in filename for region in ['bseb', 'dvc', 'gridco', 'jbvnl', 'sikkim', 'wbseb']):
            return 'ERLDC'
        
        return 'UNKNOWN'

    def _extract_station_from_name(self, name: str) -> str:
        """Best-effort station/entity extraction from a filename for SRPC/NERPC."""
        try:
            base = os.path.splitext(os.path.basename(name))[0]
            base_upper = re.sub(r'[^A-Za-z0-9_]', '_', base.upper())
            tokens = [t for t in re.split(r'_+', base_upper) if t]
            if not tokens:
                return 'UNKNOWN'
            # Remove generic tokens
            skip = {
                'SRPC','NERPC','NRLDC','WRPC','ERLDC','DSM','SRAS','TRAS','REA','SCUC','DATA','FILE','ZIP','PARQUET',
                'CSV','XLS','XLSX','RAW','PARQUET','SUPPORTING','FILES','MASTER','SUMMARY','WEEK','WK'
            }
            filtered = [t for t in tokens if t not in skip and not re.fullmatch(r'\d{6,8}', t)]
            return filtered[0] if filtered else 'UNKNOWN'
        except Exception:
            return 'UNKNOWN'

    def generate_readable_filename(self, original_filename, region, date_str):
        """Generate a readable filename for S3 storage"""
        # Extract base name without extension
        base_name = os.path.splitext(original_filename)[0]
         # Normalize extension: remove leading dot for consistent formatting
        ext = os.path.splitext(original_filename)[1].lstrip('.')
        
        # Create readable filename
        if region == 'NRLDC':
            if 'supporting_files' in base_name.lower():
                # Extract week info from NRLDC files
                week_match = re.search(r'(\d{6}-\d{6}.*?)(?:\(WK-\d+\))?', base_name)
                if week_match:
                    week_info = week_match.group(1)
                    return f"NRLDC_Supporting_Files_{week_info}_{date_str}.{ext}"
                else:
                    return f"NRLDC_Supporting_Files_{date_str}.{ext}"
            else:
                return f"NRLDC_{base_name}_{date_str}.{ext}"
        
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
                return f"WRPC_{base_name}_{date_str}.{ext}"
        
        elif region == 'ERLDC':
            if 'dsm_data' in base_name.lower():
                # Extract entity info from ERLDC files
                entity_match = re.search(r'ERLDC_(\w+)_DSM_Data', base_name)
                if entity_match:
                    entity = entity_match.group(1)
                    return f"ERLDC_{entity}_DSM_Data_{date_str}.{ext}"
                else:
                    return f"ERLDC_DSM_Data_{date_str}.{ext}"
            else:
                return f"ERLDC_{base_name}_{date_str}.{ext}"
        
        elif region == 'SRPC':
            station = self._extract_station_from_name(original_filename)
            if station != 'UNKNOWN':
                return f"SRPC_{station}_{date_str}.{ext}"
            return f"SRPC_{base_name}_{date_str}.{ext}"
        
        elif region == 'NERPC':
            station = self._extract_station_from_name(original_filename)
            if station != 'UNKNOWN':
                return f"NERPC_{station}_{date_str}.{ext}"
            return f"NERPC_{base_name}_{date_str}.{ext}"
        
        else:
            return f"{region}_{base_name}_{date_str}.{ext}"

    def auto_upload_file(self, local_path, original_filename=None):
        if not self.enabled:
            return False
        
        try:
            if original_filename is None:
                original_filename = os.path.basename(local_path)
            
            # If caller provided a full S3 key under our namespace, honor it exactly
            if isinstance(original_filename, str) and original_filename.startswith('dsm_data/'):
                s3_key = original_filename
                self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
                logger.info(f"📤 Uploaded to s3://{s3_key}")
                # Assume caller manages parquet generation for pre-partitioned paths
                return True
            
            region = self.determine_region(original_filename)  # Use original filename for region detection
            date_str = self.extract_date_from_filename(original_filename)
            
            # Generate readable filename
            readable_filename = self.generate_readable_filename(original_filename, region, date_str)
            s3_key = f"dsm_data/raw/{region}/{date_str}/{readable_filename}"
            
            # Upload raw file
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"📤 Auto-uploaded: {readable_filename} to raw/{region}/")
            
            # Optional auto-convert to parquet (disabled by default). Enable with AUTO_S3_PARQUET=true
            if os.getenv('AUTO_S3_PARQUET', 'false').lower() == 'true':
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
