#!/usr/bin/env python3
"""
Download SRPC September 1st file (010925.zip) specifically
"""
import os
import sys
import logging
import requests
import zipfile
import io
from datetime import datetime
from pathlib import Path
import urllib3
import pandas as pd

# Add parent directories to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from energy_data_extractors.common.auto_s3_upload import AutoS3Uploader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_sept1_file():
    """Download the September 1st file (010925.zip) from SRPC"""
    
    # Initialize S3 uploader
    s3_uploader = AutoS3Uploader()
    
    # September 1st file details
    date_str = "010925"  # 01/09/2025
    year = 2025
    month = 9
    filename = f"{date_str}.zip"
    
    # Construct URL
    base_url = "https://www.srpc.kar.nic.in"
    url = f"{base_url}/website/{year}/commercial/{filename}"
    
    logger.info(f"🚀 Downloading September 1st file: {filename}")
    logger.info(f"📥 URL: {url}")
    
    try:
        # Download the file with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"📥 Attempt {attempt + 1}/{max_retries} to download {filename}")
                response = requests.get(url, timeout=60, verify=False)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise e
                logger.warning(f"⚠️ Attempt {attempt + 1} failed: {e}. Retrying...")
                import time
                time.sleep(5)
        
        logger.info(f"✅ Downloaded {filename} ({len(response.content)} bytes)")
        
        # Process the ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            file_list = zip_file.namelist()
            logger.info(f"📦 ZIP contents: {file_list}")
            
            # Upload raw ZIP file to S3
            raw_key = f"dsm_data/raw/SRPC/{year}/{month:02d}/{filename}"
            try:
                # Create a temporary file for the ZIP
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                    temp_file.write(response.content)
                    temp_file_path = temp_file.name
                
                # Upload using the standard S3 client method
                s3_uploader.s3_client.upload_file(temp_file_path, s3_uploader.bucket_name, raw_key)
                logger.info(f"📤 Uploaded raw ZIP to s3://{raw_key}")
                
                # Clean up temp file
                os.unlink(temp_file_path)
            except Exception as e:
                logger.error(f"❌ Failed to upload raw ZIP: {e}")
            
            # Process each CSV in the ZIP
            for csv_name in file_list:
                if csv_name.endswith('.csv'):
                    try:
                        # Read CSV content
                        csv_content = zip_file.read(csv_name).decode('utf-8')
                        csv_df = pd.read_csv(io.StringIO(csv_content))
                        
                        logger.info(f"📊 Processing {csv_name}: {len(csv_df)} rows")
                        
                        # Upload CSV to raw S3
                        csv_filename = f"{date_str}_{csv_name}"
                        csv_raw_key = f"dsm_data/raw/SRPC/{year}/{month:02d}/{csv_filename}"
                        try:
                            # Create temporary file for upload
                            import tempfile
                            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                                temp_file.write(csv_content)
                                temp_file_path = temp_file.name
                            
                            # Use the standard upload method
                            s3_uploader.s3_client.upload_file(temp_file_path, s3_uploader.bucket_name, csv_raw_key)
                            logger.info(f"📤 Uploaded CSV to s3://{s3_uploader.bucket_name}/{csv_raw_key}")
                            
                            # Clean up temp file
                            os.unlink(temp_file_path)
                        except Exception as e:
                            logger.error(f"❌ Failed to upload CSV: {e}")
                        
                        # Process for parquet (simplified)
                        # Add date column
                        csv_df['__date__'] = datetime(year, month, 1)
                        
                        # Convert to parquet and upload
                        tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
                        
                        try:
                            csv_df.to_parquet(tmp_pq, index=False)
                            
                            # Create parquet filename
                            base_name = csv_name.replace('.csv', '')
                            pq_filename = f"SRPC_{base_name}_{year}_{month:02d}_{date_str}.parquet"
                            pq_key = f"dsm_data/parquet/SRPC/{base_name}/{year}/{month:02d}/{pq_filename}"
                            
                            # Use the standard S3 upload method
                            s3_uploader.s3_client.upload_file(str(tmp_pq), s3_uploader.bucket_name, pq_key)
                            logger.info(f"📤 Uploaded parquet to s3://{s3_uploader.bucket_name}/{pq_key}")
                            
                        except Exception as e:
                            logger.error(f"❌ Failed to upload parquet: {e}")
                        finally:
                            if tmp_pq.exists():
                                tmp_pq.unlink()
                        
                    except Exception as e:
                        logger.error(f"❌ Error processing {csv_name}: {e}")
        
        logger.info("✅ September 1st file processing completed!")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to download {filename}: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    download_sept1_file()
