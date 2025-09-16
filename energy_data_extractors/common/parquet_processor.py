#!/usr/bin/env python3
"""
Parquet Processor - Converts raw files to parquet format
Automatically processes uploaded raw files and saves parquet versions
"""

import boto3
import pandas as pd
import tempfile
import os
import logging
from datetime import datetime
import zipfile
import xlrd
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class ParquetProcessor:
    def __init__(self):
        # Load AWS credentials from environment variables
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME', 'hexa-energy-data-repository')
        
        if not self.aws_access_key or not self.aws_secret_key:
            logger.error("❌ AWS credentials not found in environment variables")
            logger.error("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file")
            raise ValueError("AWS credentials not configured")
        
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.aws_region
            )
            logger.info("✅ Parquet processor initialized")
        except Exception as e:
            logger.error(f"❌ Parquet processor failed: {e}")
            raise

    def download_raw_file(self, s3_key):
        """Download raw file from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read()
        except Exception as e:
            logger.error(f"❌ Failed to download {s3_key}: {e}")
            return None

    def process_excel_to_dataframe(self, file_content, filename):
        """Convert Excel content to DataFrame"""
        try:
            # Try pandas first (handles .xlsx)
            if filename.endswith('.xlsx'):
                df = pd.read_excel(BytesIO(file_content))
            else:
                # Use xlrd for .xls files
                with tempfile.NamedTemporaryFile(suffix='.xls', delete=False) as temp_file:
                    temp_file.write(file_content)
                    temp_path = temp_file.name
                
                df = pd.read_excel(temp_path, engine='xlrd')
                os.unlink(temp_path)
            
            logger.info(f"✅ Processed Excel: {filename} ({len(df)} rows, {len(df.columns)} cols)")
            return df
            
        except Exception as e:
            logger.error(f"❌ Excel processing failed for {filename}: {e}")
            return None

    def process_csv_to_dataframe(self, file_content, filename):
        """Convert CSV content to DataFrame"""
        try:
            df = pd.read_csv(BytesIO(file_content))
            logger.info(f"✅ Processed CSV: {filename} ({len(df)} rows, {len(df.columns)} cols)")
            return df
        except Exception as e:
            logger.error(f"❌ CSV processing failed for {filename}: {e}")
            return None

    def process_zip_to_dataframes(self, file_content, filename):
        """Extract and process files from ZIP"""
        dataframes = []
        try:
            with zipfile.ZipFile(BytesIO(file_content), 'r') as zip_file:
                for file_info in zip_file.filelist:
                    if file_info.filename.endswith(('.xls', '.xlsx', '.csv')):
                        extracted_content = zip_file.read(file_info.filename)
                        
                        if file_info.filename.endswith('.csv'):
                            df = self.process_csv_to_dataframe(extracted_content, file_info.filename)
                        else:
                            df = self.process_excel_to_dataframe(extracted_content, file_info.filename)
                        
                        if df is not None:
                            df['source_file'] = file_info.filename
                            df['archive'] = filename
                            dataframes.append(df)
            
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                logger.info(f"✅ Processed ZIP: {filename} ({len(dataframes)} files, {len(combined_df)} total rows)")
                return combined_df
            
        except Exception as e:
            logger.error(f"❌ ZIP processing failed for {filename}: {e}")
        
        return None

    def standardize_dataframe(self, df, region, filename):
        """Standardize DataFrame columns and add metadata"""
        try:
            # Add metadata columns
            df['region'] = region
            df['original_filename'] = filename
            df['processed_date'] = datetime.now().isoformat()
            
            # Clean column names
            df.columns = df.columns.astype(str)
            df.columns = [col.strip().replace(' ', '_').replace('/', '_').replace('-', '_') 
                         for col in df.columns]
            
            # Convert object columns to string to avoid parquet issues
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
            
            logger.info(f"✅ Standardized DataFrame: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"❌ Standardization failed: {e}")
            return df

    def save_parquet_to_s3(self, df, s3_key):
        """Save DataFrame as parquet to S3"""
        try:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                df.to_parquet(temp_file.name, index=False, engine='pyarrow')
                temp_path = temp_file.name
            
            # Upload to S3
            self.s3_client.upload_file(temp_path, self.bucket_name, s3_key)
            os.unlink(temp_path)
            
            logger.info(f"✅ Saved parquet: s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Parquet save failed: {e}")
            return False

    def process_raw_file_to_parquet(self, raw_s3_key):
        """Process a raw file and convert to parquet"""
        try:
            # Parse S3 key: dsm_data/raw/REGION/DATE/filename
            path_parts = raw_s3_key.split('/')
            if len(path_parts) < 5:
                logger.error(f"❌ Invalid S3 key format: {raw_s3_key}")
                return False
            
            region = path_parts[2]  # NRLDC, WRPC, ERLDC
            date_str = path_parts[3]  # 2025-08-27
            filename = path_parts[4]  # original filename
            
            # Download raw file
            file_content = self.download_raw_file(raw_s3_key)
            if file_content is None:
                return False
            
            # Process based on file type
            df = None
            if filename.endswith(('.xls', '.xlsx')):
                df = self.process_excel_to_dataframe(file_content, filename)
            elif filename.endswith('.csv'):
                df = self.process_csv_to_dataframe(file_content, filename)
            elif filename.endswith('.zip'):
                df = self.process_zip_to_dataframes(file_content, filename)
            else:
                logger.warning(f"⚠️ Unsupported file type: {filename}")
                return False
            
            if df is None or df.empty:
                logger.warning(f"⚠️ No data extracted from: {filename}")
                return False
            
            # Standardize DataFrame
            df = self.standardize_dataframe(df, region, filename)
            
            # Generate readable parquet filename
            base_name = os.path.splitext(filename)[0]
            if region == 'NRLDC':
                parquet_filename = f"NRLDC_Supporting_Files_{date_str}.parquet"
            elif region == 'WRPC':
                parquet_filename = f"WRPC_DSM_Data_{date_str}.parquet"
            elif region == 'ERLDC':
                parquet_filename = f"ERLDC_DSM_Data_{date_str}.parquet"            elif region == 'SRPC':
                parquet_filename = f"SRPC_DSM_Data_{date_str}.parquet"
            elif region == 'NERPC':
                parquet_filename = f"NERPC_DSM_Data_{date_str}.parquet"
            else:
                parquet_filename = f"{region}_{base_name}_{date_str}.parquet"
            
            parquet_s3_key = f"dsm_data/parquet/{region}/{date_str}/{parquet_filename}"
            
            # Save to S3
            success = self.save_parquet_to_s3(df, parquet_s3_key)
            
            if success:
                logger.info(f"🎉 Converted: {raw_s3_key} → {parquet_s3_key}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Parquet conversion failed for {raw_s3_key}: {e}")
            return False

# Global instance
parquet_processor = ParquetProcessor()
