#!/usr/bin/env python3
"""
Consolidated Dataset Creator
Downloads all data from S3 and creates a single consolidated file
"""

import os
import pandas as pd
import boto3
import tempfile
import logging
from datetime import datetime
from typing import List, Dict, Any
import io

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConsolidatedDatasetCreator:
    def __init__(self):
        """Initialize S3 client and configuration"""
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'dsm_data'
        self.regions = ['NERPC', 'WRPC', 'ERLDC', 'SRPC', 'NRLDC']
        
    def list_s3_files(self, prefix: str) -> List[Dict[str, Any]]:
        """List all files in S3 with given prefix"""
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
        except Exception as e:
            logger.error(f"Error listing S3 files with prefix {prefix}: {e}")
            
        return files
    
    def download_s3_file(self, s3_key: str) -> pd.DataFrame:
        """Download and read a CSV/Parquet file from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            
            if s3_key.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(response['Body'].read()))
            elif s3_key.endswith('.parquet'):
                df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            else:
                logger.warning(f"Unsupported file format: {s3_key}")
                return pd.DataFrame()
                
            # Add metadata columns
            df['source_region'] = self._extract_region_from_key(s3_key)
            df['source_file'] = os.path.basename(s3_key)
            df['download_timestamp'] = datetime.now()
            
            logger.info(f"Downloaded {s3_key}: {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading {s3_key}: {e}")
            return pd.DataFrame()
    
    def _extract_region_from_key(self, s3_key: str) -> str:
        """Extract region from S3 key"""
        parts = s3_key.split('/')
        if len(parts) >= 3:
            return parts[2]  # dsm_data/parquet/REGION/...
        return 'UNKNOWN'
    
    def _extract_station_from_key(self, s3_key: str) -> str:
        """Extract station name from S3 key"""
        parts = s3_key.split('/')
        if len(parts) >= 4:
            return parts[3]  # dsm_data/parquet/REGION/STATION/...
        return 'UNKNOWN'
    
    def consolidate_all_data(self, output_format: str = 'both') -> Dict[str, str]:
        """
        Consolidate all data from S3 into single files
        
        Args:
            output_format: 'csv', 'parquet', or 'both'
            
        Returns:
            Dictionary with file paths of created files
        """
        logger.info("🚀 Starting data consolidation from S3...")
        
        all_dataframes = []
        total_files_processed = 0
        
        # Process each region
        for region in self.regions:
            logger.info(f"📊 Processing region: {region}")
            
            # Get all parquet files for this region
            prefix = f"dsm_data/parquet/{region}/"
            files = self.list_s3_files(prefix)
            
            region_files = [f for f in files if f['key'].endswith('.parquet')]
            logger.info(f"Found {len(region_files)} parquet files for {region}")
            
            for file_info in region_files:
                df = self.download_s3_file(file_info['key'])
                if not df.empty:
                    # Add additional metadata
                    df['region'] = region
                    df['station'] = self._extract_station_from_key(file_info['key'])
                    all_dataframes.append(df)
                    total_files_processed += 1
        
        if not all_dataframes:
            logger.error("❌ No data found to consolidate!")
            return {}
        
        # Combine all dataframes
        logger.info(f"📈 Combining {len(all_dataframes)} dataframes...")
        consolidated_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        
        logger.info(f"✅ Consolidated dataset: {len(consolidated_df)} total rows")
        logger.info(f"📊 Columns: {list(consolidated_df.columns)}")
        
        # Generate output filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_files = {}
        
        # Save consolidated data
        if output_format in ['csv', 'both']:
            csv_filename = f"CONSOLIDATED_ALL_STATIONS_{timestamp}.csv"
            consolidated_df.to_csv(csv_filename, index=False)
            output_files['csv'] = os.path.abspath(csv_filename)
            logger.info(f"💾 CSV saved: {csv_filename}")
        
        if output_format in ['parquet', 'both']:
            parquet_filename = f"CONSOLIDATED_ALL_STATIONS_{timestamp}.parquet"
            consolidated_df.to_parquet(parquet_filename, index=False)
            output_files['parquet'] = os.path.abspath(parquet_filename)
            logger.info(f"💾 Parquet saved: {parquet_filename}")
        
        # Create summary statistics
        self._create_summary_report(consolidated_df, timestamp)
        
        logger.info(f"🎉 Consolidation complete! Processed {total_files_processed} files")
        return output_files
    
    def _create_summary_report(self, df: pd.DataFrame, timestamp: str):
        """Create a summary report of the consolidated data"""
        summary_filename = f"CONSOLIDATED_SUMMARY_{timestamp}.txt"
        
        with open(summary_filename, 'w') as f:
            f.write("CONSOLIDATED DATASET SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Rows: {len(df):,}\n")
            f.write(f"Total Columns: {len(df.columns)}\n\n")
            
            # Region breakdown
            f.write("REGION BREAKDOWN:\n")
            f.write("-" * 20 + "\n")
            region_counts = df['region'].value_counts()
            for region, count in region_counts.items():
                f.write(f"{region}: {count:,} rows\n")
            
            f.write("\n")
            
            # Station breakdown (top 20)
            f.write("TOP 20 STATIONS BY ROW COUNT:\n")
            f.write("-" * 30 + "\n")
            station_counts = df['station'].value_counts().head(20)
            for station, count in station_counts.items():
                f.write(f"{station}: {count:,} rows\n")
            
            f.write("\n")
            
            # Column information
            f.write("COLUMNS:\n")
            f.write("-" * 10 + "\n")
            for col in df.columns:
                non_null = df[col].count()
                f.write(f"{col}: {non_null:,} non-null values\n")
        
        logger.info(f"📋 Summary report saved: {summary_filename}")

def main():
    """Main function to run the consolidation"""
    try:
        creator = ConsolidatedDatasetCreator()
        
        print("🔍 Checking S3 connectivity...")
        # Test S3 connection
        creator.s3_client.head_bucket(Bucket=creator.bucket_name)
        print("✅ S3 connection successful!")
        
        print("\n🚀 Starting data consolidation...")
        output_files = creator.consolidate_all_data(output_format='both')
        
        if output_files:
            print("\n🎉 Consolidation completed successfully!")
            print("\n📁 Generated files:")
            for format_type, filepath in output_files.items():
                print(f"  {format_type.upper()}: {filepath}")
        else:
            print("❌ No data was consolidated!")
            
    except Exception as e:
        logger.error(f"❌ Error during consolidation: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
