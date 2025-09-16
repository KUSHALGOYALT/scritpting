#!/usr/bin/env python3
"""
Get Actual Data and Create Real Mapping
Downloads actual data from S3 and creates comprehensive station mapping
"""

import os
import pandas as pd
import boto3
import tempfile
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple
import io
import json
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ActualDataMapper:
    def __init__(self):
        """Initialize the data mapper"""
        self.bucket_name = 'dsm_data'
        self.regions = ['NERPC', 'WRPC', 'ERLDC', 'SRPC', 'NRLDC']
        self.s3_client = None
        self.station_mapping = {}
        self.region_stats = {}
        
    def setup_s3_client(self):
        """Setup S3 client with multiple credential methods"""
        try:
            # Method 1: Try environment variables
            if os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY'):
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                    region_name=os.getenv('AWS_REGION', 'us-east-1')
                )
                logger.info("✅ S3 client initialized with environment variables")
                return True
                
            # Method 2: Try AWS credentials file
            elif os.path.exists(os.path.expanduser('~/.aws/credentials')):
                self.s3_client = boto3.client('s3')
                logger.info("✅ S3 client initialized with AWS credentials file")
                return True
                
            # Method 3: Try default credentials
            else:
                self.s3_client = boto3.client('s3')
                logger.info("✅ S3 client initialized with default credentials")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to setup S3 client: {e}")
            return False
    
    def test_s3_connection(self) -> bool:
        """Test S3 connection"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("✅ S3 connection successful!")
            return True
        except Exception as e:
            logger.error(f"❌ S3 connection failed: {e}")
            return False
    
    def list_s3_files(self, prefix: str) -> List[Dict[str, Any]]:
        """List all files in S3 with given prefix"""
        files = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
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
    
    def analyze_s3_structure(self) -> Dict[str, Any]:
        """Analyze S3 bucket structure and create mapping"""
        logger.info("🔍 Analyzing S3 bucket structure...")
        
        structure = {
            'regions': {},
            'total_files': 0,
            'total_size': 0,
            'file_types': {},
            'stations': set()
        }
        
        for region in self.regions:
            logger.info(f"📊 Analyzing region: {region}")
            
            # Check raw data
            raw_prefix = f"dsm_data/raw/{region}/"
            raw_files = self.list_s3_files(raw_prefix)
            
            # Check parquet data
            parquet_prefix = f"dsm_data/parquet/{region}/"
            parquet_files = self.list_s3_files(parquet_prefix)
            
            region_info = {
                'raw_files': len(raw_files),
                'parquet_files': len(parquet_files),
                'stations': set(),
                'file_sizes': {
                    'raw': sum(f['size'] for f in raw_files),
                    'parquet': sum(f['size'] for f in parquet_files)
                }
            }
            
            # Extract station names from parquet files
            for file_info in parquet_files:
                key = file_info['key']
                parts = key.split('/')
                if len(parts) >= 4:
                    station = parts[3]  # dsm_data/parquet/REGION/STATION/...
                    region_info['stations'].add(station)
                    structure['stations'].add(station)
                
                # Count file types
                file_ext = key.split('.')[-1].lower()
                structure['file_types'][file_ext] = structure['file_types'].get(file_ext, 0) + 1
            
            structure['regions'][region] = region_info
            structure['total_files'] += len(raw_files) + len(parquet_files)
            structure['total_size'] += region_info['file_sizes']['raw'] + region_info['file_sizes']['parquet']
        
        return structure
    
    def download_sample_data(self, max_files_per_region: int = 5) -> Dict[str, pd.DataFrame]:
        """Download sample data from each region"""
        logger.info(f"📥 Downloading sample data (max {max_files_per_region} files per region)...")
        
        sample_data = {}
        
        for region in self.regions:
            logger.info(f"📊 Downloading sample data for {region}")
            
            prefix = f"dsm_data/parquet/{region}/"
            files = self.list_s3_files(prefix)
            parquet_files = [f for f in files if f['key'].endswith('.parquet')]
            
            # Take first few files as samples
            sample_files = parquet_files[:max_files_per_region]
            
            region_dataframes = []
            for file_info in sample_files:
                try:
                    response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_info['key'])
                    df = pd.read_parquet(io.BytesIO(response['Body'].read()))
                    
                    # Add metadata
                    df['source_region'] = region
                    df['source_file'] = os.path.basename(file_info['key'])
                    df['source_path'] = file_info['key']
                    df['file_size'] = file_info['size']
                    df['last_modified'] = file_info['last_modified']
                    
                    region_dataframes.append(df)
                    logger.info(f"✅ Downloaded {file_info['key']}: {len(df)} rows")
                    
                except Exception as e:
                    logger.error(f"❌ Error downloading {file_info['key']}: {e}")
            
            if region_dataframes:
                sample_data[region] = pd.concat(region_dataframes, ignore_index=True)
                logger.info(f"📈 {region} sample data: {len(sample_data[region])} total rows")
        
        return sample_data
    
    def create_comprehensive_mapping(self, sample_data: Dict[str, pd.DataFrame], structure: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive mapping of all data"""
        logger.info("🗺️ Creating comprehensive data mapping...")
        
        mapping = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_regions': len(self.regions),
                'total_stations': len(structure['stations']),
                'total_files': structure['total_files'],
                'total_size_gb': round(structure['total_size'] / (1024**3), 2)
            },
            'regions': {},
            'stations': {},
            'data_schema': {},
            'sample_data_summary': {}
        }
        
        # Region-level mapping
        for region in self.regions:
            region_info = structure['regions'][region]
            mapping['regions'][region] = {
                'stations': list(region_info['stations']),
                'file_counts': {
                    'raw': region_info['raw_files'],
                    'parquet': region_info['parquet_files']
                },
                'file_sizes_gb': {
                    'raw': round(region_info['file_sizes']['raw'] / (1024**3), 2),
                    'parquet': round(region_info['file_sizes']['parquet'] / (1024**3), 2)
                }
            }
        
        # Station-level mapping
        for region in self.regions:
            for station in structure['regions'][region]['stations']:
                mapping['stations'][station] = {
                    'region': region,
                    'data_available': True,
                    'file_types': ['raw', 'parquet']
                }
        
        # Data schema from sample data
        if sample_data:
            all_columns = set()
            for region, df in sample_data.items():
                all_columns.update(df.columns)
            
            mapping['data_schema'] = {
                'columns': list(all_columns),
                'column_count': len(all_columns),
                'sample_row_count': sum(len(df) for df in sample_data.values())
            }
            
            # Sample data summary
            for region, df in sample_data.items():
                mapping['sample_data_summary'][region] = {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'date_range': {
                        'start': df['datetime'].min() if 'datetime' in df.columns else 'N/A',
                        'end': df['datetime'].max() if 'datetime' in df.columns else 'N/A'
                    },
                    'stations_in_sample': df['station'].nunique() if 'station' in df.columns else 'N/A'
                }
        
        return mapping
    
    def save_mapping_and_data(self, mapping: Dict[str, Any], sample_data: Dict[str, pd.DataFrame]):
        """Save mapping and sample data to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save mapping as JSON
        mapping_file = f"ACTUAL_DATA_MAPPING_{timestamp}.json"
        with open(mapping_file, 'w') as f:
            json.dump(mapping, f, indent=2, default=str)
        logger.info(f"💾 Mapping saved: {mapping_file}")
        
        # Save sample data
        if sample_data:
            # Combined sample data
            all_sample_data = pd.concat(list(sample_data.values()), ignore_index=True)
            sample_csv = f"ACTUAL_SAMPLE_DATA_{timestamp}.csv"
            sample_parquet = f"ACTUAL_SAMPLE_DATA_{timestamp}.parquet"
            
            all_sample_data.to_csv(sample_csv, index=False)
            all_sample_data.to_parquet(sample_parquet, index=False)
            
            logger.info(f"💾 Sample data saved: {sample_csv}, {sample_parquet}")
            
            # Individual region samples
            for region, df in sample_data.items():
                region_csv = f"ACTUAL_SAMPLE_{region}_{timestamp}.csv"
                df.to_csv(region_csv, index=False)
                logger.info(f"💾 {region} sample saved: {region_csv}")
        
        # Create summary report
        self._create_summary_report(mapping, timestamp)
        
        return {
            'mapping_file': mapping_file,
            'sample_files': list(sample_data.keys()) if sample_data else [],
            'timestamp': timestamp
        }
    
    def _create_summary_report(self, mapping: Dict[str, Any], timestamp: str):
        """Create a human-readable summary report"""
        summary_file = f"ACTUAL_DATA_SUMMARY_{timestamp}.txt"
        
        with open(summary_file, 'w') as f:
            f.write("ACTUAL DATA MAPPING SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Regions: {mapping['metadata']['total_regions']}\n")
            f.write(f"Total Stations: {mapping['metadata']['total_stations']}\n")
            f.write(f"Total Files: {mapping['metadata']['total_files']:,}\n")
            f.write(f"Total Size: {mapping['metadata']['total_size_gb']} GB\n\n")
            
            f.write("REGION BREAKDOWN:\n")
            f.write("-" * 20 + "\n")
            for region, info in mapping['regions'].items():
                f.write(f"{region}:\n")
                f.write(f"  Stations: {len(info['stations'])}\n")
                f.write(f"  Raw Files: {info['file_counts']['raw']}\n")
                f.write(f"  Parquet Files: {info['file_counts']['parquet']}\n")
                f.write(f"  Raw Size: {info['file_sizes_gb']['raw']} GB\n")
                f.write(f"  Parquet Size: {info['file_sizes_gb']['parquet']} GB\n\n")
            
            if mapping['sample_data_summary']:
                f.write("SAMPLE DATA SUMMARY:\n")
                f.write("-" * 25 + "\n")
                for region, summary in mapping['sample_data_summary'].items():
                    f.write(f"{region}: {summary['rows']} rows, {summary['columns']} columns\n")
            
            if mapping['data_schema']:
                f.write(f"\nDATA SCHEMA:\n")
                f.write(f"Columns: {mapping['data_schema']['column_count']}\n")
                f.write(f"Sample Rows: {mapping['data_schema']['sample_row_count']}\n")
        
        logger.info(f"📋 Summary report saved: {summary_file}")

def main():
    """Main function"""
    print("🔍 Actual Data Mapping Tool")
    print("=" * 40)
    
    mapper = ActualDataMapper()
    
    # Setup S3 client
    print("🔧 Setting up S3 connection...")
    if not mapper.setup_s3_client():
        print("❌ Failed to setup S3 client")
        print("\n💡 To fix this:")
        print("1. Set AWS credentials in .env file")
        print("2. Or configure AWS CLI: aws configure")
        print("3. Or set environment variables:")
        print("   export AWS_ACCESS_KEY_ID=your_key")
        print("   export AWS_SECRET_ACCESS_KEY=your_secret")
        return
    
    # Test connection
    print("🔍 Testing S3 connection...")
    if not mapper.test_s3_connection():
        print("❌ S3 connection failed")
        return
    
    # Analyze structure
    print("📊 Analyzing S3 bucket structure...")
    structure = mapper.analyze_s3_structure()
    
    print(f"✅ Found {structure['total_stations']} stations across {len(mapper.regions)} regions")
    print(f"📁 Total files: {structure['total_files']:,}")
    print(f"💾 Total size: {structure['total_size'] / (1024**3):.2f} GB")
    
    # Download sample data
    print("\n📥 Downloading sample data...")
    sample_data = mapper.download_sample_data(max_files_per_region=3)
    
    if not sample_data:
        print("❌ No sample data could be downloaded")
        return
    
    # Create mapping
    print("\n🗺️ Creating comprehensive mapping...")
    mapping = mapper.create_comprehensive_mapping(sample_data, structure)
    
    # Save everything
    print("\n💾 Saving mapping and data...")
    output_files = mapper.save_mapping_and_data(mapping, sample_data)
    
    print("\n🎉 SUCCESS! Actual data mapping created!")
    print(f"\n📁 Generated files:")
    print(f"  - {output_files['mapping_file']}")
    print(f"  - ACTUAL_SAMPLE_DATA_{output_files['timestamp']}.csv")
    print(f"  - ACTUAL_SAMPLE_DATA_{output_files['timestamp']}.parquet")
    print(f"  - ACTUAL_DATA_SUMMARY_{output_files['timestamp']}.txt")
    
    for region in output_files['sample_files']:
        print(f"  - ACTUAL_SAMPLE_{region}_{output_files['timestamp']}.csv")

if __name__ == "__main__":
    main()
