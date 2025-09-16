#!/usr/bin/env python3
"""
Local Consolidated Dataset Creator
Creates a consolidated dataset from any local data files found
"""

import os
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any
import glob

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LocalConsolidatedDatasetCreator:
    def __init__(self):
        """Initialize the creator"""
        self.base_dir = "/Users/kushal/Downloads/kushal 8"
        self.data_patterns = [
            "**/*.csv",
            "**/*.parquet",
            "**/*.xlsx",
            "**/*.xls"
        ]
        
    def find_data_files(self) -> List[str]:
        """Find all data files in the directory tree"""
        all_files = []
        
        for pattern in self.data_patterns:
            files = glob.glob(os.path.join(self.base_dir, pattern), recursive=True)
            all_files.extend(files)
        
        # Filter out unwanted files
        filtered_files = []
        for file_path in all_files:
            filename = os.path.basename(file_path).lower()
            # Skip cache, temp, and system files
            if not any(skip in filename for skip in ['__pycache__', '.git', 'temp', 'tmp', '.DS_Store']):
                filtered_files.append(file_path)
        
        logger.info(f"Found {len(filtered_files)} data files")
        return filtered_files
    
    def read_data_file(self, file_path: str) -> pd.DataFrame:
        """Read a data file and return DataFrame"""
        try:
            filename = os.path.basename(file_path)
            file_ext = os.path.splitext(filename)[1].lower()
            
            # Add metadata columns
            metadata = {
                'source_file': filename,
                'source_path': file_path,
                'file_size': os.path.getsize(file_path),
                'processing_timestamp': datetime.now()
            }
            
            if file_ext == '.csv':
                df = pd.read_csv(file_path)
            elif file_ext == '.parquet':
                df = pd.read_parquet(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                logger.warning(f"Unsupported file format: {file_path}")
                return pd.DataFrame()
            
            # Add metadata columns
            for key, value in metadata.items():
                df[key] = value
            
            # Try to extract region and station from filename/path
            df['region'] = self._extract_region_from_path(file_path)
            df['station'] = self._extract_station_from_path(file_path)
            
            logger.info(f"Read {file_path}: {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return pd.DataFrame()
    
    def _extract_region_from_path(self, file_path: str) -> str:
        """Extract region from file path"""
        path_lower = file_path.lower()
        if 'nerpc' in path_lower:
            return 'NERPC'
        elif 'wrpc' in path_lower:
            return 'WRPC'
        elif 'erldc' in path_lower:
            return 'ERLDC'
        elif 'srpc' in path_lower:
            return 'SRPC'
        elif 'nrldc' in path_lower:
            return 'NRLDC'
        else:
            return 'UNKNOWN'
    
    def _extract_station_from_path(self, file_path: str) -> str:
        """Extract station name from file path"""
        filename = os.path.basename(file_path)
        # Try to extract station name from filename
        parts = filename.split('_')
        if len(parts) > 1:
            # Look for common patterns
            for part in parts:
                if any(region in part.upper() for region in ['NERPC', 'WRPC', 'ERLDC', 'SRPC', 'NRLDC']):
                    continue
                if len(part) > 3:  # Likely a station name
                    return part.upper()
        return 'UNKNOWN'
    
    def consolidate_local_data(self, output_format: str = 'both') -> Dict[str, str]:
        """
        Consolidate all local data files
        
        Args:
            output_format: 'csv', 'parquet', or 'both'
            
        Returns:
            Dictionary with file paths of created files
        """
        logger.info("🚀 Starting local data consolidation...")
        
        # Find all data files
        data_files = self.find_data_files()
        
        if not data_files:
            logger.warning("❌ No data files found!")
            return {}
        
        all_dataframes = []
        
        # Process each file
        for file_path in data_files:
            logger.info(f"📊 Processing: {file_path}")
            df = self.read_data_file(file_path)
            if not df.empty:
                all_dataframes.append(df)
        
        if not all_dataframes:
            logger.error("❌ No data could be read from files!")
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
            csv_filename = f"LOCAL_CONSOLIDATED_ALL_DATA_{timestamp}.csv"
            consolidated_df.to_csv(csv_filename, index=False)
            output_files['csv'] = os.path.abspath(csv_filename)
            logger.info(f"💾 CSV saved: {csv_filename}")
        
        if output_format in ['parquet', 'both']:
            parquet_filename = f"LOCAL_CONSOLIDATED_ALL_DATA_{timestamp}.parquet"
            consolidated_df.to_parquet(parquet_filename, index=False)
            output_files['parquet'] = os.path.abspath(parquet_filename)
            logger.info(f"💾 Parquet saved: {parquet_filename}")
        
        # Create summary statistics
        self._create_summary_report(consolidated_df, timestamp)
        
        logger.info(f"🎉 Local consolidation complete! Processed {len(data_files)} files")
        return output_files
    
    def _create_summary_report(self, df: pd.DataFrame, timestamp: str):
        """Create a summary report of the consolidated data"""
        summary_filename = f"LOCAL_CONSOLIDATED_SUMMARY_{timestamp}.txt"
        
        with open(summary_filename, 'w') as f:
            f.write("LOCAL CONSOLIDATED DATASET SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Rows: {len(df):,}\n")
            f.write(f"Total Columns: {len(df.columns)}\n\n")
            
            # Region breakdown
            if 'region' in df.columns:
                f.write("REGION BREAKDOWN:\n")
                f.write("-" * 20 + "\n")
                region_counts = df['region'].value_counts()
                for region, count in region_counts.items():
                    f.write(f"{region}: {count:,} rows\n")
                f.write("\n")
            
            # Station breakdown (top 20)
            if 'station' in df.columns:
                f.write("TOP 20 STATIONS BY ROW COUNT:\n")
                f.write("-" * 30 + "\n")
                station_counts = df['station'].value_counts().head(20)
                for station, count in station_counts.items():
                    f.write(f"{station}: {count:,} rows\n")
                f.write("\n")
            
            # File breakdown
            if 'source_file' in df.columns:
                f.write("SOURCE FILES:\n")
                f.write("-" * 15 + "\n")
                file_counts = df['source_file'].value_counts()
                for filename, count in file_counts.items():
                    f.write(f"{filename}: {count:,} rows\n")
                f.write("\n")
            
            # Column information
            f.write("COLUMNS:\n")
            f.write("-" * 10 + "\n")
            for col in df.columns:
                non_null = df[col].count()
                f.write(f"{col}: {non_null:,} non-null values\n")
        
        logger.info(f"📋 Summary report saved: {summary_filename}")

def main():
    """Main function to run the local consolidation"""
    try:
        creator = LocalConsolidatedDatasetCreator()
        
        print("🔍 Scanning for local data files...")
        data_files = creator.find_data_files()
        
        if not data_files:
            print("❌ No local data files found!")
            print("\n💡 To get data from S3, you need to:")
            print("1. Set up AWS credentials in a .env file")
            print("2. Run the create_consolidated_dataset.py script")
            return
        
        print(f"✅ Found {len(data_files)} data files")
        
        print("\n🚀 Starting local data consolidation...")
        output_files = creator.consolidate_local_data(output_format='both')
        
        if output_files:
            print("\n🎉 Local consolidation completed successfully!")
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
