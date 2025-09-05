#!/usr/bin/env python3
"""
Master Dataset Consolidator - Creates unified master datasets for each region
Consolidates NRLDC, ERLDC, WRPC, and other regional data into single master files
"""
import pandas as pd
import logging
import os
import json
from datetime import datetime
from pathlib import Path
import glob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MasterDatasetConsolidator:
    def __init__(self):
        self.master_data_dir = Path("master_data")
        self.consolidated_dir = Path("consolidated_master_data")
        self.consolidated_dir.mkdir(parents=True, exist_ok=True)
        
        # Regional directories
        self.regional_dirs = {
            'NRLDC': self.master_data_dir / 'NRLDC',
            'ERLDC': self.master_data_dir / 'ERLDC', 
            'WRPC': self.master_data_dir / 'WRPC',
            'SRLDC': self.master_data_dir / 'SRLDC',
            'NERLDC': self.master_data_dir / 'NERLDC'
        }
        
        # Standard columns for unified format
        self.standard_columns = [
            'Station_Name', 'Date', 'State', 'Regional_Group', 'Region',
            'Source_File', 'Extraction_Timestamp', 'Data_Quality_Score'
        ]

    def find_latest_master_dataset(self, region_dir):
        """Find the latest master dataset file for a region"""
        if not region_dir.exists():
            return None
        
        # Look for master dataset files
        patterns = [
            f"{region_dir.name}_Master_Dataset_*.csv",
            f"*_Master_Dataset_*.csv",
            "master_dataset.csv"
        ]
        
        latest_file = None
        latest_time = None
        
        for pattern in patterns:
            files = list(region_dir.glob(pattern))
            for file_path in files:
                file_time = file_path.stat().st_mtime
                if latest_time is None or file_time > latest_time:
                    latest_time = file_time
                    latest_file = file_path
        
        return latest_file

    def standardize_dataset(self, df, region_name):
        """Standardize dataset format across regions"""
        standardized_df = df.copy()
        
        # Ensure standard columns exist
        for col in self.standard_columns:
            if col not in standardized_df.columns:
                if col == 'Region':
                    standardized_df[col] = region_name
                elif col == 'Data_Quality_Score':
                    standardized_df[col] = 1.0  # Default quality score
                else:
                    standardized_df[col] = 'Unknown'
        
        # Standardize station names
        if 'Station_Name' in standardized_df.columns:
            standardized_df['Station_Name'] = standardized_df['Station_Name'].str.strip().str.upper()
        
        # Add region prefix to avoid conflicts
        standardized_df['Unique_Station_ID'] = standardized_df['Region'] + '_' + standardized_df['Station_Name'].astype(str)
        
        return standardized_df

    def create_regional_summary(self, df, region_name):
        """Create summary statistics for a region"""
        summary = {
            'region': region_name,
            'total_records': len(df),
            'unique_stations': df['Station_Name'].nunique() if 'Station_Name' in df.columns else 0,
            'states_covered': df['State'].nunique() if 'State' in df.columns else 0,
            'regional_groups': df['Regional_Group'].nunique() if 'Regional_Group' in df.columns else 0,
            'date_range': {
                'earliest': str(df['Extraction_Timestamp'].min()) if 'Extraction_Timestamp' in df.columns else 'Unknown',
                'latest': str(df['Extraction_Timestamp'].max()) if 'Extraction_Timestamp' in df.columns else 'Unknown'
            },
            'data_quality': {
                'completeness': f"{(df.count().sum() / (len(df) * len(df.columns)) * 100):.1f}%",
                'state_mapping_coverage': f"{((df['State'] != 'Unknown').sum() / len(df) * 100):.1f}%" if 'State' in df.columns else '0%'
            }
        }
        
        # State distribution
        if 'State' in df.columns:
            summary['state_distribution'] = df['State'].value_counts().to_dict()
        
        # Regional group distribution  
        if 'Regional_Group' in df.columns:
            summary['regional_group_distribution'] = df['Regional_Group'].value_counts().to_dict()
        
        return summary

    def consolidate_region(self, region_name):
        """Consolidate all data for a specific region"""
        region_dir = self.regional_dirs.get(region_name)
        if not region_dir or not region_dir.exists():
            logger.warning(f"⚠️ No data directory found for {region_name}")
            return None
        
        logger.info(f"🔄 Consolidating {region_name} data...")
        
        # Find latest master dataset
        latest_file = self.find_latest_master_dataset(region_dir)
        if not latest_file:
            logger.warning(f"⚠️ No master dataset found for {region_name}")
            return None
        
        logger.info(f"📄 Processing {latest_file.name}")
        
        try:
            # Read the dataset
            df = pd.read_csv(latest_file)
            logger.info(f"📊 Loaded {len(df)} records from {region_name}")
            
            # Standardize the dataset
            standardized_df = self.standardize_dataset(df, region_name)
            
            # Create consolidated filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            consolidated_filename = f"{region_name}_Consolidated_Master_{timestamp}.csv"
            consolidated_path = self.consolidated_dir / consolidated_filename
            
            # Save consolidated dataset
            standardized_df.to_csv(consolidated_path, index=False)
            logger.info(f"✅ Created consolidated dataset: {consolidated_filename}")
            
            # Create regional summary
            summary = self.create_regional_summary(standardized_df, region_name)
            
            # Save summary
            summary_filename = f"{region_name}_Summary_{timestamp}.json"
            summary_path = self.consolidated_dir / summary_filename
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"📋 Regional summary: {summary['total_records']} records, {summary['unique_stations']} stations")
            
            return {
                'region': region_name,
                'consolidated_file': str(consolidated_path),
                'summary_file': str(summary_path),
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"❌ Error consolidating {region_name}: {e}")
            return None

    def create_unified_master_dataset(self):
        """Create a single unified master dataset from all regions"""
        logger.info("🌐 Creating unified master dataset from all regions...")
        
        all_data = []
        regional_summaries = []
        
        # Process each region
        for region_name in self.regional_dirs.keys():
            result = self.consolidate_region(region_name)
            if result:
                regional_summaries.append(result['summary'])
                
                # Load the consolidated data
                consolidated_df = pd.read_csv(result['consolidated_file'])
                all_data.append(consolidated_df)
        
        if not all_data:
            logger.error("❌ No regional data found to consolidate")
            return None
        
        # Combine all regional data
        unified_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"🔄 Combined data from {len(all_data)} regions")
        
        # Create unified master dataset
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unified_filename = f"UNIFIED_MASTER_DATASET_{timestamp}.csv"
        unified_path = self.consolidated_dir / unified_filename
        
        unified_df.to_csv(unified_path, index=False)
        logger.info(f"✅ Created unified master dataset: {unified_filename}")
        
        # Create unified summary
        unified_summary = {
            'creation_timestamp': datetime.now().isoformat(),
            'total_records': len(unified_df),
            'total_unique_stations': unified_df['Unique_Station_ID'].nunique(),
            'regions_included': len(regional_summaries),
            'regional_breakdown': {summary['region']: {
                'records': summary['total_records'],
                'stations': summary['unique_stations'],
                'states': summary['states_covered']
            } for summary in regional_summaries},
            'overall_statistics': {
                'states_covered': unified_df['State'].nunique(),
                'regional_groups': unified_df['Regional_Group'].nunique(),
                'data_quality_avg': unified_df['Data_Quality_Score'].mean(),
                'completeness': f"{(unified_df.count().sum() / (len(unified_df) * len(unified_df.columns)) * 100):.1f}%"
            }
        }
        
        # Save unified summary
        unified_summary_filename = f"UNIFIED_SUMMARY_{timestamp}.json"
        unified_summary_path = self.consolidated_dir / unified_summary_filename
        with open(unified_summary_path, 'w') as f:
            json.dump(unified_summary, f, indent=2)
        
        logger.info(f"🎉 Unified dataset complete: {len(unified_df)} total records from {len(regional_summaries)} regions")
        
        return {
            'unified_file': str(unified_path),
            'summary_file': str(unified_summary_path),
            'summary': unified_summary
        }

    def run_consolidation(self):
        """Run the complete consolidation process"""
        logger.info("🚀 Starting master dataset consolidation...")
        
        # Create individual regional consolidated datasets
        regional_results = []
        for region_name in self.regional_dirs.keys():
            result = self.consolidate_region(region_name)
            if result:
                regional_results.append(result)
        
        # Create unified master dataset
        unified_result = self.create_unified_master_dataset()
        
        # Final report
        logger.info("📊 Consolidation Summary:")
        logger.info(f"   • Regional datasets: {len(regional_results)}")
        if unified_result:
            logger.info(f"   • Total records: {unified_result['summary']['total_records']}")
            logger.info(f"   • Unique stations: {unified_result['summary']['total_unique_stations']}")
            logger.info(f"   • Unified file: {unified_result['unified_file']}")
        
        return {
            'regional_results': regional_results,
            'unified_result': unified_result
        }

def main():
    """Main execution function"""
    consolidator = MasterDatasetConsolidator()
    result = consolidator.run_consolidation()
    
    if result['unified_result']:
        logger.info("✅ Master dataset consolidation completed successfully!")
    else:
        logger.error("❌ Master dataset consolidation failed!")

if __name__ == "__main__":
    main()
