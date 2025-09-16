#!/usr/bin/env python3
"""
Create Sample Consolidated Dataset
Creates a sample consolidated dataset for testing purposes
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def create_sample_data():
    """Create sample data for all regions"""
    
    # Sample stations for each region
    regions_data = {
        'NERPC': ['STATION_A', 'STATION_B', 'STATION_C'],
        'WRPC': ['STATION_D', 'STATION_E', 'STATION_F'],
        'ERLDC': ['STATION_G', 'STATION_H', 'STATION_I'],
        'SRPC': ['STATION_J', 'STATION_K', 'STATION_L'],
        'NRLDC': ['STATION_M', 'STATION_N', 'STATION_O']
    }
    
    all_data = []
    
    # Generate sample data for each region
    for region, stations in regions_data.items():
        for station in stations:
            # Generate 100 rows of sample data per station
            n_rows = 100
            
            # Create datetime range
            start_date = datetime.now() - timedelta(days=30)
            dates = [start_date + timedelta(hours=i) for i in range(n_rows)]
            
            # Generate sample data
            station_data = pd.DataFrame({
                'datetime': dates,
                'region': region,
                'station': station,
                'power_mw': np.random.uniform(50, 500, n_rows),
                'voltage_kv': np.random.uniform(220, 765, n_rows),
                'frequency_hz': np.random.uniform(49.5, 50.5, n_rows),
                'temperature_c': np.random.uniform(20, 40, n_rows),
                'humidity_percent': np.random.uniform(30, 80, n_rows),
                'status': np.random.choice(['ONLINE', 'OFFLINE', 'MAINTENANCE'], n_rows),
                'source_file': f'{region}_{station}_sample.csv',
                'processing_timestamp': datetime.now()
            })
            
            all_data.append(station_data)
    
    # Combine all data
    consolidated_df = pd.concat(all_data, ignore_index=True)
    
    return consolidated_df

def main():
    """Main function to create sample consolidated data"""
    print("🚀 Creating sample consolidated dataset...")
    
    # Create sample data
    df = create_sample_data()
    
    print(f"✅ Created sample dataset: {len(df)} rows, {len(df.columns)} columns")
    print(f"📊 Regions: {df['region'].value_counts().to_dict()}")
    print(f"🏭 Stations: {len(df['station'].unique())}")
    
    # Generate output filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save as CSV
    csv_filename = f"SAMPLE_CONSOLIDATED_ALL_STATIONS_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"💾 CSV saved: {csv_filename}")
    
    # Save as Parquet
    parquet_filename = f"SAMPLE_CONSOLIDATED_ALL_STATIONS_{timestamp}.parquet"
    df.to_parquet(parquet_filename, index=False)
    print(f"💾 Parquet saved: {parquet_filename}")
    
    # Create summary
    summary_filename = f"SAMPLE_CONSOLIDATED_SUMMARY_{timestamp}.txt"
    with open(summary_filename, 'w') as f:
        f.write("SAMPLE CONSOLIDATED DATASET SUMMARY\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Rows: {len(df):,}\n")
        f.write(f"Total Columns: {len(df.columns)}\n\n")
        
        f.write("REGION BREAKDOWN:\n")
        f.write("-" * 20 + "\n")
        region_counts = df['region'].value_counts()
        for region, count in region_counts.items():
            f.write(f"{region}: {count:,} rows\n")
        
        f.write("\nSTATION BREAKDOWN:\n")
        f.write("-" * 20 + "\n")
        station_counts = df['station'].value_counts()
        for station, count in station_counts.items():
            f.write(f"{station}: {count:,} rows\n")
        
        f.write("\nCOLUMNS:\n")
        f.write("-" * 10 + "\n")
        for col in df.columns:
            non_null = df[col].count()
            f.write(f"{col}: {non_null:,} non-null values\n")
    
    print(f"📋 Summary saved: {summary_filename}")
    
    print("\n🎉 Sample consolidated dataset created successfully!")
    print("\n📁 Files created:")
    print(f"  - {csv_filename}")
    print(f"  - {parquet_filename}")
    print(f"  - {summary_filename}")
    
    print("\n💡 You can use these files to test your analysis tools!")

if __name__ == "__main__":
    main()
