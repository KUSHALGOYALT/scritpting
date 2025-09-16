# Consolidated Dataset Creation Guide

## 📊 Overview

This guide explains how to create a consolidated dataset containing all energy data across all stations from all regions (NERPC, WRPC, ERLDC, SRPC, NRLDC).

## 🎯 What You Get

A consolidated dataset containing:
- **All station data** from all regions in a single file
- **Both CSV and Parquet formats** for flexibility
- **Summary statistics** and breakdowns
- **Metadata** including source files, regions, and stations

## 🚀 Quick Start (Sample Data)

If you want to test the functionality immediately, run:

```bash
python create_sample_consolidated_data.py
```

This creates sample data with 1,500 rows across all regions for testing.

## 🔐 Getting Real Data from S3

### Option 1: Interactive Setup

```bash
python setup_aws_and_consolidate.py
```

This script will:
1. Help you set up AWS credentials
2. Test S3 connectivity
3. Download all data and create consolidated files

### Option 2: Manual Setup

1. **Create AWS credentials file:**
   ```bash
   cp aws_config_template.txt .env
   # Edit .env with your actual AWS credentials
   ```

2. **Run consolidation:**
   ```bash
   python create_consolidated_dataset.py
   ```

## 📋 AWS Credentials Setup

You need AWS credentials with S3 read access. Here's how to get them:

1. **Go to AWS Console** → IAM → Users
2. **Create a new user** (or use existing)
3. **Attach policy:** `AmazonS3ReadOnlyAccess`
4. **Create Access Key** and download credentials
5. **Add to .env file:**
   ```
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   AWS_REGION=us-east-1
   AWS_S3_BUCKET=dsm_data
   ```

## 📁 Output Files

The consolidation process creates:

### Data Files
- `CONSOLIDATED_ALL_STATIONS_YYYYMMDD_HHMMSS.csv` - All data in CSV format
- `CONSOLIDATED_ALL_STATIONS_YYYYMMDD_HHMMSS.parquet` - All data in Parquet format

### Summary Files
- `CONSOLIDATED_SUMMARY_YYYYMMDD_HHMMSS.txt` - Detailed statistics and breakdowns

## 📊 Data Structure

The consolidated dataset includes:

### Core Data Columns
- Power generation data (MW)
- Voltage levels (kV)
- Frequency measurements (Hz)
- Temperature and humidity
- Station status information
- Timestamps

### Metadata Columns
- `region` - NERPC, WRPC, ERLDC, SRPC, NRLDC
- `station` - Station name/identifier
- `source_file` - Original file name
- `source_path` - S3 path to original file
- `processing_timestamp` - When data was consolidated

## 🔍 Data Breakdown

The summary report includes:
- **Total rows and columns**
- **Region breakdown** - Rows per region
- **Top stations** - Most active stations
- **Column statistics** - Non-null value counts
- **File sources** - Which files contributed data

## 🛠️ Troubleshooting

### "Unable to locate credentials"
- Ensure `.env` file exists with valid AWS credentials
- Check that credentials have S3 read permissions

### "Access Denied"
- Verify S3 bucket name is correct
- Ensure user has `s3:GetObject` and `s3:ListBucket` permissions

### "No data found"
- Check that data exists in S3 bucket
- Verify bucket name and region are correct

## 📈 Usage Examples

### Python Analysis
```python
import pandas as pd

# Load consolidated data
df = pd.read_parquet('CONSOLIDATED_ALL_STATIONS_20250912_014525.parquet')

# Filter by region
nerpc_data = df[df['region'] == 'NERPC']

# Group by station
station_summary = df.groupby('station')['power_mw'].mean()

# Time series analysis
df['datetime'] = pd.to_datetime(df['datetime'])
df.set_index('datetime', inplace=True)
```

### Excel/CSV Analysis
- Open the CSV file in Excel or any spreadsheet application
- Use filters to analyze specific regions or stations
- Create pivot tables for data summarization

## 🔄 Updating Data

To get the latest data:
1. Run the consolidation script again
2. New files will be created with updated timestamps
3. Compare with previous versions to see changes

## 📞 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Verify AWS credentials and permissions
3. Ensure S3 bucket contains data
4. Check the log output for specific error messages

---

**Note:** The sample data is for testing purposes only. Use the S3-based consolidation for real production data.
