# NERPC Dynamic Extractor

## Overview

The NERPC (North Eastern Regional Power Committee) Dynamic Extractor is designed to automatically download and process energy data from the NERPC website. It handles both regular and revised data patterns, extracts data from ZIP files containing multiple CSV sheets, and creates master datasets.

## Features

- **Dynamic Data Discovery**: Automatically discovers data files from the NERPC website
- **Date Range Filtering**: Only processes files from the past 7 days
- **Pattern Recognition**: Handles both regular and revised data patterns:
  - Regular: `Data_File_25.08.2025to31.08.2025.zip`
  - Revised: `DSMR1_DATAFILE_21.07.25to27.07.25.zip`
- **ZIP Processing**: Extracts and processes multiple CSV files from ZIP archives
- **Master Dataset Creation**: Combines all processed data into a single master dataset
- **S3 Integration**: Automatically uploads processed files to AWS S3
- **Duplicate Prevention**: Tracks processed files to avoid reprocessing

## Data Source

- **Website**: https://nerpc.gov.in/?page_id=5823
- **Data Type**: DSM, SRAS, TRAS, and SCUC accounts
- **Format**: ZIP files containing CSV data
- **Update Frequency**: Weekly data releases

## Usage

### Standalone Usage

```python
from nerpc_extractor import NERPCDynamicExtractor

# Initialize extractor
extractor = NERPCDynamicExtractor()

# Run extraction
result = extractor.run_extraction()

if result['status'] == 'success':
    print(f"Processed {result['files_processed']} files")
    if result.get('master_dataset'):
        print(f"Master dataset: {result['master_dataset']}")
```

### Command Line Usage

```bash
# Run only NERPC extractor
python run_nerpc_only.py

# Run full pipeline (including NERPC)
python run_pipeline.py
```

### Testing

```bash
# Test the extractor without downloading files
python test_nerpc_extractor.py
```

## Output Structure

### Local Storage
```
local_data/NERPC/
├── processed_files.json          # Track of processed files
├── Data_File_25.08.2025to31.08.2025_Sheet1.csv
├── Data_File_25.08.2025to31.08.2025_Sheet1.parquet
└── ...
```

### Master Data
```
master_data/NERPC/
├── NERPC_Master_Dataset_20250911_113000.csv
├── NERPC_Master_Dataset_20250911_113000.parquet
└── NERPC_Master_Summary_20250911_113000.json
```

### S3 Storage
```
s3://bucket-name/nerpc/
├── Data_File_25.08.2025to31.08.2025/
│   ├── Sheet1.parquet
│   └── Sheet2.parquet
└── master/
    └── NERPC_Master_Dataset_20250911_113000.csv
```

## Configuration

The extractor uses the following configuration:

- **Base URL**: https://nerpc.gov.in
- **Data Page**: https://nerpc.gov.in/?page_id=5823
- **Date Range**: Past 7 days from execution
- **File Patterns**: 
  - Regular: `Data_File_*.zip`
  - Revised: `DSMR1_DATAFILE_*.zip`

## Data Processing

1. **Discovery**: Scans the NERPC data page for available files
2. **Filtering**: Filters files to only include those from the past 7 days
3. **Download**: Downloads ZIP files from the discovered URLs
4. **Extraction**: Extracts CSV files from ZIP archives
5. **Processing**: Converts CSV to Parquet format for efficiency
6. **Upload**: Uploads processed files to S3
7. **Master Dataset**: Creates a combined master dataset
8. **Tracking**: Records processed files to prevent duplicates

## Error Handling

- **Network Errors**: Retries with exponential backoff
- **File Processing Errors**: Logs errors and continues with other files
- **S3 Upload Errors**: Logs errors but continues processing
- **Duplicate Files**: Automatically skips already processed files

## Dependencies

- `requests`: HTTP requests
- `pandas`: Data processing
- `beautifulsoup4`: HTML parsing
- `boto3`: AWS S3 integration
- `pyarrow`: Parquet file support

## Integration

The NERPC extractor is integrated into the main pipeline (`run_pipeline.py`) and runs alongside other regional extractors (ERLDC, WRPC, SRPC).

## Monitoring

The extractor provides detailed logging including:
- File discovery progress
- Download status
- Processing results
- S3 upload status
- Error messages

## Future Enhancements

- Support for additional data formats
- Enhanced date parsing for different formats
- Real-time monitoring and alerting
- Data validation and quality checks
