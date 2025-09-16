# NERPC BeautifulSoup Extractor

A robust Python script that uses BeautifulSoup to extract ZIP file links from the "Data File" column in HTML tables and downloads only new or updated files.

## 🎯 Features

- **Dynamic Column Detection**: Automatically finds the "Data File" column in HTML tables
- **Smart File Management**: Downloads only new or updated files using `Last-Modified` headers
- **Robust Link Extraction**: Handles various link formats (relative, absolute, full URLs)
- **S3 Integration**: Automatically uploads files to S3 with proper partitioning
- **Comprehensive Logging**: Detailed logs of all actions and file status
- **Error Handling**: Robust error handling for network issues and file processing

## 📋 Requirements

```bash
pip install beautifulsoup4 requests pandas pyarrow
```

## 🚀 Quick Start

### 1. Basic Usage

```python
from nerpc_beautifulsoup_extractor import NERPCBeautifulSoupExtractor

# Initialize extractor
extractor = NERPCBeautifulSoupExtractor()

# Run extraction (will use default URL from config)
results = extractor.run_extraction()

# Or specify a custom URL
results = extractor.run_extraction("https://www.nerpc.gov.in/custom-data-page")
```

### 2. Configuration

Update the URLs in `nerpc_config.py`:

```python
NERPC_BASE_URL = "https://www.nerpc.gov.in"  # Your actual NERPC URL
NERPC_DATA_PAGES = [
    "https://www.nerpc.gov.in/data-files",
    "https://www.nerpc.gov.in/reports",
    # Add more URLs as needed
]
```

### 3. Run the Extractor

```bash
python nerpc_beautifulsoup_extractor.py
```

## 🔍 How It Works

### Step 1: HTML Table Analysis
- Fetches the HTML page containing data tables
- Searches for table headers containing "Data File"
- Identifies the correct column index

### Step 2: Link Extraction
- Iterates through all table rows in the `<tbody>`
- Extracts all `<a>` tags from the Data File column
- Identifies ZIP files by extension or text content

### Step 3: Smart Downloading
- Checks if file exists locally
- Compares `Last-Modified` headers with local file timestamps
- Downloads only new or updated files
- Skips unchanged files to save bandwidth

### Step 4: S3 Upload
- Uploads raw files to: `dsm_data/raw/NERPC/{year}/{month}/{filename}`
- Processes ZIP contents and uploads as parquet: `dsm_data/parquet/NERPC/{station}/{year}/{month}/{filename}`

## 📊 Output Example

```
📊 EXTRACTION RESULTS:
================================================================================
Table: 0 | Row: 0 | File: dsm_20250901.zip | Action: downloaded | Reason: new_or_updated
Table: 0 | Row: 1 | File: sras_20250902.zip | Action: skipped | Reason: up_to_date
Table: 0 | Row: 2 | File: tras_20250903.zip | Action: downloaded | Reason: new_or_updated
================================================================================
SUMMARY:
  Downloaded: 2 files
  Skipped: 1 files
Total files processed: 3
```

## 🛠️ Configuration Options

### Table Configuration
```python
TABLE_CONFIG = {
    "data_file_headers": [
        "Data File (DSM/SRAS/TRAS/SCUC)",
        "Data File",
        "DSM Data File",
        # Add more header variations
    ],
    "target_extensions": [".zip", ".rar", ".7z"],
    "processable_extensions": [".csv", ".xls", ".xlsx", ".txt"]
}
```

### Download Configuration
```python
DOWNLOAD_CONFIG = {
    "timeout": 60,
    "retry_attempts": 3,
    "retry_delay": 5,
    "chunk_size": 8192,
    "local_storage_dir": "downloads"
}
```

## 🧪 Testing

Run the test script to verify functionality:

```bash
python test_nerpc_beautifulsoup.py
```

This will test:
- HTML parsing
- Column detection
- Link extraction
- File update checking

## 🔧 Customization

### Adding New File Types
To support additional file types, update the `target_extensions` in `nerpc_config.py`:

```python
"target_extensions": [".zip", ".rar", ".7z", ".tar.gz", ".tar.bz2"]
```

### Custom S3 Paths
Modify the S3 path patterns in `nerpc_config.py`:

```python
S3_CONFIG = {
    "raw_path_pattern": "your_custom_path/raw/{year}/{month}/{filename}",
    "parquet_path_pattern": "your_custom_path/parquet/{station}/{year}/{month}/{filename}",
}
```

### Multiple Data Pages
Add multiple URLs to process different NERPC data pages:

```python
# In your main script
urls = [
    "https://www.nerpc.gov.in/data-files",
    "https://www.nerpc.gov.in/reports", 
    "https://www.nerpc.gov.in/archive"
]

for url in urls:
    results = extractor.run_extraction(url)
```

## 🚨 Error Handling

The extractor handles various error scenarios:

- **Network timeouts**: Automatic retries with exponential backoff
- **Missing columns**: Fallback to alternative column detection methods
- **Invalid URLs**: Skips broken links and logs warnings
- **File processing errors**: Continues processing other files
- **S3 upload failures**: Logs errors but doesn't stop extraction

## 📝 Logging

All actions are logged with timestamps and detailed information:

- File discovery and extraction
- Download status and file sizes
- S3 upload confirmations
- Error messages and warnings
- Summary statistics

## 🔄 Automation

For automated runs, consider:

1. **Cron job**: Run daily/weekly
2. **CI/CD pipeline**: Integrate with your deployment process
3. **Monitoring**: Set up alerts for failed extractions

Example cron job:
```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/nerpc && python nerpc_beautifulsoup_extractor.py
```

## 🤝 Contributing

To extend the extractor:

1. Add new column header patterns to `TABLE_CONFIG`
2. Implement custom file processing in `_process_zip_and_upload_parquet`
3. Add new file type support in the link extraction logic

## 📄 License

This project is part of the energy data extractors suite.