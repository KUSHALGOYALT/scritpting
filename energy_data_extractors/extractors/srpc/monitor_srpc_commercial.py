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
from bs4 import BeautifulSoup
import json
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from energy_data_extractors.common.auto_s3_upload import AutoS3Uploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_existing_files():
    """Get list of files that have already been processed"""
    tracking_file = Path(__file__).parent / "processed_files.json"
    if tracking_file.exists():
        with open(tracking_file, 'r') as f:
            return set(json.load(f))
    return set()

def save_processed_file(filename):
    """Save filename to tracking file"""
    tracking_file = Path(__file__).parent / "processed_files.json"
    existing_files = get_existing_files()
    existing_files.add(filename)
    
    with open(tracking_file, 'w') as f:
        json.dump(list(existing_files), f, indent=2)

def monitor_srpc_commercial():
    """Monitor SRPC commercial data page for new files"""
    s3_uploader = AutoS3Uploader()
    base_url = "https://www.srpc.kar.nic.in"
    commercial_url = "https://www.srpc.kar.nic.in/html/xml-search/commercial.html"
    
    logger.info(f"🔍 Monitoring SRPC commercial data page: {commercial_url}")
    
    # Get existing processed files
    processed_files = get_existing_files()
    logger.info(f"📋 Found {len(processed_files)} previously processed files")
    
    try:
        # Get the commercial page
        response = requests.get(commercial_url, timeout=60, verify=False)
        response.raise_for_status()
        
        # Parse HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all data file links
        data_links = []
        
        # Look for links in tables with "Data File" column
        tables = soup.find_all('table')
        for table in tables:
            # Check if this table has a "Data File" header
            headers = table.find_all('th')
            has_data_file_header = any('data file' in th.get_text().lower() for th in headers)
            
            if has_data_file_header:
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 7:  # Check if we have enough columns
                        # Look for download links in the 7th column (Data File column)
                        data_file_cell = cells[6] if len(cells) > 6 else None
                        if data_file_cell:
                            links = data_file_cell.find_all('a', href=True)
                            for link in links:
                                href = link.get('href', '')
                                text = link.get_text(strip=True)
                                
                                # Check if it's a data file (ZIP, XLS, XLSX, CSV)
                                if any(ext in href.lower() for ext in ['.zip', '.xls', '.xlsx', '.csv']):
                                    full_url = base_url + href if href.startswith('/') else href
                                    data_links.append({
                                        'filename': text,
                                        'url': full_url,
                                        'href': href
                                    })
        
        # Also look for any direct download links on the page
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Check if it's a data file
            if any(ext in href.lower() for ext in ['.zip', '.xls', '.xlsx', '.csv']):
                full_url = base_url + href if href.startswith('/') else href
                data_links.append({
                    'filename': text,
                    'url': full_url,
                    'href': href
                })
        
        logger.info(f"📊 Found {len(data_links)} potential data files")
        
        # Remove duplicates
        unique_links = {}
        for link in data_links:
            key = link['url']
            if key not in unique_links:
                unique_links[key] = link
        
        logger.info(f"📊 Found {len(unique_links)} unique data files")
        
        # Check for new files
        new_files = []
        for url, link_info in unique_links.items():
            filename = link_info['filename']
            if filename not in processed_files:
                new_files.append(link_info)
        
        if new_files:
            logger.info(f"🆕 Found {len(new_files)} NEW files to process:")
            for file_info in new_files:
                logger.info(f"  📄 {file_info['filename']}")
            
            # Process each new file
            for file_info in new_files:
                try:
                    download_and_process_file(file_info, s3_uploader)
                    save_processed_file(file_info['filename'])
                    logger.info(f"✅ Successfully processed: {file_info['filename']}")
                except Exception as e:
                    logger.error(f"❌ Failed to process {file_info['filename']}: {e}")
        else:
            logger.info("✅ No new files found - all files already processed")
            
    except Exception as e:
        logger.error(f"❌ Error monitoring SRPC commercial page: {e}")

def download_and_process_file(file_info, s3_uploader):
    """Download and process a single file"""
    filename = file_info['filename']
    url = file_info['url']
    
    logger.info(f"📥 Downloading: {filename}")
    
    # Download the file with retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=60, verify=False)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise e
            logger.warning(f"⚠️ Attempt {attempt + 1} failed: {e}. Retrying...")
            time.sleep(5)
    
    # Extract date from filename or use current date
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month
    date_str = current_date.strftime("%Y%m%d")
    
    # Upload raw file to S3
    raw_key = f"dsm_data/raw/SRPC/{year}/{month:02d}/{filename}"
    
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=Path(filename).suffix, delete=False) as temp_file:
            temp_file.write(response.content)
            temp_file_path = temp_file.name
        
        s3_uploader.s3_client.upload_file(temp_file_path, s3_uploader.bucket_name, raw_key)
        logger.info(f"📤 Uploaded raw file to s3://{raw_key}")
        
        os.unlink(temp_file_path)
    except Exception as e:
        logger.error(f"❌ Failed to upload raw file: {e}")
        return
    
    # Process the file based on its type
    if filename.lower().endswith('.zip'):
        process_zip_file(response.content, filename, year, month, date_str, s3_uploader)
    elif filename.lower().endswith(('.xls', '.xlsx')):
        process_excel_file(response.content, filename, year, month, date_str, s3_uploader)
    elif filename.lower().endswith('.csv'):
        process_csv_file(response.content, filename, year, month, date_str, s3_uploader)

def process_zip_file(content, filename, year, month, date_str, s3_uploader):
    """Process ZIP file and extract CSVs"""
    logger.info(f"📦 Processing ZIP file: {filename}")
    
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
            file_list = zip_file.namelist()
            logger.info(f"📋 Found {len(file_list)} files in ZIP")
            
            for csv_name in file_list:
                if csv_name.endswith('.csv'):
                    try:
                        with zip_file.open(csv_name) as csv_file:
                            file_content = csv_file.read()
                            csv_df = pd.read_csv(io.BytesIO(file_content))
                            
                            # Upload CSV to raw S3
                            csv_filename = f"{date_str}_{csv_name}"
                            csv_raw_key = f"dsm_data/raw/SRPC/{year}/{month:02d}/{csv_filename}"
                            
                            import tempfile
                            with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_csv:
                                temp_csv.write(file_content)
                                temp_csv_path = temp_csv.name
                            
                            s3_uploader.s3_client.upload_file(temp_csv_path, s3_uploader.bucket_name, csv_raw_key)
                            logger.info(f"📤 Uploaded CSV to s3://{csv_raw_key}")
                            
                            os.unlink(temp_csv_path)
                            
                            # Process for parquet
                            csv_df['__date__'] = datetime(year, month, 1)
                            csv_df['__source_file__'] = filename
                            csv_df['__csv_name__'] = csv_name
                            
                            # Convert to parquet and upload
                            import tempfile
                            tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
                            
                            try:
                                csv_df.to_parquet(tmp_pq, index=False)
                                
                                # Create parquet filename
                                base_name = csv_name.replace('.csv', '')
                                pq_filename = f"SRPC_{base_name}_{year}_{month:02d}_{date_str}.parquet"
                                pq_key = f"dsm_data/parquet/SRPC/{base_name}/{year}/{month:02d}/{pq_filename}"
                                
                                s3_uploader.s3_client.upload_file(str(tmp_pq), s3_uploader.bucket_name, pq_key)
                                logger.info(f"📤 Uploaded parquet to s3://{pq_key}")
                                
                                tmp_pq.unlink()
                            except Exception as e:
                                logger.error(f"❌ Failed to process CSV {csv_name}: {e}")
                                if tmp_pq.exists():
                                    tmp_pq.unlink()
                                    
                    except Exception as e:
                        logger.error(f"❌ Failed to process CSV {csv_name}: {e}")
                        
    except Exception as e:
        logger.error(f"❌ Failed to process ZIP file: {e}")

def process_excel_file(content, filename, year, month, date_str, s3_uploader):
    """Process Excel file"""
    logger.info(f"📊 Processing Excel file: {filename}")
    
    try:
        # Upload Excel to raw S3 (already done in main function)
        
        # Convert Excel to DataFrame and process
        excel_df = pd.read_excel(io.BytesIO(content))
        
        # Add metadata
        excel_df['__date__'] = datetime(year, month, 1)
        excel_df['__source_file__'] = filename
        
        # Convert to parquet and upload
        import tempfile
        tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
        
        try:
            excel_df.to_parquet(tmp_pq, index=False)
            
            # Create parquet filename
            base_name = filename.replace('.xlsx', '').replace('.xls', '')
            pq_filename = f"SRPC_{base_name}_{year}_{month:02d}_{date_str}.parquet"
            pq_key = f"dsm_data/parquet/SRPC/{base_name}/{year}/{month:02d}/{pq_filename}"
            
            s3_uploader.s3_client.upload_file(str(tmp_pq), s3_uploader.bucket_name, pq_key)
            logger.info(f"📤 Uploaded parquet to s3://{pq_key}")
            
            tmp_pq.unlink()
        except Exception as e:
            logger.error(f"❌ Failed to process Excel file: {e}")
            if tmp_pq.exists():
                tmp_pq.unlink()
                
    except Exception as e:
        logger.error(f"❌ Failed to process Excel file: {e}")

def process_csv_file(content, filename, year, month, date_str, s3_uploader):
    """Process CSV file"""
    logger.info(f"📄 Processing CSV file: {filename}")
    
    try:
        # Upload CSV to raw S3 (already done in main function)
        
        # Convert CSV to DataFrame and process
        csv_df = pd.read_csv(io.BytesIO(content))
        
        # Add metadata
        csv_df['__date__'] = datetime(year, month, 1)
        csv_df['__source_file__'] = filename
        
        # Convert to parquet and upload
        import tempfile
        tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
        
        try:
            csv_df.to_parquet(tmp_pq, index=False)
            
            # Create parquet filename
            base_name = filename.replace('.csv', '')
            pq_filename = f"SRPC_{base_name}_{year}_{month:02d}_{date_str}.parquet"
            pq_key = f"dsm_data/parquet/SRPC/{base_name}/{year}/{month:02d}/{pq_filename}"
            
            s3_uploader.s3_client.upload_file(str(tmp_pq), s3_uploader.bucket_name, pq_key)
            logger.info(f"📤 Uploaded parquet to s3://{pq_key}")
            
            tmp_pq.unlink()
        except Exception as e:
            logger.error(f"❌ Failed to process CSV file: {e}")
            if tmp_pq.exists():
                tmp_pq.unlink()
                
    except Exception as e:
        logger.error(f"❌ Failed to process CSV file: {e}")

if __name__ == "__main__":
    monitor_srpc_commercial()
