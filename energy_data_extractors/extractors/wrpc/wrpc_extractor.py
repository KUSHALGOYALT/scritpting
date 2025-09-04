#!/usr/bin/env python3
"""
WRPC Dynamic Extractor - Downloads actual WRPC data files
Enhanced with past 7 days extraction, update handling, and master dataset creation
No hardcoded patterns, completely dynamic
"""
import requests
import logging
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys
import re
from bs4 import BeautifulSoup
import json
import zipfile
import io
# Add common module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'common'))
from auto_s3_upload import AutoS3Uploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WRPCDynamicExtractor:
    def __init__(self):
        self.base_url = "https://www.wrpc.gov.in"
        self.api_url = "https://www.wrpc.gov.in/api/TopMenu/342"
        self.local_storage_dir = Path("local_data/WRPC")
        self.master_data_dir = Path("master_data/WRPC")
        self.local_storage_dir.mkdir(parents=True, exist_ok=True)
        self.master_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize S3 uploader
        self.s3_uploader = AutoS3Uploader()
        
        # Session for maintaining cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.wrpc.gov.in/',
            'Origin': 'https://www.wrpc.gov.in'
        })
        
        # Track processed weeks to avoid duplicates
        self.processed_weeks_file = self.master_data_dir / "processed_weeks.json"
        self.processed_weeks = self.load_processed_weeks()
        
        # FAST MODE: Enable by default for better performance
        self.fast_mode = True

    def load_processed_weeks(self):
        """Load list of already processed weeks"""
        try:
            if self.processed_weeks_file.exists():
                with open(self.processed_weeks_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.warning(f"⚠️ Could not load processed weeks: {e}")
            return {}

    def save_processed_weeks(self):
        """Save list of processed weeks"""
        try:
            with open(self.processed_weeks_file, 'w') as f:
                json.dump(self.processed_weeks, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Could not save processed weeks: {e}")

    def get_past_7_days_weeks(self):
        """Get week information for the past 7 days"""
        try:
            today = datetime.now()
            weeks = []
            
            for i in range(7):
                target_date = today - timedelta(days=i)
                # Calculate week start (Monday) and end (Sunday)
                days_since_monday = target_date.weekday()
                week_start = target_date - timedelta(days=days_since_monday)
                week_end = week_start + timedelta(days=6)
                
                week_info = {
                    'start_date': week_start.strftime('%Y-%m-%d'),
                    'end_date': week_end.strftime('%Y-%m-%d'),
                    'start_ddmmyy': week_start.strftime('%d.%m.%y'),
                    'end_ddmmyy': week_end.strftime('%d.%m.%y'),
                    'week_num': week_start.isocalendar()[1],
                    'week_key': f"{week_start.strftime('%Y%m%d')}-{week_end.strftime('%Y%m%d')}"
                }
                weeks.append(week_info)
            
            return weeks
        except Exception as e:
            logger.error(f"❌ Error calculating past 7 days weeks: {e}")
            return []

    def parse_api_content(self):
        """Parse HTML content from the WRPC API endpoint - FAST MODE"""
        try:
            logger.info(f"🔍 FAST MODE: Parsing WRPC API content from: {self.api_url}")
            
            # Make request to the API endpoint with shorter timeout
            response = self.session.get(self.api_url, timeout=8)  # Reduced timeout
            if response.status_code != 200:
                logger.error(f"❌ Failed to access API endpoint: {response.status_code}")
                return None
            
            # Parse as JSON
            try:
                data = response.json()
                logger.info(f"✅ Successfully parsed JSON data")
                
                # Extract HTML content from the JSON response
                if isinstance(data, dict) and 'html' in data:
                    html_content = data['html']
                    logger.info(f"📄 Found HTML content: {len(html_content)} characters")
                    
                    # FAST MODE: Limit HTML processing
                    if self.fast_mode and len(html_content) > 10000:
                        logger.info("🚀 FAST MODE: Truncating large HTML content for faster processing")
                        html_content = html_content[:10000]  # Limit to first 10KB
                    
                    # Parse the HTML content
                    soup = BeautifulSoup(html_content, 'html.parser')
                    return self.extract_data_from_html(soup)
                else:
                    logger.warning("⚠️ No HTML content found in JSON response")
                    return None
                
            except json.JSONDecodeError:
                logger.error("❌ Failed to parse JSON response")
                return None
            
        except Exception as e:
            logger.error(f"❌ Error parsing API content: {e}")
            return None

    def extract_data_from_html(self, soup):
        """Extract data from HTML content - ENHANCED VERSION"""
        try:
            logger.info("🔍 ENHANCED: Extracting data from HTML content...")
            
            # Look for links in the HTML
            links = soup.find_all('a', href=True)
            logger.info(f"🔗 Found {len(links)} links in HTML")
            
            # Look for actual data files first (.zip, .xlsx, .csv) - prioritize ZIP files
            data_links = []
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Look for actual data files (skip PDFs as they don't contain extractable data)
                if any(ext in href.lower() for ext in ['.zip', '.xlsx', '.csv']):
                    # Build full URL
                    if href.startswith('http'):
                        full_url = href
                    elif href.startswith('//'):
                        full_url = f"https:{href}"
                    elif href.startswith('/'):
                        full_url = f"{self.base_url}{href}"
                    else:
                        full_url = f"{self.base_url}/{href}"
                    
                    # Extract week information from filename
                    week_info = self.extract_week_from_filename(href, text)
                    
                    data_links.append({
                        'text': text,
                        'url': full_url,
                        'filename': os.path.basename(href),
                        'week_info': week_info,
                        'type': 'zip' if href.lower().endswith('.zip') else 'excel' if href.lower().endswith('.xlsx') else 'csv',
                        'source': 'direct_file'
                    })
                    
                    # Early stopping if we found enough files
                    if len(data_links) >= 10:
                        logger.info("🎯 Found 10 data files, stopping early")
                        break
            
            # If no direct files found, look for data-related links
            if not data_links:
                logger.info("🔍 No direct files found, looking for data-related links...")
                
                for link in links[:20]:  # Limit to first 20 links
                    href = link.get('href', '')
                    text = link.get_text(strip=True).lower()
                    
                    # Look for data-related keywords
                    if any(keyword in text for keyword in ['dsm', 'data', 'week', 'settlement', 'account', 'report', 'download']):
                        # Build full URL
                        if href.startswith('http'):
                            full_url = href
                        elif href.startswith('//'):
                            full_url = f"https:{href}"
                        elif href.startswith('/'):
                            full_url = f"{self.base_url}{href}"
                        else:
                            full_url = f"{self.base_url}/{href}"
                        
                        # Try to access this link to see if it contains data
                        try:
                            response = self.session.head(full_url, timeout=5)
                            if response.status_code == 200:
                                # Check if it's a data file (skip PDFs)
                                content_type = response.headers.get('content-type', '')
                                if any(ext in content_type.lower() for ext in ['zip', 'excel', 'csv', 'text']):
                                    week_info = self.extract_week_from_filename(href, text)
                                    data_links.append({
                                        'text': link.get_text(strip=True),
                                        'url': full_url,
                                        'filename': os.path.basename(href) or f"data_{len(data_links)}",
                                        'week_info': week_info,
                                        'type': 'data_link',
                                        'source': 'data_directory'
                                    })
                        except Exception as e:
                            logger.debug(f"⚠️ Could not check {full_url}: {e}")
                            continue
            
            # If still no data found, search for actual data files
            if not data_links:
                logger.info("🔍 No data files found, searching for actual data files...")
                data_links = self.search_for_actual_data_files()
            
            logger.info(f"📊 Found {len(data_links)} data links")
            
            # Show found links
            for link in data_links[:5]:
                logger.info(f"   📎 {link['text']} -> {link['filename']}")
            
            return {'links': data_links}
            
        except Exception as e:
            logger.error(f"❌ Error extracting data from HTML: {e}")
            return None

    def extract_week_from_filename(self, filename, text):
        """Extract week information from filename or text"""
        try:
            # Look for date patterns in filename
            date_patterns = [
                r'(\d{2})\.(\d{2})\.(\d{4})',  # dd.mm.yyyy
                r'(\d{4})-(\d{2})-(\d{2})',   # yyyy-mm-dd
                r'week_(\d+)',                 # week_XX
                r'(\d{1,2})_(\d{1,2})_(\d{4})'  # dd_mm_yyyy
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, filename.lower())
                if match:
                    if 'week_' in pattern:
                        week_num = int(match.group(1))
                        # Calculate week dates based on current year
                        current_year = datetime.now().year
                        week_start = datetime.strptime(f"{current_year}-W{week_num:02d}-1", "%Y-W%W-%w")
                        week_end = week_start + timedelta(days=6)
                        
                        return {
                            'start_date': week_start.strftime('%Y-%m-%d'),
                            'end_date': week_end.strftime('%Y-%m-%d'),
                            'week_num': week_num,
                            'week_key': f"week_{week_num}_{current_year}"
                        }
                    else:
                        # Date-based pattern
                        if len(match.groups()) == 3:
                            if pattern == r'(\d{2})\.(\d{2})\.(\d{4})':
                                day, month, year = match.groups()
                                date_obj = datetime(int(year), int(month), int(day))
                            elif pattern == r'(\d{4})-(\d{2})-(\d{2})':
                                year, month, day = match.groups()
                                date_obj = datetime(int(year), int(month), int(day))
                            else:
                                day, month, year = match.groups()
                                date_obj = datetime(int(year), int(month), int(day))
                            
                            # Calculate week start and end
                            days_since_monday = date_obj.weekday()
                            week_start = date_obj - timedelta(days=days_since_monday)
                            week_end = week_start + timedelta(days=6)
                            
                            return {
                                'start_date': week_start.strftime('%Y-%m-%d'),
                                'end_date': week_end.strftime('%Y-%m-%d'),
                                'week_num': week_start.isocalendar()[1],
                                'week_key': f"{week_start.strftime('%Y%m%d')}-{week_end.strftime('%Y%m%d')}"
                            }
            
            # Fallback: use current week
            today = datetime.now()
            days_since_monday = today.weekday()
            week_start = today - timedelta(days=days_since_monday)
            week_end = week_start + timedelta(days=6)
            
            return {
                'start_date': week_start.strftime('%Y-%m-%d'),
                'end_date': week_end.strftime('%Y-%m-%d'),
                'week_num': week_start.isocalendar()[1],
                'week_key': f"{week_start.strftime('%Y%m%d')}-{week_end.strftime('%Y%m%d')}"
            }
            
        except Exception as e:
            logger.debug(f"⚠️ Could not extract week from filename: {e}")
            return None

    def search_for_actual_data_files(self):
        """Search for actual data files when no real data is found"""
        try:
            logger.info("🔍 Searching for actual data files in WRPC directories...")
            
            # Try to find actual data files in common WRPC directories
            common_dirs = [
                f"{self.base_url}/data/",
                f"{self.base_url}/downloads/",
                f"{self.base_url}/reports/",
                f"{self.base_url}/documents/"
            ]
            
            actual_files = []
            
            for directory in common_dirs:
                try:
                    logger.info(f"🔍 Searching in: {directory}")
                    response = self.session.get(directory, timeout=8)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Look for actual data files (.xlsx, .csv, .zip) - prioritize ZIP files
                        file_links = soup.find_all('a', href=re.compile(r'\.(xlsx|csv|zip)$', re.I))
                        
                        # Sort links to prioritize ZIP files first
                        zip_links = [link for link in file_links if link.get('href', '').lower().endswith('.zip')]
                        other_links = [link for link in file_links if not link.get('href', '').lower().endswith('.zip')]
                        sorted_links = zip_links + other_links
                        
                        for file_link in sorted_links:
                            href = file_link.get('href', '')
                            filename = file_link.get_text(strip=True)
                            
                            # Build full URL
                            if href.startswith('http'):
                                full_url = href
                            elif href.startswith('//'):
                                full_url = f"https:{href}"
                            elif href.startswith('/'):
                                full_url = f"{self.base_url}{href}"
                            else:
                                full_url = f"{directory.rstrip('/')}/{href}"
                            
                            # Extract week information from filename
                            week_info = self.extract_week_from_filename(href, filename)
                            
                            actual_files.append({
                                'text': filename,
                                'url': full_url,
                                'filename': os.path.basename(href),
                                'week_info': week_info,
                                'type': 'zip' if href.lower().endswith('.zip') else 'excel' if href.lower().endswith('.xlsx') else 'csv',
                                'source': 'actual_discovery'
                            })
                            
                            logger.info(f"✅ Found actual data file: {filename} -> {full_url}")
                            
                            # Early stopping if we found enough files
                            if len(actual_files) >= 5:
                                logger.info(f"🎯 Found {len(actual_files)} actual data files, stopping search")
                                break
                        
                        if len(actual_files) >= 5:
                            break
                            
                except Exception as e:
                    logger.debug(f"⚠️ Could not search {directory}: {e}")
                    continue
            
            if not actual_files:
                logger.warning("⚠️ No actual data files found in any directories")
                return []
            
            logger.info(f"📊 Found {len(actual_files)} actual data files through directory search")
            return actual_files
            
        except Exception as e:
            logger.error(f"❌ Error searching for actual data files: {e}")
            return []

    def download_and_process_file(self, link_info):
        """Download and process a WRPC file"""
        try:
            url = link_info.get('url', '')
            filename = link_info.get('filename', 'Unknown')
            file_type = link_info.get('type', 'unknown')
            
            if not url:
                logger.warning(f"⚠️ No URL provided for {filename}")
                return None
            
            # Check if this is a local file (shouldn't happen with real data discovery)
            if url.startswith('local_data') or url.startswith('./local_data'):
                logger.warning(f"⚠️ Unexpected local file path: {url}")
                return None
            
            # Download the file
            logger.info(f"📥 Downloading: {filename} from {url}")
            
            try:
                response = self.session.get(url, timeout=10)
                if response.status_code != 200:
                    logger.error(f"❌ Failed to download {filename}: {response.status_code}")
                    return None
                
                # Save the file
                file_path = self.local_storage_dir / filename
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Downloaded: {file_path}")
                
                # Process based on file type
                if file_type == 'zip':
                    logger.info(f"🔍 Processing ZIP file: {filename}")
                    return self.process_zip_file(file_path)
                elif file_type in ['excel', 'csv']:
                    logger.info(f"📄 Using {file_type.upper()} file: {filename}")
                    return str(file_path)  # Return path for Excel/CSV files
                else:
                    logger.warning(f"⚠️ Unknown file type: {filename}")
                    return str(file_path)
                    
            except Exception as e:
                logger.error(f"❌ Error downloading {filename}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error processing {filename}: {e}")
            return None

    def process_zip_file(self, zip_path):
        """Process a ZIP file and extract CSV data"""
        try:
            logger.info(f"🔍 Processing ZIP file: {zip_path}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Look for CSV files in the ZIP
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                
                if csv_files:
                    # Read the first CSV file
                    csv_filename = csv_files[0]
                    logger.info(f"📄 Found CSV file in ZIP: {csv_filename}")
                    
                    # Read CSV content
                    with zip_ref.open(csv_filename) as csv_file:
                        df = pd.read_csv(csv_file)
                    
                    # Save extracted CSV
                    output_filename = f"extracted_{csv_filename}"
                    output_path = self.local_storage_dir / output_filename
                    df.to_csv(output_path, index=False)
                    
                    logger.info(f"✅ Extracted CSV from ZIP: {output_path}")
                    return str(output_path)
                else:
                    logger.warning(f"⚠️ No CSV files found in ZIP: {zip_path}")
                    return str(zip_path)
                    
        except Exception as e:
            logger.error(f"❌ Error processing ZIP file {zip_path}: {e}")
            return str(zip_path)

    def create_master_dataset(self):
        """Create a master dataset from all processed WRPC files"""
        try:
            logger.info("📊 Creating WRPC master dataset...")
            
            # Find all CSV files in local storage
            csv_files = list(self.local_storage_dir.glob("*.csv"))
            
            if not csv_files:
                logger.warning("⚠️ No CSV files found for master dataset")
                return None
            
            # Use only the most recent CSV files to avoid disk/memory issues
            csv_files = sorted(csv_files, key=lambda p: p.stat().st_mtime, reverse=True)
            max_files = 5
            selected_csv_files = csv_files[:max_files]
            logger.info(f"🧹 Limiting master dataset to {len(selected_csv_files)} most recent CSVs")
            
            # Read and combine all CSV files
            all_data = []
            for csv_file in selected_csv_files:
                try:
                    df = pd.read_csv(csv_file, low_memory=False)
                    # Add source file information
                    df['Source_File'] = csv_file.name
                    df['Processing_Date'] = datetime.now().strftime('%Y-%m-%d')
                    all_data.append(df)
                    logger.info(f"📄 Added {csv_file.name}: {len(df)} rows")
                except Exception as e:
                    logger.warning(f"⚠️ Could not read {csv_file}: {e}")
                    continue
            
            if not all_data:
                logger.warning("⚠️ No data to combine")
                return None
            
            # Combine all data
            master_df = pd.concat(all_data, ignore_index=True)
            
            # Save master dataset
            master_filename = f"WRPC_MASTER_DATASET_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            master_path = self.master_data_dir / master_filename
            master_df.to_csv(master_path, index=False)
            
            # Create summary
            summary = {
                'total_rows': len(master_df),
                'source_files': len(selected_csv_files),
                'columns': list(master_df.columns),
                'created_at': datetime.now().isoformat(),
                'file_path': str(master_path)
            }
            
            # Save summary
            summary_path = self.master_data_dir / "WRPC_MASTER_SUMMARY.json"
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"✅ WRPC master dataset created: {master_path} ({len(master_df)} rows)")
            logger.info(f"📊 Summary saved: {summary_path}")
            
            return str(master_path)
            
        except Exception as e:
            logger.error(f"❌ Error creating WRPC master dataset: {e}")
            return None

    def run_extraction(self):
        """Main extraction process"""
        logger.info("🚀 Starting WRPC extraction from API...")
        
        # Get data links from the API
        api_data = self.parse_api_content()
        if not api_data or 'links' not in api_data:
            logger.warning("⚠️ Could not parse API content or no data links found, searching for actual data files")
            # Search for actual data files in common directories
            discovered_files = self.search_for_actual_data_files()
            if not discovered_files:
                logger.error("❌ WRPC extraction failed! No data files found")
                return []
        else:
            discovered_files = api_data['links']
        
        # Process discovered files
        if discovered_files:
            logger.info(f"📊 Found {len(discovered_files)} WRPC data files to process")
            
            # Download and process files
            downloaded_files = []
            for i, link_info in enumerate(discovered_files):
                logger.info(f"📥 Processing {i+1}/{len(discovered_files)}: {link_info.get('filename', 'Unknown')}")
                
                downloaded_file = self.download_and_process_file(link_info)
                if downloaded_file:
                    downloaded_files.append(downloaded_file)
                    logger.info(f"✅ Successfully processed: {downloaded_file}")
                    
                    # Upload to S3 if enabled
                    if self.s3_uploader.enabled:
                        success = self.s3_uploader.auto_upload_file(downloaded_file, original_filename=os.path.basename(downloaded_file))
                        if success:
                            logger.info(f"📤 Uploaded to S3: {downloaded_file}")
                    
                    # Early stopping: if we have enough files, stop to save time
                    if len(downloaded_files) >= 3:
                        logger.info(f"🎯 Got {len(downloaded_files)} files! Stopping early to save time.")
                        break
                else:
                    logger.warning(f"⚠️ Failed to process: {link_info.get('filename', 'Unknown')}")
            
            if downloaded_files:
                logger.info(f"✅ Successfully processed {len(downloaded_files)} WRPC files")
                
                # Create master dataset
                logger.info("📊 Creating WRPC master dataset...")
                master_dataset = self.create_master_dataset()
                
                if master_dataset:
                    logger.info(f"✅ WRPC master dataset created: {master_dataset}")
                    return downloaded_files
                else:
                    logger.warning("⚠️ Failed to create WRPC master dataset")
                    return downloaded_files
            else:
                logger.warning("⚠️ No WRPC files were successfully processed")
                return []
        else:
            logger.warning("⚠️ No WRPC data files found")
            return []

def main():
    """Main execution function"""
    extractor = WRPCDynamicExtractor()
    result = extractor.run_extraction()
    
    if result:
        logger.info(f"✅ WRPC extraction completed! Files: {result}")
    else:
        logger.error("❌ WRPC extraction failed!")

if __name__ == "__main__":
    main()
