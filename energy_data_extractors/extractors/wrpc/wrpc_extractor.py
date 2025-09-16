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
        
        # Track processed weeks to avoid duplicates (no local storage)
        self.processed_weeks = self.load_processed_weeks()
        
        # FAST MODE: Enable by default for better performance
        self.fast_mode = True

    def load_processed_weeks(self):
        """Load list of already processed weeks (no local storage)"""
        # For now, we'll skip file tracking to avoid local storage
        # In production, this could be stored in S3 or a database
        return set()

    def save_processed_weeks(self):
        """Save list of processed weeks (no local storage)"""
        # For now, we'll skip file tracking to avoid local storage
        # In production, this could be stored in S3 or a database
        pass

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
                
                # Save the file to temporary location
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}") as temp_file:
                    temp_file.write(response.content)
                    file_path = temp_file.name
                
                logger.info(f"✅ Downloaded: {file_path}")
                
                # Process based on file type
                if file_type == 'zip':
                    logger.info(f"🔍 Processing ZIP file: {filename}")
                    result = self.process_zip_file(file_path)
                    # process_zip_file now returns a list of extracted files
                    return result if isinstance(result, list) else [result]
                elif file_type in ['excel', 'csv']:
                    logger.info(f"📄 Using {file_type.upper()} file: {filename}")
                    return [str(file_path)]  # Return list for consistency
                else:
                    logger.warning(f"⚠️ Unknown file type: {filename}")
                    return [str(file_path)]
                    
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
                    logger.info(f"📄 Found {len(csv_files)} CSV files in ZIP")
                    extracted_files = []
                    all_dataframes = []  # Collect all dataframes for parquet export
                    
                    # Process ALL CSV files in the ZIP
                    for csv_filename in csv_files:
                        try:
                            logger.info(f"📄 Processing CSV file: {csv_filename}")
                            
                            # Read CSV content
                            with zip_ref.open(csv_filename) as csv_file:
                                df = pd.read_csv(csv_file)
                            
                            # Derive station name from CSV filename (e.g., ACBIL_DSM-2024_Data.csv -> ACBIL)
                            try:
                                station_token = os.path.basename(csv_filename).split('_')[0].strip()
                                if station_token:
                                    df['Station_Name'] = station_token
                                    logger.info(f"🏷️ Station name extracted: {station_token}")
                                else:
                                    logger.warning(f"⚠️ Could not extract station name from: {csv_filename}")
                            except Exception as e:
                                logger.warning(f"⚠️ Error extracting station name from {csv_filename}: {e}")
                            
                            # Add source file information
                            df['Source_File'] = f"extracted_{csv_filename}"
                            
                            # Save extracted CSV to temporary file
                            output_filename = f"extracted_{csv_filename}"
                            import tempfile
                            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                                df.to_csv(temp_file.name, index=False)
                                output_path = temp_file.name
                            
                            logger.info(f"✅ Extracted CSV from ZIP: {output_filename}")
                            
                            # Auto-upload to S3 with new path structure
                            try:
                                # Raw: dsm_data/raw/WRPC/{year}/{month}/{filename}
                                from datetime import datetime as dt
                                current_date = dt.now()
                                s3_key = f"dsm_data/raw/WRPC/{current_date.year}/{current_date.month:02d}/{output_filename}"
                                self.s3_uploader.auto_upload_file(str(output_path), original_filename=s3_key)
                                logger.info(f"📤 Uploaded to S3: {s3_key}")
                            except Exception as e:
                                logger.warning(f"⚠️ S3 upload failed for {csv_filename}: {e}")
                            
                            extracted_files.append(str(output_path))
                            all_dataframes.append(df)
                            
                        except Exception as e:
                            logger.error(f"❌ Error processing CSV file {csv_filename}: {e}")
                            continue
                    
                    # Combine all dataframes and export parquet files
                    if all_dataframes:
                        try:
                            logger.info(f"🔄 Combining {len(all_dataframes)} dataframes for parquet export...")
                            combined_df = pd.concat(all_dataframes, ignore_index=True)
                            logger.info(f"📊 Combined dataframe has {len(combined_df)} rows")
                            
                            # Export parquet files using the existing function
                            self._export_partitioned_to_s3(combined_df)
                            logger.info("✅ Parquet files exported successfully")
                        except Exception as e:
                            logger.warning(f"⚠️ Parquet export failed: {e}")
                    
                    logger.info(f"✅ Successfully processed {len(extracted_files)} CSV files from ZIP")
                    return extracted_files
                else:
                    logger.warning(f"⚠️ No CSV files found in ZIP: {zip_path}")
                    return str(zip_path)
                    
        except Exception as e:
            logger.error(f"❌ Error processing ZIP file {zip_path}: {e}")
            return str(zip_path)


    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names by removing units"""
        try:
            column_mapping = {}
            for col in df.columns:
                col_str = str(col).strip()
                col_lower = col_str.lower()
                
                # Remove units from energy column names
                if 'actual' in col_lower and ('mwh' in col_lower or 'kwh' in col_lower):
                    column_mapping[col] = 'actual'
                elif 'schedule' in col_lower and ('mwh' in col_lower or 'kwh' in col_lower):
                    column_mapping[col] = 'schedule'
                elif 'deviation' in col_lower and ('mwh' in col_lower or 'kwh' in col_lower):
                    column_mapping[col] = 'deviation'
                elif 'freq' in col_lower and 'hz' in col_lower:
                    column_mapping[col] = 'frequency'
                elif col_lower in ['station', 'station_name', 'entity']:
                    column_mapping[col] = 'station_name'
                elif col_lower in ['date', 'datetime', 'timestamp']:
                    column_mapping[col] = 'date'
            
            # Apply mapping
            df = df.rename(columns=column_mapping)
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ Error standardizing column names: {e}")
            return df

    def _convert_kwh_to_mwh(self, df: pd.DataFrame) -> None:
        """Convert KWh data to MWh by dividing by 1000"""
        try:
            # Energy columns that might be in KWh
            energy_columns = ['actual', 'schedule', 'deviation']
            
            for col in energy_columns:
                if col in df.columns:
                    # Check if this column contains KWh data
                    if df[col].dtype in ['object', 'string']:
                        # Try to convert to numeric first
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Check if values are in KWh range (typically much larger than MWh)
                    if df[col].dtype in ['int64', 'float64']:
                        non_null_values = df[col].dropna()
                        if len(non_null_values) > 0:
                            # If values are in thousands range, likely KWh
                            median_value = non_null_values.median()
                            if median_value > 100:  # KWh values are typically much larger
                                logger.info(f"🔄 Converting {col} from KWh to MWh (dividing by 1000)")
                                df[col] = df[col] / 1000
                                logger.info(f"✅ Converted {col} to MWh")
            
        except Exception as e:
            logger.warning(f"⚠️ Error converting KWh to MWh: {e}")

    def _export_partitioned_to_s3(self, master_df: pd.DataFrame) -> None:
        """Export CSV and Parquet per station/year/month to S3 under dsm_data/raw and dsm_data/parquet for WRPC."""
        try:
            if self.s3_uploader is None or not hasattr(self.s3_uploader, 'auto_upload_file'):
                logger.info("⏭️ S3 uploader not configured; skipping S3 export (WRPC)")
                return
            if master_df.empty:
                return
            # Determine station column
            station_col = None
            for c in ['Station_Name','Station','Entity','Utility','Member']:
                if c in master_df.columns:
                    station_col = c
                    break
            if station_col is None:
                # Extract station name from Source_File column if available
                if 'Source_File' in master_df.columns:
                    # Extract station name from filename (e.g., "extracted_ACBIL_DSM-2024_Data.csv" -> "ACBIL")
                    def extract_station_name(filename):
                        try:
                            # Remove "extracted_" prefix if present
                            clean_name = filename.replace('extracted_', '')
                            # Split by underscore and take first part
                            station_name = clean_name.split('_')[0]
                            return station_name.upper()
                        except:
                            return 'WRPC'
                    
                    master_df = master_df.copy()
                    master_df['Station_Name'] = master_df['Source_File'].apply(extract_station_name)
                    station_col = 'Station_Name'
                else:
                    station_col = 'WRPC'
                    master_df = master_df.copy()
                    master_df[station_col] = 'WRPC'
            # Parse Date column if exists
            if 'Date' in master_df.columns:
                date_series = pd.to_datetime(master_df['Date'], errors='coerce')
            else:
                date_series = pd.to_datetime(datetime.now())
            df = master_df.copy()
            df['__date__'] = date_series
            df['__year__'] = df['__date__'].dt.year.fillna(datetime.now().year).astype(int)
            df['__month__'] = df['__date__'].dt.month.fillna(datetime.now().month).astype(int)
            base_raw = 'dsm_data/raw'
            base_parquet = 'dsm_data/parquet'
            for station, g1 in df.groupby(station_col):
                safe_station = str(station).strip().replace('/', '_').replace(' ', '_')
                for (year, month), g2 in g1.groupby(['__year__','__month__']):
                    part_df = g2.drop(columns=[c for c in ['__date__','__year__','__month__'] if c in g2.columns]).copy()
                    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                    csv_name = f"WRPC_{safe_station}_{year}_{month:02d}_{ts}.csv"
                    pq_name = f"WRPC_{safe_station}_{year}_{month:02d}_{ts}.parquet"
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_file:
                        part_df.to_csv(csv_file.name, index=False)
                        tmp_csv = csv_file.name
                    
                    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as parquet_file:
                        tmp_pq = parquet_file.name
                    
                    try:
                        from datetime import datetime as _dt
                        _week = _dt.now().isocalendar().week
                        # Raw: dsm_data/raw/WRPC/{year}/{month}/{filename}
                        s3_key = f"dsm_data/raw/WRPC/{year}/{month:02d}/{csv_name}"
                        self.s3_uploader.auto_upload_file(str(tmp_csv), original_filename=s3_key)
                        logger.info(f"📤 Uploaded CSV to s3://{s3_key}")
                        # Clean up temp CSV file
                        os.unlink(tmp_csv)
                    except Exception as e:
                        logger.warning(f"⚠️ CSV upload failed (WRPC {safe_station} {year}-{month:02d}): {e}")
                        # Clean up temp CSV file
                        if os.path.exists(tmp_csv):
                            os.unlink(tmp_csv)
                    try:
                        # Final cleanup before parquet conversion - remove duplicate columns
                        part_df_clean = part_df.loc[:, ~part_df.columns.duplicated()]
                        
                        # Additional cleanup for columns with .1, .2 suffixes
                        final_columns = []
                        seen_base_names = set()
                        for col in part_df_clean.columns:
                            base_name = col.split('.')[0]
                            if base_name not in seen_base_names:
                                final_columns.append(col)
                                seen_base_names.add(base_name)
                        
                        part_df_clean = part_df_clean[final_columns]
                        
                        part_df_clean.to_parquet(tmp_pq, index=False)
                        # Parquet: dsm_data/parquet/WRPC/{station_name}/{year}/{month}/{filename}
                        s3_key_p = f"dsm_data/parquet/WRPC/{safe_station}/{year}/{month:02d}/{pq_name}"
                        self.s3_uploader.auto_upload_file(str(tmp_pq), original_filename=s3_key_p)
                        logger.info(f"✅ Uploaded Parquet: {s3_key_p}")
                        # Clean up temp parquet file
                        os.unlink(tmp_pq)
                    except Exception as e:
                        logger.warning(f"⚠️ Parquet upload failed (WRPC {safe_station} {year}-{month:02d}): {e}")
                        # Clean up temp parquet file
                        if os.path.exists(tmp_pq):
                            os.unlink(tmp_pq)
        except Exception as e:
            logger.warning(f"⚠️ Partitioned export encountered an error (WRPC): {e}")

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
                
                downloaded_file_list = self.download_and_process_file(link_info)
                if downloaded_file_list:
                    # downloaded_file_list is now a list of files
                    downloaded_files.extend(downloaded_file_list)
                    logger.info(f"✅ Successfully processed {len(downloaded_file_list)} files from {link_info.get('filename', 'Unknown')}")
                    
                    # Early stopping: if we have enough files, stop to save time
                    if len(downloaded_files) >= 10:  # Increased limit since we're processing more files per ZIP
                        logger.info(f"🎯 Got {len(downloaded_files)} files! Stopping early to save time.")
                        break
                else:
                    logger.warning(f"⚠️ Failed to process: {link_info.get('filename', 'Unknown')}")
            
            if downloaded_files:
                logger.info(f"✅ Successfully processed {len(downloaded_files)} WRPC files")
                
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
