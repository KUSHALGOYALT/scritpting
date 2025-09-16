#!/usr/bin/env python3
"""
SRPC (Southern Regional Power Committee) Data Extractor
This module extracts commercial data from SRPC website.
URL pattern: https://www.srpc.kar.nic.in/website/YYYY/commercial/DDMMYY.zip
File format: DDMMYY.zip (e.g., 010925.zip for 01/09/2025)
"""
import os
import sys
import logging
import pandas as pd
import requests
import zipfile
import io
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
import re

# Add parent directories to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from energy_data_extractors.common.auto_s3_upload import AutoS3Uploader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SRPCExtractor:
    """Extractor for SRPC commercial data"""
    
    def __init__(self, base_url: str = "https://www.srpc.kar.nic.in", 
                 data_source_name: str = None, 
                 region_name: str = None):
        """
        Initialize SRPC extractor
        
        Args:
            base_url: Base URL for SRPC website
            data_source_name: Data source name (defaults to SRPC or env var)
            region_name: Region name (defaults to SRPC or env var)
        """
        self.base_url = base_url
        
        # Initialize S3 uploader
        self.s3_uploader = AutoS3Uploader()
        
        # Set configurable data source and region names
        self._data_source_name = data_source_name or os.getenv('SRPC_DATA_SOURCE', 'SRPC')
        self._region_name = region_name or os.getenv('SRPC_REGION', 'SRPC')
        
        # Initialize session with SSL bypass for problematic websites
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Disable SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Processed files tracking (no local storage)
        self.processed_files = {"processed_dates": []}
        
        logger.info("SRPC Extractor initialized with S3-only storage")

    # =====================
    # New: Index parsing for last-7-days discovery with fallback
    # =====================
    def _format_ddmmyy(self, dt: datetime) -> str:
        return dt.strftime('%d%m%y')

    def _parse_index_for_zip_links(self) -> List[str]:
        """Parse SRPC commercial index to collect .zip links with DDMMYY patterns.

        Source: https://www.srpc.kar.nic.in/html/xml-search/commercial.html
        Returns absolute URLs to .zip files that match DDMMYY.zip or DDMMYY-DDMMYY.zip patterns.
        """
        try:
            index_url = "https://www.srpc.kar.nic.in/html/xml-search/commercial.html"
            r = self.session.get(index_url, timeout=60)
            if r.status_code != 200:
                logger.warning(f"Index fetch failed ({r.status_code})")
                return []
            html = r.text
            
            # Extract hrefs to .zip files
            hrefs = re.findall(r'href\s*=\s*"([^"]+\.zip)"', html, flags=re.IGNORECASE)
            
            # Filter for DDMMYY.zip or DDMMYY-DDMMYY.zip patterns (indicating updated data)
            weekly_patterns = []
            for href in hrefs:
                # Check for DDMMYY.zip pattern (6 digits + .zip)
                if re.search(r'\d{6}\.zip$', href, re.IGNORECASE):
                    weekly_patterns.append(href)
                    logger.info(f"📅 Found DDMMYY.zip pattern: {href}")
                # Check for DDMMYY-DDMMYY.zip pattern (date range)
                elif re.search(r'\d{6}-\d{6}\.zip$', href, re.IGNORECASE):
                    weekly_patterns.append(href)
                    logger.info(f"📅 Found DDMMYY-DDMMYY.zip pattern (updated data): {href}")
            
            # Convert to absolute URLs
            urls = []
            for href in weekly_patterns:
                if href.startswith('http'):
                    urls.append(href)
                else:
                    urls.append(self.base_url.rstrip('/') + '/' + href.lstrip('/'))
            
            logger.info(f"🧭 Discovered {len(urls)} weekly zip links from index (DDMMYY patterns)")
            for url in urls:
                logger.info(f"   📎 {url}")
            return urls
        except Exception as e:
            logger.warning(f"Index parse error: {e}")
            return []

    def _discover_last_7_days_urls(self) -> List[Dict[str, str]]:
        """Discover URLs prioritizing weekly links from index.

        - Prioritize URLs from index that match DDMMYY.zip or DDMMYY-DDMMYY.zip patterns
        - These patterns indicate updated/weekly data
        - Fallback to last 7 days if no index links found
        """
        # First, get weekly links from index (these are the most important)
        index_urls = self._parse_index_for_zip_links()
        
        results: List[Dict[str, str]] = []
        
        # Process weekly links from index (these are updated data)
        for url in index_urls:
            # Extract DDMMYY from filename
            filename = url.split('/')[-1].replace('.zip', '')
            
            # Handle DDMMYY-DDMMYY pattern (date range - indicates updated data)
            if re.search(r'\d{6}-\d{6}$', filename):
                # Extract start date from range
                start_ddmmyy = filename.split('-')[0]
                try:
                    # Parse DDMMYY format
                    day = int(start_ddmmyy[:2])
                    month = int(start_ddmmyy[2:4])
                    year = int('20' + start_ddmmyy[4:6])  # Assuming 20XX
                    dt = datetime(year, month, day)
                    
                    results.append({
                        'date': dt.strftime('%Y-%m-%d'),
                        'ddmmyy': start_ddmmyy,
                        'year': str(dt.year),
                        'source': 'index_weekly',
                        'url': url,
                        'is_updated': True
                    })
                    logger.info(f"📅 Found weekly updated data: {filename} -> {dt.strftime('%Y-%m-%d')}")
                except ValueError:
                    logger.warning(f"⚠️ Could not parse date from {filename}")
                    continue
            
            # Handle single DDMMYY pattern
            elif re.search(r'\d{6}$', filename):
                try:
                    # Parse DDMMYY format
                    day = int(filename[:2])
                    month = int(filename[2:4])
                    year = int('20' + filename[4:6])  # Assuming 20XX
                    dt = datetime(year, month, day)
                    
                    results.append({
                        'date': dt.strftime('%Y-%m-%d'),
                        'ddmmyy': filename,
                        'year': str(dt.year),
                        'source': 'index_daily',
                        'url': url,
                        'is_updated': False
                    })
                    logger.info(f"📅 Found daily data: {filename} -> {dt.strftime('%Y-%m-%d')}")
                except ValueError:
                    logger.warning(f"⚠️ Could not parse date from {filename}")
                    continue
        
        # If we found weekly links, prioritize them and limit to recent ones
        if results:
            # Sort by date (newest first)
            results.sort(key=lambda x: x['date'], reverse=True)
            # Limit to most recent 10 entries to avoid processing too much
            results = results[:10]
            logger.info(f"🎯 Using {len(results)} weekly links from index (prioritizing updated data)")
            return results
        
        # Fallback: Generate last 7 days URLs if no index links found
        logger.info("🔄 No weekly links found in index, falling back to last 7 days")
        lastN = [datetime.now() - timedelta(days=i) for i in range(7)]
        ddmmyys = {self._format_ddmmyy(d): d for d in lastN}
        
        for key, dt in ddmmyys.items():
            year = str(dt.year)
            fallback = f"https://www.srpc.kar.nic.in/website/{year}/commercial/{key}.zip"
            results.append({
                'date': dt.strftime('%Y-%m-%d'),
                'ddmmyy': key,
                'year': year,
                'source': 'fallback',
                'url': fallback,
                'is_updated': False
            })
        
        # Sort newest first
        results.sort(key=lambda x: x['ddmmyy'], reverse=True)
        logger.info(f"🎯 Prepared {len(results)} fallback URLs for last 7 days")
        return results

    def discover_last_7_days(self) -> List[Dict[str, str]]:
        """Public helper: return last-7-days URL plan (no download)."""
        return self._discover_last_7_days_urls()
    
    def _load_processed_files(self) -> Dict[str, Any]:
        """Load processed files tracking data (no local storage)"""
        # For now, we'll skip file tracking to avoid local storage
        # In production, this could be stored in S3 or a database
        return {"processed_dates": [], "last_updated": None}
    
    def _save_processed_files(self):
        """Save processed files tracking data (no local storage)"""
        # For now, we'll skip file tracking to avoid local storage
        # In production, this could be stored in S3 or a database
        pass
    
    def _discover_available_dates(self) -> List[datetime]:
        """Return only the last 7 days (most recent first)."""
        logger.info("🔍 Using last 7 days window only")
        dates = []
        today = datetime.now()
        for i in range(7):
            dates.append(today - timedelta(days=i))
        return dates

    # Removed deprecated helpers: _get_last_15_days, _generate_historical_dates, _get_last_7_days

    def _check_date_for_updates(self, date: datetime, available_years: List[str]) -> bool:
        """Check if a specific date has new or updated files"""
        try:
            date_str = self._format_date_for_url(date)
            
            # Check if we already processed this date recently
            if self._is_date_recently_processed(date_str):
                logger.debug(f"⏭️ Date {date_str} recently processed, checking for updates...")
            
            # Use the actual year from the date
            actual_year = str(date.year)
            url = self._get_url_for_date(date, actual_year)
            logger.debug(f"🔍 Checking URL: {url}")
            
            # Check if file exists and get its metadata
            file_info = self._get_file_metadata(url)
            if file_info:
                logger.debug(f"✅ File exists: {date_str} in {actual_year} (size: {file_info['size']} bytes)")
                # Check if this is new data or updated data
                if self._is_new_or_updated_data(date_str, actual_year, file_info):
                    logger.info(f"🆕 Found new/updated data: {date_str} in {actual_year}")
                    return True
            else:
                logger.debug(f"❌ No file found: {date_str} in {actual_year}")
            
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking date {date.strftime('%Y-%m-%d')} for updates: {e}")
            return False

    def _is_date_recently_processed(self, date_str: str) -> bool:
        """Check if a date was recently processed (within last 24 hours)"""
        try:
            processed_dates = self.processed_files.get("processed_dates", [])
            if date_str in processed_dates:
                # Check when it was last processed
                last_updated = self.processed_files.get("last_updated")
                if last_updated:
                    last_updated_dt = datetime.fromisoformat(last_updated)
                    hours_since_update = (datetime.now() - last_updated_dt).total_seconds() / 3600
                    return hours_since_update < 24  # Processed within last 24 hours
            return False
        except Exception as e:
            logger.warning(f"⚠️ Error checking if date recently processed: {e}")
            return False

    def _get_file_metadata(self, url: str) -> Optional[Dict[str, Any]]:
        """Get file metadata (size, last modified) from URL"""
        try:
            response = self.session.head(url, timeout=30)
            
            if response.status_code == 200:
                content_length = response.headers.get('content-length', '0')
                last_modified = response.headers.get('last-modified')
                
                return {
                    'size': int(content_length) if content_length else 0,
                    'last_modified': last_modified,
                    'status_code': response.status_code
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"❌ Error getting file metadata for {url}: {e}")
            return None

    def _is_new_or_updated_data(self, date_str: str, year: str, file_info: Dict[str, Any]) -> bool:
        """Check if this is new data or if existing data has been updated"""
        try:
            # Check if file size is reasonable (not an error page)
            if file_info['size'] < 1000:  # Less than 1KB is likely an error page
                return False
            
            # Check if we have processed this date before
            processed_dates = self.processed_files.get("processed_dates", [])
            if date_str not in processed_dates:
                logger.info(f"🆕 New data found: {date_str} (not previously processed)")
                return True
            
            # Check if file has been updated since last processing
            last_updated = self.processed_files.get("last_updated")
            if last_updated and file_info.get('last_modified'):
                try:
                    last_updated_dt = datetime.fromisoformat(last_updated)
                    file_modified_dt = datetime.strptime(file_info['last_modified'], '%a, %d %b %Y %H:%M:%S %Z')
                    
                    if file_modified_dt > last_updated_dt:
                        logger.info(f"🔄 Updated data found: {date_str} (file modified after last processing)")
                        return True
                except Exception as e:
                    logger.debug(f"⚠️ Error comparing timestamps: {e}")
            
            # Check if file size has changed (indicates update)
            stored_file_info = self.processed_files.get("file_metadata", {}).get(f"{date_str}_{year}")
            if stored_file_info and stored_file_info.get('size') != file_info['size']:
                logger.info(f"🔄 Updated data found: {date_str} (file size changed)")
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking if data is new/updated: {e}")
            return False

    def _discover_dates_in_year(self, year: str) -> List[datetime]:
        """Discover available dates in a specific year using smart pattern searching"""
        available_dates = []
        
        try:
            current_date = datetime.now()
            year_int = int(year)
            
            # Use smart pattern discovery instead of testing every date
            available_dates = self._smart_pattern_discovery(year_int, current_date)
            
            logger.info(f"📅 Found {len(available_dates)} available dates in {year}")
            
        except Exception as e:
            logger.warning(f"⚠️ Error discovering dates in year {year}: {e}")
        
        return available_dates

    def _smart_pattern_discovery(self, year: int, current_date: datetime) -> List[datetime]:
        """Smart pattern discovery using DDMMYY format - test strategic dates to find data patterns"""
        available_dates = []
        
        # Define strategic test dates using DDMMYY pattern
        test_dates = []
        
        # Test recent dates (last 30 days) - most likely to have data
        for i in range(30):
            test_date = current_date - timedelta(days=i)
            if test_date.year == year:
                test_dates.append(test_date)
        
        # Test month-end dates (more likely to have data) - DDMMYY pattern
        for month in range(1, 13):
            if month <= current_date.month or year < current_date.year:
                # Test last day of month
                if month in [1, 3, 5, 7, 8, 10, 12]:
                    test_dates.append(datetime(year, month, 31))
                elif month in [4, 6, 9, 11]:
                    test_dates.append(datetime(year, month, 30))
                elif month == 2:
                    # February - test 28th and 29th
                    test_dates.append(datetime(year, month, 28))
                    if year % 4 == 0:  # Leap year
                        test_dates.append(datetime(year, month, 29))
        
        # Test some mid-month dates (1st and 15th) - common reporting dates
        for month in range(1, 13):
            if month <= current_date.month or year < current_date.year:
                test_dates.append(datetime(year, month, 1))
                test_dates.append(datetime(year, month, 15))
        
        # Remove duplicates and sort
        test_dates = list(set(test_dates))
        test_dates.sort(reverse=True)
        
        # Test each date efficiently using DDMMYY format
        found_count = 0
        for test_date in test_dates:
            # Skip if too old (more than 2 years)
            if (current_date - test_date).days > 730:
                continue
                
            # Test using DDMMYY format
            if self._test_date_availability_ddmmyy(test_date, str(year)):
                available_dates.append(test_date)
                found_count += 1
                
                # Stop if we found enough dates
                if found_count >= 15:  # Max 15 dates per year
                    break
        
        return available_dates

    def _test_date_availability_ddmmyy(self, date: datetime, year: str) -> bool:
        """Test if data is available for a specific date using DDMMYY format"""
        try:
            # Format date as DDMMYY for URL
            date_str = self._format_date_for_url(date)  # This already uses DDMMYY format
            url = self._get_url_for_date(date, year)
            
            logger.debug(f"🔍 Testing DDMMYY pattern: {date_str} -> {url}")
            
            # Quick HEAD request to check if file exists
            response = self.session.head(url, timeout=10)
            
            if response.status_code == 200:
                # Check if file size is reasonable (not an error page)
                content_length = response.headers.get('content-length', '0')
                if content_length and int(content_length) > 1000:  # At least 1KB
                    logger.debug(f"✅ Data available for DDMMYY {date_str} in {year}")
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"❌ Data not available for DDMMYY {date.strftime('%d%m%y')} in {year}: {e}")
            return False

    def _test_date_availability(self, date: datetime, year: str) -> bool:
        """Test if data is available for a specific date"""
        try:
            date_str = self._format_date_for_url(date)
            url = self._get_url_for_date(date, year)
            
            # Quick HEAD request to check if file exists
            response = self.session.head(url, timeout=10)
            
            if response.status_code == 200:
                # Check if file size is reasonable (not an error page)
                content_length = response.headers.get('content-length', '0')
                if content_length and int(content_length) > 1000:  # At least 1KB
                    logger.debug(f"✅ Data available for {date_str} in {year}")
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"❌ Data not available for {date.strftime('%Y-%m-%d')} in {year}: {e}")
            return False
    
    def _format_date_for_url(self, date: datetime) -> str:
        """Format date as DDMMYY for URL"""
        return date.strftime("%d%m%y")
    
    def _detect_available_years(self) -> list:
        """Dynamically detect available years from SRPC website"""
        try:
            # Try to access the main website to detect available years
            main_url = f"{self.base_url}/website/"
            
            # Create session with SSL verification disabled
            session = requests.Session()
            session.verify = False
            
            # Disable SSL warnings
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # Add headers to mimic browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = session.get(main_url, timeout=20, headers=headers)
            if response.status_code != 200:
                return self._get_fallback_years()
            
            # Look for year patterns in the response
            years = set()
            year_matches = re.findall(r'(\d{4})', response.text)
            years.update(year_matches)
            
            # Also try common year patterns
            current_year = datetime.now().year
            for i in range(5):  # Try last 5 years
                years.add(str(current_year - i))
            
            # Sort years (newest first) and return as list
            sorted_years = sorted(list(years), reverse=True)
            logger.info(f"📅 Detected available years: {sorted_years}")
            return sorted_years if sorted_years else self._get_fallback_years()
            
        except Exception as e:
            logger.warning(f"⚠️ Could not detect years, using fallback: {e}")
            return self._get_fallback_years()

    def _get_fallback_years(self) -> list:
        """Get fallback years in order of likelihood (current year backwards)"""
        try:
            current_year = datetime.now().year
            fallback_years = []
            
            # Generate years going backwards from current year
            for i in range(5):  # Try last 5 years
                year = current_year - i
                fallback_years.append(str(year))
            
            logger.info(f"📅 Using fallback years: {fallback_years}")
            return fallback_years
            
        except Exception as e:
            logger.warning(f"⚠️ Error generating fallback years: {e}")
            return ['2025', '2024', '2023', '2022', '2021']  # Ultimate fallback

    def _get_url_for_date(self, date: datetime, year: str = None) -> str:
        """Generate URL for a specific date with dynamic year detection"""
        if year is None:
            year = str(date.year)
        date_str = self._format_date_for_url(date)
        return f"{self.base_url}/website/{year}/commercial/{date_str}.zip"
    
    def _download_zip_file(self, url: str, local_path: Path) -> bool:
        """Download zip file from URL with SSL verification bypass"""
        try:
            logger.info(f"Downloading: {url}")
            
            # Create session with SSL verification disabled for problematic websites
            session = requests.Session()
            session.verify = False
            
            # Disable SSL warnings
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # Try with different headers to mimic browser requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/zip,application/octet-stream,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = session.get(url, timeout=60, headers=headers)
            response.raise_for_status()
            
            # Check if we got actual content (not an error page)
            if len(response.content) < 1000:  # Likely an error page if very small
                logger.warning(f"Downloaded file seems too small ({len(response.content)} bytes), might be an error page")
                return False
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded: {local_path} ({len(response.content)} bytes)")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to download {url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            return False
    
    def _extract_zip_contents(self, zip_path: Path) -> List[Path]:
        """Extract zip file and return list of extracted files"""
        extracted_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract all files
                zip_ref.extractall(zip_path.parent)
                
                # Get list of extracted files
                for file_info in zip_ref.infolist():
                    if not file_info.is_dir():
                        extracted_path = zip_path.parent / file_info.filename
                        extracted_files.append(extracted_path)
                        logger.info(f"Extracted: {extracted_path}")
            
            return extracted_files
            
        except zipfile.BadZipFile as e:
            logger.error(f"Bad zip file {zip_path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error extracting {zip_path}: {e}")
            return []
    
    def _read_data_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Read data file (CSV, XLS, XLSX) into DataFrame"""
        try:
            file_ext = file_path.suffix.lower()
            
            if file_ext == '.csv':
                # Try different encodings and delimiters
                for encoding in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        if len(df.columns) > 1:  # Valid CSV with multiple columns
                            break
                    except:
                        continue
                else:
                    # Try with different delimiters
                    df = pd.read_csv(file_path, sep=None, engine='python')
            
            elif file_ext in ['.xls', '.xlsx']:
                df = pd.read_excel(file_path)
            
            else:
                logger.warning(f"Unsupported file format: {file_ext}")
                return None
            
            logger.info(f"Read {file_path}: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return None
    
    def _extract_station_info(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Extract station information from DataFrame with dynamic detection"""
        station_info = {
            "station_name": "UNKNOWN",
            "data_source": self._get_data_source_name(),
            "file_type": self._infer_file_type_from_filename(filename),
            "columns": list(df.columns),
            "sample_data": df.head(3).to_dict('records') if len(df) > 0 else []
        }
        
        # Try to infer station name from filename or data
        station_name = self._detect_station_name(df, filename)
        if station_name:
            station_info["station_name"] = station_name
        
        return station_info

    def _create_dynamic_station_mapping(self) -> Dict[str, str]:
        """Create dynamic station mapping based on content analysis and patterns"""
        # This will be populated dynamically by analyzing actual file content
        # and filename patterns. No hardcoded mappings.
        return {}

    def _detect_station_name(self, df: pd.DataFrame, filename: str) -> Optional[str]:
        """Dynamically detect station name from filename and data content analysis"""
        try:
            # Method 1: Analyze filename patterns
            station_name = self._extract_station_from_filename(filename)
            if station_name:
                return station_name
            
            # Method 2: Analyze data content for station indicators
            station_name = self._extract_station_from_content(df, filename)
            if station_name:
                return station_name
            
            # Method 3: Use filename as fallback
            return self._create_fallback_station_name(filename)
            
        except Exception as e:
            logger.warning(f"⚠️ Error detecting station name for {filename}: {e}")
            return "UNKNOWN_STATION"

    def _extract_station_from_filename(self, filename: str) -> Optional[str]:
        """Extract station name from filename patterns based on actual SRPC data patterns"""
        try:
            filename_upper = filename.upper()
            filename_lower = filename.lower()
            
            # Remove file extension
            filename_clean = filename_upper.replace('.CSV', '').replace('.XLS', '').replace('.XLSX', '')
            
            # Pattern 1: Direct station/company names (highest priority)
            direct_stations = {
                'APTRANSCO': 'APTRANSCO',
                'KPTCL': 'KPTCL', 
                'KSEB': 'KSEB',
                'TSTRANSCO': 'TSTRANSCO',
                'TNEB': 'TNEB'
            }
            
            for station_key, station_name in direct_stations.items():
                if filename_clean.startswith(station_key):
                    return station_name
            
            # Pattern 2: Commercial development files (commercial_dev2022_*)
            if filename_lower.startswith('commercial_dev'):
                # Extract station name from commercial_dev2022_stationname.csv
                parts = filename_lower.split('_')
                if len(parts) >= 3:
                    station_part = parts[2]  # Get the station name part
                    # Clean up the station name
                    station_clean = re.sub(r'[^a-z0-9]', '_', station_part)
                    station_clean = re.sub(r'_+', '_', station_clean).strip('_')
                    if station_clean and len(station_clean) >= 2:
                        return station_clean.upper()
            
            # Pattern 3: Commercial actual files with location names
            if filename_lower.startswith('commercial_actual_'):
                # Extract location from commercial_actual_locationname_*.csv
                parts = filename_lower.split('_')
                if len(parts) >= 3:
                    location_part = parts[2]  # Get the location name part
                    # Clean up the location name
                    location_clean = re.sub(r'[^a-z0-9]', '_', location_part)
                    location_clean = re.sub(r'_+', '_', location_clean).strip('_')
                    if location_clean and len(location_clean) >= 2:
                        # Check if this is a known station name (don't prefix with ACTUAL_)
                        known_stations = [
                            'amgreen', 'adani', 'adyah', 'arpspl', 'atb', 'ath', 'atk', 
                            'avdsol', 'ayana', 'betam', 'jsw', 'simhadri', 'rstps', 
                            'vallur', 'ntpc', 'kudgi', 'nlc', 'talcher'
                        ]
                        # Check if any part of the location matches known stations
                        for known_station in known_stations:
                            if known_station in location_clean:
                                # Handle special cases like adani7 -> adani
                                if known_station == 'adani' and 'adani' in location_clean:
                                    return 'ADANI'  # Normalize adani7, adani9, etc. to ADANI
                                return known_station.upper()  # Return just the station name
                        
                        # If no known station found, use ACTUAL_ prefix
                        return f"ACTUAL_{location_clean.upper()}"
            
            # Pattern 4: Commercial files with specific data types
            commercial_data_types = {
                'commercial_actual_freq': 'FREQUENCY_DATA',
                'commercial_actual_marketrate': 'MARKET_RATE_DATA',
                'commercial_actual_meter_data_entitywise': 'METER_DATA_ENTITY',
                'commercial_actual_meter_data_meterwise': 'METER_DATA_METER',
                'commercial_actual_sras': 'SRAS_DATA',
                'commercial_cleared_mbas_tras': 'MBAS_TRAS_DATA',
                'commercial_dam_acprate': 'DAM_ACP_RATE',
                'commercial_cepl_ppa_data': 'CEPL_PPA_DATA',
                'commercial_curtailment_beneficiary': 'CURTAILMENT_DATA'
            }
            
            for pattern, station_name in commercial_data_types.items():
                if filename_lower.startswith(pattern):
                    return station_name
            
            # Pattern 5: Service type files (these are multi-entity)
            service_types = ['dsm', 'sras', 'tras', 'mbas', 'rea', 'scuc', 'srldc']
            for service_type in service_types:
                if filename_lower == f'{service_type}.csv':
                    return f'{service_type.upper()}_MULTI_ENTITY'
            
            # Pattern 6: State/region codes
            state_codes = {
                'andhrapradesh': 'STATE_AP',
                'karnataka': 'STATE_KA', 
                'kerala': 'STATE_KL',
                'tamilnadu': 'STATE_TN',
                'telangana': 'STATE_TG'
            }
            
            for state_key, state_code in state_codes.items():
                if state_key in filename_lower:
                    return state_code
            
            # Pattern 7: Power plant names in commercial files
            power_plants = [
                'simhadri', 'rstps', 'vallur', 'ntpc', 'kudgi', 'nlc', 'talcher',
                'amgreen', 'adani', 'adyah', 'amplpvg', 'ampltumk', 'arpspl',
                'atb', 'ath', 'atk', 'avdsol', 'ayana', 'betam', 'jsw'
            ]
            
            for plant in power_plants:
                if plant in filename_lower:
                    return plant.upper()
            
            # Pattern 8: Generic commercial files
            if filename_lower.startswith('commercial_'):
                # Extract the main identifier after 'commercial_'
                parts = filename_lower.split('_')
                if len(parts) >= 2:
                    main_part = parts[1]
                    if main_part and len(main_part) >= 2:
                        return f"COMMERCIAL_{main_part.upper()}"
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error extracting station from filename {filename}: {e}")
            return None

    def _extract_station_from_content(self, df: pd.DataFrame, filename: str) -> Optional[str]:
        """Extract station name from data content analysis based on actual SRPC data patterns"""
        try:
            if df.empty:
                return None
            
            # Pattern 1: Look for entity/station name columns
            entity_cols = [col for col in df.columns if any(keyword in col.lower() 
                          for keyword in ['entity', 'station', 'name', 'provider', 'company'])]
            
            for col in entity_cols:
                unique_values = df[col].dropna().unique()
                if len(unique_values) > 0:
                    # Get the most common value
                    value_counts = df[col].value_counts()
                    most_common = value_counts.index[0]
                    if isinstance(most_common, str) and len(most_common.strip()) > 0:
                        return most_common.upper().strip()
            
            # Pattern 2: Look for power plant names in column names (SRAS data)
            power_plant_columns = []
            for col in df.columns:
                col_lower = col.lower()
                # Look for known power plant names in column names
                power_plants = [
                    'simhadri', 'rstps', 'vallur', 'ntpc', 'kudgi', 'nlc', 'talcher',
                    'amgreen', 'adani', 'adyah', 'amplpvg', 'ampltumk', 'arpspl'
                ]
                for plant in power_plants:
                    if plant in col_lower:
                        power_plant_columns.append(plant.upper())
            
            if power_plant_columns:
                # If multiple power plants, this is multi-plant data
                if len(power_plant_columns) > 1:
                    return 'MULTI_PLANT_DATA'
                else:
                    return power_plant_columns[0]
            
            # Pattern 3: Look for transmission line names in column names
            transmission_columns = []
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['line', 'ps1', 'ps2', 'ps3', '33ack', '33frvap', '33ach', '33tata', '33acb', '33azu', '33ntpcline']):
                    transmission_columns.append(col)
            
            if transmission_columns:
                # Extract location from transmission data
                if 'ananthapuramu' in filename.lower():
                    return 'ANANTHAPURAMU_TRANSMISSION'
                elif 'pavagada' in filename.lower():
                    return 'PAVAGADA_TRANSMISSION'
                else:
                    return 'TRANSMISSION_DATA'
            
            # Pattern 4: Look for transaction/export/import data
            transaction_columns = []
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['transcation', 'appno', 'trader', 'from_entity', 'to_entity', 'sch', 'sch_sr']):
                    transaction_columns.append(col)
            
            if transaction_columns:
                # Determine if this is export or import data
                if 'exp' in filename.lower():
                    return 'EXPORT_DATA'
                elif 'imp' in filename.lower():
                    return 'IMPORT_DATA'
                else:
                    return 'TRANSACTION_DATA'
            
            # Pattern 5: Service type files - these contain multiple entities, so we'll handle them specially
            filename_lower = filename.lower()
            service_types = ['dsm', 'sras', 'tras', 'mbas', 'rea', 'scuc', 'srldc']
            for service_type in service_types:
                if service_type in filename_lower:
                    return f'{service_type.upper()}_MULTI_ENTITY'  # Special marker for multi-entity files
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error extracting station from content {filename}: {e}")
            return None

    def _create_fallback_station_name(self, filename: str) -> str:
        """Create a fallback station name from filename"""
        try:
            # Clean filename
            filename_clean = re.sub(r'[^A-Za-z0-9]', '_', filename.upper())
            filename_clean = re.sub(r'_+', '_', filename_clean).strip('_')
            
            # Remove common suffixes
            suffixes_to_remove = ['CSV', 'XLS', 'XLSX', 'ZIP', 'DATA', 'FILE']
            for suffix in suffixes_to_remove:
                if filename_clean.endswith(f'_{suffix}'):
                    filename_clean = filename_clean[:-len(f'_{suffix}')]
                elif filename_clean.endswith(suffix):
                    filename_clean = filename_clean[:-len(suffix)]
            
            # Remove service type patterns (these are not station names)
            service_types = ['DSM', 'SRAS', 'TRAS', 'MBAS', 'REA', 'SCUC', 'SRLDC']
            for service_type in service_types:
                if filename_clean == service_type:
                    return f'{service_type}_SERVICE'  # Mark as service type
                if filename_clean.startswith(f'{service_type}_'):
                    filename_clean = filename_clean[len(f'{service_type}_'):]
            
            # Remove common words that don't add meaning
            common_words = ['COMMERCIAL', 'ACTUAL', 'METER', 'ENTITYWISE', 'METERWISE', 'ENT', 'SCH', 'PX', 'RTM', 'DAM', 'MBAS', 'TRAS', 'RATE']
            for word in common_words:
                if filename_clean == word:
                    return f'{word}_DATA'
                if filename_clean.startswith(f'{word}_'):
                    filename_clean = filename_clean[len(f'{word}_'):]
            
            # Remove any remaining .CSV, .XLS, .XLSX from the cleaned filename
            filename_clean = filename_clean.replace('.CSV', '').replace('.XLS', '').replace('.XLSX', '')
            
            # Clean up any remaining underscores
            filename_clean = re.sub(r'_+', '_', filename_clean).strip('_')
            
            # If the filename is still too long or contains unwanted parts, truncate it
            if len(filename_clean) > 20:
                filename_clean = filename_clean[:20]
            
            # Final cleanup - remove any remaining unwanted characters
            filename_clean = re.sub(r'[^A-Z0-9_]', '', filename_clean)
            
            # If still empty or too short, use generic name
            if not filename_clean or len(filename_clean) < 2:
                return 'UNKNOWN_STATION'
            
            return filename_clean
            
        except Exception as e:
            logger.warning(f"⚠️ Error creating fallback station name for {filename}: {e}")
            return 'UNKNOWN_STATION'

    def _is_station_data_file(self, filename: str) -> bool:
        """Check if file contains station data - INCLUDE ALL CSV FILES"""
        try:
            filename_lower = filename.lower()
            
            # Skip HTML files
            if filename_lower.endswith('.html'):
                return False
            
            # Skip empty or very small files
            if filename_lower.endswith(('.txt', '.log', '.tmp')):
                return False
            
            # Skip files with no data indicators
            skip_patterns = [
                'empty', 'null', 'zero', 'blank', 'test', 'sample',
                'temp', 'backup', 'old', 'archive', 'copy'
            ]
            
            for pattern in skip_patterns:
                if pattern in filename_lower:
                    return False
            
            # INCLUDE ALL CSV FILES - No filtering, process everything!
            if filename_lower.endswith('.csv'):
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking if file is station data: {e}")
            return True  # Default to include if unsure
    
    def _normalize_dataframe(self, df: pd.DataFrame, station_info: Dict[str, Any]) -> pd.DataFrame:
        """Normalize DataFrame to common schema with dynamic column detection"""
        # Create a copy to avoid modifying original
        normalized_df = df.copy()
        
        # Add metadata columns
        normalized_df['Station_Name'] = station_info['station_name']
        normalized_df['Data_Source'] = station_info['data_source']
        normalized_df['Region'] = self._get_region_name()
        normalized_df['Sheet_Type'] = station_info['file_type']
        normalized_df['Source_File'] = station_info.get('filename', '')
        
        # Add processing timestamp
        normalized_df['Processing_Date'] = datetime.now().isoformat()
        
        # Dynamic date column detection
        date_col = self._detect_date_column(normalized_df)
        if date_col:
            try:
                normalized_df['Date'] = pd.to_datetime(normalized_df[date_col], errors='coerce')
                logger.debug(f"📅 Using date column '{date_col}' for SRPC data")
            except Exception as e:
                logger.warning(f"⚠️ Error parsing date column '{date_col}': {e}")
        
        # Dynamic column mapping
        self._apply_dynamic_column_mapping(normalized_df)
        
        return normalized_df

    def _detect_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """Dynamically detect the best date column in the DataFrame"""
        try:
            # Priority order for date columns
            date_priority = [
                'Date', 'DATE', 'date', 'Date_Time', 'DATETIME', 'datetime',
                'Time_Date', 'TIMESTAMP', 'timestamp', 'Record_Date', 'RECORD_DATE'
            ]
            
            # First try priority list
            for col in date_priority:
                if col in df.columns:
                    return col
            
            # If no priority column found, look for columns containing 'date' or 'time'
            date_columns = [col for col in df.columns if any(keyword in str(col).lower() for keyword in ['date', 'time', 'timestamp'])]
            if date_columns:
                return date_columns[0]  # Return first match
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error detecting date column: {e}")
            return None

    def _apply_dynamic_column_mapping(self, df: pd.DataFrame) -> None:
        """Apply fully dynamic column mapping based on content analysis"""
        try:
            # Dynamic column type detection
            for col in df.columns:
                col_str = str(col).upper()
                col_lower = str(col).lower()
                
                # Detect column type dynamically
                column_type = self._detect_column_type(col_str, col_lower)
                
                if column_type:
                    # Apply dynamic mapping based on detected type
                    new_col_name = self._generate_standardized_column_name(col_str, column_type)
                    
                    if new_col_name != col:
                        df[new_col_name] = df[col]
                        logger.debug(f"📊 Dynamically mapped '{col}' -> '{new_col_name}' (type: {column_type})")
            
            # Special handling for SRPC actual generation columns (they end with _act)
            actual_cols = [col for col in df.columns if str(col).endswith('_act')]
            for col in actual_cols:
                station_name = str(col).replace('_act', '').upper()
                new_col_name = f"{station_name}_actual"
                df[new_col_name] = df[col]
                logger.debug(f"📊 Mapped SRPC actual column '{col}' -> '{new_col_name}'")
            
            # Convert KWh to MWh (divide by 1000)
            self._convert_kwh_to_mwh(df)
            
        except Exception as e:
            logger.warning(f"⚠️ Error applying dynamic column mapping: {e}")

    def _detect_column_type(self, col_str: str, col_lower: str) -> Optional[str]:
        """Dynamically detect column type from name patterns"""
        try:
            # Date/Time patterns
            if any(pattern in col_lower for pattern in ['date', 'time', 'record_id', 'timestamp']):
                return 'date_time'
            
            # Frequency patterns
            if any(pattern in col_lower for pattern in ['freq', 'frequency', 'hz']):
                return 'frequency'
            
            # Actual generation patterns
            if any(pattern in col_lower for pattern in ['actual', '_act', 'generation']):
                return 'actual'
            
            # Schedule patterns
            if any(pattern in col_lower for pattern in ['schedule', '_sch', 'scheduled']):
                return 'schedule'
            
            # Deviation/UI patterns
            if any(pattern in col_lower for pattern in ['deviation', 'ui', 'drawal', 'loss']):
                return 'deviation'
            
            # DSM patterns
            if any(pattern in col_lower for pattern in ['under_drawl', 'over_drawl', 'postfacto', 'final_charges', 'payable', 'receivable']):
                return 'dsm_charges'
            
            # SRAS patterns
            if any(pattern in col_lower for pattern in ['sras_up', 'sras_down', 'net_energy', 'energy_charges', 'compensation', 'incentive']):
                return 'sras'
            
            # TRAS patterns
            if any(pattern in col_lower for pattern in ['tras_up', 'tras_down', 'total_charges_compensation']):
                return 'tras'
            
            # Entity/Station patterns
            if any(pattern in col_lower for pattern in ['entity', 'provider', 'station', 'name']):
                return 'entity'
            
            # Meter patterns
            if any(pattern in col_lower for pattern in ['meter', 'entitywise', 'meterwise']):
                return 'meter'
            
            # Commercial patterns
            if any(pattern in col_lower for pattern in ['market_rate', 'weighted_average', 'acprate', 'rate']):
                return 'commercial'
            
            # Transmission patterns
            if any(pattern in col_lower for pattern in ['transmission', 'charges']):
                return 'transmission'
            
            # PPA patterns
            if any(pattern in col_lower for pattern in ['ppa', 'agreement']):
                return 'ppa'
            
            # URS patterns
            if any(pattern in col_lower for pattern in ['urs', 'unscheduled']):
                return 'urs'
            
            # Time slot patterns
            if any(pattern in col_lower for pattern in ['time_slot', 'block', 'slot', 'period', 'minute']):
                return 'time_slot'
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error detecting column type for {col_str}: {e}")
            return None

    def _generate_standardized_column_name(self, col_str: str, column_type: str) -> str:
        """Generate standardized column name based on detected type"""
        try:
            # Remove units from column names
            col_clean = self._remove_units_from_column_name(col_str)
            
            # Map to standardized names based on type
            type_mapping = {
                'date_time': 'Date_Time',
                'frequency': 'Freq_Hz',
                'actual': 'actual',
                'schedule': 'schedule',
                'deviation': 'deviation',
                'dsm_charges': col_clean,
                'sras': col_clean,
                'tras': col_clean,
                'entity': 'Entity_Name',
                'meter': 'Meter_Data',
                'commercial': col_clean,
                'transmission': 'Transmission_Charges_Rs',
                'ppa': 'PPA_Data',
                'urs': 'URS_Data',
                'time_slot': 'Time_Slot'
            }
            
            return type_mapping.get(column_type, col_clean)
            
        except Exception as e:
            logger.warning(f"⚠️ Error generating standardized column name: {e}")
            return col_str

    def _remove_units_from_column_name(self, col_str: str) -> str:
        """Remove units from column names dynamically"""
        try:
            # Common unit patterns to remove
            unit_patterns = [
                r'\([^)]*\)',  # Remove anything in parentheses
                r'\[[^\]]*\]',  # Remove anything in brackets
                r'_MWH$', r'_KWH$', r'_MW$', r'_KW$',  # Remove energy units
                r'_HZ$', r'_Hz$',  # Remove frequency units
                r'_RS$', r'_Rs$', r'_INR$',  # Remove currency units
                r'_PER_', r'_PER$',  # Remove "per" indicators
                r'_RATE$', r'_RATES$',  # Remove rate indicators
                r'_CHARGES$', r'_CHARGE$',  # Remove charge indicators
            ]
            
            col_clean = col_str
            for pattern in unit_patterns:
                col_clean = re.sub(pattern, '', col_clean, flags=re.IGNORECASE)
            
            # Clean up multiple underscores
            col_clean = re.sub(r'_+', '_', col_clean).strip('_')
            
            return col_clean if col_clean else col_str
            
        except Exception as e:
            logger.warning(f"⚠️ Error removing units from column name {col_str}: {e}")
            return col_str

    def _convert_kwh_to_mwh(self, df: pd.DataFrame) -> None:
        """Convert KWh data to MWh by dividing by 1000"""
        try:
            # Energy columns that might be in KWh
            energy_columns = ['actual', 'schedule', 'deviation', 'sras_up', 'sras_down', 'tras_up', 'tras_down', 'net_energy', 'drawal', 'loss']
            
            for col in energy_columns:
                if col in df.columns:
                    # Check if this column contains KWh data by looking at original column names
                    # This is a heuristic - if the original data was in KWh, convert it
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
    
    def _check_s3_file_exists(self, s3_key: str) -> bool:
        """Check if file already exists in S3"""
        try:
            self.s3_uploader.s3_client.head_object(Bucket=self.s3_uploader.bucket_name, Key=s3_key)
            return True
        except:
            return False
    
    def _sanitize_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitize DataFrame for Parquet format"""
        sanitized_df = df.copy()
        
        # Convert object columns to string to avoid mixed types
        for col in sanitized_df.columns:
            if sanitized_df[col].dtype == 'object':
                sanitized_df[col] = sanitized_df[col].astype(str)
        
        # Handle datetime columns
        for col in sanitized_df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                try:
                    sanitized_df[col] = pd.to_datetime(sanitized_df[col], errors='coerce')
                except:
                    pass
        
        return sanitized_df
    
    def _upload_to_s3(self, df: pd.DataFrame, station_name: str, date: datetime) -> Dict[str, int]:
        """Upload DataFrame to S3 in both CSV and Parquet formats"""
        results = {"successful": 0, "failed": 0, "skipped": 0}
        
        if df.empty:
            logger.warning(f"Empty DataFrame for {station_name}, skipping upload")
            return results
        
        # Sanitize for Parquet
        clean_df = self._sanitize_for_parquet(df)
        
        # Generate S3 keys
        year = date.year
        month = f"{date.month:02d}"
        safe_station = re.sub(r'[^A-Z0-9_]', '_', station_name.upper())
        
        csv_filename = f"SRPC_{safe_station}_{date.strftime('%Y%m%d')}.csv"
        pq_filename = f"SRPC_{safe_station}_{date.strftime('%Y%m%d')}.parquet"
        
        csv_s3_key = f"dsm_data/raw/SRPC/{year}/{month}/{csv_filename}"
        pq_s3_key = f"dsm_data/parquet/SRPC/{safe_station}/{year}/{month}/{pq_filename}"
        
        # Check if files already exist
        csv_exists = self._check_s3_file_exists(csv_s3_key)
        pq_exists = self._check_s3_file_exists(pq_s3_key)
        
        # Upload CSV
        if not csv_exists:
            try:
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_file:
                    clean_df.to_csv(csv_file.name, index=False)
                    tmp_csv = csv_file.name
                self.s3_uploader.auto_upload_file(str(tmp_csv), original_filename=csv_s3_key)
                os.unlink(tmp_csv)  # Clean up temp file
                logger.info(f"📤 Uploaded CSV to s3://{csv_s3_key} ({len(clean_df)} rows)")
                results["successful"] += 1
            except Exception as e:
                logger.warning(f"⚠️ CSV upload failed (SRPC {safe_station}): {e}")
                results["failed"] += 1
        else:
            logger.info(f"⏭️ CSV already exists, skipping: s3://{csv_s3_key}")
            results["skipped"] += 1
        
        # Upload Parquet
        if not pq_exists:
            try:
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as parquet_file:
                    clean_df.to_parquet(parquet_file.name, index=False)
                    tmp_pq = parquet_file.name
                self.s3_uploader.auto_upload_file(str(tmp_pq), original_filename=pq_s3_key)
                os.unlink(tmp_pq)  # Clean up temp file
                logger.info(f"📤 Uploaded Parquet to s3://{pq_s3_key} ({len(clean_df)} rows)")
                results["successful"] += 1
            except Exception as e:
                logger.warning(f"⚠️ Parquet upload failed (SRPC {safe_station}): {e}")
                results["failed"] += 1
        else:
            logger.info(f"⏭️ Parquet already exists, skipping: s3://{pq_s3_key}")
            results["skipped"] += 1
        
        return results
    
    def extract_past_7_days(self) -> Dict[str, Any]:
        """Extract data prioritizing weekly links from index with station-wise consolidation"""
        logger.info("🚀 Starting SRPC extraction prioritizing weekly links from index")
        
        # Use weekly link discovery from index (prioritizes DDMMYY-DDMMYY.zip patterns)
        plan = self._discover_last_7_days_urls()
        
        # Initialize station-wise data consolidation
        station_data_consolidated = {}
        
        total_results = {"successful": 0, "failed": 0, "skipped": 0, "stations": set(), "weekly_links": 0, "updated_data": 0}
        
        for item in plan:
            date_str = item['ddmmyy']
            year = item['year']
            url = item['url']
            date = datetime.strptime(item['date'], '%Y-%m-%d')
            source = item.get('source', 'unknown')
            is_updated = item.get('is_updated', False)
            
            # Log the type of data being processed
            if source == 'index_weekly':
                total_results['weekly_links'] += 1
                if is_updated:
                    total_results['updated_data'] += 1
                    logger.info(f"📅 Processing WEEKLY UPDATED data: {date.strftime('%Y-%m-%d')} ({date_str}) - DDMMYY-DDMMYY pattern")
                else:
                    logger.info(f"📅 Processing weekly data: {date.strftime('%Y-%m-%d')} ({date_str})")
            elif source == 'index_daily':
                logger.info(f"📅 Processing daily data from index: {date.strftime('%Y-%m-%d')} ({date_str})")
            else:
                logger.info(f"📅 Processing fallback data: {date.strftime('%Y-%m-%d')} ({date_str})")
            
            # Skip if already processed (but allow updated data to be reprocessed)
            if date_str in self.processed_files.get("processed_dates", []) and not is_updated:
                logger.info(f"⏭️ Date {date_str} already processed, skipping")
                continue
            
            # Use discovered URL directly
            download_successful = False
            zip_filename = f"{date_str}_{year}.zip"
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as zip_file:
                zip_path = Path(zip_file.name)
            logger.info(f"🔍 Fetching: {url}")
            if self._download_zip_file(url, zip_path):
                download_successful = True
                logger.info(f"✅ Downloaded {url}")
                
                # Upload ZIP file to S3 raw storage
                if self.s3_uploader and self.s3_uploader.enabled:
                    zip_s3_key = f"dsm_data/raw/SRPC/{year}/{month:02d}/{zip_filename}"
                    try:
                        self.s3_uploader.auto_upload_file(str(zip_path), original_filename=zip_s3_key)
                        logger.info(f"📤 Uploaded ZIP to S3: {zip_s3_key}")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to upload ZIP to S3: {e}")
                
                result = self._process_date_with_consolidation(date, year, station_data_consolidated)
                for key in ['successful', 'failed', 'skipped']:
                    total_results[key] += result[key]
                total_results['stations'].update(result['stations'])
            else:
                logger.info(f"❌ Failed to download from {url}")
                if zip_path.exists():
                    zip_path.unlink()
            
            if not download_successful:
                logger.warning(f"Failed to download data for {date_str} from any available year")
                continue
        
        # Upload consolidated station data to S3
        if station_data_consolidated:
            logger.info(f"📤 Uploading consolidated data for {len(station_data_consolidated)} stations...")
            consolidation_results = self._upload_consolidated_station_data(station_data_consolidated)
            
            # Add consolidation results to totals
            total_results['consolidated_successful'] = consolidation_results.get('consolidated_successful', 0)
            total_results['consolidated_failed'] = consolidation_results.get('consolidated_failed', 0)
            total_results['consolidated_stations'] = consolidation_results.get('consolidated_stations', set())
        
        # Update last processed timestamp
        self.processed_files["last_updated"] = datetime.now().isoformat()
        self._save_processed_files()
        
        # No cleanup needed - using temporary files
        
        logger.info(f"🎉 SRPC extraction and consolidation completed!")
        logger.info(f"📊 Results: {total_results['successful']} successful, {total_results['failed']} failed, {total_results['skipped']} skipped")
        logger.info(f"🏭 Stations processed: {len(total_results['stations'])}")
        logger.info(f"📅 Weekly links processed: {total_results['weekly_links']}")
        logger.info(f"🔄 Updated data files (DDMMYY-DDMMYY): {total_results['updated_data']}")
        if 'consolidated_successful' in total_results:
            logger.info(f"📤 Consolidated uploads: {total_results['consolidated_successful']} successful, {total_results['consolidated_failed']} failed")
        
        return total_results

    def _store_file_metadata(self, date_str: str, year: str, zip_path: Path) -> None:
        """Store file metadata for future update detection"""
        try:
            if zip_path.exists():
                file_size = zip_path.stat().st_size
                file_modified = datetime.fromtimestamp(zip_path.stat().st_mtime)
                
                # Initialize file_metadata if not exists
                if "file_metadata" not in self.processed_files:
                    self.processed_files["file_metadata"] = {}
                
                # Store metadata
                self.processed_files["file_metadata"][f"{date_str}_{year}"] = {
                    'size': file_size,
                    'last_modified': file_modified.isoformat(),
                    'processed_at': datetime.now().isoformat()
                }
                
                logger.debug(f"📊 Stored metadata for {date_str}_{year}: {file_size} bytes")
                
        except Exception as e:
            logger.warning(f"⚠️ Error storing file metadata: {e}")

    def _process_date_with_consolidation(self, date: datetime, year: str, station_data_consolidated: Dict) -> Dict[str, Any]:
        """Process a single date and consolidate data by station"""
        date_str = self._format_date_for_url(date)
        logger.info(f"📅 Processing date: {date.strftime('%Y-%m-%d')} ({date_str})")
        
        # Skip if already processed
        if date_str in self.processed_files.get("processed_dates", []):
            logger.info(f"⏭️ Date {date_str} already processed, skipping")
            return {"successful": 0, "failed": 0, "skipped": 1, "stations": set()}
        
        # Generate URL and local path
        url = self._get_url_for_date(date, year)
        zip_filename = f"{date_str}_{year}.zip"
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as zip_file:
            zip_path = Path(zip_file.name)
        
        logger.info(f"🔍 Trying year {year}: {url}")
        
        # Download zip file
        if not self._download_zip_file(url, zip_path):
            logger.info(f"❌ Failed to download from year {year}")
            if zip_path.exists():
                zip_path.unlink()
            return {"successful": 0, "failed": 1, "skipped": 0, "stations": set()}
        
        logger.info(f"✅ Successfully downloaded from year {year}")
        
        # Upload ZIP file to S3 raw storage
        if self.s3_uploader and self.s3_uploader.enabled:
            month = date.month
            zip_s3_key = f"dsm_data/raw/SRPC/{year}/{month:02d}/{zip_filename}"
            try:
                self.s3_uploader.auto_upload_file(str(zip_path), original_filename=zip_s3_key)
                logger.info(f"📤 Uploaded ZIP to S3: {zip_s3_key}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to upload ZIP to S3: {e}")
        
        # Extract zip contents
        extracted_files = self._extract_zip_contents(zip_path)
        if not extracted_files:
            logger.warning(f"No files extracted from {zip_path}")
            return {"successful": 0, "failed": 1, "skipped": 0, "stations": set()}
        
        # Process each extracted file and consolidate by station
        total_results = {"successful": 0, "failed": 0, "skipped": 0, "stations": set()}
        
        for file_path in extracted_files:
            if file_path.suffix.lower() in ['.csv', '.xls', '.xlsx']:
                # Filter for station data only - skip unnecessary files
                if not self._is_station_data_file(file_path.name):
                    logger.info(f"⏭️ Skipping non-station file: {file_path.name}")
                    continue
                
                # Read data
                df = self._read_data_file(file_path)
                if df is None or df.empty:
                    continue
                
                # If this file contains multiple entities, split and process each entity separately
                try:
                    if self._is_multi_entity_file(file_path.name, df):
                        processed_count = self._process_multi_entity_file(
                            df=df,
                            filename=file_path.name,
                            date=date,
                            year=year,
                            station_data_consolidated=station_data_consolidated
                        )
                        total_results['successful'] += processed_count
                        logger.info(f"✅ Processed {processed_count} entity slices from {file_path.name}")
                        continue
                except Exception as e:
                    logger.warning(f"⚠️ Error in multi-entity detection/processing for {file_path.name}: {e}")
                
                # Extract station info
                station_info = self._extract_station_info(df, file_path.name)
                station_info['filename'] = file_path.name
                station_info['date'] = date
                station_info['year'] = year
                station_info['data_source'] = self._get_data_source_name()  # Ensure data_source is set
                station_info['file_type'] = self._infer_file_type_from_filename(file_path.name)  # Ensure file_type is set
                
                # Normalize data
                normalized_df = self._normalize_dataframe(df, station_info)
                
                # Consolidate by station
                self._consolidate_station_data(station_data_consolidated, normalized_df, station_info)
                
                total_results['successful'] += 1
                total_results['stations'].add(station_info['station_name'])
                
                logger.info(f"✅ Processed {file_path.name} -> {station_info['station_name']}")
        
        # Mark date as processed and store file metadata
        if date_str not in self.processed_files.get("processed_dates", []):
            self.processed_files.setdefault("processed_dates", []).append(date_str)
        
        # Store file metadata for future update detection
        self._store_file_metadata(date_str, year, zip_path)
        
        # Clean up zip file
        if zip_path.exists():
            zip_path.unlink()
        
        return total_results

    def _is_multi_entity_file(self, filename: str, df: pd.DataFrame) -> bool:
        """Heuristically determine if a CSV contains multiple entities/stations to be split per-entity."""
        try:
            name = filename.lower()
            columns_lower = [c.lower() for c in df.columns]
            # Direct filename hints
            if any(key in name for key in ['dsm', 'entitywise', 'meterwise', 'rea', 'sras', 'tras', 'scuc']):
                return True
            # Presence of canonical entity columns
            entity_name_columns = {'entity', 'entity_name', 'station', 'from_entity', 'to_entity', 'sras provider', 'tras provider', 'scuc generator'}
            if any(col in entity_name_columns for col in columns_lower):
                return True
            return False
        except Exception:
            return False

    def _process_multi_entity_file(
        self,
        df: pd.DataFrame,
        filename: str,
        date: datetime,
        year: str,
        station_data_consolidated: Dict
    ) -> int:
        """Split a multi-entity DataFrame into per-entity slices and consolidate each slice."""
        processed = 0
        name = filename.lower()

        def _clean_entity(value: Any) -> Optional[str]:
            if pd.isna(value):
                return None
            text = str(value).strip()
            if not text:
                return None
            # Remove HTML remnants and headers
            text = text.replace('</center>', '')
            text = text.replace('\r', ' ').replace('\n', ' ')
            text = ' '.join(text.split())
            return text

        # Determine entity key column(s)
        entity_columns_priority = [
            'entity', 'entity_name', 'station', 'from_entity', 'to_entity', 'SRAS Provider', 'TRAS Provider', 'SCUC Generator'
        ]
        # Map lower->original for case-insensitive lookup
        lower_to_original = {c.lower(): c for c in df.columns}

        # DSM, Entity-wise, Meter-wise, REA, SRAS
        for key in entity_columns_priority:
            key_lower = key.lower()
            if key_lower in lower_to_original:
                col = lower_to_original[key_lower]
                entities = (
                    df[col]
                    .dropna()
                    .map(_clean_entity)
                    .dropna()
                    .unique()
                )
                # Filter out header-like markers
                entities = [e for e in entities if not any(tok in e.lower() for tok in ['states/ut', 'entity', 'total amount to the pool'])]
                for entity in entities:
                    try:
                        slice_df = df[df[col].map(lambda x: _clean_entity(x) == entity)].copy()
                        if slice_df.empty:
                            continue
                        station_name = self._canonicalize_station_name(entity)
                        station_info = {
                            'station_name': station_name,
                            'filename': filename,
                            'date': date,
                            'year': year,
                            'data_type': self._infer_data_type_from_filename(filename),
                            'data_source': self._get_data_source_name(),
                            'file_type': self._infer_file_type_from_filename(filename)
                        }
                        normalized_df = self._normalize_dataframe(slice_df, station_info)
                        self._consolidate_station_data(station_data_consolidated, normalized_df, station_info)
                        processed += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Failed processing entity '{entity}' in {filename}: {e}")
                return processed

        # If no explicit entity column, fallback: treat as single-entity
        station_info = self._extract_station_info(df, filename)
        station_info['filename'] = filename
        station_info['date'] = date
        station_info['year'] = year
        station_info['data_source'] = self._get_data_source_name()  # Ensure data_source is set
        station_info['file_type'] = self._infer_file_type_from_filename(filename)  # Ensure file_type is set
        normalized_df = self._normalize_dataframe(df, station_info)
        self._consolidate_station_data(station_data_consolidated, normalized_df, station_info)
        return 1

    def _canonicalize_station_name(self, name: str) -> str:
        """Normalize station/entity name to a canonical uppercase underscore form."""
        try:
            cleaned = re.sub(r"[^A-Za-z0-9]+", "_", name.upper()).strip("_")
            return cleaned or 'UNKNOWN_STATION'
        except Exception:
            return 'UNKNOWN_STATION'

    def _infer_data_type_from_filename(self, filename: str) -> str:
        """Dynamically infer data type from filename patterns"""
        try:
            name = filename.lower()
            
            # Dynamic data type patterns with priority order
            data_type_patterns = [
                ('dsm', 'DSM'),
                ('rea', 'REA'),
                ('sras', 'SRAS'),
                ('tras', 'TRAS'),
                ('mbas', 'MBAS'),
                ('scuc', 'SCUC'),
                ('srldc', 'SRLDC'),
                ('meterwise', 'METERWISE'),
                ('entitywise', 'ENTITYWISE'),
                ('commercial', 'COMMERCIAL'),
                ('schedule', 'SCHEDULE'),
                ('actual', 'ACTUAL'),
                ('px', 'POWER_EXCHANGE'),
                ('rtm', 'REAL_TIME_MARKET'),
                ('dam', 'DAY_AHEAD_MARKET'),
                ('urs', 'UNSCHEDULED_INTERCHANGE'),
                ('freq', 'FREQUENCY'),
                ('rate', 'RATE'),
                ('charges', 'CHARGES'),
                ('ppa', 'POWER_PURCHASE_AGREEMENT'),
                ('transmission', 'TRANSMISSION'),
                ('renewable', 'RENEWABLE'),
                ('solar', 'SOLAR'),
                ('wind', 'WIND'),
                ('thermal', 'THERMAL'),
                ('hydro', 'HYDRO'),
                ('nuclear', 'NUCLEAR'),
                ('coal', 'COAL'),
                ('gas', 'GAS'),
                ('diesel', 'DIESEL')
            ]
            
            # Find the most specific match
            for pattern, data_type in data_type_patterns:
                if pattern in name:
                    return data_type
            
            # Default fallback
            return 'UNKNOWN'
            
        except Exception as e:
            logger.warning(f"⚠️ Error inferring data type from filename {filename}: {e}")
            return 'UNKNOWN'

    def _infer_file_type_from_filename(self, filename: str) -> str:
        """Dynamically infer file type from filename patterns"""
        try:
            filename_lower = filename.lower()
            
            # Get dynamic file type patterns
            file_type_patterns = self._get_dynamic_file_type_patterns()
            
            # Find the most specific match
            for pattern, file_type in file_type_patterns:
                if pattern in filename_lower:
                    return file_type
            
            # Default fallback
            return 'unknown'
            
        except Exception as e:
            logger.warning(f"⚠️ Error inferring file type from filename {filename}: {e}")
            return 'unknown'

    def _get_dynamic_file_type_patterns(self) -> List[tuple]:
        """Get dynamic file type patterns - completely configurable"""
        try:
            # Load all patterns dynamically from multiple sources
            all_patterns = []
            
            # 1. Load from built-in configuration file
            builtin_patterns = self._load_builtin_patterns()
            all_patterns.extend(builtin_patterns)
            
            # 2. Load from external configuration file
            external_patterns = self._load_external_patterns()
            all_patterns.extend(external_patterns)
            
            # 3. Load from environment variables
            env_patterns = self._load_environment_patterns()
            all_patterns.extend(env_patterns)
            
            # 4. Load from remote configuration (if available)
            remote_patterns = self._load_remote_patterns()
            all_patterns.extend(remote_patterns)
            
            # 5. Load from database or API (if configured)
            api_patterns = self._load_api_patterns()
            all_patterns.extend(api_patterns)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_patterns = []
            for pattern, file_type in all_patterns:
                if (pattern, file_type) not in seen:
                    seen.add((pattern, file_type))
                    unique_patterns.append((pattern, file_type))
            
            # Sort by specificity (longer patterns first for more specific matches)
            unique_patterns.sort(key=lambda x: len(x[0]), reverse=True)
            
            logger.debug(f"📊 Loaded {len(unique_patterns)} file type patterns from {len(all_patterns)} total sources")
            return unique_patterns
            
        except Exception as e:
            logger.warning(f"⚠️ Error getting dynamic file type patterns: {e}")
            return [('unknown', 'unknown')]

    def _load_builtin_patterns(self) -> List[tuple]:
        """Load built-in patterns from configuration file (no local storage)"""
        try:
            # For now, we'll skip external config files to avoid local storage
            # In production, this could be loaded from S3 or embedded in code
            # Return default patterns
            return self._get_essential_patterns()
            
        except Exception as e:
            logger.debug(f"⚠️ Error loading built-in patterns: {e}")
            return self._get_essential_patterns()

    def _load_external_patterns(self) -> List[tuple]:
        """Load patterns from external configuration file (no local storage)"""
        try:
            # For now, we'll skip external config files to avoid local storage
            # In production, this could be loaded from S3 or embedded in code
            # Return empty list
            return []
            
        except Exception as e:
            logger.debug(f"⚠️ Error loading external patterns: {e}")
            return []

    def _load_environment_patterns(self) -> List[tuple]:
        """Load patterns from environment variables"""
        try:
            patterns = []
            
            # Load from environment variable
            env_patterns = os.getenv('SRPC_FILE_TYPE_PATTERNS')
            if env_patterns:
                # Parse patterns from environment variable
                # Format: "pattern1:type1,pattern2:type2"
                for pattern_pair in env_patterns.split(','):
                    if ':' in pattern_pair:
                        pattern, file_type = pattern_pair.split(':', 1)
                        patterns.append((pattern.strip(), file_type.strip()))
                logger.debug(f"📊 Loaded {len(patterns)} environment patterns")
            
            return patterns
            
        except Exception as e:
            logger.debug(f"⚠️ Error loading environment patterns: {e}")
            return []

    def _load_remote_patterns(self) -> List[tuple]:
        """Load patterns from remote configuration URL"""
        try:
            remote_url = os.getenv('SRPC_PATTERNS_URL')
            if not remote_url:
                return []
            
            # Try to load from remote URL
            response = self.session.get(remote_url, timeout=10)
            if response.status_code == 200:
                config_data = response.json()
                if 'patterns' in config_data:
                    patterns = []
                    for pattern, file_type in config_data['patterns'].items():
                        patterns.append((pattern, file_type))
                    logger.debug(f"📊 Loaded {len(patterns)} remote patterns")
                    return patterns
            
            return []
            
        except Exception as e:
            logger.debug(f"⚠️ Error loading remote patterns: {e}")
            return []

    def _load_api_patterns(self) -> List[tuple]:
        """Load patterns from API endpoint"""
        try:
            api_url = os.getenv('SRPC_PATTERNS_API_URL')
            api_key = os.getenv('SRPC_PATTERNS_API_KEY')
            
            if not api_url:
                return []
            
            headers = {}
            if api_key:
                headers['Authorization'] = f'Bearer {api_key}'
            
            # Try to load from API
            response = self.session.get(api_url, headers=headers, timeout=10)
            if response.status_code == 200:
                config_data = response.json()
                if 'patterns' in config_data:
                    patterns = []
                    for pattern, file_type in config_data['patterns'].items():
                        patterns.append((pattern, file_type))
                    logger.debug(f"📊 Loaded {len(patterns)} API patterns")
                    return patterns
            
            return []
            
        except Exception as e:
            logger.debug(f"⚠️ Error loading API patterns: {e}")
            return []

    def _get_essential_patterns(self) -> List[tuple]:
        """Get essential patterns as fallback"""
        return [
            ('commercial', 'commercial'),
            ('dsm', 'dsm'),
            ('sras', 'sras'),
            ('rea', 'rea'),
            ('tras', 'tras'),
            ('mbas', 'mbas'),
            ('scuc', 'scuc'),
            ('srldc', 'srldc'),
            ('schedule', 'schedule'),
            ('actual', 'actual'),
            ('px', 'power_exchange'),
            ('rtm', 'real_time_market'),
            ('dam', 'day_ahead_market'),
            ('urs', 'unscheduled_interchange'),
            ('freq', 'frequency'),
            ('rate', 'rate'),
            ('charges', 'charges'),
            ('ppa', 'power_purchase_agreement'),
            ('transmission', 'transmission'),
            ('renewable', 'renewable'),
            ('solar', 'solar'),
            ('wind', 'wind'),
            ('thermal', 'thermal'),
            ('hydro', 'hydro'),
            ('nuclear', 'nuclear'),
            ('coal', 'coal'),
            ('gas', 'gas'),
            ('diesel', 'diesel'),
            ('unknown', 'unknown')
        ]

    def _load_additional_file_type_patterns(self) -> List[tuple]:
        """Load additional file type patterns from configuration files or environment"""
        try:
            additional_patterns = []
            
            # Try to load from environment variable
            env_patterns = os.getenv('SRPC_FILE_TYPE_PATTERNS')
            if env_patterns:
                # Parse patterns from environment variable
                # Format: "pattern1:type1,pattern2:type2"
                for pattern_pair in env_patterns.split(','):
                    if ':' in pattern_pair:
                        pattern, file_type = pattern_pair.split(':', 1)
                        additional_patterns.append((pattern.strip(), file_type.strip()))
            
            # Skip configuration file loading to avoid local storage
            # In production, this could be loaded from S3 or embedded in code
            
            return additional_patterns
            
        except Exception as e:
            logger.debug(f"⚠️ Error loading additional file type patterns: {e}")
            return []

    def _get_data_source_name(self) -> str:
        """Get the data source name dynamically"""
        return self._data_source_name

    def _get_region_name(self) -> str:
        """Get the region name dynamically"""
        return self._region_name

    def _consolidate_station_data(self, station_data_consolidated: Dict, df: pd.DataFrame, station_info: Dict[str, Any]) -> None:
        """Consolidate data by station name"""
        try:
            station_name = station_info['station_name']
            
            # Initialize station data if not exists
            if station_name not in station_data_consolidated:
                station_data_consolidated[station_name] = {
                    'data_frames': [],
                    'metadata': {
                        'station_name': station_name,
                        'files_processed': [],
                        'data_types': set(),
                        'columns_found': set(),
                        'date_range': {'min': None, 'max': None}
                    }
                }
            
            # Add metadata
            station_data_consolidated[station_name]['metadata']['files_processed'].append({
                'filename': station_info['filename'],
                'date': station_info['date'],
                'year': station_info['year'],
                'data_type': station_info.get('data_type', 'unknown'),
                'rows': len(df),
                'columns': list(df.columns)
            })
            
            # Track data types and columns
            station_data_consolidated[station_name]['metadata']['data_types'].add(station_info.get('data_type', 'unknown'))
            station_data_consolidated[station_name]['metadata']['columns_found'].update(df.columns)
            
            # Track date range
            if 'date' in df.columns:
                try:
                    date_col = pd.to_datetime(df['date'], errors='coerce')
                    valid_dates = date_col.dropna()
                    if not valid_dates.empty:
                        min_date = valid_dates.min()
                        max_date = valid_dates.max()
                        
                        if station_data_consolidated[station_name]['metadata']['date_range']['min'] is None:
                            station_data_consolidated[station_name]['metadata']['date_range']['min'] = min_date
                            station_data_consolidated[station_name]['metadata']['date_range']['max'] = max_date
                        else:
                            station_data_consolidated[station_name]['metadata']['date_range']['min'] = min(
                                station_data_consolidated[station_name]['metadata']['date_range']['min'], min_date
                            )
                            station_data_consolidated[station_name]['metadata']['date_range']['max'] = max(
                                station_data_consolidated[station_name]['metadata']['date_range']['max'], max_date
                            )
                except Exception as e:
                    logger.warning(f"⚠️ Error processing dates for {station_name}: {e}")
            
            # Add DataFrame to consolidation
            station_data_consolidated[station_name]['data_frames'].append(df)
            
            logger.debug(f"📊 Consolidated data for {station_name}: {len(df)} rows, {len(df.columns)} columns")
            
        except Exception as e:
            logger.error(f"❌ Error consolidating data for {station_info.get('station_name', 'unknown')}: {e}")

    def _upload_consolidated_station_data(self, station_data_consolidated: Dict) -> Dict[str, Any]:
        """Upload consolidated station data to S3 with dynamic structure"""
        logger.info(f"📤 Starting upload of consolidated data for {len(station_data_consolidated)} stations")
        
        results = {"consolidated_successful": 0, "consolidated_failed": 0, "consolidated_stations": set()}
        
        for station_name, station_data in station_data_consolidated.items():
            try:
                logger.info(f"🔄 Processing consolidated data for station: {station_name}")
                
                # Combine all DataFrames for this station
                combined_df = self._combine_station_dataframes(station_data['data_frames'])
                
                if combined_df.empty:
                    logger.warning(f"⚠️ No data to upload for station {station_name}")
                    continue
                
                # Add metadata columns
                combined_df = self._add_metadata_columns(combined_df, station_data['metadata'])
                
                # Upload to S3 with dynamic structure
                upload_success = self._upload_consolidated_to_s3(combined_df, station_name, station_data['metadata'])
                
                if upload_success:
                    results["consolidated_successful"] += 1
                    results["consolidated_stations"].add(station_name)
                    logger.info(f"✅ Successfully uploaded consolidated data for {station_name}")
                else:
                    results["consolidated_failed"] += 1
                    logger.error(f"❌ Failed to upload consolidated data for {station_name}")
                
            except Exception as e:
                logger.error(f"❌ Error processing consolidated data for {station_name}: {e}")
                results["consolidated_failed"] += 1
        
        logger.info(f"📊 Consolidation results: {results['consolidated_successful']} successful, {results['consolidated_failed']} failed")
        return results

    def _combine_station_dataframes(self, data_frames: List[pd.DataFrame]) -> pd.DataFrame:
        """Combine multiple DataFrames for a station with dynamic column handling"""
        try:
            if not data_frames:
                return pd.DataFrame()
            
            if len(data_frames) == 1:
                return data_frames[0].copy()
            
            # Find common columns
            all_columns = set()
            for df in data_frames:
                all_columns.update(df.columns)
            
            # Create a unified schema
            unified_columns = sorted(list(all_columns))
            
            # Align all DataFrames to the same schema
            aligned_dfs = []
            for df in data_frames:
                aligned_df = df.reindex(columns=unified_columns)
                aligned_dfs.append(aligned_df)
            
            # Combine all DataFrames
            combined_df = pd.concat(aligned_dfs, ignore_index=True, sort=False)
            
            # Remove duplicates based on key columns if they exist
            key_columns = ['date', 'time', 'record_id', 'Date', 'Time']
            existing_key_columns = [col for col in key_columns if col in combined_df.columns]
            
            if existing_key_columns:
                combined_df = combined_df.drop_duplicates(subset=existing_key_columns, keep='first')
            
            logger.debug(f"📊 Combined {len(data_frames)} DataFrames: {len(combined_df)} rows, {len(combined_df.columns)} columns")
            return combined_df
            
        except Exception as e:
            logger.error(f"❌ Error combining DataFrames: {e}")
            return pd.DataFrame()

    def _add_metadata_columns(self, df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """Add metadata columns to the DataFrame"""
        try:
            # Add station metadata
            df['station_name'] = metadata['station_name']
            df['data_types'] = ','.join(sorted(metadata['data_types']))
            df['files_count'] = len(metadata['files_processed'])
            df['columns_count'] = len(metadata['columns_found'])
            
            # Add date range info
            if metadata['date_range']['min'] is not None:
                df['date_range_min'] = metadata['date_range']['min']
                df['date_range_max'] = metadata['date_range']['max']
            
            # Add processing timestamp
            df['consolidated_at'] = datetime.now().isoformat()
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error adding metadata columns: {e}")
            return df

    def _upload_consolidated_to_s3(self, df: pd.DataFrame, station_name: str, metadata: Dict) -> bool:
        """Upload consolidated station data to S3 with dynamic structure"""
        try:
            # Determine date range for S3 path
            date_range = metadata['date_range']
            if date_range['min'] is not None and date_range['max'] is not None:
                # Use the date range for year/month
                year = date_range['min'].year
                month = f"{date_range['min'].month:02d}"
            else:
                # Fallback to current date
                now = datetime.now()
                year = now.year
                month = f"{now.month:02d}"
            
            # Create S3 key with pattern: raw|parquet/SRPC/station_name/year/month
            safe_station = re.sub(r'[^A-Z0-9_]', '_', station_name.upper())
            
            # Upload raw CSV
            csv_key = f"raw/SRPC/{safe_station}/{year}/{month}/{safe_station}_consolidated_{year}_{month}.csv"
            csv_success = self._upload_dataframe_to_s3(df, csv_key, 'csv')
            
            # Upload parquet
            parquet_key = f"parquet/SRPC/{safe_station}/{year}/{month}/{safe_station}_consolidated_{year}_{month}.parquet"
            parquet_success = self._upload_dataframe_to_s3(df, parquet_key, 'parquet')
            
            return csv_success and parquet_success
            
        except Exception as e:
            logger.error(f"❌ Error uploading consolidated data to S3: {e}")
            return False

    def _upload_dataframe_to_s3(self, df: pd.DataFrame, s3_key: str, format_type: str) -> bool:
        """Upload DataFrame to S3 in specified format"""
        try:
            # Check if file already exists
            if self._check_s3_file_exists(s3_key):
                logger.info(f"⏭️ File already exists in S3: {s3_key}")
                return True
            
            # Convert DataFrame to specified format
            if format_type == 'csv':
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                content = buffer.getvalue()
                content_type = 'text/csv'
            elif format_type == 'parquet':
                buffer = BytesIO()
                df.to_parquet(buffer, index=False, engine='pyarrow')
                content = buffer.getvalue()
                content_type = 'application/octet-stream'
            else:
                logger.error(f"❌ Unsupported format type: {format_type}")
                return False
            
            # Upload to S3
            self.s3_uploader.s3_client.put_object(
                Bucket=self.s3_uploader.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type
            )
            
            logger.info(f"✅ Uploaded {format_type.upper()} to S3: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading {format_type.upper()} to S3: {e}")
            return False

def main():
    """Main function to run SRPC extractor"""
    extractor = SRPCExtractor()
    results = extractor.extract_past_7_days()
    
    print("\n" + "="*60)
    print("SRPC EXTRACTION SUMMARY")
    print("="*60)
    print(f"Successful uploads: {results['successful']}")
    print(f"Failed uploads: {results['failed']}")
    print(f"Skipped (duplicates): {results['skipped']}")
    print(f"Stations processed: {len(results['stations'])}")
    print(f"Weekly links processed: {results['weekly_links']}")
    print(f"Updated data files (DDMMYY-DDMMYY): {results['updated_data']}")
    if results['stations']:
        print(f"Station names: {', '.join(sorted(results['stations']))}")
    print("="*60)

if __name__ == "__main__":
    main()
