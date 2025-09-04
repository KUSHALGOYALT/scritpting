#!/usr/bin/env python3
"""
ERLDC Dynamic Extractor - Downloads actual ERLDC data files
Enhanced with past 7 days extraction, update handling, and master dataset creation
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
import urllib3
import json
import zipfile
# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Add common module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'common'))
from auto_s3_upload import AutoS3Uploader
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ERLDCDynamicExtractor:
    def __init__(self):
        # Use the ERPC website as base, but discover everything dynamically
        self.base_url = "https://erpc.gov.in"
        self.local_storage_dir = Path("local_data/ERLDC")
        self.master_data_dir = Path("master_data/ERLDC")
        self.local_storage_dir.mkdir(parents=True, exist_ok=True)
        self.master_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize S3 uploader
        self.s3_uploader = AutoS3Uploader()
        
        # Session for maintaining cookies with SSL verification disabled
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
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
                    'start_ddmmyy': week_start.strftime('%d.%m.%Y'),
                    'end_ddmmyy': week_end.strftime('%d.%m.%Y'),
                    'week_num': week_start.isocalendar()[1],
                    'week_key': f"{week_start.strftime('%Y%m%d')}-{week_end.strftime('%Y%m%d')}"
                }
                weeks.append(week_info)
            
            return weeks
        except Exception as e:
            logger.error(f"❌ Error calculating past 7 days weeks: {e}")
            return []

    def discover_erldc_entities(self):
        """Dynamically discover ERLDC entities from existing data"""
        try:
            logger.info("🔍 Discovering ERLDC entities dynamically...")
            
            # Check if we have existing ERLDC data to analyze
            existing_data_path = Path("../../final_output/cleanup/original_data_backup/ERLDC")
            if existing_data_path.exists():
                entities = set()
                for file_path in existing_data_path.glob("*.csv"):
                    try:
                        df = pd.read_csv(file_path)
                        if 'Constituents' in df.columns:
                            entities.update(df['Constituents'].unique())
                        elif 'Entity' in df.columns:
                            entities.update(df['Entity'].unique())
                    except Exception as e:
                        logger.debug(f"⚠️ Could not read {file_path}: {e}")
                
                if entities:
                    logger.info(f"✅ Discovered {len(entities)} ERLDC entities from existing data")
                    return list(entities)
            
            # Fallback: try to get entities from the ERPC website
            logger.info("🔍 Trying to discover entities from ERPC website...")
            try:
                response = self.session.get(self.base_url, timeout=15)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # Look for entity names in the website content
                    text_content = soup.get_text().lower()
                    potential_entities = re.findall(r'\b[a-z]{2,10}\b', text_content)
                    # Filter potential entities
                    entities = [ent.upper() for ent in potential_entities if len(ent) >= 3 and ent not in ['the', 'and', 'for', 'with', 'from']]
                    if entities:
                        logger.info(f"✅ Discovered {len(entities)} potential ERLDC entities from ERPC website")
                        return entities[:10]  # Limit to top 10
            except Exception as e:
                logger.debug(f"⚠️ ERPC website access failed: {e}")
            
            logger.warning("⚠️ Could not discover entities dynamically, using generic approach")
            return ['ERLDC_ENTITY']
            
        except Exception as e:
            logger.error(f"❌ Error discovering entities: {e}")
            return ['ERLDC_ENTITY']

    def discover_upload_patterns(self, soup):
        """Dynamically discover upload patterns from website content"""
        try:
            logger.info("🔍 Discovering upload patterns from website...")
            
            patterns = {
                'upload_paths': set(),
                'date_patterns': set(),
                'file_naming_patterns': set(),
                'data_types': set()
            }
            
            # Look for common patterns in the HTML
            text_content = soup.get_text().lower()
            
            # Discover upload paths
            upload_patterns = re.findall(r'upload.*?path|path.*?upload', text_content)
            patterns['upload_paths'].update(upload_patterns)
            
            # Discover date patterns
            date_patterns = re.findall(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', text_content)
            patterns['date_patterns'].update(date_patterns)
            
            # Discover file naming patterns
            file_patterns = re.findall(r'[a-z]+_[a-z]+\.(?:xlsx?|csv|pdf)', text_content)
            patterns['file_naming_patterns'].update(file_patterns)
            
            # Discover data types
            data_types = re.findall(r'dsm|sras|tras|scuc|bilateral|reactive', text_content)
            patterns['data_types'].update(data_types)
            
            logger.info(f"✅ Discovered patterns: {patterns}")
            return patterns
            
        except Exception as e:
            logger.error(f"❌ Error discovering patterns: {e}")
            return {}

    def fast_scan_for_xlsx_files(self):
        """Fast scanning for .xlsx files from past 7 days"""
        try:
            logger.info("🔍 Fast scanning for .xlsx files...")
            
            # Get past 7 days weeks
            past_weeks = self.get_past_7_days_weeks()
            
            discovered_files = []
            
            # Try to access the main website and common data directories
            scan_urls = [
                self.base_url,  # Main page
                f"{self.base_url}/uploads/",  # Common upload directory
                f"{self.base_url}/downloads/",  # Common download directory
                f"{self.base_url}/data/",  # Common data directory
                f"{self.base_url}/reports/",  # Common reports directory
                f"{self.base_url}/documents/",  # Common documents directory
            ]
            
            for scan_url in scan_urls:
                try:
                    logger.info(f"🔍 Scanning: {scan_url}")
                    response = self.session.get(scan_url, timeout=15)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Look for .xlsx files in the content
                        xlsx_links = soup.find_all('a', href=re.compile(r'\.xlsx$', re.I))
                        
                        for link in xlsx_links:
                            href = link.get('href', '')
                            text = link.get_text(strip=True)
                            
                            # Build full URL
                            if href.startswith('http'):
                                full_url = href
                            elif href.startswith('/'):
                                full_url = f"{self.base_url}{href}"
                            else:
                                full_url = f"{scan_url.rstrip('/')}/{href}"
                            
                            # Check if this file matches our week or looks like data
                            is_data_file = False
                            if any(date_str in href for date_str in [week_info['start_date'], week_info['end_date']] for week_info in past_weeks):
                                is_data_file = True
                            elif any(keyword in text.lower() for keyword in ['dsm', 'data', 'report', 'week', 'settlement']):
                                is_data_file = True
                            elif any(keyword in href.lower() for keyword in ['dsm', 'data', 'report', 'week']):
                                is_data_file = True
                            
                            if is_data_file:
                                discovered_files.append({
                                    'url': full_url,
                                    'filename': os.path.basename(href),
                                    'text': text,
                                    'week_info': past_weeks[0],  # Use first week as default
                                    'source': 'website_discovery',
                                    'priority': 'high'
                                })
                                logger.info(f"✅ Found data file: {os.path.basename(href)}")
                        
                        # If we found high-priority files, stop scanning to save time
                        high_priority_count = sum(1 for f in discovered_files if f.get('priority') == 'high')
                        if high_priority_count >= 3:
                            logger.info(f"🎯 Found {high_priority_count} high-priority files! Stopping scan to save time.")
                            break
            
            except Exception as e:
                logger.debug(f"⚠️ Fast scan failed for {scan_url}: {e}")
                continue
            
            logger.info(f"📊 Fast scan complete! Found {len(discovered_files)} unique .xlsx files")
            return discovered_files
            
        except Exception as e:
            logger.error(f"❌ Fast scan failed: {e}")
            return []

    def discover_real_data_files(self):
        """Discover actual existing data files on the ERLDC website"""
        try:
            logger.info("🔍 Discovering real data files on ERLDC website...")
            
            # Start with the main page
            response = self.session.get(self.base_url, timeout=15)
            if response.status_code != 200:
                logger.error(f"❌ Failed to access main page: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for all links that might contain data
            all_links = soup.find_all('a', href=True)
            logger.info(f"🔗 Found {len(all_links)} total links on main page")
            
            # Look for data-related links
            data_links = []
            for link in all_links:
                href = link.get('href', '').lower()
                text = link.get_text(strip=True).lower()
                
                # Check if this looks like a data link
                if any(keyword in text for keyword in ['dsm', 'data', 'report', 'week', 'settlement', 'download']):
                    # Build full URL - handle different URL formats
                    if href.startswith('http'):
                        full_url = href
                    elif href.startswith('//'):
                        full_url = f"https:{href}"
                    elif href.startswith('/'):
                        full_url = f"{self.base_url}{href}"
                    elif href.startswith('./'):
                        full_url = f"{self.base_url}/{href[2:]}"
                    elif href.startswith('../'):
                        # Handle relative paths going up directories
                        full_url = f"{self.base_url}/{href[3:]}"
                    else:
                        # Assume relative to current page
                        full_url = f"{self.base_url}/{href}"
                    
                    # Log the URL construction for debugging
                    logger.debug(f"🔗 URL construction: {href} -> {full_url}")
                    
                    data_links.append({
                        'url': full_url,
                        'text': link.get_text(strip=True),
                        'href': href,
                        'source': 'main_page_discovery'
                    })
            
            logger.info(f"📊 Found {len(data_links)} potential data links")
            
            # Show some examples
            for link in data_links[:5]:
                logger.info(f"   📎 {link['text']} -> {link['href']}")
                logger.info(f"      Full URL: {link['url']}")
            
            return data_links
            
        except Exception as e:
            logger.error(f"❌ Error discovering real data files: {e}")
            return []

    def drill_down_for_xlsx_files(self, accessible_links):
        """Drill down into accessible directories to find actual .xlsx files - FAST MODE"""
        try:
            logger.info("🔍 FAST MODE: Drilling down for .xlsx files (limited depth)...")
            
            xlsx_files = []
            max_pages_to_scan = 3  # Limit to 3 pages max
            pages_scanned = 0
            
            for link in accessible_links:
                if pages_scanned >= max_pages_to_scan:
                    logger.info(f"🎯 Reached max pages limit ({max_pages_to_scan}), stopping scan")
                    break
                
                url = link['url']
                text = link['text']
                
                # Skip if this is already an .xlsx file
                if url.lower().endswith('.xlsx'):
                    xlsx_files.append(link)
                    continue
                
                # Try to access the directory/page to find .xlsx files - FAST MODE
                try:
                    logger.info(f"🔍 FAST SCAN: {text} -> {url}")
                    response = self.session.get(url, timeout=8)  # Reduced timeout
                    if response.status_code == 200:
                        pages_scanned += 1
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Look for .xlsx files in this page
                        xlsx_links = soup.find_all('a', href=re.compile(r'\.xlsx$', re.I))
                        
                        for xlsx_link in xlsx_links[:5]:  # Limit to first 5 .xlsx files
                            href = xlsx_link.get('href', '')
                            link_text = xlsx_link.get_text(strip=True)
                            
                            # Build full URL for the .xlsx file
                    if href.startswith('http'):
                        full_url = href
                            elif href.startswith('//'):
                                full_url = f"https:{href}"
                    elif href.startswith('/'):
                                full_url = f"{self.base_url}{href}"
                    else:
                                # Relative to the current directory
                                full_url = f"{url.rstrip('/')}/{href}"
                    
                            logger.info(f"✅ Found .xlsx file: {link_text} -> {full_url}")
                            
                            xlsx_files.append({
                        'url': full_url,
                                'text': link_text,
                                'href': href,
                                'source': 'fast_drill_down',
                                'parent_page': text
                            })
                            
                            # Early stopping: if we found 3+ files, that's enough
                            if len(xlsx_files) >= 3:
                                logger.info(f"🎯 Found {len(xlsx_files)} .xlsx files, stopping early")
                                return xlsx_files
                        
                        # FAST MODE: Skip recursive sub-page scanning
                        logger.info(f"⏭️ FAST MODE: Skipping sub-page scanning for {text}")
            
        except Exception as e:
                    logger.debug(f"⚠️ FAST SCAN failed for {url}: {e}")
                    continue
            
            logger.info(f"📊 FAST drill down complete! Found {len(xlsx_files)} .xlsx files")
            return xlsx_files
            
        except Exception as e:
            logger.error(f"❌ Error in FAST drill down: {e}")
            return []

    def search_for_actual_data_files(self):
        """Search for actual data files in discovered directories"""
        try:
            logger.info("🔍 Searching for actual data files in discovered directories...")
            
            # Get the data links we discovered earlier
            data_links = self.discover_real_data_files()
            if not data_links:
                logger.warning("⚠️ No data links to search in")
                return []
            
            actual_files = []
            
            # Search in each discovered directory for actual data files
            for link in data_links[:3]:  # Limit to first 3 directories
                url = link['url']
                text = link['text']
                
                logger.info(f"🔍 Searching in: {text} -> {url}")
                
                try:
                    # Try to access the directory
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Look for actual data files (.xlsx, .csv, .pdf, .zip)
                        file_links = soup.find_all('a', href=re.compile(r'\.(xlsx|csv|pdf|zip)$', re.I))
                        
                        for file_link in file_links:
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
                                full_url = f"{url.rstrip('/')}/{href}"
                            
                            # Extract week information from filename
                            week_info = self.extract_week_from_filename(href, filename)
                            
                            actual_files.append({
                        'url': full_url,
                        'filename': os.path.basename(href),
                                'text': filename,
                                'week_info': week_info,
                                'source': 'actual_discovery',
                                'priority': 'high'
                            })
                            
                            logger.info(f"✅ Found actual data file: {filename} -> {full_url}")
                            
                            # Early stopping if we found enough files
                            if len(actual_files) >= 5:
                                logger.info(f"🎯 Found {len(actual_files)} actual data files, stopping search")
                                break
                        
                        if len(actual_files) >= 5:
                            break
            
        except Exception as e:
                    logger.debug(f"⚠️ Could not search {url}: {e}")
                    continue
            
            logger.info(f"📊 Found {len(actual_files)} actual data files through directory search")
            return actual_files
            
        except Exception as e:
            logger.error(f"❌ Error searching for actual data files: {e}")
            return []

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

    def validate_urls(self, data_links):
        """Validate discovered URLs to see which ones are actually accessible - FAST MODE"""
        try:
            logger.info("🔍 FAST MODE: Quick URL validation...")
            
            valid_links = []
            max_urls_to_check = 5  # Limit to 5 URLs max
            
            for i, link in enumerate(data_links):
                if i >= max_urls_to_check:
                    logger.info(f"🎯 Reached max URL check limit ({max_urls_to_check}), stopping validation")
                    break
                    
                url = link['url']
                try:
                    # FAST MODE: Skip validation, just assume they're accessible
                    logger.info(f"⏭️ FAST MODE: Skipping validation for {url}")
                    link['accessible'] = True
                    valid_links.append(link)
                    
                    # Early stopping: if we have 3+ accessible links, that's enough
                    if len(valid_links) >= 3:
                        logger.info(f"🎯 Found {len(valid_links)} accessible links, stopping validation early")
                        break
                        
                except Exception as e:
                    logger.debug(f"⚠️ FAST validation failed: {url} - {e}")
                    link['accessible'] = False
            
            logger.info(f"📊 FAST URL validation complete: {len(valid_links)}/{min(len(data_links), max_urls_to_check)} URLs assumed accessible")
            return valid_links
            
        except Exception as e:
            logger.error(f"❌ Error in FAST URL validation: {e}")
            return data_links[:3]  # Return first 3 links if validation fails

    def _is_dsm_blockwise_filename(self, filename: str) -> bool:
        try:
            name = os.path.basename(filename).lower()
            return re.fullmatch(r'dsm_blockwise_data_\d{4}-\d{2}-\d{2}-\d{4}-\d{2}-\d{2}\.xlsx', name) is not None
        except Exception:
            return False

    def _process_zip_to_csv(self, zip_path: Path) -> str | None:
        try:
            logger.info(f"🔍 Processing ZIP file (ERLDC): {zip_path}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                if not csv_files:
                    logger.warning(f"⚠️ No CSV files found in ZIP: {zip_path}")
                    return None
                csv_filename = csv_files[0]
                with zip_ref.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file)
                output_filename = f"extracted_{os.path.basename(csv_filename)}"
                output_path = self.local_storage_dir / output_filename
                df.to_csv(output_path, index=False)
                logger.info(f"✅ Extracted CSV from ZIP: {output_path}")
                return str(output_path)
        except Exception as e:
            logger.error(f"❌ Error processing ZIP (ERLDC): {e}")
            return None

    def generate_dynamic_urls(self, patterns):
        """Generate strict DSM blockwise URLs for the past 7 days windows"""
        try:
            dynamic_urls = []
            past_weeks = self.get_past_7_days_weeks()
            # Try ERPC WordPress uploads structure observed on site
            # https://erpc.gov.in/wp-content/uploads/{YYYY}/{MM}/dsm_blockwise_data_{YYYY-MM-DD}-{YYYY-MM-DD}.xlsx
            for week_info in past_weeks:
                start_str = week_info['start_date']
                end_str = week_info['end_date']
                dt = datetime.strptime(start_str, '%Y-%m-%d')
                year = dt.strftime('%Y')
                month = dt.strftime('%m')
                filename = f"dsm_blockwise_data_{start_str}-{end_str}.xlsx"
                url = f"/wp-content/uploads/{year}/{month}/{filename}"
                dynamic_urls.append({
                    'url': f"{self.base_url}{url}",
                    'filename': filename,
                    'date_range': f"{start_str} to {end_str}",
                    'type': 'DSM_Data',
                    'source': 'dynamic_generation',
                    'week_info': week_info,
                    'priority': 'high'
                })
            logger.info(f"🔗 Generated {len(dynamic_urls)} strict DSM blockwise URLs")
            return dynamic_urls
        except Exception as e:
            logger.error(f"❌ Error generating strict DSM URLs: {e}")
            return []

    def generate_direct_dsm_urls(self):
        """Generate direct ERLDC DSM blockwise URLs for last 7 days windows."""
        items = []
        weeks = self.get_past_7_days_weeks()
        for w in weeks:
            start = w['start_date']
            end = w['end_date']
            dt = datetime.strptime(start, '%Y-%m-%d')
            year = dt.strftime('%Y')
            month = dt.strftime('%m')
            filename = f"DSM_Blockwise_Data_{start}-{end}.xlsx"
            url = f"{self.base_url}/wp-content/uploads/{year}/{month}/{filename}"
            items.append({'url': url, 'filename': filename, 'week_info': w, 'source': 'direct_pattern', 'priority': 'high'})
        return items

    def discover_data_patterns(self, soup):
        """Dynamically discover data patterns from website content"""
        try:
            patterns = {
                'keywords': set(),
                'file_extensions': set(),
                'date_formats': set(),
                'naming_conventions': set()
            }
            
            # Extract text content
            text_content = soup.get_text().lower()
            
            # Discover keywords
            keywords = re.findall(r'\b[a-z]{3,15}\b', text_content)
            patterns['keywords'].update([kw for kw in keywords if kw not in ['the', 'and', 'for', 'with', 'from', 'this', 'that']])
                
            # Discover file extensions
            extensions = re.findall(r'\.(xlsx?|csv|pdf|zip)', text_content)
            patterns['file_extensions'].update(extensions)
            
            # Discover date formats
            date_formats = re.findall(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', text_content)
            patterns['date_formats'].update(date_formats)
            
            # Discover naming conventions
            naming = re.findall(r'[a-z]+_[a-z]+|[a-z]+-[a-z]+', text_content)
            patterns['naming_conventions'].update(naming)
            
            logger.info(f"✅ Discovered data patterns: {patterns}")
            return patterns
            
        except Exception as e:
            logger.error(f"❌ Error discovering data patterns: {e}")
            return {}

    def download_erldc_file(self, erldc_link):
        """Download ERLDC file"""
        try:
            url = erldc_link['url']
            filename = erldc_link['filename']
            week_info = erldc_link.get('week_info', {})
            
            logger.info(f"📥 Downloading .xlsx file: {filename}")
            
            # Check if we already have this week and if it's newer
            week_key = week_info.get('week_key', filename)
            if week_key in self.processed_weeks:
                existing_timestamp = self.processed_weeks[week_key].get('timestamp', '')
                current_timestamp = datetime.now().isoformat()
                
                # If this is an update, log it
                if existing_timestamp < current_timestamp:
                    logger.info(f"🔄 Updating existing week: {week_key}")
                else:
                    logger.info(f"⏭️ Week already processed: {week_key}")
                    return None
            
            # Download the file
            head_ok = False
            try:
                head_resp = self.session.head(url, timeout=10)
                if head_resp.status_code == 200:
                    content_type = head_resp.headers.get('content-type', '').lower()
                    # Allow only excel/csv/zip content types; skip pdf/html
                    if any(t in content_type for t in ['excel', 'spreadsheet', 'csv', 'zip', 'octet-stream']):
                        head_ok = True
                    else:
                        logger.info(f"⏭️ Skipping {filename} due to content-type: {content_type}")
                        return None
            except Exception as e:
                logger.debug(f"⚠️ HEAD check failed for {url}: {e}")
                # Proceed but we'll still enforce extension-based guard below
            
            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                logger.warning(f"⚠️ Failed to download {filename}: {response.status_code}")
                return None
            
            # Enforce extension guard before saving
            if not filename.lower().endswith(('.xlsx', '.csv', '.zip')):
                logger.info(f"⏭️ Skipping after download due to unsupported extension: {filename}")
                return None
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            local_filename = f"ERLDC_Real_Data_{timestamp}{Path(filename).suffix}"
            local_path = self.local_storage_dir / local_filename
            
            # Save the file
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            # If ZIP, extract to CSV and use that CSV path
            if local_path.suffix.lower() == '.zip':
                extracted_csv = self._process_zip_to_csv(local_path)
                if extracted_csv:
                    saved_path = extracted_csv
                else:
                    saved_path = str(local_path)
            else:
                saved_path = str(local_path)
            
            logger.info(f"✅ Downloaded data file: {saved_path}")
            
            # Update processed weeks
            self.processed_weeks[week_key] = {
                'timestamp': datetime.now().isoformat(),
                'filename': filename,
                'local_file': os.path.basename(saved_path),
                'url': url,
                'week_info': week_info
            }
            self.save_processed_weeks()
            
            return saved_path
            
        except Exception as e:
            logger.error(f"❌ Download failed for {filename}: {e}")
            return None

    def create_master_dataset(self):
        """Create a master dataset from all processed ERLDC files"""
        try:
            logger.info("🔧 Creating ERLDC master dataset...")
            
            # Find all data files (both Excel and CSV)
            excel_files = list(self.local_storage_dir.glob("*.xlsx"))
            csv_files = list(self.local_storage_dir.glob("*.csv"))
            all_files = excel_files + csv_files
            
            if not all_files:
                logger.warning("⚠️ No data files found to create master dataset")
                return None
            
            # Read and combine all data files
            all_data = []
            for data_file in all_files:
                try:
                    if data_file.suffix.lower() == '.xlsx':
                        df = pd.read_excel(data_file)
                    else:  # CSV file
                        df = pd.read_csv(data_file)
                    
                    # Add source file information
                    df['Source_File'] = data_file.name
                    df['Processing_Date'] = datetime.now().strftime('%Y-%m-%d')
                    df['Region'] = 'ERLDC'
                    
                    all_data.append(df)
                    logger.info(f"📄 Added {data_file.name}: {len(df)} rows")
                except Exception as e:
                    logger.warning(f"⚠️ Could not read {data_file}: {e}")
                    continue
            
            if not all_data:
                logger.warning("⚠️ No data to combine")
                return None
                
            # Combine all data
            master_df = pd.concat(all_data, ignore_index=True)
            
            # Save master dataset
            master_filename = f"ERLDC_MASTER_DATASET_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            master_path = self.master_data_dir / master_filename
            master_df.to_csv(master_path, index=False)
            
            logger.info(f"✅ ERLDC master dataset created: {master_path} ({len(master_df)} rows)")
 
            return str(master_path)
                
        except Exception as e:
            logger.error(f"❌ Error creating ERLDC master dataset: {e}")
            return None

    def run_extraction(self):
        """Main extraction process"""
        logger.info("🚀 Starting ERLDC extraction for .xlsx files only with smart early stopping...")
        
        # Get past 7 days weeks
        past_weeks = self.get_past_7_days_weeks()
        logger.info(f"📅 Processing {len(past_weeks)} weeks from past 7 days")
        
        # Prefer direct pattern URLs for last 7 days
        logger.info("🔍 Step 1: Generating direct DSM blockwise URLs for last 7 days...")
        discovered_files = self.generate_direct_dsm_urls()
        
        # If no direct files found, try fast scanning for .xlsx files
        if not discovered_files:
            logger.info("🔍 Step 2: Fast scanning for .xlsx files...")
            discovered_files = self.fast_scan_for_xlsx_files()
            
        # If still no files found, search directories
        if not discovered_files:
            logger.info("🔍 Step 3: Searching directories for actual files...")
            discovered_files = self.search_for_actual_data_files()
        
        # Filter and process files
        if discovered_files:
            logger.info(f"📊 Found {len(discovered_files)} ERLDC data links, filtering for best candidates...")
            
            # Filter out invalid links to save time
            valid_links = []
            for link in discovered_files:
                filename = link.get('filename', '')
                url = link.get('url', '')
                source = link.get('source', '')
                text = (link.get('text') or '').lower()
                
                # Skip invalid links
                if not filename or filename in ['', '#', 'erpc.gov.in']:
                    continue
                if not url or url == '#' or url.endswith('erpc.gov.in'):
                    continue
                
                lower_name = filename.lower()
                # Strict DSM blockwise .xlsx
                if self._is_dsm_blockwise_filename(filename):
                    valid_links.append(link)
                    logger.info(f"✅ Accepting DSM blockwise file: {filename} (Source: {source})")
                    continue
                
                # Also accept DSM-related ZIPs (will extract CSV) without changing output structure
                if lower_name.endswith('.zip') and (('dsm' in lower_name) or ('blockwise' in lower_name) or ('dsm' in text) or ('blockwise' in text)):
                    valid_links.append(link)
                    logger.info(f"✅ Accepting DSM-related ZIP: {filename} (Source: {source})")
                    continue
            
                logger.debug(f"⏭️ Skipping non-DSM file: {filename}")
            
            logger.info(f"🔍 Filtered to {len(valid_links)} valid links out of {len(discovered_files)} total")
            
            # Sort links by priority (high priority first) and then by source
            valid_links.sort(key=lambda x: (x.get('priority', 'low') == 'high', x.get('source', '') == 'actual_discovery'), reverse=True)
            
            # Download ERLDC files with smart early stopping
            downloaded_files = []
            for i, erldc_link in enumerate(valid_links):
                source = erldc_link.get('source', 'unknown')
                priority = erldc_link.get('priority', 'low')
                logger.info(f"📊 Processing {i+1}/{len(valid_links)}: {erldc_link['filename']} (Source: {source}, Priority: {priority})")
                

                
                downloaded_file = self.download_erldc_file(erldc_link)
                if downloaded_file:
                    downloaded_files.append(downloaded_file)
                    logger.info(f"✅ Successfully downloaded: {erldc_link['filename']}")
                    
                    # Upload to S3 if enabled
                    if self.s3_uploader.enabled:
                        success = self.s3_uploader.auto_upload_file(downloaded_file, original_filename=os.path.basename(downloaded_file))
                        if success:
                            logger.info(f"📤 Uploaded to S3: {downloaded_file}")
                    
                    # If we have at least 2 high-priority data files, that's enough
                    high_priority_count = sum(1 for f in downloaded_files if erldc_link.get('priority') == 'high')
                    if high_priority_count >= 2:
                        logger.info(f"🎯 Got {high_priority_count} high-priority data files! Stopping early to save time.")
                        break
                    
                    # If we have at least 3 files total, that's also enough
                    if len(downloaded_files) >= 3:
                        logger.info(f"🎯 Got {len(downloaded_files)} total files! Stopping early to save time.")
                        break
                        
                else:
                    logger.warning(f"⚠️ Failed to download: {erldc_link['filename']}")
                    
                    # If we've tried 5 files and got nothing, stop to save time
                    if i >= 4 and len(downloaded_files) == 0:
                        logger.warning(f"⚠️ Tried 5 files with no success. Stopping to save time.")
                        break
                        
                # Check if we have enough DSM data files specifically
                dsm_files_count = sum(1 for f in downloaded_files if 'dsm' in f.lower())
                if dsm_files_count >= 2:
                    logger.info(f"🎯 Got {dsm_files_count} DSM data files! This is exactly what we need.")
                    break
        else:
            logger.warning("⚠️ No ERLDC data links found using any discovery method")
            return []
        
        # Create master dataset
        if downloaded_files:
            master_file = self.create_master_dataset()
            if master_file:
                downloaded_files.append(master_file)
        
        if downloaded_files:
            logger.info(f"🎉 ERLDC extraction complete! Downloaded {len(downloaded_files)} real files in minimal time")
        else:
            logger.warning("⚠️ No ERLDC files were successfully downloaded")
            
        return downloaded_files

def main():
    """Main execution function"""
    extractor = ERLDCDynamicExtractor()
    result = extractor.run_extraction()
    
    if result:
        logger.info(f"✅ ERLDC extraction completed! Files: {result}")
    else:
        logger.error("❌ ERLDC extraction failed!")

if __name__ == "__main__":
    main()
