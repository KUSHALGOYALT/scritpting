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
import typing
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
        
        # Initialize S3 uploader
        self.s3_uploader = AutoS3Uploader()
        
        # Session for maintaining cookies with SSL verification disabled
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Track processed weeks to avoid duplicates (no local storage)
        self.processed_weeks = {}
        
        # FAST MODE: Enable by default for better performance
        self.fast_mode = True

    def _ensure_unique_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has unique, clean column names and drop exact duplicate columns."""
        try:
            # Normalize names: strip, collapse spaces, remove trailing units in name decorations
            new_cols = []
            seen = {}
            for col in df.columns:
                base = str(col).strip()
                # Unify common unit decorations in headers (do not touch data)
                base = re.sub(r"\s*\(MWH\)|\s*\(KWH\)|\s*\(Hz\)", "", base, flags=re.I)
                base = base.replace("\n", " ").strip()
                candidate = base
                idx = 1
                while candidate in seen:
                    idx += 1
                    candidate = f"{base}.{idx-1}"
                seen[candidate] = True
                new_cols.append(candidate)
            df.columns = new_cols
            # Drop truly duplicated columns (same name after normalization keeps first occurrence)
            df = df.loc[:, ~df.columns.duplicated()]
            return df
        except Exception:
            return df

    def load_processed_weeks(self):
        """Load list of already processed weeks (no local storage)"""
        # For now, we'll skip file tracking to avoid local storage
        # In production, this could be stored in S3 or a database
        return {}

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
                            if any(date_str in href for week_info in past_weeks for date_str in [week_info['start_date'], week_info['end_date']]):
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
                href = link.get('href', '')
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
                    
                    # Extract filename from URL
                    filename = os.path.basename(full_url)
                    
                    data_links.append({
                        'url': full_url,
                        'filename': filename,
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

    def _process_zip_to_csv(self, zip_path: Path) -> typing.Optional[str]:
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
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_file:
                    df.to_csv(csv_file.name, index=False)
                    output_path = csv_file.name
                logger.info(f"✅ Extracted CSV from ZIP: {output_filename}")
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
            
            # Save the file to temporary location
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(filename).suffix) as temp_file:
                temp_file.write(response.content)
                local_path = Path(temp_file.name)
            
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

    def _parse_erldc_sheet(self, df, sheet_name):
        """Parse ERLDC sheet data with proper structure"""
        try:
            if df.empty:
                return None
            
            # Find the data start row (look for 'Date' in first column)
            data_start_row = None
            station_name = sheet_name  # Default to sheet name
            
            for i, row in df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip() == 'Date':
                    data_start_row = i
                    break
                # Also check for station name in row 1
                if i == 1 and pd.notna(row.iloc[0]) and 'Station :' in str(row.iloc[0]):
                    station_name = str(row.iloc[0]).replace('Station :', '').strip()
            
            if data_start_row is None:
                logger.warning(f"⚠️ Could not find data start row in sheet {sheet_name}")
                return None
            
            # Use the row with 'Date' as headers
            headers = df.iloc[data_start_row].tolist()
            
            # Get data starting from the next row
            data_df = df.iloc[data_start_row + 1:].copy()
            data_df.columns = headers
            
            # Clean up the data
            data_df = data_df.dropna(how='all')  # Remove completely empty rows
            
            # Add station information
            data_df['Station_Name'] = station_name
            data_df['Sheet_Name'] = sheet_name
            
            # Convert date column if it exists
            if 'Date' in data_df.columns:
                try:
                    data_df['Date'] = pd.to_datetime(data_df['Date'], errors='coerce')
                except:
                    pass
            
            logger.info(f"📊 Parsed sheet {sheet_name}: {len(data_df)} rows, station: {station_name}")
            return data_df
            
        except Exception as e:
            logger.warning(f"⚠️ Error parsing sheet {sheet_name}: {e}")
            return None



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

    def _process_xlsx_to_dataframe(self, file_path, filename):
        """Process XLSX file and extract dataframes"""
        try:
            import pandas as pd
            
            # Read the XLSX file
            xlsx_file = pd.ExcelFile(file_path)
            
            # Get all sheet names
            sheet_names = xlsx_file.sheet_names
            logger.info(f"📊 Found {len(sheet_names)} sheets in {filename}: {sheet_names}")
            
            all_dataframes = []
            
            for sheet_name in sheet_names:
                try:
                    # Read the sheet
                    df = pd.read_excel(xlsx_file, sheet_name=sheet_name)
                    
                    if df.empty:
                        logger.info(f"⏭️ Skipping empty sheet: {sheet_name}")
                        continue
                    
                    # Add metadata columns
                    df['Source_File'] = filename
                    df['Sheet_Name'] = sheet_name
                    
                    # Try to extract station name from sheet name or filename
                    station_name = self._extract_station_name(sheet_name, filename)
                    if station_name:
                        df['Station_Name'] = station_name
                    else:
                        df['Station_Name'] = 'ERLDC'
                    
                    all_dataframes.append(df)
                    logger.info(f"✅ Processed sheet '{sheet_name}' from {filename} ({len(df)} rows)")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error processing sheet '{sheet_name}' from {filename}: {e}")
                    continue
            
            # Combine all sheets into one dataframe
            if all_dataframes:
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                logger.info(f"📊 Combined {len(all_dataframes)} sheets into dataframe with {len(combined_df)} rows")
                return combined_df
            else:
                logger.warning(f"⚠️ No valid sheets found in {filename}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error processing XLSX file {filename}: {e}")
            return None
    
    def _extract_station_name(self, sheet_name, filename):
        """Extract station name from sheet name or filename"""
        try:
            # Try to extract from sheet name first
            if sheet_name and sheet_name != 'Sheet1':
                # Clean the sheet name
                clean_name = sheet_name.strip().replace(' ', '_').replace('-', '_')
                if len(clean_name) > 3:  # Avoid very short names
                    return clean_name
            
            # Try to extract from filename
            if filename:
                # Remove common prefixes and suffixes
                clean_filename = filename.replace('DSM_Blockwise_Data_', '').replace('.xlsx', '')
                # Extract date range part and use as station identifier
                if 'to' in clean_filename:
                    return 'ERLDC_' + clean_filename.split('to')[0].strip()
            
            return 'ERLDC'
        except:
            return 'ERLDC'

    def _export_partitioned_to_s3(self, master_df: pd.DataFrame) -> None:
        """Export CSV and Parquet per station/year/month to S3 under dsm_data/raw and dsm_data/parquet."""
        try:
            if self.s3_uploader is None or not hasattr(self.s3_uploader, 'auto_upload_file'):
                logger.info("⏭️ S3 uploader not configured; skipping S3 export")
                return
            if master_df.empty:
                return
            if 'Station_Name' not in master_df.columns:
                logger.info("⏭️ 'Station_Name' column missing; skipping S3 partitioned export")
                return
            # Ensure Date is parsed
            date_series = None
            if 'Date' in master_df.columns:
                date_series = pd.to_datetime(master_df['Date'], errors='coerce')
            else:
                date_series = pd.to_datetime(datetime.now())
            df = master_df.copy()
            df['__date__'] = date_series
            df['__year__'] = df['__date__'].dt.year.fillna(datetime.now().year).astype(int)
            df['__month__'] = df['__date__'].dt.month.fillna(datetime.now().month).astype(int)
            # Iterate partitions
            base_raw = 'dsm_data/raw'
            base_parquet = 'dsm_data/parquet'
            for station, g1 in df.groupby('Station_Name'):
                # Choose best station name: Station_Name → station_name → Sheet_Name/sheet_name → provided group key
                candidates = []
                try:
                    if 'Station_Name' in g1.columns:
                        vals = g1['Station_Name'].dropna().astype(str)
                        if not vals.empty:
                            candidates.append(vals.mode().iloc[0])
                except Exception:
                    pass
                try:
                    if 'station_name' in g1.columns:
                        vals = g1['station_name'].dropna().astype(str)
                        if not vals.empty:
                            candidates.append(vals.mode().iloc[0])
                except Exception:
                    pass
                try:
                    if 'Sheet_Name' in g1.columns:
                        vals = g1['Sheet_Name'].dropna().astype(str)
                        if not vals.empty:
                            candidates.append(vals.mode().iloc[0])
                    elif 'sheet_name' in g1.columns:
                        vals = g1['sheet_name'].dropna().astype(str)
                        if not vals.empty:
                            candidates.append(vals.mode().iloc[0])
                except Exception:
                    pass
                if isinstance(station, str) and station.strip():
                    candidates.append(station)
                # Filter out generic/unknown tokens from candidates
                def _is_valid_name(name: typing.Any) -> bool:
                    if name is None:
                        return False
                    s = str(name).strip()
                    if not s:
                        return False
                    bad = {
                        'UNKNOWN_STATION', 'UNKNOWN_SHEET', 'UNKNOWN',
                        'NR', 'SR', 'WR', 'ER', 'NER', 'REGION', 'EAST CENTRAL RAILWAY'
                    }
                    return s.upper() not in bad

                filtered = [c for c in candidates if _is_valid_name(c)]
                chosen_station = filtered[0] if filtered else 'Unknown_Station'
                # Avoid generic region codes as station names
                if chosen_station.upper() in {'NR', 'SR', 'WR', 'ER', 'NER', 'EAST CENTRAL RAILWAY'} and 'Sheet_Name' in g1.columns:
                    try:
                        sheet_vals = g1['Sheet_Name'].dropna().astype(str)
                        if not sheet_vals.empty:
                            chosen_station = sheet_vals.mode().iloc[0]
                    except Exception:
                        pass
                # If still unknown, try any sheet_name and sanitize
                if (not _is_valid_name(chosen_station)):
                    for alt_col in ['Sheet_Name', 'sheet_name']:
                        if alt_col in g1.columns:
                            try:
                                vals = g1[alt_col].dropna().astype(str)
                                if not vals.empty:
                                    # pick first valid
                                    for v in vals.mode().tolist():
                                        if _is_valid_name(v):
                                            chosen_station = v
                                            break
                                    if _is_valid_name(chosen_station):
                                        break
                            except Exception:
                                pass
                # Strip common region codes from start/end
                try:
                    tokens_to_strip = {'NR','SR','WR','ER','NER','EAST CENTRAL RAILWAY','REGION'}
                    name_upper = str(chosen_station).upper().strip()
                    if name_upper in tokens_to_strip:
                        # fallback to sheet name again if purely a token
                        if 'Sheet_Name' in g1.columns:
                            vals = g1['Sheet_Name'].dropna().astype(str)
                            if not vals.empty:
                                for v in vals.mode().tolist():
                                    if _is_valid_name(v):
                                        chosen_station = v
                                        break
                except Exception:
                    pass
                safe_station = str(chosen_station).strip().replace('/', '_').replace(' ', '_')
                for (year, month), g2 in g1.groupby(['__year__','__month__']):
                    part_df = g2.drop(columns=[c for c in ['__date__','__year__','__month__'] if c in g2.columns]).copy()
                    # Ensure unique columns before serialization
                    part_df = self._ensure_unique_columns(part_df)
                    # Prepare temporary files
                    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                    csv_name = f"ERLDC_{safe_station}_{year}_{month:02d}_{ts}.csv"
                    pq_name = f"ERLDC_{safe_station}_{year}_{month:02d}_{ts}.parquet"
                    
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_file:
                        part_df.to_csv(csv_file.name, index=False)
                        tmp_csv = csv_file.name
                    
                    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as parquet_file:
                        tmp_pq = parquet_file.name
                    
                    # Skip CSV uploads to raw - only store original XLSX files
                    try:
                        logger.info(f"📄 Created CSV temporarily: {csv_name}")
                    except Exception as e:
                        logger.warning(f"⚠️ CSV creation failed for {safe_station} {year}-{month:02d}: {e}")
                    # Write Parquet
                    try:
                        # Clean and prepare data for parquet conversion
                        clean_df = part_df.copy()
                        
                        # Handle all columns to ensure parquet compatibility
                        for col in clean_df.columns:
                            try:
                                # Convert all data to string first to avoid mixed type issues
                                clean_df[col] = clean_df[col].astype(str)
                                
                                # Then try to convert back to appropriate types
                                col_str = str(col).lower()
                                
                                # Handle date columns
                                if col_str.startswith('date') or col_str in {'time', 'processing_date'}:
                                    clean_df[col] = pd.to_datetime(clean_df[col], errors='coerce').astype(str)
                                
                                # Handle numeric columns
                                elif col_str in {'block', 'value', 'amount', 'price', 'rate', 'mw', 'mwh'}:
                                    # Try to convert to numeric, keep as string if fails
                                    numeric_series = pd.to_numeric(clean_df[col], errors='coerce')
                                    if not numeric_series.isna().all():
                                        clean_df[col] = numeric_series.fillna(0).astype(str)
                                
                                # Handle unnamed columns - convert to string to avoid mixed type issues
                                elif 'unnamed' in col_str:
                                    clean_df[col] = clean_df[col].astype(str)
                                
                            except Exception as col_error:
                                # If any conversion fails, keep as string
                                clean_df[col] = clean_df[col].astype(str)
                                logger.debug(f"Column {col} kept as string due to conversion error: {col_error}")
                        
                        # Remove any completely empty columns
                        clean_df = clean_df.dropna(axis=1, how='all')
                        
                        # Ensure all columns have string data types for parquet compatibility
                        for col in clean_df.columns:
                            clean_df[col] = clean_df[col].astype(str)
                        
                        # Convert to parquet
                        clean_df.to_parquet(tmp_pq, index=False, engine='pyarrow')
                        
                        # Parquet: dsm_data/parquet/ERLDC/{station_name}/{year}/{month}/{filename}
                        s3_key_p = f"{base_parquet}/ERLDC/{safe_station}/{year}/{month:02d}/{pq_name}"
                        self.s3_uploader.auto_upload_file(str(tmp_pq), original_filename=s3_key_p)
                        logger.info(f"📤 Uploaded Parquet to s3://{s3_key_p}")
                        
                        # Clean up temporary files
                        os.unlink(tmp_csv)
                        os.unlink(tmp_pq)
                    except Exception as e:
                        logger.warning(f"⚠️ Parquet upload failed for {safe_station} {year}-{month:02d}: {e}")
                        # Clean up temporary files even if upload failed
                        if os.path.exists(tmp_csv):
                            os.unlink(tmp_csv)
                        if os.path.exists(tmp_pq):
                            os.unlink(tmp_pq)
        except Exception as e:
            logger.warning(f"⚠️ Partitioned export encountered an error: {e}")

    def run_extraction(self):
        """Main extraction process"""
        logger.info("🚀 Starting ERLDC extraction for .xlsx files only with smart early stopping...")
        
        # Get past 7 days weeks
        past_weeks = self.get_past_7_days_weeks()
        logger.info(f"📅 Processing {len(past_weeks)} weeks from past 7 days")
        
        # First try to discover actual existing files on the website
        logger.info("🔍 Step 1: Discovering actual existing files on ERLDC website...")
        discovered_files = self.discover_real_data_files()
        
        # If no files found, try fast scanning for .xlsx files
        if not discovered_files:
            logger.info("🔍 Step 2: Fast scanning for .xlsx files...")
            discovered_files = self.fast_scan_for_xlsx_files()
            
        # If still no files found, search directories
        if not discovered_files:
            logger.info("🔍 Step 3: Searching directories for actual files...")
            discovered_files = self.search_for_actual_data_files()
            
        # If still no files found, try direct pattern URLs as last resort
        if not discovered_files:
            logger.info("🔍 Step 4: Trying direct pattern URLs as last resort...")
            discovered_files = self.generate_direct_dsm_urls()
        
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
                # Accept DSM blockwise .xlsx files (flexible matching)
                if (lower_name.endswith('.xlsx') and 
                    ('dsm' in lower_name) and 
                    ('blockwise' in lower_name) and 
                    ('data' in lower_name)):
                    valid_links.append(link)
                    logger.info(f"✅ Accepting DSM blockwise file: {filename} (Source: {source})")
                    continue
                
                # Also accept DSM-related ZIPs (will extract CSV) without changing output structure
                if lower_name.endswith('.zip') and (("dsm" in lower_name) or ("blockwise" in lower_name) or ("dsm" in text) or ("blockwise" in text)):
                    valid_links.append(link)
                    logger.info(f"✅ Accepting DSM-related ZIP: {filename} (Source: {source})")
                    continue
            
                logger.debug(f"⏭️ Skipping non-DSM file: {filename}")
            
            logger.info(f"🔍 Filtered to {len(valid_links)} valid links out of {len(discovered_files)} total")
            
            # Sort links by priority (high priority first) and then by source
            valid_links.sort(key=lambda x: (x.get('priority', 'low') == 'high', x.get('source', '') == 'actual_discovery'), reverse=True)
            
            # Remove duplicate files based on filename to avoid processing the same file multiple times
            unique_links = []
            seen_filenames = set()
            for link in valid_links:
                filename = link.get('filename', '')
                if filename not in seen_filenames:
                    unique_links.append(link)
                    seen_filenames.add(filename)
                else:
                    logger.info(f"⏭️ Skipping duplicate file: {filename}")
            
            logger.info(f"🔍 After deduplication: {len(unique_links)} unique files out of {len(valid_links)} total")
            
            # Download ERLDC files with smart early stopping
            downloaded_files = []
            all_dataframes = []  # Collect all dataframes for parquet export
            processed_filenames = set()  # Track processed files to avoid duplicates
            
            for i, erldc_link in enumerate(unique_links):
                source = erldc_link.get('source', 'unknown')
                priority = erldc_link.get('priority', 'low')
                filename = erldc_link['filename']
                
                # Skip if we've already processed this filename
                if filename in processed_filenames:
                    logger.info(f"⏭️ Skipping already processed file: {filename}")
                    continue
                
                logger.info(f"📊 Processing {i+1}/{len(unique_links)}: {filename} (Source: {source}, Priority: {priority})")
                
                downloaded_file = self.download_erldc_file(erldc_link)
                if downloaded_file:
                    downloaded_files.append(downloaded_file)
                    processed_filenames.add(filename)
                    logger.info(f"✅ Successfully downloaded: {filename}")
                    
                    # Process the downloaded file and extract dataframes
                    try:
                        if downloaded_file.lower().endswith('.xlsx'):
                            # Read the XLSX file and extract dataframes
                            df = self._process_xlsx_to_dataframe(downloaded_file, filename)
                            if df is not None and not df.empty:
                                all_dataframes.append(df)
                                logger.info(f"📊 Extracted dataframe with {len(df)} rows from {filename}")
                    except Exception as e:
                        logger.warning(f"⚠️ Error processing {filename} for parquet export: {e}")
                    
                    # Upload to S3 if enabled
                    if self.s3_uploader.enabled:
                        # raw/ERPC/{YEAR}/{MONTH}/{filename}
                        from datetime import datetime as _dt
                        now = _dt.now()
                        # Only upload XLSX files to raw (not CSV files)
                        if downloaded_file.lower().endswith('.xlsx'):
                            raw_key = f"dsm_data/raw/ERLDC/{now.year}/{now.month:02d}/{os.path.basename(downloaded_file)}"
                        success = self.s3_uploader.auto_upload_file(downloaded_file, original_filename=raw_key)
                        if success:
                                logger.info(f"📤 Uploaded XLSX to S3: {raw_key}")
                        else:
                            logger.info(f"📄 Skipping non-XLSX file upload to raw: {downloaded_file}")
                    
                    # If we have at least 2 high-priority data files, that's enough
                    high_priority_count = sum(1 for f in downloaded_files if erldc_link.get('priority') == 'high')
                    if high_priority_count >= 2:
                        logger.info(f"🎯 Got {high_priority_count} high-priority data files! Stopping early to save time.")
                        break
                    
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
                        
        else:
            logger.warning("⚠️ No ERLDC data links found using any discovery method")
            return []
        
        # No master dataset creation needed
        
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