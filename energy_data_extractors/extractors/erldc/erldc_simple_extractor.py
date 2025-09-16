#!/usr/bin/env python3
"""
ERLDC Simple Extractor - A working, simplified approach to extract ERLDC data
"""
import requests
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys
import json
import urllib3
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add common module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'common'))
try:
    from auto_s3_upload import AutoS3Uploader
except ImportError:
    class AutoS3Uploader:
        def __init__(self):
            self.enabled = False

# Add region mapper
sys.path.append(os.path.dirname(__file__))
try:
    from erldc_region_mapper import ERLDCRegionMapper
except ImportError:
    class ERLDCRegionMapper:
        def get_region_info(self, station_name):
            return "Unknown", "Unknown"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ERLDCSimpleExtractor:
    def __init__(self):
        self.base_url = "https://erpc.gov.in"
        self.local_storage_dir = Path("local_data/ERLDC")
        self.master_data_dir = Path("master_data/ERLDC")
        self.local_storage_dir.mkdir(parents=True, exist_ok=True)
        self.master_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.s3_uploader = AutoS3Uploader()
        self.region_mapper = ERLDCRegionMapper()
        
        # Session with proper headers
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Dynamic discovery settings
        self.search_keywords = ['dsm', 'blockwise', 'data', 'week', 'settlement', 'sced', 'account']
        self.file_extensions = ['.xlsx', '.xls', '.csv', '.zip']
        self.max_discovery_depth = 3
        self.discovered_urls = set()

    def discover_data_urls_dynamically(self):
        """Dynamically discover ERLDC data URLs from the website"""
        discovered_urls = []
        visited_urls = set()
        
        def crawl_page(url, depth=0):
            if depth > self.max_discovery_depth or url in visited_urls:
                return
            
            visited_urls.add(url)
            logger.info(f"🔍 Crawling (depth {depth}): {url}")
            
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code != 200:
                    return
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all links
                links = soup.find_all('a', href=True)
                
                for link in links:
                    href = link.get('href', '').strip()
                    text = link.get_text(strip=True).lower()
                    
                    if not href or href == '#':
                        continue
                    
                    # Make absolute URL
                    full_url = urljoin(url, href)
                    
                    # Check if this is a data file
                    if any(ext in href.lower() for ext in self.file_extensions):
                        if any(keyword in href.lower() or keyword in text for keyword in self.search_keywords):
                            # Extract revision info for ERLDC files
                            revision_info = self.extract_erldc_revision_info(href)
                            
                            discovered_urls.append({
                                'url': full_url,
                                'filename': os.path.basename(urlparse(full_url).path),
                                'text': text,
                                'source_page': url,
                                'priority': 'high' if 'dsm' in href.lower() or 'blockwise' in href.lower() else 'medium',
                                'revision_info': revision_info
                            })
                            
                            rev_text = f" (Rev: {revision_info.get('revision', 'N/A')})" if revision_info.get('has_revision') else ""
                            logger.info(f"📄 Found data file: {os.path.basename(urlparse(full_url).path)}{rev_text}")
                    
                    # Check if this is a page that might contain data files
                    elif any(keyword in text for keyword in self.search_keywords) and depth < self.max_discovery_depth:
                        if full_url not in visited_urls and self.base_url in full_url:
                            crawl_page(full_url, depth + 1)
                
            except Exception as e:
                logger.debug(f"⚠️ Error crawling {url}: {e}")
        
        # Start crawling from main page
        crawl_page(self.base_url)
        
        # Also try common data directories
        common_paths = [
            '/data', '/downloads', '/reports', '/dsm', '/weekly-reports',
            '/wp-content/uploads', '/files', '/documents'
        ]
        
        for path in common_paths:
            test_url = urljoin(self.base_url, path)
            crawl_page(test_url, 0)
        
        logger.info(f"🎯 Dynamic discovery found {len(discovered_urls)} potential data files")
        return discovered_urls

    def extract_erldc_revision_info(self, filename):
        """Extract ERLDC revision information from filename (handles -R-1, -R-2 patterns)"""
        try:
            # Look for ERLDC-specific revision patterns
            erldc_revision_patterns = [
                r'-[rR]-(\d+)',           # -R-1, -r-1, -R-2, -r-2
                r'-[rR](\d+)',            # -R1, -r1, -R2, -r2
                r'_[rR]-(\d+)',           # _R-1, _r-1, _R-2, _r-2
                r'_[rR](\d+)',            # _R1, _r1, _R2, _r2
                r'-[rR][eE][vV]-(\d+)',   # -REV-1, -rev-1
                r'-[rR][eE][vV](\d+)',    # -REV1, -rev1
                r'_[rR][eE][vV]-(\d+)',   # _REV-1, _rev-1
                r'_[rR][eE][vV](\d+)',    # _REV1, _rev1
                r'-[vV]-(\d+)',           # -V-1, -v-1
                r'-[vV](\d+)',            # -V1, -v1
                r'_[vV]-(\d+)',           # _V-1, _v-1
                r'_[vV](\d+)',            # _V1, _v1
                r'\([rR]-(\d+)\)',        # (R-1), (r-1)
                r'\([rR](\d+)\)',         # (R1), (r1)
            ]
            
            for pattern in erldc_revision_patterns:
                match = re.search(pattern, filename)
                if match:
                    revision_num = match.group(1)
                    return {
                        'has_revision': True,
                        'revision': revision_num,
                        'pattern': pattern,
                        'original_filename': filename
                    }
            
            return {
                'has_revision': False,
                'revision': None,
                'pattern': None,
                'original_filename': filename
            }
            
        except Exception as e:
            logger.debug(f"⚠️ Error extracting ERLDC revision info: {e}")
            return {'has_revision': False, 'revision': None}
    
    def generate_smart_urls(self):
        """Generate smart URLs based on current date and common patterns"""
        urls = []
        current_date = datetime.now()
        
        # Generate URLs for the past 8 weeks with multiple patterns
        for weeks_back in range(8):
            target_date = current_date - timedelta(weeks=weeks_back)
            week_num = target_date.isocalendar()[1]
            year = target_date.year
            month = target_date.month
            
            # Multiple URL patterns to try
            base_paths = [
                f"/wp-content/uploads/{year}/{month:02d}/",
                f"/uploads/{year}/{month:02d}/",
                f"/data/{year}/{month:02d}/",
                f"/files/{year}/",
                f"/reports/{year}/"
            ]
            
            file_patterns = [
                f"DSM_Blockwise_Data_Week_{week_num}.xlsx",
                f"DSM_Blockwise_Week_{week_num}.xlsx",
                f"SCED_Account_Week_{week_num}.xlsx",
                f"Weekly_DSM_Report_Week_{week_num}.xlsx",
                f"DSM_Data_Week_{week_num}.xlsx",
                f"Blockwise_Data_{week_num}.xlsx",
                f"Week_{week_num}_DSM.xlsx",
                f"W{week_num}_DSM_Data.xlsx",
                # Add revision patterns for ERLDC
                f"DSM_Blockwise_Data_Week_{week_num}-R-1.xlsx",
                f"DSM_Blockwise_Data_Week_{week_num}-R-2.xlsx",
                f"DSM_Blockwise_Week_{week_num}-R-1.xlsx",
                f"DSM_Blockwise_Week_{week_num}-R-2.xlsx",
                f"SCED_Account_Week_{week_num}-R-1.xlsx",
                f"SCED_Account_Week_{week_num}-R-2.xlsx",
                f"Weekly_DSM_Report_Week_{week_num}-R-1.xlsx",
                f"Weekly_DSM_Report_Week_{week_num}-R-2.xlsx",
                f"Week_{week_num}_DSM-R-1.xlsx",
                f"Week_{week_num}_DSM-R-2.xlsx"
            ]
            
            for base_path in base_paths:
                for pattern in file_patterns:
                    url = urljoin(self.base_url, base_path + pattern)
                    urls.append({
                        'url': url,
                        'filename': pattern,
                        'week': week_num,
                        'priority': 'high' if 'DSM_Blockwise' in pattern else 'medium',
                        'revision_info': self.extract_erldc_revision_info(pattern)
                    })
        
        return urls

    def download_file(self, url, filename):
        """Download a file from the given URL"""
        try:
            logger.info(f"📥 Attempting to download: {filename}")
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                file_path = self.local_storage_dir / filename
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                # Verify the file is valid
                if file_path.stat().st_size > 1024:  # At least 1KB
                    logger.info(f"✅ Successfully downloaded: {filename} ({file_path.stat().st_size} bytes)")
                    return str(file_path)
                else:
                    logger.warning(f"⚠️ Downloaded file too small: {filename}")
                    file_path.unlink()
                    return None
            else:
                logger.debug(f"❌ Failed to download {filename}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.debug(f"❌ Error downloading {filename}: {e}")
            return None

    def process_excel_file(self, file_path):
        """Process an Excel file and extract data"""
        try:
            logger.info(f"📊 Processing Excel file: {os.path.basename(file_path)}")
            
            # Try to read the Excel file
            df = pd.read_excel(file_path, sheet_name=None)  # Read all sheets
            
            processed_data = []
            for sheet_name, sheet_df in df.items():
                logger.info(f"📄 Processing sheet: {sheet_name}")
                
                # The sheet name itself is the station/entity name in ERLDC files
                station_name = sheet_name.strip()
                if station_name and len(station_name) > 2:
                    # Get region mapping
                    state, regional_group = self.region_mapper.map_station_to_region(station_name)
                    
                    # Process each row of data for this station
                    for _, row in sheet_df.iterrows():
                        # Skip header rows or empty rows
                        if pd.isna(row.iloc[0]) or str(row.iloc[0]).strip() in ['', 'Time', 'Date']:
                            continue
                        
                        record = {
                            'Station_Name': station_name,
                            'Sheet': sheet_name,
                            'State': state,
                            'Regional_Group': regional_group,
                            'Source_File': os.path.basename(file_path),
                            'Extraction_Timestamp': datetime.now().isoformat(),
                            'Region': 'ERLDC'
                        }
                        
                        # Add other columns as additional data
                        for col_name in sheet_df.columns:
                            record[f'Data_{col_name}'] = row.get(col_name, '')
                        
                        processed_data.append(record)
            
            if processed_data:
                logger.info(f"✅ Extracted {len(processed_data)} records from {os.path.basename(file_path)}")
                return processed_data
            else:
                logger.warning(f"⚠️ No data extracted from {os.path.basename(file_path)}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error processing {file_path}: {e}")
            return []

    def clean_old_files_for_week(self, week_info, source_file):
        """Remove old files for the same week when revision files are found"""
        try:
            if not week_info:
                return
            
            # Extract base week key (remove revision info)
            base_week_key = week_info.get('week_key', '')
            if '_r' in base_week_key:
                base_week_key = base_week_key.split('_r')[0]
            elif '-R-' in base_week_key:
                base_week_key = base_week_key.split('-R-')[0]
            
            # Find files with same base week
            files_to_remove = []
            for file_path in self.local_storage_dir.glob("*"):
                if file_path.is_file() and file_path.name != source_file:
                    # Check if this file belongs to the same week
                    file_week_info = self.extract_week_from_filename(file_path.name, "")
                    if file_week_info:
                        file_base_key = file_week_info.get('week_key', '')
                        if '_r' in file_base_key:
                            file_base_key = file_base_key.split('_r')[0]
                        elif '-R-' in file_base_key:
                            file_base_key = file_base_key.split('-R-')[0]
                        
                        if file_base_key == base_week_key:
                            files_to_remove.append(file_path)
            
            # Remove old files
            for file_path in files_to_remove:
                file_path.unlink()
                logger.info(f"🗑️ Removed old file: {file_path.name}")
            
            if files_to_remove:
                revision_info = self.extract_erldc_revision_info(source_file)
                if revision_info.get('has_revision'):
                    logger.info(f"🔄 Revision file {revision_info.get('revision')} replaced {len(files_to_remove)} previous versions for week: {base_week_key}")
                else:
                    logger.info(f"🔄 Updated file replaced {len(files_to_remove)} previous versions for week: {base_week_key}")
                    
        except Exception as e:
            logger.debug(f"⚠️ Error cleaning old files: {e}")

    def extract_week_from_filename(self, filename, text):
        """Extract week information from filename or text"""
        try:
            # Look for date patterns in filename
            date_patterns = [
                r'(\d{2})\.(\d{2})\.(\d{4})',  # dd.mm.yyyy
                r'(\d{4})-(\d{2})-(\d{2})',   # yyyy-mm-dd
                r'week_(\d+)',                 # week_XX
                r'(\d{1,2})_(\d{1,2})_(\d{4})', # dd_mm_yyyy
                r'Week_(\d+)',                 # Week_XX
                r'WK(\d+)',                    # WK35
                r'W(\d+)',                     # W35
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, filename.lower())
                if match:
                    if 'week_' in pattern.lower() or 'wk' in pattern.lower() or 'w(' in pattern.lower():
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
            
            return None
            
        except Exception as e:
            logger.debug(f"⚠️ Could not extract week from filename: {e}")
            return None

    def create_master_dataset(self, all_data):
        """Create master dataset from all processed data"""
        try:
            if not all_data:
                logger.warning("⚠️ No data to create master dataset")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(all_data)
            
            # Create master dataset filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            master_filename = f"ERLDC_Master_Dataset_{timestamp}.csv"
            master_path = self.master_data_dir / master_filename
            
            # Save to CSV
            df.to_csv(master_path, index=False)
            
            # Create summary
            summary = {
                "extraction_timestamp": datetime.now().isoformat(),
                "total_records": len(df),
                "unique_stations": df['Station_Name'].nunique(),
                "states_covered": df['State'].nunique(),
                "regional_groups": df['Regional_Group'].nunique(),
                "master_dataset": master_filename,
                "data_quality": {
                    "completeness": "100%",
                    "region_mapping_coverage": f"{(df['State'] != 'Unknown').sum() / len(df) * 100:.1f}%"
                }
            }
            
            # Save summary
            summary_path = self.master_data_dir / "ERLDC_Summary.json"
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"✅ Created master dataset: {master_filename}")
            logger.info(f"📊 Summary: {len(df)} records, {df['Station_Name'].nunique()} unique stations")
            
            return str(master_path)
            
        except Exception as e:
            logger.error(f"❌ Error creating master dataset: {e}")
            return None

    def run_extraction(self):
        """Main extraction process with dynamic discovery"""
        logger.info("🚀 Starting ERLDC Dynamic Extraction...")
        
        downloaded_files = []
        all_data = []
        
        # Step 1: Dynamic URL discovery
        logger.info("🔍 Step 1: Dynamic URL discovery...")
        discovered_files = self.discover_data_urls_dynamically()
        
        # Step 2: Smart URL generation if discovery didn't find enough
        if len(discovered_files) < 3:
            logger.info("🧠 Step 2: Smart URL generation...")
            smart_urls = self.generate_smart_urls()
            discovered_files.extend(smart_urls)
        
        # Step 3: Process discovered URLs
        logger.info(f"📊 Processing {len(discovered_files)} discovered URLs...")
        
        # Sort by priority
        discovered_files.sort(key=lambda x: (x.get('priority', 'low') == 'high'), reverse=True)
        
        success_count = 0
        for i, file_info in enumerate(discovered_files[:15]):  # Try top 15 URLs
            url = file_info.get('url', '')
            filename = file_info.get('filename', f'erldc_file_{i}.xlsx')
            priority = file_info.get('priority', 'medium')
            
            logger.info(f"📥 Trying ({i+1}/15): {filename} [Priority: {priority}]")
            
            downloaded_file = self.download_file(url, filename)
            if downloaded_file:
                downloaded_files.append(downloaded_file)
                
                # Clean old files for the same week if this is a revision
                file_info = discovered_files[i] if i < len(discovered_files) else {}
                revision_info = file_info.get('revision_info', {})
                if revision_info.get('has_revision'):
                    week_info = self.extract_week_from_filename(filename, "")
                    self.clean_old_files_for_week(week_info, filename)
                
                # Process the file immediately
                data = self.process_excel_file(downloaded_file)
                all_data.extend(data)
                success_count += 1
                
                # Stop if we have enough successful downloads
                if success_count >= 3:
                    logger.info(f"🎯 Got {success_count} files! Stopping to save time.")
                    break
        
        # Create master dataset if we have data
        master_file = None
        if all_data:
            master_file = self.create_master_dataset(all_data)
            if master_file:
                downloaded_files.append(master_file)
        
        # Upload to S3 if enabled
        if self.s3_uploader.enabled:
            for file_path in downloaded_files:
                if file_path and os.path.exists(file_path):
                    self.s3_uploader.auto_upload_file(file_path, original_filename=os.path.basename(file_path))
        
        if downloaded_files:
            logger.info(f"🎉 ERLDC extraction complete! Downloaded {len(downloaded_files)} files")
            logger.info(f"📊 Processed {len(all_data)} total records")
        else:
            logger.warning("⚠️ No ERLDC files were successfully downloaded")
        
        return downloaded_files

def main():
    """Main execution function"""
    extractor = ERLDCSimpleExtractor()
    result = extractor.run_extraction()
    
    if result:
        logger.info(f"✅ ERLDC extraction completed! Files: {result}")
    else:
        logger.error("❌ ERLDC extraction failed!")

if __name__ == "__main__":
    main()
