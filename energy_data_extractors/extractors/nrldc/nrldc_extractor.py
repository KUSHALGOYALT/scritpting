#!/usr/bin/env python3
"""
NRLDC Working DSA Extractor - Downloads actual DSA PDF files and converts to CSV
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
import json

# Add common module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'common'))
from auto_s3_upload import AutoS3Uploader

# Import region mapper
from nrldc_region_mapper import NRLDCRegionMapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NRLDCWorkingDSAExtractor:
    def __init__(self):
        self.base_url = "http://164.100.60.165"
        self.local_storage_dir = Path("local_data/NRLDC")
        self.master_data_dir = Path("master_data/NRLDC")
        self.local_storage_dir.mkdir(parents=True, exist_ok=True)
        self.master_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize S3 uploader
        self.s3_uploader = AutoS3Uploader()
        
        # Initialize region mapper
        self.region_mapper = NRLDCRegionMapper()
        
        # Session for maintaining cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Track processed weeks to avoid duplicates
        self.processed_weeks_file = self.master_data_dir / "processed_weeks.json"
        self.processed_weeks = self.load_processed_weeks()
        self.dsa_page_url = f"{self.base_url}/comm/dsa.html"

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
                
                # Format for NRLDC (DDMMYY format)
                start_str = week_start.strftime('%d%m%y')
                end_str = week_end.strftime('%d%m%y')
                week_num = week_start.isocalendar()[1]
                
                week_info = {
                    'start_date': week_start.strftime('%Y-%m-%d'),
                    'end_date': week_end.strftime('%Y-%m-%d'),
                    'start_ddmmyy': start_str,
                    'end_ddmmyy': end_str,
                    'week_num': week_num,
                    'week_key': f"{start_str}-{end_str}_WK{week_num}"
                }
                weeks.append(week_info)
            
            return weeks
        except Exception as e:
            logger.error(f"❌ Error calculating past 7 days weeks: {e}")
            return []

    def get_dsa_links(self):
        """Get DSA links from the main page with enhanced revision detection"""
        try:
            logger.info("🔍 Getting DSA links from NRLDC DSA page...")
            
            # Get the DSA listing page
            response = self.session.get(self.dsa_page_url, timeout=20)
            if response.status_code != 200:
                logger.error(f"❌ Failed to access main page: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find supporting file links with enhanced pattern matching
            links = soup.find_all('a', href=True)
            logger.info(f"🔗 Found {len(links)} total links")
            dsa_links = []
            
            # Enhanced patterns to detect various file naming conventions
            supporting_patterns = [
                r'supporting_files.*\.xls',
                r'supporting.*files.*\.xls', 
                r'dsm.*supporting.*\.xls',
                r'dsa.*supporting.*\.xls',
                r'.*supporting.*_[rR]\d+.*\.xls',    # Revision files (_r1, _R1)
                r'.*supporting.*_[rR][eE][vV]\d+.*\.xls',  # Revision files (_rev1, _REV1)
                r'.*supporting.*_[vV]\d+.*\.xls'     # Version files (_v1, _V1)
            ]
            
            for a in links:
                text = (a.get_text() or '').strip().lower()
                href = a.get('href', '')
                if not href:
                    continue
                
                href_l = href.lower()
                
                # Check against all patterns
                is_supporting_file = False
                for pattern in supporting_patterns:
                    if re.search(pattern, href_l):
                        is_supporting_file = True
                        break
                
                # Also check for exact matches
                if ('supporting_files.xls' in href_l or href_l.endswith('supporting_files.xls')):
                    is_supporting_file = True
                
                if is_supporting_file:
                    full_url = href if href.startswith('http') else f"{self.base_url}/{href.lstrip('/')}"
                    
                    # Extract revision info for better tracking
                    revision_info = self.extract_revision_info(href)
                    
                    dsa_links.append({
                        'text': a.get_text().strip(),
                        'url': full_url,
                        'filename': os.path.basename(href),
                        'revision_info': revision_info
                    })
            
            logger.info(f"📊 Found {len(dsa_links)} DSA links (including revisions)")
            
            # Show found links with revision info
            for link in dsa_links[:5]:
                rev_info = link.get('revision_info', {})
                rev_text = f" (Rev: {rev_info.get('revision', 'N/A')})" if rev_info.get('has_revision') else ""
                logger.info(f"   📎 {link['text']} -> {link['filename']}{rev_text}")
            
            return dsa_links
            
        except Exception as e:
            logger.error(f"❌ Error getting DSA links: {e}")
            return []

    def extract_revision_info(self, filename):
        """Extract revision information from filename"""
        try:
            # Look for revision patterns (case-insensitive)
            revision_patterns = [
                r'_[rR](\d+)',           # _r1, _R1, _r2, _R2
                r'_[rR][eE][vV](\d+)',   # _rev1, _REV1, _Rev1  
                r'_[rR][eE][vV][iI][sS][iI][oO][nN](\d+)',  # _revision1, _REVISION1
                r'_[vV](\d+)',           # _v1, _V1, _v2, _V2
                r'_[vV][eE][rR][sS][iI][oO][nN](\d+)',      # _version1, _VERSION1
                r'\([rR](\d+)\)',        # (r1), (R1), (r2), (R2)
                r'\([rR][eE][vV](\d+)\)', # (rev1), (REV1), (Rev1)
            ]
            
            for pattern in revision_patterns:
                match = re.search(pattern, filename.lower())
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
            logger.debug(f"⚠️ Error extracting revision info: {e}")
            return {'has_revision': False, 'revision': None}

    def extract_week_from_url(self, url):
        """Extract week information from URL with revision detection"""
        try:
            # Look for revision indicators (case-insensitive: _r1, _R1, _rev1, _REV1, etc.)
            revision_pattern = r'_[rR](\d+)|_[rR][eE][vV](\d+)|_[rR][eE][vV][iI][sS][iI][oO][nN](\d+)|_[vV](\d+)'
            revision_match = re.search(revision_pattern, url)
            revision_num = None
            if revision_match:
                # Get the first non-None group
                revision_num = next((g for g in revision_match.groups() if g), None)
                logger.info(f"🔄 Detected revision indicator: r{revision_num}")
            
            # Look for date patterns in URL like "110825-170825(WK-20)"
            date_pattern = r'(\d{6})-(\d{6})\(WK-(\d+)\)'
            match = re.search(date_pattern, url)
            if match:
                start_date = match.group(1)
                end_date = match.group(2)
                week_num = match.group(3)
                base_key = f"{start_date}-{end_date}_WK{week_num}"
                return f"{base_key}_r{revision_num}" if revision_num else base_key
            
            # Look for date patterns in URL like "110825-170825"
            date_pattern2 = r'(\d{6})-(\d{6})'
            match = re.search(date_pattern2, url)
            if match:
                start_date = match.group(1)
                end_date = match.group(2)
                # Try to extract week from the URL path
                week_match = re.search(r'wk-?(\d+)', url.lower())
                week_num = week_match.group(1) if week_match else "UNK"
                base_key = f"{start_date}-{end_date}_WK{week_num}"
                return f"{base_key}_r{revision_num}" if revision_num else base_key
            
            return "unknown_week"
            
        except Exception as e:
            logger.error(f"❌ Error extracting week from URL: {e}")
            return "unknown_week"

    def download_dsa_data(self, dsa_link):
        """Download DSA supporting .xls and convert to CSV (preserve columns)"""
        try:
            logger.info(f"📥 Downloading DSA file: {dsa_link['filename']}")
            
            # Download the file
            response = self.session.get(dsa_link['url'], timeout=30)
            if response.status_code != 200:
                logger.error(f"❌ Failed to download: {response.status_code}")
                return None
            
            # Extract week info from URL/text if present
            week_info = self.extract_week_from_url(dsa_link['url'])
            
            # Check for existing files of the same week (including base and revision files)
            base_week_key = week_info.split('_r')[0] if '_r' in week_info else week_info
            files_to_remove = []
            
            # Find all files for this week (base + any revisions)
            for existing_key in list(self.processed_weeks.keys()):
                existing_base_key = existing_key.split('_r')[0] if '_r' in existing_key else existing_key
                if existing_base_key == base_week_key:
                    files_to_remove.append(existing_key)
            
            # Remove all existing files for this week
            for key_to_remove in files_to_remove:
                existing_file = self.processed_weeks[key_to_remove].get('csv_file', '')
                if existing_file:
                    old_csv_path = self.local_storage_dir / existing_file
                    if old_csv_path.exists():
                        old_csv_path.unlink()
                        logger.info(f"🗑️ Removed old CSV: {existing_file}")
                
                # Also delete old XLS file
                old_xls_name = existing_file.replace('.csv', '.xls') if existing_file else ''
                if old_xls_name:
                    old_xls_path = self.local_storage_dir / old_xls_name
                    if old_xls_path.exists():
                        old_xls_path.unlink()
                        logger.info(f"🗑️ Removed old XLS: {old_xls_name}")
                
                # Remove from tracking
                del self.processed_weeks[key_to_remove]
            
            if files_to_remove:
                revision_info = self.extract_revision_info(dsa_link['filename'])
                if revision_info.get('has_revision'):
                    logger.info(f"🔄 Revision file {revision_info.get('revision')} replacing all previous versions for week: {base_week_key}")
                else:
                    logger.info(f"🔄 Updated file replacing previous versions for week: {base_week_key}")
            else:
                logger.info(f"📥 New week data: {week_info}")
            
            # Save the file
            file_path = self.local_storage_dir / dsa_link['filename']
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"✅ Downloaded: {dsa_link['filename']} ({len(response.content)} bytes)")
            
            # Convert XLS to CSV preserving columns and add region mapping
            csv_path = None
            try:
                df = pd.read_excel(file_path)
                
                # Add region mapping if station name column exists
                station_col = None
                for col in df.columns:
                    if 'stn' in str(col).lower() or 'station' in str(col).lower() or col == df.columns[0]:
                        station_col = col
                        break
                
                if station_col is not None:
                    # Apply region mapping
                    df_mapped = self.region_mapper.map_dataframe_regions(df, station_col)
                    logger.info(f"✅ Added region mapping to {len(df_mapped)} rows")
                    
                    # Get region summary
                    region_summary = self.region_mapper.get_region_summary(df_mapped, station_col)
                    logger.info(f"📊 Region summary: {region_summary['by_group']}")
                    
                    df = df_mapped
                
                csv_filename = dsa_link['filename'].rsplit('.', 1)[0] + '.csv'
                csv_path = self.local_storage_dir / csv_filename
                df.to_csv(csv_path, index=False)
                logger.info(f"✅ Converted XLS to CSV with region mapping: {csv_path} ({len(df)} rows, {len(df.columns)} cols)")
                csv_path = str(csv_path)
            except Exception as e:
                logger.warning(f"⚠️ Could not convert XLS to CSV: {e}")
                csv_path = str(file_path)
            
            # Update processed weeks
            self.processed_weeks[week_info] = {
                'timestamp': datetime.now().isoformat(),
                'filename': dsa_link['filename'],
                'csv_file': os.path.basename(csv_path) if csv_path else None,
                'url': dsa_link['url']
            }
            self.save_processed_weeks()
            
            return csv_path
            
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            return None

    def create_master_dataset(self):
        """Create master dataset for NRLDC region"""
        try:
            logger.info("🔧 Creating NRLDC master dataset...")
            
            # Get all CSV files
            csv_files = list(self.local_storage_dir.glob("*.csv"))
            if not csv_files:
                logger.warning("⚠️ No CSV files found to create master dataset")
                return None
            
            # Read and combine all CSV files
            all_data = []
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    df['Source_File'] = csv_file.name
                    all_data.append(df)
                    logger.info(f"📊 Added {csv_file.name}: {len(df)} rows")
                except Exception as e:
                    logger.warning(f"⚠️ Could not read {csv_file.name}: {e}")
            
            if not all_data:
                logger.error("❌ No data to combine")
                return None
            
            # Combine all data
            master_df = pd.concat(all_data, ignore_index=True)
            
            # Add metadata
            master_df['Master_Dataset_Created'] = datetime.now().isoformat()
            master_df['Total_Records'] = len(master_df)
            master_df['Region'] = 'NRLDC'
            
            # Generate region-based summary if region columns exist
            if 'State' in master_df.columns and 'Regional_Group' in master_df.columns:
                region_stats = {
                    'total_stations': len(master_df.iloc[:, 0].unique()) if len(master_df) > 0 else 0,
                    'states_covered': master_df['State'].nunique(),
                    'regional_groups': master_df['Regional_Group'].nunique(),
                    'state_distribution': master_df['State'].value_counts().to_dict(),
                    'group_distribution': master_df['Regional_Group'].value_counts().to_dict()
                }
                
                # Save region summary
                summary_file = self.master_data_dir / "NRLDC_Summary.json"
                with open(summary_file, 'w') as f:
                    json.dump(region_stats, f, indent=2)
                
                logger.info(f"📊 Region Summary: {region_stats['group_distribution']}")
                logger.info(f"✅ Saved region summary: {summary_file}")
            
            # Save master dataset
            master_file = self.master_data_dir / f"NRLDC_Master_Dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            master_df.to_csv(master_file, index=False)
            
            logger.info(f"✅ NRLDC master dataset created: {master_file} ({len(master_df)} total rows)")
            
            return str(master_file)
            
        except Exception as e:
            logger.error(f"❌ Error creating master dataset: {e}")
            return None

    def generate_supporting_urls(self):
        """Generate strict Supporting_files.xls URLs for past 7 days (NRLDC 2021-22 path)"""
        urls = []
        weeks = self.get_past_7_days_weeks()
        for w in weeks:
            start = datetime.strptime(w['start_date'], '%Y-%m-%d').strftime('%d%m%y')
            end = datetime.strptime(w['end_date'], '%Y-%m-%d').strftime('%d%m%y')
            week_num = w['week_num']
            path = f"/comm/2021-22/dsa/{start}-{end}(WK-{week_num})/Supporting_files.xls"
            urls.append({
                'url': f"{self.base_url}{path}",
                'filename': f"Supporting_files_{start}-{end}_WK{week_num}.xls",
                'week_key': f"{start}-{end}_WK{week_num}"
            })
        return urls

    def parse_weeks_from_dsa_page(self):
        """Parse week tokens like 110825-170825(WK-20) from the DSA page and construct URLs."""
        try:
            resp = self.session.get(self.dsa_page_url, timeout=20)
            if resp.status_code != 200:
                logger.warning(f"⚠️ Could not load DSA page: {resp.status_code}")
                return []
            text = resp.text
            # Find tokens like 110825-170825(WK-20)
            matches = re.findall(r"(\d{6})-(\d{6})\(WK-?(\d{1,2})\)", text, flags=re.I)
            items = []
            for start, end, wk in matches:
                path = f"/comm/2021-22/dsa/{start}-{end}(WK-{wk})/Supporting_files.xls"
                items.append({
                    'url': f"{self.base_url}{path}",
                    'filename': f"Supporting_files_{start}-{end}_WK{wk}.xls",
                    'week_key': f"{start}-{end}_WK{wk}"
                })
            logger.info(f"📅 Parsed {len(items)} Supporting_files URLs from DSA page")
            return items
        except Exception as e:
            logger.error(f"❌ Failed to parse DSA page weeks: {e}")
            return []

    def download_supporting_xls(self, item):
        """Download .xls and also write a CSV preserving columns (no reshaping)."""
        try:
            url = item['url']
            filename = item['filename']
            week_key = item['week_key']
            logger.info(f"📥 Downloading Supporting XLS: {filename}")

            # Check for existing files of the same week (including base and revision files)
            base_week_key = week_key.split('_r')[0] if '_r' in week_key else week_key
            files_to_remove = []
            
            # Find all files for this week (base + any revisions)
            for existing_key in list(self.processed_weeks.keys()):
                existing_base_key = existing_key.split('_r')[0] if '_r' in existing_key else existing_key
                if existing_base_key == base_week_key:
                    files_to_remove.append(existing_key)
            
            # Remove all existing files for this week
            for key_to_remove in files_to_remove:
                existing_file = self.processed_weeks[key_to_remove].get('csv_file', '')
                if existing_file:
                    old_csv_path = self.local_storage_dir / existing_file
                    if old_csv_path.exists():
                        old_csv_path.unlink()
                        logger.info(f"🗑️ Removed old CSV: {existing_file}")
                
                # Also delete old XLS file
                old_xls_name = self.processed_weeks[key_to_remove].get('filename', '')
                if old_xls_name:
                    old_xls_path = self.local_storage_dir / old_xls_name
                    if old_xls_path.exists():
                        old_xls_path.unlink()
                        logger.info(f"🗑️ Removed old XLS: {old_xls_name}")
                
                # Remove from tracking
                del self.processed_weeks[key_to_remove]
            
            if files_to_remove:
                revision_info = self.extract_revision_info(filename)
                if revision_info.get('has_revision'):
                    logger.info(f"🔄 Revision file {revision_info.get('revision')} replacing all previous versions for week: {base_week_key}")
                else:
                    logger.info(f"🔄 Updated file replacing previous versions for week: {base_week_key}")
            else:
                logger.info(f"📥 New week data: {week_key}")

            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"⚠️ Not found ({resp.status_code}): {url}")
                return None

            xls_path = self.local_storage_dir / filename
            with open(xls_path, 'wb') as f:
                f.write(resp.content)
            logger.info(f"✅ Saved XLS: {xls_path}")

            # Convert to CSV with all columns intact and add region mapping
            try:
                df = pd.read_excel(xls_path, engine='xlrd')
                
                # Add region mapping if station name column exists
                station_col = None
                for col in df.columns:
                    if 'stn' in str(col).lower() or 'station' in str(col).lower() or col == df.columns[0]:
                        station_col = col
                        break
                
                if station_col is not None:
                    # Apply region mapping
                    df_mapped = self.region_mapper.map_dataframe_regions(df, station_col)
                    logger.info(f"✅ Added region mapping to {len(df_mapped)} rows")
                    
                    # Get region summary
                    region_summary = self.region_mapper.get_region_summary(df_mapped, station_col)
                    logger.info(f"📊 Region summary: {region_summary['by_group']}")
                    
                    df = df_mapped
                
                csv_filename = filename.replace('.xls', '.csv')
                csv_path = self.local_storage_dir / csv_filename
                df.to_csv(csv_path, index=False)
                logger.info(f"✅ Wrote CSV with region mapping: {csv_path} ({len(df)} rows, {len(df.columns)} cols)")
                csv_saved = str(csv_path)
            except Exception as ce:
                logger.warning(f"⚠️ Could not parse XLS to CSV: {ce}")
                csv_saved = None

            # Track
            self.processed_weeks[week_key] = {
                'timestamp': datetime.now().isoformat(),
                'filename': filename,
                'csv_file': os.path.basename(csv_saved) if csv_saved else None,
                'url': url
            }
            self.save_processed_weeks()
            return csv_saved or str(xls_path)
        except Exception as e:
            logger.error(f"❌ Supporting XLS download failed: {e}")
            return None

    def run_extraction(self):
        """Main extraction process (supporting .xls only)"""
        logger.info("🚀 Starting NRLDC DSA extraction (supporting .xls only)...")
        # Prefer constructing URLs from the live DSA page tokens
        items = self.parse_weeks_from_dsa_page()
        if not items:
            # Fallback to past-7-days generator (may 404 if not present under 2021-22)
            items = self.generate_supporting_urls()
            logger.info(f"📅 Generated {len(items)} week URLs under 2021-22 (fallback)")

        downloaded = []
        # Limit attempts to keep it fast
        for item in items[:10]:
            res = self.download_supporting_xls(item)
            if res:
                downloaded.append(res)
            else:
                logger.info(f"⏭️ Skipped/failed: {item['filename']}")

        if downloaded:
            master_file = self.create_master_dataset()
            if master_file:
                downloaded.append(master_file)
        logger.info(f"🎉 Supporting extraction complete. Files: {len(downloaded)}")
        return downloaded

def main():
    """Main execution function"""
    extractor = NRLDCWorkingDSAExtractor()
    result = extractor.run_extraction()
    
    if result:
        logger.info(f"✅ NRLDC DSA extraction completed! Files: {result}")
    else:
        logger.error("❌ NRLDC DSA extraction failed!")

if __name__ == "__main__":
    main()
