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
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
import json
import typing
import numpy as np

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
        
        # Initialize S3 uploader
        self.s3_uploader = AutoS3Uploader()
        
        # Initialize region mapper
        self.region_mapper = NRLDCRegionMapper()
        
        # Session for maintaining cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Track processed weeks to avoid duplicates (no local storage)
        self.processed_weeks = {}
        self.dsa_page_url = f"{self.base_url}/comm/dsa.html"
        # Pipeline mode: download XLS then convert to CSV; master uses CSV only
        self.csv_only = False

    def _is_state_name(self, name: str) -> bool:
        try:
            if not name:
                return False
            candidate = str(name).strip().lower()
            if not candidate:
                return False
            # Common NR states/UTs and generic state tokens
            state_tokens = {
                'delhi','nct of delhi','haryana','punjab','rajasthan','uttar pradesh','uttarakhand',
                'jammu','jammu & kashmir','jammu and kashmir','jammu-kashmir','j&k','ladakh','chandigarh',
                'himachal pradesh','hp','up','uk','jk',
                'state','state total','state-wise','discom','discoms','utility','distribution company'
            }
            # Exact match or startswith/endswith with known tokens
            if candidate in state_tokens:
                return True
            # Heuristics: ends with 'state', contains 'state total', equals two-word with 'pradesh'
            if candidate.endswith(' state') or 'state total' in candidate:
                return True
            if ' pradesh' in candidate and len(candidate.split()) <= 3:
                return True
            # Single token that is a known abbreviation
            if candidate in {'up','uk','hp','jk','dl','hr','pb','rj'}:
                return True
            return False
        except Exception:
            return False

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

    def get_csv_links(self):
        """Get Supporting CSV links from the main DSA page (CSV-only)."""
        try:
            logger.info("🔍 Getting CSV Supporting links from NRLDC DSA page...")
            response = self.session.get(self.dsa_page_url, timeout=20)
            if response.status_code != 200:
                logger.error(f"❌ Failed to access main page: {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a', href=True)
            csv_links = []
            for a in links:
                text = (a.get_text() or '').strip()
                href = a.get('href', '')
                if not href:
                    continue
                href_l = href.lower()
                # Prefer CSV supporting files
                if href_l.endswith('.csv'):
                    full_url = href if href.startswith('http') else f"{self.base_url}/{href.lstrip('/')}"
                    filename = os.path.basename(href)
                    week_key = self.extract_week_from_url(full_url)
                    csv_links.append({
                        'text': text,
                        'url': full_url,
                        'filename': filename,
                        'week_key': week_key
                    })
            logger.info(f"📊 Found {len(csv_links)} CSV links on DSA page")
            return csv_links
        except Exception as e:
            logger.error(f"❌ Error getting CSV links: {e}")
            return []

    def extract_week_from_url(self, url):
        """Extract week information from URL"""
        pass
        
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
            if self.csv_only:
                logger.info("⏭️ CSV-only mode is enabled. Skipping XLS download.")
                return None
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
            
            # Save the file to temporary location
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{dsa_link['filename']}") as temp_file:
                temp_file.write(response.content)
                file_path = temp_file.name
            
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
                logger.warning(f"⚠️ Sheet parse failed: {e}")
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

    def _extract_station_from_sheet(self, df: pd.DataFrame) -> typing.Optional[str]:
        try:
            max_scan = min(15, len(df))
            pattern = re.compile(r"station\s*:?\s*(.+)", re.IGNORECASE)
            for i in range(max_scan):
                row = df.iloc[i]
                # as string line
                row_text = " ".join([str(x) for x in row.tolist() if pd.notna(x)]).strip()
                if not row_text:
                    continue
                m = pattern.search(row_text)
                if m:
                    name = m.group(1).strip()
                    name = re.split(r"\s{2,}|,|;|\|", name)[0].strip()
                    if name:
                        return name
                # explicit "Station" cell, next value in row
                for idx, val in row.items():
                    try:
                        if isinstance(val, str) and re.match(r"^\s*station\s*:?$", val.strip(), re.IGNORECASE):
                            # find next non-empty cell
                            started = False
                            for v in row.tolist():
                                if not started:
                                    if v is val:
                                        started = True
                                    continue
                                if pd.notna(v) and str(v).strip():
                                    return str(v).strip()
                    except Exception:
                        continue
            return None
        except Exception:
            return None

    def _extract_station_from_csv_structure(self, df: pd.DataFrame, filename: str) -> typing.Optional[str]:
        """Extract station name from CSV structure, handling different formats"""
        try:
            # Check if this is a Supporting_files CSV with Stn_Name in second row
            if 'supporting_files' in filename.lower():
                # Look for Stn_Name in the first few rows
                for i in range(min(3, len(df))):
                    row_str = ' '.join([str(x).lower() for x in df.iloc[i].values if pd.notna(x)])
                    if 'stn_name' in row_str:
                        # This row contains the headers, next row has actual station data
                        if i + 1 < len(df):
                            first_station = str(df.iloc[i + 1, 0]).strip()
                            if first_station and not self._is_state_name(first_station):
                                return first_station
                        break
            
            # Check if this is a DSA_Week CSV with Constituents column
            elif 'dsa_week' in filename.lower():
                # Look for Constituents column
                constituents_col = None
                for col in df.columns:
                    if 'constituent' in str(col).lower():
                        constituents_col = col
                        break
                
                if constituents_col is not None:
                    # Get unique constituents (states/entities)
                    constituents = df[constituents_col].dropna().unique()
                    # Filter out state names, keep only station-like names
                    station_candidates = [c for c in constituents if not self._is_state_name(str(c))]
                    if station_candidates:
                        return str(station_candidates[0])
            
            return None
        except Exception as e:
            logger.warning(f"⚠️ Error extracting station from CSV structure {filename}: {e}")
            return None

    def download_supporting_csv(self, item):
        """Download Supporting CSV file as-is (no transformation)."""
        try:
            url = item['url']
            filename = item['filename']
            week_key = item.get('week_key') or self.extract_week_from_url(url)
            logger.info(f"📥 Downloading Supporting CSV: {filename}")

            # Skip if processed
            if week_key in self.processed_weeks and self.processed_weeks[week_key].get('csv_file'):
                logger.info(f"⏭️ CSV already processed: {week_key}")
                return None

            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"⚠️ Not found ({resp.status_code}): {url}")
                return None
                
            import tempfile
            csv_path = Path(tempfile.mktemp(suffix='.csv'))
            with open(csv_path, 'wb') as f:
                f.write(resp.content)
            logger.info(f"✅ Saved CSV: {csv_path}")

            # Track
            self.processed_weeks[week_key] = {
                'timestamp': datetime.now().isoformat(),
                'filename': filename,
                'csv_file': filename,
                'url': url
            }
            self.save_processed_weeks()
            return str(csv_path)
        except Exception as e:
            logger.error(f"❌ Supporting CSV download failed: {e}")
            return None

    def create_station_mapping(self):
        """Create comprehensive station mapping across all sheet types"""
        try:
            logger.info("🗺️ Creating comprehensive station mapping...")
            
            # Get all CSV files (exclude earlier aggregated exports)
            # Use temporary files instead of local storage
            csv_files = []
            if not csv_files:
                logger.warning("⚠️ No CSV files found to create station mapping")
                return None
            
            logger.info(f"📊 Found {len(csv_files)} CSV files to analyze for station mapping")
            
            # Helper to robustly read messy CSVs
            def _robust_read_csv(path: Path) -> pd.DataFrame:
                try:
                    df = pd.read_csv(path, low_memory=False)
                    if df.shape[1] > 1:
                        return df
                except Exception:
                    pass
                # Try automatic delimiter detection
                try:
                    df = pd.read_csv(path, engine='python', sep=None)
                    if df.shape[1] > 1:
                        return df
                except Exception:
                    pass
                # Try tab
                try:
                    df = pd.read_csv(path, sep='\t', engine='python', low_memory=False)
                    if df.shape[1] > 1:
                        return df
                except Exception:
                    pass
                # Try whitespace
                try:
                    df = pd.read_csv(path, delim_whitespace=True, engine='python', low_memory=False)
                    return df
                except Exception as e:
                    logger.debug(f"⚠️ Robust read failed for {path.name}: {e}")
                    return pd.DataFrame()

            # Attempt to detect header row in frames that include metadata rows
            def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
                if df.empty:
                    return df
                # Drop fully empty rows
                df_clean = df.dropna(how='all')
                # If columns look like 0..N-1 and there exists a row containing typical headers, use it
                candidate = None
                header_keywords = ['time', 'block', 'date']
                for i in range(min(10, len(df_clean))):
                    row_vals = df_clean.iloc[i].astype(str).str.strip().str.lower().tolist()
                    if any(k in row_vals for k in header_keywords):
                        candidate = i
                        break
                if candidate is not None:
                    new_cols = df_clean.iloc[candidate].astype(str).str.strip().tolist()
                    df_norm = df_clean.iloc[candidate+1:].copy()
                    df_norm.columns = new_cols
                    return df_norm
                return df_clean

            def _infer_station_from_df(df_raw: pd.DataFrame, df_norm: pd.DataFrame) -> typing.Optional[str]:
                try:
                    # 1) If Station_Name exists and has non-empty values in df_norm
                    for col in df_norm.columns:
                        if str(col).strip().lower() in ['station_name','station','entity']:
                            vals = df_norm[col].dropna().astype(str).str.strip()
                            if not vals.empty and vals.iloc[0]:
                                return vals.iloc[0]
                    # 2) Scan first rows of raw df for patterns like 'Station : NAME'
                    max_scan = min(15, len(df_raw))
                    pattern = re.compile(r"station\s*:?\s*(.+)", re.IGNORECASE)
                    for i in range(max_scan):
                        row = df_raw.iloc[i]
                        row_text = " ".join([str(x) for x in row.tolist() if pd.notna(x)]).strip()
                        if not row_text:
                            continue
                        m = pattern.search(row_text)
                        if m:
                            name = m.group(1).strip()
                            name = re.split(r"\s{2,}|,|;|\|", name)[0].strip()
                            if name:
                                return name
                        # explicit 'Station' label then next value on the row
                        for val in row.tolist():
                            if isinstance(val, str) and re.match(r"^\s*station\s*:?$", val.strip(), re.IGNORECASE):
                                # next non-empty value in the row
                                started = False
                                for v in row.tolist():
                                    if not started:
                                        if v is val:
                                            started = True
                                        continue
                                    if pd.notna(v) and str(v).strip():
                                        return str(v).strip()
                    return None
                except Exception:
                    return None

            # Build comprehensive station mapping
            station_mapping = {}
            all_stations = set()
            
            for csv_file in csv_files:
                try:
                    df_raw = _robust_read_csv(csv_file)
                    df = _normalize_headers(df_raw)
                    # Drop unnamed columns that originate from blank headers/indexes
                    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
                    # Drop completely empty rows
                    df = df.dropna(how='all')
                    # Sometimes a leading metadata row may remain with all empty strings
                    if not df.empty:
                        non_empty_mask = ~(df.astype(str).apply(lambda s: s.str.strip()).eq('').all(axis=1))
                        df = df.loc[non_empty_mask]
                    
                    if df.empty:
                        continue
                    
                    # Determine sheet type dynamically from filename and content
                    sheet_type = self._detect_sheet_type(csv_file.name, df)
                    
                    # Skip non-station files
                    if sheet_type is None or 'states' in str(csv_file.name).lower():
                        continue
                    
                    # Extract station information
                    stations_in_file = set()
                    
                    # Check for Stn_Name column first
                    if 'Stn_Name' in df.columns:
                        valid_stations = df['Stn_Name'].notna() & (df['Stn_Name'].astype(str).str.strip() != '') & (df['Stn_Name'].astype(str).str.strip() != 'nan')
                        # Further filter out state names
                        for idx, stn_name in df.loc[valid_stations, 'Stn_Name'].items():
                            stn_name_clean = str(stn_name).strip()
                            if not self._is_state_name(stn_name_clean):
                                stations_in_file.add(stn_name_clean)
                    
                    # If no Stn_Name, try to infer from content
                    if not stations_in_file:
                        inferred = _infer_station_from_df(df_raw, df)
                        if inferred and not self._is_state_name(inferred):
                            stations_in_file.add(inferred)
                    
                    # Add to station mapping
                    for station in stations_in_file:
                        if station not in station_mapping:
                            station_mapping[station] = {
                                'canonical_name': station,
                                'aliases': set(),
                                'data_sources': set(),
                                'total_records': 0,
                                'date_range': {'earliest': None, 'latest': None}
                            }
                        
                        station_mapping[station]['data_sources'].add(sheet_type)
                        station_mapping[station]['total_records'] += len(df)
                        all_stations.add(station)
                        
                        logger.info(f"📊 Mapped station '{station}' in {sheet_type}: {csv_file.name} ({len(df)} rows)")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Could not analyze {csv_file.name}: {e}")
                    continue
            
            # Create canonical mapping and aliases
            canonical_mapping = {}
            for station, info in station_mapping.items():
                # Use the most common name as canonical
                canonical_name = station
                canonical_mapping[station] = canonical_name
                
                # Convert sets to lists for JSON serialization
                info['aliases'] = list(info['aliases'])
                info['data_sources'] = list(info['data_sources'])
            
            # Save comprehensive station mapping
            mapping_file = self.master_data_dir / f"NRLDC_Station_Mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            mapping_data = {
                'metadata': {
                    'total_stations': len(station_mapping),
                    'total_data_sources': len(set().union(*[info['data_sources'] for info in station_mapping.values()])),
                    'created_at': datetime.now().isoformat(),
                    'extractor_version': 'NRLDC_Working_DSA_Extractor_v2.0'
                },
                'station_mapping': station_mapping,
                'canonical_mapping': canonical_mapping,
                'data_source_summary': {
                    source: len([s for s in station_mapping.values() if source in s['data_sources']])
                    for source in set().union(*[info['data_sources'] for info in station_mapping.values()])
                }
            }
            
            with open(mapping_file, 'w') as f:
                json.dump(mapping_data, f, indent=2, default=str)
            
            logger.info(f"✅ Station mapping created: {mapping_file}")
            logger.info(f"📊 Found {len(station_mapping)} unique stations across {len(set().union(*[info['data_sources'] for info in station_mapping.values()]))} data sources")
            
            # Print summary
            for source, count in mapping_data['data_source_summary'].items():
                logger.info(f"   - {source}: {count} stations")
            
            return str(mapping_file)
            
        except Exception as e:
            logger.error(f"❌ Error creating station mapping: {e}")
            return None

            
            # Helper to robustly read messy CSVs
            def _robust_read_csv(path: Path) -> pd.DataFrame:
                try:
                    df = pd.read_csv(path, low_memory=False)
                    if df.shape[1] > 1:
                        return df
                except Exception:
                    pass
                # Try automatic delimiter detection
                try:
                    df = pd.read_csv(path, engine='python', sep=None)
                    if df.shape[1] > 1:
                        return df
                except Exception:
                    pass
                # Try tab
                try:
                    df = pd.read_csv(path, sep='\t', engine='python', low_memory=False)
                    if df.shape[1] > 1:
                        return df
                except Exception:
                    pass
                # Try whitespace
                try:
                    df = pd.read_csv(path, delim_whitespace=True, engine='python', low_memory=False)
                    return df
                except Exception as e:
                    logger.debug(f"⚠️ Robust read failed for {path.name}: {e}")
                    return pd.DataFrame()

            # Attempt to detect header row in frames that include metadata rows
            def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
                if df.empty:
                    return df
                # Drop fully empty rows
                df_clean = df.dropna(how='all')
                # If columns look like 0..N-1 and there exists a row containing typical headers, use it
                candidate = None
                header_keywords = ['time', 'block', 'date']
                for i in range(min(10, len(df_clean))):
                    row_vals = df_clean.iloc[i].astype(str).str.strip().str.lower().tolist()
                    if any(k in row_vals for k in header_keywords):
                        candidate = i
                        break
                if candidate is not None:
                    new_cols = df_clean.iloc[candidate].astype(str).str.strip().tolist()
                    df_norm = df_clean.iloc[candidate+1:].copy()
                    df_norm.columns = new_cols
                    return df_norm
                return df_clean

            def _infer_station_from_df(df_raw: pd.DataFrame, df_norm: pd.DataFrame) -> typing.Optional[str]:
                try:
                    # 1) If Station_Name exists and has non-empty values in df_norm
                    for col in df_norm.columns:
                        if str(col).strip().lower() in ['station_name','station','entity']:
                            vals = df_norm[col].dropna().astype(str).str.strip()
                            if not vals.empty and vals.iloc[0]:
                                return vals.iloc[0]
                    # 2) Scan first rows of raw df for patterns like 'Station : NAME'
                    max_scan = min(15, len(df_raw))
                    pattern = re.compile(r"station\s*:?\s*(.+)", re.IGNORECASE)
                    for i in range(max_scan):
                        row = df_raw.iloc[i]
                        row_text = " ".join([str(x) for x in row.tolist() if pd.notna(x)]).strip()
                        if not row_text:
                            continue
                        m = pattern.search(row_text)
                        if m:
                            name = m.group(1).strip()
                            name = re.split(r"\s{2,}|,|;|\|", name)[0].strip()
                            if name:
                                return name
                        # explicit 'Station' label then next value on the row
                        for val in row.tolist():
                            if isinstance(val, str) and re.match(r"^\s*station\s*:?$", val.strip(), re.IGNORECASE):
                                # next non-empty value in the row
                                started = False
                                for v in row.tolist():
                                    if not started:
                                        if v is val:
                                            started = True
                                        continue
                                    if pd.notna(v) and str(v).strip():
                                        return str(v).strip()
                    return None
                except Exception:
                    return None

            # Process all station-related files and create unified station mapping
            station_data_by_type = {}  # Group by sheet type
            all_stations = set()  # Track all unique stations
            
            for csv_file in csv_files:
                try:
                    df_raw = _robust_read_csv(csv_file)
                    df = _normalize_headers(df_raw)
                    # Drop unnamed columns that originate from blank headers/indexes
                    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
                    # Drop completely empty rows
                    df = df.dropna(how='all')
                    # Sometimes a leading metadata row may remain with all empty strings
                    if not df.empty:
                        non_empty_mask = ~(df.astype(str).apply(lambda s: s.str.strip()).eq('').all(axis=1))
                        df = df.loc[non_empty_mask]
                    
                    if df.empty:
                        logger.info(f"⏭️ Skipping empty after-clean file: {csv_file.name}")
                        continue
                    
                    # Determine sheet type dynamically from filename and content
                    sheet_type = self._detect_sheet_type(csv_file.name, df)
                    
                    # Skip non-station files
                    if sheet_type is None or 'states' in str(csv_file.name).lower():
                        logger.info(f"⏭️ Skipping non-station file: {csv_file.name}")
                        continue
                    
                    # STRICT: Only keep data with valid station names
                    has_valid_station = False
                    
                    # Check for Stn_Name column first
                    if 'Stn_Name' in df.columns:
                        # Filter rows with valid station names
                        valid_stations = df['Stn_Name'].notna() & (df['Stn_Name'].astype(str).str.strip() != '') & (df['Stn_Name'].astype(str).str.strip() != 'nan')
                        # Further filter out state names
                        station_mask = valid_stations.copy()
                        for idx, stn_name in df.loc[valid_stations, 'Stn_Name'].items():
                            if self._is_state_name(str(stn_name).strip()):
                                station_mask.loc[idx] = False
                        
                        if station_mask.any():
                            df = df.loc[station_mask].copy()
                            df['Station_Name'] = df['Stn_Name'].astype(str).str.strip()
                            has_valid_station = True
                            
                            # Track unique stations
                            unique_stations = df['Station_Name'].unique()
                            all_stations.update(unique_stations)
                            
                            logger.info(f"📊 Added {sheet_type} data: {csv_file.name} ({len(df)} rows, {len(unique_stations)} stations)")
                    
                    # If no Stn_Name, try to infer from CSV structure
                    if not has_valid_station:
                        inferred = self._extract_station_from_csv_structure(df_raw, csv_file.name)
                        if inferred and not self._is_state_name(inferred):
                            df['Station_Name'] = inferred
                            has_valid_station = True
                            all_stations.add(inferred)
                            logger.info(f"📊 Added {sheet_type} data from CSV structure: {csv_file.name} ({len(df)} rows)")
                        else:
                            # Fallback to original inference method
                            inferred = _infer_station_from_df(df_raw, df)
                            if inferred and not self._is_state_name(inferred):
                                df['Station_Name'] = inferred
                                has_valid_station = True
                                all_stations.add(inferred)
                                logger.info(f"📊 Added {sheet_type} data from inference: {csv_file.name} ({len(df)} rows)")
                    
                    # Skip files without valid stations
                    if not has_valid_station:
                        logger.info(f"⏭️ Skipping file without valid stations: {csv_file.name}")
                        continue
                    
                    # Add to appropriate sheet type group
                    if sheet_type not in station_data_by_type:
                        station_data_by_type[sheet_type] = []
                    station_data_by_type[sheet_type].append(df)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Could not read {csv_file.name}: {e}")
            
            # Create unified station mapping
            logger.info(f"📊 Found {len(all_stations)} unique stations across all sheets")
            logger.info(f"📊 Station data by type: {list(station_data_by_type.keys())}")
            
            # Create comprehensive master dataset by combining all station data
            if not station_data_by_type:
                logger.error("❌ No station data found to create master dataset")
                return None
            
            # Combine all station data into one unified dataset
            all_station_data = []
            for sheet_type, data_list in station_data_by_type.items():
                if data_list:
                    # Ensure all dataframes have consistent columns before concatenating
                    combined_df = pd.concat(data_list, ignore_index=True, sort=False)
                    combined_df['Data_Source'] = sheet_type
                    all_station_data.append(combined_df)
                    logger.info(f"📊 Combined {sheet_type}: {len(combined_df)} rows")
            
            if not all_station_data:
                logger.error("❌ No combined station data available")
                return None
            
            # Simple concatenation with sort=False to handle different columns
            try:
                master_df = pd.concat(all_station_data, ignore_index=True, sort=False)
                logger.info(f"✅ Successfully concatenated {len(all_station_data)} dataframes")
            except Exception as e:
                logger.error(f"❌ Concatenation failed: {e}")
                # Try alternative approach - concatenate one by one
                master_df = all_station_data[0].copy()
                for i, df in enumerate(all_station_data[1:], 1):
                    try:
                        master_df = pd.concat([master_df, df], ignore_index=True, sort=False)
                        logger.info(f"✅ Concatenated dataframe {i+1}")
                    except Exception as e2:
                        logger.error(f"❌ Failed to concat dataframe {i+1}: {e2}")
                        continue
            # Final cleanup on combined frame
            try:
                master_df = master_df.loc[:, ~master_df.columns.astype(str).str.startswith('Unnamed')]
                master_df = master_df.dropna(how='all')
            except Exception as e:
                logger.error(f"❌ Error in final cleanup: {e}")
                # Try alternative cleanup approach
                master_df = master_df.dropna(how='all')

            # STRICT: require 'Stn_Name' and use only those rows; drop everything else
            if 'Stn_Name' in master_df.columns:
                stn_raw = master_df['Stn_Name']
                stn_series = stn_raw.astype(str).str.strip()
                # Valid if non-empty, not 'nan', and not state-level token
                def _valid_stn(x: typing.Any) -> bool:
                    if pd.isna(x):
                        return False
                    s = str(x).strip()
                    if not s or s.lower() == 'nan':
                        return False
                    return not self._is_state_name(s)
                # Create a simple mask for valid station names
                mask = stn_raw.notna() & (stn_series != '') & (stn_series.str.lower() != 'nan')
                # Apply state name filter
                mask = mask & stn_series.apply(lambda v: not self._is_state_name(v))
                
                # Apply mask to get valid rows
                master_df = master_df.loc[mask].copy()
                master_df['Station_Name'] = stn_series.loc[mask].astype(str).str.strip()
            else:
                # If 'Stn_Name' missing entirely, drop all to avoid incorrect station partitions
                logger.warning("⏭️ 'Stn_Name' column missing in combined data; skipping master dataset build to enforce station-only policy")
                return None

            # Preferred column ordering when present
            preferred_order = [
                'Date','Time','Block','Freq (Hz)','Actual (MWH)','Schedule (MWH)',
                'SRAS (MWH)','Deviation (MWH)','Deviation (%)','DSM Payable (Rs.)',
                'DSM Receivable (Rs.)'
            ]
            ordered_cols = [c for c in preferred_order if c in master_df.columns]
            other_cols = [c for c in master_df.columns if c not in ordered_cols]
            master_df = master_df[ordered_cols + other_cols]

            # Normalize headers: coalesce rate fields and drop banner/constant columns
            def _standardize_col(col: str) -> str:
                c = str(col).strip()
                cl = c.lower().replace('\n',' ').replace('\r',' ')
                cl = re.sub(r"\s+"," ", cl)
                # Banner rows
                if 'northern regional power committee' in cl:
                    return '__DROP__'
                # Frequency (remove units from name)
                if cl in ['freq(hz)','freq (hz)','frequency(hz)','frequency (hz)','freq']:
                    return 'frequency'
                # Deviation (remove units from name)
                if cl in ['deviation(mwh)','deviation (mwh)','ui (mwh)','ui_mwh','deviation(kwh)','deviation (kwh)','ui(kwh)','ui (kwh)']:
                    return 'deviation'
                # Normal DSM Rate
                if ('normal' in cl and 'rate' in cl) or ('hpdap normal' in cl) or ('hpdap normal rate' in cl):
                    return 'Normal DSM Rate (p/KWH)'
                # Reference DSM Rate (covers D6, ref, HPDAM ref)
                if ('ref' in cl and 'rate' in cl) or ('reference' in cl and 'rate' in cl) or ('d6' in cl):
                    return 'Reference DSM Rate (p/KWH)'
                # Weighted Avg Hybrid DSM Rate
                if ('wt' in cl or 'weighted' in cl) and ('avg' in cl or 'average' in cl) and ('hybrid' in cl) and ('rate' in cl):
                    return 'Wt.Avg. DSM Rate (Hybrid Gen) Applicable (p/KWH)'
                # Constituents/Entity
                if cl in ['constituents','entity','entity code']:
                    return 'Entity'
                return c

            master_df = master_df.rename(columns=_standardize_col)
            # Drop banner columns
            if '__DROP__' in master_df.columns:
                master_df = master_df.drop(columns=['__DROP__'])
            
            # Convert KWh to MWh (divide by 1000)
            self._convert_kwh_to_mwh(master_df)

            # Coalesce duplicate rate columns if variants exist
            def _coalesce_cols(df: pd.DataFrame, targets: list[str]) -> pd.Series:
                base = pd.Series(pd.NA, index=df.index, dtype='object')
                for t in targets:
                    if t in df.columns:
                        s = df[t]
                        base = base.where(base.notna() & base.astype(str).str.strip().ne(''), s)
                return base

            # Build unified rate columns
            if any(c in master_df.columns for c in ['Normal DSM Rate (p/KWH)']):
                master_df['Normal DSM Rate (p/KWH)'] = _coalesce_cols(master_df, ['Normal DSM Rate (p/KWH)'])
            if any(c in master_df.columns for c in ['Reference DSM Rate (p/KWH)']):
                master_df['Reference DSM Rate (p/KWH)'] = _coalesce_cols(master_df, ['Reference DSM Rate (p/KWH)'])
            if any(c in master_df.columns for c in ['Wt.Avg. DSM Rate (Hybrid Gen) Applicable (p/KWH)']):
                master_df['Wt.Avg. DSM Rate (Hybrid Gen) Applicable (p/KWH)'] = _coalesce_cols(master_df, ['Wt.Avg. DSM Rate (Hybrid Gen) Applicable (p/KWH)'])

            # Remove legacy rate headers if present
            legacy_cols = [c for c in master_df.columns if re.search(r"(hpd[a]?m|normal|ref|d6).*(rate)", str(c), flags=re.I) and c not in ['Normal DSM Rate (p/KWH)','Reference DSM Rate (p/KWH)','Wt.Avg. DSM Rate (Hybrid Gen) Applicable (p/KWH)']]
            if legacy_cols:
                master_df = master_df.drop(columns=legacy_cols, errors='ignore')
 
            
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
            
            # Create and save station mapping
            station_mapping = {}
            if 'Station_Name' in master_df.columns:
                # Create mapping of stations to their data sources
                for station in sorted(all_stations):
                    station_data = master_df[master_df['Station_Name'] == station]
                    data_sources = station_data['Data_Source'].unique().tolist() if 'Data_Source' in station_data.columns else ['Unknown']
                    station_mapping[station] = {
                        'data_sources': data_sources,
                        'total_records': len(station_data),
                        'date_range': {
                            'earliest': station_data['Date'].min().isoformat() if 'Date' in station_data.columns and not station_data['Date'].isna().all() else None,
                            'latest': station_data['Date'].max().isoformat() if 'Date' in station_data.columns and not station_data['Date'].isna().all() else None
                        }
                    }
                
                # Save station mapping
                mapping_file = self.master_data_dir / f"NRLDC_Station_Mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                import json
                with open(mapping_file, 'w') as f:
                    json.dump(station_mapping, f, indent=2)
                logger.info(f"✅ Station mapping saved: {mapping_file}")
            
            # Save master dataset with unified station data
            master_file = self.master_data_dir / f"NRLDC_Master_Dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            master_df.to_csv(master_file, index=False)
            
            logger.info(f"✅ NRLDC master dataset created (unified station data): {master_file} ({len(master_df)} total rows)")
            logger.info(f"📊 Unique stations found: {len(all_stations)}")
            logger.info(f"📊 Data sources: {list(station_data_by_type.keys())}")
            
            # Export partitioned to S3 (station/year/month) similar to WRPC
            try:
                self._export_partitioned_to_s3(master_df)
            except Exception as e:
                logger.warning(f"⚠️ Partitioned S3 export failed (NRLDC): {e}")
            
            return str(master_file)
            
        except Exception as e:
            logger.error(f"❌ Error creating master dataset: {e}")
            return None

    def create_master_dataset_with_mapping(self) -> bool:
        """Create master dataset using comprehensive station mapping"""
        try:
            # Load comprehensive station mapping (ALL 16 sheets)
            mapping_file = Path("energy_data_extractors/master_data/NRLDC/comprehensive_station_mapping_all_sheets.json")
            if not mapping_file.exists():
                logger.error(f"❌ Comprehensive mapping not found: {mapping_file}")
                return False
            
            with open(mapping_file, 'r') as f:
                station_mapping = json.load(f)
            
            logger.info(f"📊 Loaded comprehensive mapping for {len(station_mapping)} stations")
            
            # Load the XLS file to get actual data - use latest available file
            xls_files = list(self.local_data_dir.glob("Supporting_files_*.xls"))
            if not xls_files:
                logger.error(f"❌ No XLS files found in {self.local_data_dir}")
                return False
            
            # Use the most recent file
            xls_file = max(xls_files, key=lambda f: f.stat().st_mtime)
            logger.info(f"📁 Using XLS file: {xls_file.name}")
            
            # Read all sheets from XLS
            all_sheets = pd.read_excel(xls_file, sheet_name=None, header=None)
            logger.info(f"📋 Found {len(all_sheets)} sheets in XLS file")
            
            # Process each station from the mapping
            consolidated_data = []
            
            for station_name, station_info in station_mapping.items():
                logger.info(f"🔄 Processing station: {station_name} ({station_info['total_records']} records across {station_info['total_sheets']} sheets)")
                
                station_dataframes = []
                
                # Process each sheet for this station
                for sheet_name, sheet_info in station_info['sheets'].items():
                    if sheet_name in all_sheets:
                        try:
                            # Get the sheet data
                            df_raw = all_sheets[sheet_name]
                            
                            # Find header row
                            header_row = None
                            for i in range(min(5, len(df_raw))):
                                row_vals = [str(x).strip() for x in df_raw.iloc[i].values if pd.notna(x)]
                                if 'Stn_Name' in row_vals:
                                    header_row = i
                                    break
                            
                            if header_row is not None:
                                # Set headers and get data
                                df_clean = df_raw.copy()
                                df_clean.columns = df_clean.iloc[header_row].astype(str).str.strip()
                                df_clean = df_clean.iloc[header_row + 1:].reset_index(drop=True)
                                
                                if 'Stn_Name' in df_clean.columns:
                                    # Filter for this specific station
                                    station_df = df_clean[df_clean['Stn_Name'] == station_name].copy()
                                    
                                    if len(station_df) > 0:
                                        # Add metadata
                                        station_df['Station_Name'] = station_name
                                        station_df['Data_Source'] = sheet_name
                                        station_df['Sheet_Type'] = sheet_name
                                        
                                        station_dataframes.append(station_df)
                                        logger.info(f"   📊 {sheet_name}: {len(station_df)} records")
                        
                        except Exception as e:
                            logger.warning(f"⚠️ Error processing {sheet_name} for {station_name}: {e}")
                            continue
                
                # Consolidate all data for this station
                if station_dataframes:
                    try:
                        # Concatenate all sheet data for this station
                        station_consolidated = pd.concat(station_dataframes, ignore_index=True, sort=False)
                        
                        # Add station-level metadata
                        station_consolidated['Total_Sheets'] = len(station_dataframes)
                        station_consolidated['Total_Records'] = len(station_consolidated)
                        
                        consolidated_data.append(station_consolidated)
                        logger.info(f"   ✅ Consolidated {station_name}: {len(station_consolidated)} total records")
                        
                    except Exception as e:
                        logger.error(f"❌ Error consolidating {station_name}: {e}")
                        continue
            
            if not consolidated_data:
                logger.error("❌ No consolidated data created")
                return False
            
            # Create final master dataset
            try:
                master_df = pd.concat(consolidated_data, ignore_index=True, sort=False)
                logger.info(f"✅ Successfully created master dataset with {len(consolidated_data)} stations")
            except Exception as e:
                logger.error(f"❌ Final concatenation failed: {e}")
                return False
            
            # Save master dataset
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            master_file = self.master_data_dir / f"NRLDC_Master_Dataset_Mapped_{timestamp}.csv"
            master_df.to_csv(master_file, index=False)
            
            logger.info(f"✅ Master dataset created: {master_file}")
            logger.info(f"📊 Total records: {len(master_df)}")
            logger.info(f"📊 Total stations: {len(consolidated_data)}")
            logger.info(f"📊 Columns: {list(master_df.columns)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating master dataset with mapping: {e}")
            import traceback
            traceback.print_exc()
            return False

    def create_station_files_and_upload_to_s3(self) -> bool:
        """Create individual files for each station and upload to S3 in station/year/month pattern"""
        try:
            # Load comprehensive station mapping (ALL 16 sheets)
            mapping_file = Path("energy_data_extractors/master_data/NRLDC/comprehensive_station_mapping_all_sheets.json")
            if not mapping_file.exists():
                logger.error(f"❌ Comprehensive mapping not found: {mapping_file}")
                return False
            
            with open(mapping_file, 'r') as f:
                station_mapping = json.load(f)
            
            # Load the XLS file to get actual data - use latest available file
            xls_files = list(self.local_data_dir.glob("Supporting_files_*.xls"))
            if not xls_files:
                logger.error(f"❌ No XLS files found in {self.local_data_dir}")
                return False
            
            # Use the most recent file
            xls_file = max(xls_files, key=lambda f: f.stat().st_mtime)
            logger.info(f"📁 Using XLS file: {xls_file.name}")
            
            # Read all sheets from XLS
            all_sheets = pd.read_excel(xls_file, sheet_name=None, header=None)
            logger.info(f"📋 Loaded {len(all_sheets)} sheets from XLS file")
            
            # Process each station and create individual files
            successful_uploads = 0
            failed_uploads = 0
            skipped_duplicates = 0
            
            for station_name, station_info in station_mapping.items():
                logger.info(f"🔄 Processing station: {station_name} ({station_info['total_records']} records across {station_info['total_sheets']} sheets)")
                
                station_dataframes = []
                
                # Process each sheet for this station
                for sheet_name, sheet_info in station_info['sheets'].items():
                    if sheet_name in all_sheets:
                        try:
                            # Get the sheet data
                            df_raw = all_sheets[sheet_name]
                            
                            # Find header row
                            header_row = None
                            for i in range(min(5, len(df_raw))):
                                row_vals = [str(x).strip() for x in df_raw.iloc[i].values if pd.notna(x)]
                                if any(keyword in row_vals for keyword in ['Stn_Name', 'Station_Name', 'Entity_Name']):
                                    header_row = i
                                    break
                            
                            if header_row is not None:
                                # Set headers and get data
                                df_clean = df_raw.copy()
                                df_clean.columns = df_clean.iloc[header_row].astype(str).str.strip()
                                df_clean = df_clean.iloc[header_row + 1:].reset_index(drop=True)
                                
                                # Find station name column
                                station_col = None
                                for col in df_clean.columns:
                                    if any(keyword in str(col).lower() for keyword in ['stn_name', 'station_name', 'entity_name']):
                                        station_col = col
                                        break
                                
                                if station_col:
                                    # Get data for this station
                                    station_df = df_clean[df_clean[station_col] == station_name].copy()
                                    
                                    if len(station_df) > 0:
                                        # Add metadata
                                        station_df['Station_Name'] = station_name
                                        station_df['Data_Source'] = sheet_name
                                        station_df['Sheet_Type'] = sheet_name
                                        
                                        # Add date column if not present
                                        if 'Date' not in station_df.columns:
                                            # Try to find date column
                                            date_col = None
                                            for col in station_df.columns:
                                                if 'date' in str(col).lower():
                                                    date_col = col
                                                    break
                                            if date_col:
                                                station_df['Date'] = station_df[date_col]
                                            else:
                                                # Do not hardcode a default date; leave as missing to be parsed later
                                                station_df['Date'] = pd.NaT
                                        
                                        station_dataframes.append(station_df)
                                        logger.info(f"   📊 {sheet_name}: {len(station_df)} records")
                        except Exception as e:
                            logger.warning(f"   ⚠️ Error processing {sheet_name} for {station_name}: {e}")
                            continue
                
                # Consolidate all data for this station with clean grouping
                if station_dataframes:
                    try:
                        # Group data by sheet type for cleaner organization
                        grouped_dataframes = {}
                        for df in station_dataframes:
                            sheet_type = df['Sheet_Type'].iloc[0] if 'Sheet_Type' in df.columns else 'Unknown'
                            if sheet_type not in grouped_dataframes:
                                grouped_dataframes[sheet_type] = []
                            grouped_dataframes[sheet_type].append(df)
                        
                        # Concatenate each sheet type separately for clean grouping
                        clean_dataframes = []
                        for sheet_type, dfs in grouped_dataframes.items():
                            if dfs:
                                sheet_consolidated = pd.concat(dfs, ignore_index=True, sort=False)
                                sheet_consolidated['Sheet_Type'] = sheet_type
                                sheet_consolidated['Total_Sheets'] = len(dfs)
                                sheet_consolidated['Total_Records'] = len(sheet_consolidated)
                                clean_dataframes.append(sheet_consolidated)
                        
                        # Final consolidation with clean grouping
                        station_consolidated = pd.concat(clean_dataframes, ignore_index=True, sort=False)
                        
                        # Parse dates and extract year/month
                        station_consolidated['__date__'] = pd.to_datetime(station_consolidated['Date'], dayfirst=True, errors='coerce')
                        
                        # Get date range for this station
                        date_range = station_consolidated['__date__'].agg(['min', 'max'])
                        year_range = f"{date_range['min'].year}-{date_range['max'].year}"
                        month_range = f"{date_range['min'].month:02d}-{date_range['max'].month:02d}"
                        
                        # Create safe station name for S3
                        safe_station = str(station_name).strip().replace(' ', '_').replace('/', '_').replace('\\', '_')
                        
                        # Create dynamic filename based on station and date range
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        csv_name = f"NRLDC_{safe_station}_{year_range}_{month_range}_{timestamp}.csv"
                        pq_name = f"NRLDC_{safe_station}_{year_range}_{month_range}_{timestamp}.parquet"
                        
                        # Clean up the dataframe (remove internal columns)
                        clean_df = station_consolidated.drop(columns=[c for c in ['__date__'] if c in station_consolidated.columns]).copy()
                        
                        # Create temporary files
                        import tempfile
                        tmp_csv = Path(tempfile.mktemp(suffix='.csv'))
                        tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
                        
                        # Save CSV
                        clean_df.to_csv(tmp_csv, index=False)
                        
                        # Save Parquet (with sanitization)
                        pq_df = self._sanitize_for_parquet(clean_df)
                        pq_df.to_parquet(tmp_pq, index=False)
                        
                        # Upload with region-first partitions and weekly bucket for raw
                        year = date_range['min'].year
                        month = date_range['min'].month
                        from datetime import datetime as _dt
                        _week = _dt.now().isocalendar().week
                        # Raw: dsm_data/raw/{REGION}/{YEAR}/{MONTH}/{FILENAME}
                        csv_s3_key = f"dsm_data/raw/NRLDC/{year}/{month:02d}/{csv_name}"
                        # Parquet: dsm_data/parquet/{REGION}/{STATION}/{YEAR}/{MONTH}/{FILENAME}
                        pq_s3_key = f"dsm_data/parquet/NRLDC/{safe_station}/{year}/{month:02d}/{pq_name}"
                        
                        # Check if files already exist in S3 to avoid duplicates
                        csv_exists = self._check_s3_file_exists(csv_s3_key)
                        pq_exists = self._check_s3_file_exists(pq_s3_key)
                        
                        # Skip CSV upload to raw directory - only original files should be in raw
                        logger.info(f"⏭️ Skipping CSV upload to raw directory (only original files allowed)")
                        
                        # Upload Parquet (only if it doesn't exist)
                        if not pq_exists:
                            try:
                                self.s3_uploader.auto_upload_file(str(tmp_pq), original_filename=pq_s3_key)
                                logger.info(f"📤 Uploaded Parquet to s3://{pq_s3_key} ({len(clean_df)} rows)")
                            except Exception as e:
                                logger.warning(f"⚠️ Parquet upload failed (NRLDC {safe_station}): {e}")
                        else:
                            logger.info(f"⏭️ Parquet already exists, skipping: s3://{pq_s3_key}")
                            skipped_duplicates += 1
                        
                        # Clean up temporary files
                        if tmp_csv.exists():
                            tmp_csv.unlink()
                        if tmp_pq.exists():
                            tmp_pq.unlink()
                        
                        logger.info(f"   ✅ Processed {station_name}: {len(clean_df)} records")
                        
                    except Exception as e:
                        logger.error(f"   ❌ Failed to process {station_name}: {e}")
                        failed_uploads += 1
                        continue
            
            logger.info(f"📊 Upload Summary:")
            logger.info(f"   ✅ Successful uploads: {successful_uploads}")
            logger.info(f"   ❌ Failed uploads: {failed_uploads}")
            logger.info(f"   ⏭️ Skipped duplicates: {skipped_duplicates}")
            logger.info(f"   📁 S3 Pattern: dsm_data/{{raw|parquet}}/NRLDC/{{station_name}}/{{year}}/{{month}}/{{filename}}")
            
            return successful_uploads > 0
                
        except Exception as e:
            logger.error(f"❌ Error creating station files and uploading to S3: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _sanitize_for_parquet(self, df_in: pd.DataFrame) -> pd.DataFrame:
        """Coerce mostly-numeric columns to float; cast others to string to avoid mixed-type parquet errors."""
        try:
            df_out = df_in.copy()
            for col in df_out.columns:
                s = df_out[col]
                if pd.api.types.is_datetime64_any_dtype(s):
                    continue
                s_num = pd.to_numeric(s, errors='coerce')
                if s_num.notna().sum() >= max(1, int(0.6 * len(s))):
                    df_out[col] = s_num
                else:
                    df_out[col] = s.astype(str)
            return df_out
        except Exception as e:
            logger.warning(f"⚠️ Parquet sanitization failed: {e}")
            return df_in.astype(str)

    def _convert_kwh_to_mwh(self, df: pd.DataFrame) -> None:
        """Convert KWh data to MWh by dividing by 1000"""
        try:
            # Energy columns that might be in KWh
            energy_columns = ['deviation', 'schedule', 'actual']
            
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

    def _check_s3_file_exists(self, s3_key: str) -> bool:
        """Check if a file already exists in S3 to avoid duplicates"""
        try:
            if self.s3_uploader is None or not hasattr(self.s3_uploader, 's3_client'):
                return False
            
            # Use the S3 client from the uploader to check if file exists
            self.s3_uploader.s3_client.head_object(Bucket=self.s3_uploader.bucket_name, Key=s3_key)
            return True
        except Exception:
            # File doesn't exist or other error occurred
            return False

    def _export_partitioned_to_s3(self, master_df: pd.DataFrame) -> None:
        """Export CSV and Parquet per station/year/month to S3 under dsm_data/raw and dsm_data/parquet for NRLDC."""
        try:
            if self.s3_uploader is None or not hasattr(self.s3_uploader, 'auto_upload_file'):
                logger.info("⏭️ S3 uploader not configured; skipping S3 export (NRLDC)")
                return
            if master_df.empty:
                return
            # Determine station column
            station_col = None
            for c in ['Station_Name','Stn_Name','Station','Entity']:
                if c in master_df.columns:
                    station_col = c
                    break
            if station_col is None:
                station_col = 'NRLDC'
                master_df = master_df.copy()
                master_df[station_col] = 'NRLDC'
            # Parse Date column if exists
            if 'Date' in master_df.columns:
                # Use day-first to avoid DD/MM vs MM/DD confusion
                date_series = pd.to_datetime(master_df['Date'], errors='coerce', dayfirst=True)
            elif 'DATE' in master_df.columns:
                date_series = pd.to_datetime(master_df['DATE'], errors='coerce', dayfirst=True)
            else:
                date_series = pd.to_datetime(datetime.now())
            df = master_df.copy()
            df['__date__'] = date_series
            # Add year/month partitions similar to ERLDC/WRPC
            try:
                # Prefer explicit Year/Month columns if present and valid
                if 'Year' in df.columns and 'Month' in df.columns:
                    y = pd.to_numeric(df['Year'], errors='coerce')
                    m = pd.to_numeric(df['Month'], errors='coerce')
                    df['__year__'] = y.fillna(df['__date__'].dt.year).fillna(datetime.now().year).astype(int)
                    df['__month__'] = m.fillna(df['__date__'].dt.month).fillna(datetime.now().month).astype(int)
                else:
                    df['__year__'] = df['__date__'].dt.year.fillna(datetime.now().year).astype(int)
                    df['__month__'] = df['__date__'].dt.month.fillna(datetime.now().month).astype(int)
            except Exception:
                df['__year__'] = datetime.now().year
                df['__month__'] = datetime.now().month
            base_raw = 'dsm_data/raw'
            base_parquet = 'dsm_data/parquet'

            def _sanitize_for_parquet(df_in: pd.DataFrame) -> pd.DataFrame:
                """Coerce mostly-numeric columns to float; cast others to string to avoid mixed-type parquet errors."""
                try:
                    df_out = df_in.copy()
                    for col in df_out.columns:
                        s = df_out[col]
                        if pd.api.types.is_datetime64_any_dtype(s):
                            continue
                        s_num = pd.to_numeric(s, errors='coerce')
                        if s_num.notna().sum() >= max(1, int(0.6 * len(s))):
                            df_out[col] = s_num
                        else:
                            df_out[col] = s.astype(str)
                    return df_out
                except Exception:
                    return df_in.astype(str)

            # Canonicalize and alias station names
            def _canonicalize(name: str) -> str:
                if name is None:
                    return ''
                s = str(name).strip().upper()
                s = s.replace('&', ' AND ')
                s = re.sub(r'[^A-Z0-9]+', '_', s)
                s = re.sub(r'_+', '_', s).strip('_')
                return s

            alias_map = {}
            try:
                mapping_path = Path('energy_data_extractors/master_data/NRLDC/station_mapping.json')
                if mapping_path.exists():
                    with open(mapping_path, 'r') as f:
                        raw_map = json.load(f)
                    for k, v in raw_map.items():
                        alias_map[_canonicalize(k)] = _canonicalize(v)
            except Exception as e:
                logger.warning(f"⚠️ Could not load station_mapping.json: {e}")

            def _apply_alias(name: str) -> str:
                base = _canonicalize(name)
                return alias_map.get(base, base)

            df['__station_canonical__'] = df[station_col].map(_apply_alias)

            # Group by station only (consolidate all data for each station)
            for station, station_df in df.groupby('__station_canonical__'):
                safe_station = str(station).strip()
                
                # Get date range for this station
                if '__date__' in station_df.columns:
                    date_range = station_df['__date__'].agg(['min', 'max'])
                    year_range = f"{date_range['min'].year}-{date_range['max'].year}"
                    month_range = f"{date_range['min'].month:02d}-{date_range['max'].month:02d}"
                    # Extract year and month for S3 path (use min date for consistency)
                    year = date_range['min'].year
                    month = date_range['min'].month
                else:
                    year_range = "unknown"
                    month_range = "unknown"
                    # Use current date as fallback
                    now = datetime.now()
                    year = now.year
                    month = now.month
                
                # Create consolidated filename for this station
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                csv_name = f"NRLDC_{safe_station}_{year_range}_{month_range}_{ts}.csv"
                pq_name = f"NRLDC_{safe_station}_{year_range}_{month_range}_{ts}.parquet"
                
                # Clean up the dataframe (remove internal columns)
                part_df = station_df.drop(columns=[c for c in ['__date__','__year__','__month__'] if c in station_df.columns]).copy()
                
                import tempfile
                tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
                    
                try:
                    pq_df = _sanitize_for_parquet(part_df)
                    pq_df.to_parquet(tmp_pq, index=False)
                    s3_key_p = f"{base_parquet}/NRLDC/{safe_station}/{year}/{month:02d}/{pq_name}"
                    self.s3_uploader.auto_upload_file(str(tmp_pq), original_filename=s3_key_p)
                    logger.info(f"📤 Uploaded Parquet to s3://{s3_key_p} ({len(part_df)} rows)")
                except Exception as e:
                    logger.warning(f"⚠️ Parquet upload failed (NRLDC {safe_station}): {e}")
            
            logger.info(f"📊 Consolidated {len(df['__station_canonical__'].unique())} stations into individual files")
        except Exception as e:
            logger.warning(f"⚠️ Partitioned export encountered an error (NRLDC): {e}")

    def _detect_available_years(self) -> list:
        """Dynamically detect available years from the DSA page"""
        try:
            response = self.session.get(self.dsa_page_url, timeout=20)
            if response.status_code != 200:
                return ['2021-22']  # Fallback to known year
            
            soup = BeautifulSoup(response.text, 'html.parser')
            years = set()
            
            # Look for year patterns in links and text
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                # Look for patterns like /comm/2021-22/, /comm/2022-23/, etc.
                year_match = re.search(r'/comm/(\d{4}-\d{2})/', href)
                if year_match:
                    years.add(year_match.group(1))
            
            # Also check text content for year references
            text = response.text
            year_matches = re.findall(r'(\d{4}-\d{2})', text)
            years.update(year_matches)
            
            # Sort years (newest first) and return as list
            sorted_years = sorted(list(years), reverse=True)
            logger.info(f"📅 Detected available years: {sorted_years}")
            return sorted_years if sorted_years else ['2021-22']
            
        except Exception as e:
            logger.warning(f"⚠️ Could not detect years, using fallback: {e}")
            return ['2021-22']

    def export_parquet_from_local_csvs_simple(self):
        """Simple parquet export without master mapping: per station/year/month from local CSVs."""
        try:
            if self.s3_uploader is None or not hasattr(self.s3_uploader, 'auto_upload_file'):
                logger.info("⏭️ S3 uploader not configured; skipping simple parquet export (NRLDC)")
                return False
            # No local storage - use empty list
            csv_files = []
            if not csv_files:
                logger.warning("⚠️ No CSVs found for simple parquet export (NRLDC)")
                return False
            uploaded = 0
            base_parquet = 'dsm_data/parquet'

            def _infer_station_from_filename(name: str) -> str:
                base = os.path.splitext(os.path.basename(name))[0]
                base = re.sub(r'^NRLDC_', '', base, flags=re.I)
                # strip trailing date/time tokens
                base = re.sub(r'_20\d{2}(_\d{2}){1,3}.*$', '', base)
                base = re.sub(r'_\d{8}_\d{6}$', '', base)
                base = re.sub(r'[^A-Za-z0-9]+', '_', base).strip('_')
                return base or 'UNKNOWN'

            for p in csv_files:
                try:
                    df = pd.read_csv(p)
                    if df.empty:
                        continue
                    # Find station
                    station_col = None
                    for c in ['Station_Name','Stn_Name','Station','Entity']:
                        if c in df.columns:
                            station_col = c
                            break
                    if station_col is not None:
                        station = str(df[station_col].dropna().astype(str).str.strip().mode().iloc[0]) if not df[station_col].dropna().empty else _infer_station_from_filename(p.name)
                    else:
                        station = _infer_station_from_filename(p.name)
                    safe_station = re.sub(r'[^A-Za-z0-9]+', '_', str(station).strip()).strip('_') or 'UNKNOWN'

                    # Find date
                    date_series = None
                    for dc in ['Date','DATE','date','Date_Time','Timestamp','TIME']:
                        if dc in df.columns:
                            date_series = pd.to_datetime(df[dc], errors='coerce', dayfirst=True)
                            break
                    if date_series is None:
                        date_series = pd.to_datetime(datetime.now())
                    df['__date__'] = date_series
                    year = int(pd.to_datetime(df['__date__'], errors='coerce').dt.year.mode().dropna().iloc[0]) if df['__date__'].notna().any() else datetime.now().year
                    month = int(pd.to_datetime(df['__date__'], errors='coerce').dt.month.mode().dropna().iloc[0]) if df['__date__'].notna().any() else datetime.now().month

                    # Clean internal cols and types
                    out_df = df.drop(columns=[c for c in ['__date__'] if c in df.columns]).copy()
                    try:
                        import tempfile
                        tmp_parquet = Path(tempfile.mktemp(suffix='.parquet'))
                        out_df.to_parquet(tmp_parquet, index=False)
                    except Exception:
                        # sanitize types
                        for col in out_df.columns:
                            s = out_df[col]
                            s_num = pd.to_numeric(s, errors='coerce')
                            if s_num.notna().sum() >= max(1, int(0.6*len(s))):
                                out_df[col] = s_num
                            else:
                                out_df[col] = s.astype(str)

                    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                    pq_name = f"NRLDC_{safe_station}_{year}_{month:02d}_{ts}.parquet"
                    import tempfile
                    tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
                    out_df.to_parquet(tmp_pq, index=False)
                    s3_key_p = f"{base_parquet}/NRLDC/{safe_station}/{year}/{month:02d}/{pq_name}"
                    self.s3_uploader.auto_upload_file(str(tmp_pq), original_filename=s3_key_p)
                    logger.info(f"📤 Uploaded Parquet to s3://{s3_key_p}")
                    uploaded += 1
                except Exception as e:
                    logger.warning(f"⚠️ Simple parquet export failed for {p.name}: {e}")
                    continue
            logger.info(f"✅ Simple parquet export complete: {uploaded} files uploaded")
            return uploaded > 0
        except Exception as e:
            logger.error(f"❌ Simple parquet export error (NRLDC): {e}")
            return False

    def export_single_parquet_per_station(self):
        """Aggregate all local CSVs and upload one Parquet per station containing all its data."""
        try:
            if self.s3_uploader is None or not hasattr(self.s3_uploader, 'auto_upload_file'):
                logger.info("⏭️ S3 uploader not configured; skipping single-file export (NRLDC)")
                return False
            # No local storage - use empty list
            csv_files = []
            if not csv_files:
                logger.warning("⚠️ No CSVs found for single-file export (NRLDC)")
                return False

            def _infer_station_from_filename(name: str) -> str:
                base = os.path.splitext(os.path.basename(name))[0]
                base = re.sub(r'^NRLDC_', '', base, flags=re.I)
                base = re.sub(r'_20\d{2}(_\d{2}){1,3}.*$', '', base)
                base = re.sub(r'[^A-Za-z0-9]+', '_', base).strip('_')
                return base or 'UNKNOWN'

            station_to_frames = {}
            for p in csv_files:
                try:
                    df = pd.read_csv(p)
                    if df.empty:
                        continue
                    station_col = None
                    for c in ['Station_Name','Stn_Name','Station','Entity']:
                        if c in df.columns:
                            station_col = c
                            break
                    if station_col is not None:
                        if df[station_col].dropna().empty:
                            station = _infer_station_from_filename(p.name)
                        else:
                            station = str(df[station_col].dropna().astype(str).str.strip().mode().iloc[0])
                    else:
                        station = _infer_station_from_filename(p.name)
                    safe_station = re.sub(r'[^A-Za-z0-9]+', '_', str(station).strip()).strip('_') or 'UNKNOWN'
                    station_to_frames.setdefault(safe_station, []).append(df)
                except Exception as e:
                    logger.warning(f"⚠️ Skipping {p.name} in single-file export: {e}")
                    continue

            uploaded = 0
            base_parquet = 'dsm_data/parquet'
            for station, frames in station_to_frames.items():
                try:
                    combined = pd.concat(frames, ignore_index=True)
                    # Light type sanitization for Parquet
                    try:
                        import tempfile
                        tmp_single_parquet = Path(tempfile.mktemp(suffix='.parquet'))
                        combined.to_parquet(tmp_single_parquet, index=False)
                    except Exception:
                        for col in combined.columns:
                            s = combined[col]
                            s_num = pd.to_numeric(s, errors='coerce')
                            if s_num.notna().sum() >= max(1, int(0.6*len(s))):
                                combined[col] = s_num
                            else:
                                combined[col] = s.astype(str)
                    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                    # determine year/month from data if possible
                    year = datetime.now().year
                    month = f"{datetime.now().month:02d}"
                    for cand in ['Date', 'date', 'DATE', 'Date_Time', 'datetime']:
                        if cand in combined.columns:
                            try:
                                ds = pd.to_datetime(combined[cand], errors='coerce')
                                valid = ds.dropna()
                                if not valid.empty:
                                    year = int(valid.iloc[0].year)
                                    month = f"{int(valid.iloc[0].month):02d}"
                                    break
                            except Exception:
                                pass
                    pq_name = f"NRLDC_{station}_ALL_{ts}.parquet"
                    import tempfile
                    tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
                    combined.to_parquet(tmp_pq, index=False)
                    s3_key_p = f"{base_parquet}/NRLDC/{station}/{year}/{month}/{pq_name}"
                    self.s3_uploader.auto_upload_file(str(tmp_pq), original_filename=s3_key_p)
                    logger.info(f"📤 Uploaded single-station Parquet to s3://{s3_key_p}")
                    uploaded += 1
                except Exception as e:
                    logger.warning(f"⚠️ Single-file export failed for station {station}: {e}")
            logger.info(f"✅ Single-file export complete: {uploaded} stations uploaded")
            return uploaded > 0
        except Exception as e:
            logger.error(f"❌ Single-file export error (NRLDC): {e}")
            return False

    def generate_supporting_urls(self):
        """Generate Supporting_files.xls URLs for past 7 days with dynamic year detection and flexible filename discovery"""
        urls = []
        weeks = self.get_past_7_days_weeks()
        available_years = self._detect_available_years()
        
        for w in weeks:
            start = datetime.strptime(w['start_date'], '%Y-%m-%d').strftime('%d%m%y')
            end = datetime.strptime(w['end_date'], '%Y-%m-%d').strftime('%d%m%y')
            week_num = w['week_num']
            
            # Try each available year
            for year in available_years:
                # Generate URLs with flexible filename patterns
                flexible_urls = self._generate_flexible_filename_urls(year, start, end, week_num)
                urls.extend(flexible_urls)
        
        return urls

    def _generate_flexible_filename_urls(self, year, start, end, week_num):
        """Generate URLs by dynamically discovering filename patterns from the DSA page"""
        urls = []
        base_path = f"/comm/{year}/dsa/{start}-{end}(WK-{week_num})"
        week_key = f"{start}-{end}_WK{week_num}"
        
        # Try to learn working patterns first (most efficient)
        discovered_patterns = self._learn_filename_patterns()
        
        # If no patterns learned, try to discover from DSA page
        if not discovered_patterns:
            discovered_patterns = self._discover_filename_patterns(year, start, end, week_num)
        
        # If still no patterns, use intelligent pattern generation as last resort
        if not discovered_patterns:
            discovered_patterns = self._generate_intelligent_patterns(year, start, end, week_num)
        
        # Generate URLs for discovered patterns
        for pattern in discovered_patterns:
            urls.append({
                'url': f"{self.base_url}{base_path}/{pattern}",
                'filename': pattern,
                'week_key': week_key,
                'year': year,
                'base_filename': pattern.split('_')[0] if '_' in pattern else pattern.split('.')[0]
            })
        
        logger.info(f"🔍 Generated {len(urls)} dynamic filename URLs for {week_key}")
        return urls

    def _discover_filename_patterns(self, year, start, end, week_num):
        """Dynamically discover filename patterns from the DSA page content"""
        try:
            # Try to find filename patterns in the DSA page content
            resp = self.session.get(self.dsa_page_url, timeout=10)
            if resp.status_code != 200:
                return []
            
            text = resp.text.lower()
            patterns = []
            
            # Look for any .xls file references in the page
            xls_matches = re.findall(r'([a-zA-Z_\-\(\)]+\.xls)', text)
            
            # Filter for likely data file patterns
            for match in xls_matches:
                if any(keyword in match for keyword in ['supporting', 'data', 'files', 'weekly', 'dsa', 'station']):
                    patterns.append(match)
            
            # Remove duplicates and return unique patterns
            unique_patterns = list(set(patterns))
            logger.debug(f"🔍 Discovered {len(unique_patterns)} filename patterns from DSA page")
            return unique_patterns[:10]  # Limit to 10 patterns
            
        except Exception as e:
            logger.debug(f"🔍 Filename discovery failed: {e}")
            return []

    def _generate_intelligent_patterns(self, year, start, end, week_num):
        """Generate intelligent filename patterns based on common data file naming conventions"""
        patterns = []
        
        # Common base names that data files might use
        base_names = ['Supporting_files', 'Data_files', 'Weekly_data', 'DSA_files', 'Station_data', 'Files', 'Data']
        
        # Common revision/version patterns
        revision_patterns = [
            '',           # No revision
            '_r1', '_r2', '_r3', '_R1', '_R2', '_R3',  # Simple revision
            '_r_1', '_r_2', '_R_1', '_R_2',            # Underscore revision
            '_rev1', '_rev2', '_REV1', '_REV2',         # Revision with rev
            '_revision1', '_revision2',                 # Full revision
            '_v1', '_v2', '_V1', '_V2',                 # Version
            '_version1', '_version2',                    # Full version
            '(r1)', '(r2)', '(R1)', '(R2)',             # Parentheses revision
            '(rev1)', '(rev2)', '(REV1)', '(REV2)',     # Parentheses with rev
        ]
        
        # Generate combinations
        for base in base_names:
            for revision in revision_patterns:
                pattern = f"{base}{revision}.xls"
                patterns.append(pattern)
                # Also add lowercase and uppercase variants
                patterns.append(pattern.lower())
                patterns.append(pattern.upper())
        
        # Remove duplicates and limit
        unique_patterns = list(set(patterns))
        logger.debug(f"🔍 Generated {len(unique_patterns)} intelligent patterns")
        return unique_patterns[:20]  # Limit to 20 patterns

    def _learn_filename_patterns(self):
        """Learn filename patterns by testing common variations and remembering what works"""
        try:
            # Get a sample week to test patterns
            weeks = self.get_past_7_days_weeks()
            if not weeks:
                return []
            
            sample_week = weeks[0]
            start = datetime.strptime(sample_week['start_date'], '%Y-%m-%d').strftime('%d%m%y')
            end = datetime.strptime(sample_week['end_date'], '%Y-%m-%d').strftime('%d%m%y')
            week_num = sample_week['week_num']
            
            # Try different years to find working patterns
            available_years = self._detect_available_years()
            
            working_patterns = []
            for year in available_years[:2]:  # Test first 2 years
                base_path = f"/comm/{year}/dsa/{start}-{end}(WK-{week_num})"
                
                # Test a minimal set of the most likely patterns
                test_patterns = [
                    'Supporting_files.xls',
                    'supporting_files.xls',
                    'Data_files.xls',
                    'Weekly_data.xls',
                    'Files.xls',
                    'Data.xls'
                ]
                
                for pattern in test_patterns:
                    test_url = f"{self.base_url}{base_path}/{pattern}"
                    try:
                        response = self.session.head(test_url, timeout=3)
                        if response.status_code == 200:
                            working_patterns.append(pattern)
                            logger.info(f"✅ Learned working pattern: {pattern}")
                            break  # Found a working pattern for this year
                    except:
                        continue
                
                if working_patterns:
                    break  # Found working patterns, no need to test more years
            
            logger.info(f"🎓 Learned {len(working_patterns)} working filename patterns")
            return working_patterns
            
        except Exception as e:
            logger.debug(f"🎓 Pattern learning failed: {e}")
            return []

    def parse_weeks_from_dsa_page(self):
        """Parse week tokens like 110825-170825(WK-20) from the DSA page and construct URLs with flexible filename discovery."""
        try:
            resp = self.session.get(self.dsa_page_url, timeout=20)
            if resp.status_code != 200:
                logger.warning(f"⚠️ Could not load DSA page: {resp.status_code}")
                return []
            text = resp.text
            # Find tokens like 110825-170825(WK-20)
            matches = re.findall(r"(\d{6})-(\d{6})\(WK-?(\d{1,2})\)", text, flags=re.I)
            items = []
            
            # Get available years for flexible URL generation
            available_years = self._detect_available_years()
            
            for start, end, wk in matches:
                # Generate flexible URLs for each year
                for year in available_years[:3]:  # Limit to first 3 years to avoid too many URLs
                    flexible_urls = self._generate_flexible_filename_urls(year, start, end, wk)
                    items.extend(flexible_urls)
            
            logger.info(f"📅 Parsed {len(matches)} week tokens and generated {len(items)} flexible URLs from DSA page")
            return items
        except Exception as e:
            logger.error(f"❌ Failed to parse DSA page weeks: {e}")
            return []

    def _find_working_filename(self, urls):
        """Find the first working filename from a list of URLs by testing them with HEAD requests"""
        import requests
        
        for url_info in urls:
            try:
                response = self.session.head(url_info['url'], timeout=5)
                if response.status_code == 200:
                    logger.info(f"✅ Found working filename: {url_info['filename']}")
                    return url_info
            except Exception as e:
                logger.debug(f"❌ URL failed: {url_info['filename']} - {e}")
                continue
        
        logger.warning(f"⚠️ No working filename found from {len(urls)} patterns")
        return None

    def find_file_links_by_position(self, soup: BeautifulSoup, target_position: str = "supp_files") -> List[Dict[str, str]]:
        """
        Find file links by their structural position in the HTML table
        
        Args:
            soup: BeautifulSoup object of the page
            target_position: Target identifier - can be:
                - "supp_files": Links with name/id 'supp_files'
                - "column_index": Links in a specific column index
                - "anchor_class": Links with specific CSS class
                - "last_column": Links in the last column of each row
        """
        try:
            logger.info(f"🔍 Searching for file links by position: {target_position}")
            file_links = []
            
            # Method 1: Search by anchor name/id
            if target_position == "supp_files":
                # Look for anchors with name or id 'supp_files'
                anchors = soup.find_all('a', {'name': 'supp_files'}) + soup.find_all('a', {'id': 'supp_files'})
                
                for anchor in anchors:
                    href = anchor.get('href', '').strip()
                    if href:
                        filename = os.path.basename(href) or anchor.get_text(strip=True)
                        file_links.append({
                            'url': href if href.startswith('http') else f"{self.base_url}/{href.lstrip('/')}",
                            'filename': filename,
                            'position': 'supp_files',
                            'anchor_text': anchor.get_text(strip=True),
                            'row_context': self._get_row_context(anchor)
                        })
                        logger.info(f"📎 Found file by supp_files: {filename}")
            
            # Method 2: Search by column index
            elif target_position.startswith("column_"):
                column_index = int(target_position.split("_")[1])
                tables = soup.find_all('table')
                
                for table_idx, table in enumerate(tables):
                    rows = table.find_all('tr')
                    for row_idx, row in enumerate(rows):
                        cells = row.find_all(['td', 'th'])
                        if len(cells) > column_index:
                            target_cell = cells[column_index]
                            links = target_cell.find_all('a', href=True)
                            
                            for link in links:
                                href = link.get('href', '').strip()
                                if href:
                                    filename = os.path.basename(href) or link.get_text(strip=True)
                                    file_links.append({
                                        'url': href if href.startswith('http') else f"{self.base_url}/{href.lstrip('/')}",
                                        'filename': filename,
                                        'position': f'column_{column_index}',
                                        'table': table_idx,
                                        'row': row_idx,
                                        'anchor_text': link.get_text(strip=True),
                                        'row_context': self._get_row_context(link)
                                    })
                                    logger.info(f"📎 Found file by column {column_index}: {filename}")
            
            # Method 3: Search by CSS class
            elif target_position.startswith("class_"):
                class_name = target_position.split("_", 1)[1]
                anchors = soup.find_all('a', {'class': class_name})
                
                for anchor in anchors:
                    href = anchor.get('href', '').strip()
                    if href:
                        filename = os.path.basename(href) or anchor.get_text(strip=True)
                        file_links.append({
                            'url': href if href.startswith('http') else f"{self.base_url}/{href.lstrip('/')}",
                            'filename': filename,
                            'position': f'class_{class_name}',
                            'anchor_text': anchor.get_text(strip=True),
                            'row_context': self._get_row_context(anchor)
                        })
                        logger.info(f"📎 Found file by class {class_name}: {filename}")
            
            # Method 4: Search in last column of each row
            elif target_position == "last_column":
                tables = soup.find_all('table')
                
                for table_idx, table in enumerate(tables):
                    rows = table.find_all('tr')
                    for row_idx, row in enumerate(rows):
                        cells = row.find_all(['td', 'th'])
                        if cells:  # Ensure row has cells
                            last_cell = cells[-1]
                            links = last_cell.find_all('a', href=True)
                            
                            for link in links:
                                href = link.get('href', '').strip()
                                if href:
                                    filename = os.path.basename(href) or link.get_text(strip=True)
                                    file_links.append({
                                        'url': href if href.startswith('http') else f"{self.base_url}/{href.lstrip('/')}",
                                        'filename': filename,
                                        'position': 'last_column',
                                        'table': table_idx,
                                        'row': row_idx,
                                        'anchor_text': link.get_text(strip=True),
                                        'row_context': self._get_row_context(link)
                                    })
                                    logger.info(f"📎 Found file in last column: {filename}")
            
            # Method 5: Search for specific file patterns (fallback)
            elif target_position == "file_pattern":
                # Look for common file extensions
                file_extensions = ['.xls', '.xlsx', '.csv', '.zip', '.pdf']
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    href = link.get('href', '').strip()
                    if any(href.lower().endswith(ext) for ext in file_extensions):
                        filename = os.path.basename(href) or link.get_text(strip=True)
                        file_links.append({
                            'url': href if href.startswith('http') else f"{self.base_url}/{href.lstrip('/')}",
                            'filename': filename,
                            'position': 'file_pattern',
                            'anchor_text': link.get_text(strip=True),
                            'row_context': self._get_row_context(link)
                        })
                        logger.info(f"📎 Found file by pattern: {filename}")
            
            logger.info(f"📊 Found {len(file_links)} file links by position '{target_position}'")
            return file_links
            
        except Exception as e:
            logger.error(f"❌ Error finding file links by position: {e}")
            return []

    def _get_row_context(self, element) -> str:
        """Get context information about the row containing the element"""
        try:
            # Find the parent row
            row = element.find_parent('tr')
            if row:
                cells = row.find_all(['td', 'th'])
                if cells:
                    # Get text from first few cells for context
                    context_cells = cells[:3]  # First 3 cells
                    context_texts = [cell.get_text(strip=True) for cell in context_cells if cell.get_text(strip=True)]
                    return " | ".join(context_texts)
            return "No context available"
        except:
            return "Context extraction failed"

    def extract_week_urls_from_dsa(self) -> List[Dict[str, str]]:
        """Extract week URLs from the main DSA page using position-based discovery"""
        try:
            logger.info("🔍 Fetching DSA page to extract week URLs using position-based discovery...")
            response = self.session.get(self.dsa_page_url, timeout=20)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            week_urls = []
            
            # Look for dropdown with name="wk"
            dropdown = soup.select_one('select[name="wk"]')
            if dropdown:
                options = dropdown.find_all('option')
                for option in options:
                    value = option.get('value', '').strip()
                    text = option.get_text(strip=True)
                    
                    if value and value != '':
                        # Construct week URL
                        week_url = f"{self.base_url}/comm/{value}/"
                        week_urls.append({
                            'url': week_url,
                            'week_text': text,
                            'week_value': value
                        })
            
            logger.info(f"📅 Found {len(week_urls)} week URLs from DSA page using position-based discovery")
            return week_urls
            
        except Exception as e:
            logger.error(f"❌ Error extracting week URLs using position-based discovery: {e}")
            return []

    def fetch_week_page_and_extract_files(self, week_info: Dict[str, str], target_position: str = "supp_files") -> List[Dict[str, str]]:
        """Fetch a week's page and extract files using position-based discovery"""
        try:
            logger.info(f"🔍 Fetching week page: {week_info['week_text']}")
            response = self.session.get(week_info['url'], timeout=20)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find file links by position
            file_links = self.find_file_links_by_position(soup, target_position)
            
            # Add week context to each file link
            for file_link in file_links:
                file_link['week_text'] = week_info['week_text']
                file_link['week_value'] = week_info['week_value']
            
            logger.info(f"📎 Found {len(file_links)} files for week {week_info['week_text']}")
            return file_links
            
        except Exception as e:
            logger.error(f"❌ Error fetching week page {week_info['week_text']}: {e}")
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

            import tempfile
            xls_path = Path(tempfile.mktemp(suffix='.xls'))
            with open(xls_path, 'wb') as f:
                f.write(resp.content)
            logger.info(f"✅ Saved XLS: {xls_path}")
            
            # Upload original supporting XLS to raw/NRLDC/supporting_files
            try:
                if self.s3_uploader and hasattr(self.s3_uploader, 'auto_upload_file'):
                    # Extract year and month from filename for proper S3 path
                    import re
                    
                    # Try multiple date patterns in order of preference
                    year = None
                    month = None
                    
                    def is_valid_date(day, month, year):
                        """Check if the parsed date components are valid"""
                        try:
                            day_int = int(day)
                            month_int = int(month)
                            year_int = int(year)
                            
                            # Basic validation
                            if not (1 <= day_int <= 31):
                                return False
                            if not (1 <= month_int <= 12):
                                return False
                            if not (2020 <= year_int <= 2030):  # Reasonable year range
                                return False
                            return True
                        except (ValueError, TypeError):
                            return False
                    
                    # Pattern 1: YYYYMMDD (e.g., 20250815 -> year=2025, month=08, day=15)
                    alt_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
                    if alt_match:
                        year_candidate = alt_match.group(1)
                        month_candidate = alt_match.group(2)
                        day_candidate = alt_match.group(3)
                        if is_valid_date(day_candidate, month_candidate, year_candidate):
                            year = year_candidate
                            month = month_candidate
                            logger.info(f"📅 Parsed date from filename {filename}: year={year}, month={month}, day={day_candidate}")
                    
                    # Pattern 2: DDMMYYYY (e.g., 15082025 -> day=15, month=08, year=2025)
                    if not year or not month:
                        full_date_match = re.search(r'(\d{2})(\d{2})(\d{4})', filename)
                        if full_date_match:
                            day_candidate = full_date_match.group(1)
                            month_candidate = full_date_match.group(2)
                            year_candidate = full_date_match.group(3)
                            if is_valid_date(day_candidate, month_candidate, year_candidate):
                                year = year_candidate
                                month = month_candidate
                                logger.info(f"📅 Parsed date from filename {filename}: day={day_candidate}, month={month}, year={year}")
                    
                    # Pattern 3: Old pattern DDMMYY (e.g., 150825 -> day=15, month=08, year=2025)
                    if not year or not month:
                        old_date_match = re.search(r'(\d{2})(\d{2})(\d{2})', filename)
                        if old_date_match:
                            day_candidate = old_date_match.group(1)
                            month_candidate = old_date_match.group(2)
                            year_yy = old_date_match.group(3)
                            # Assume 20xx for 2-digit years
                            year_candidate = "20" + year_yy
                            if is_valid_date(day_candidate, month_candidate, year_candidate):
                                year = year_candidate
                                month = month_candidate
                                logger.info(f"📅 Parsed date from filename {filename}: day={day_candidate}, month={month}, year={year}")
                    
                    if year and month:
                        raw_key = f"dsm_data/raw/NRLDC/{year}/{month}/{filename}"
                    else:
                        # Fallback to current year/month if no date found in filename
                        from datetime import datetime as _dt
                        now = _dt.now()
                        year = now.year
                        month = f"{now.month:02d}"
                        raw_key = f"dsm_data/raw/NRLDC/{year}/{month}/{filename}"
                        logger.warning(f"⚠️ Could not parse date from filename {filename}, using current date: {year}/{month}")
                    self.s3_uploader.auto_upload_file(str(xls_path), original_filename=raw_key)
                    logger.info(f"📤 Uploaded raw supporting file to s3://{raw_key}")
            except Exception as ue:
                logger.warning(f"⚠️ Raw XLS upload skipped: {ue}")

            # Process all 16 sheets from the XLS file
            csv_saved = None
            csv_paths = []
            processed_dataframes = []  # Collect dataframes for parquet export
            try:
                workbook = pd.read_excel(xls_path, sheet_name=None, engine='xlrd')
                if isinstance(workbook, dict):
                    logger.info(f"📊 Found {len(workbook)} sheets: {list(workbook.keys())}")
                    
                    for sheet_name, df_sheet in workbook.items():
                        try:
                            df_local = df_sheet.dropna(how='all')
                            if df_local.empty:
                                logger.info(f"⏭️ Skipping empty sheet: {sheet_name}")
                                continue
                            
                            # Process each sheet type appropriately
                            df_processed = self._process_sheet_by_type(sheet_name, df_local)
                            if df_processed is not None and not df_processed.empty:
                                out_csv = filename.replace('.xls', f"_{sheet_name}.csv")
                                import tempfile
                                out_path = Path(tempfile.mktemp(suffix='.csv'))
                                df_processed.to_csv(out_path, index=False)
                                logger.info(f"✅ Processed sheet '{sheet_name}': {out_path} ({len(df_processed)} rows, {len(df_processed.columns)} cols)")
                                csv_paths.append(str(out_path))
                                processed_dataframes.append(df_processed)  # Add to dataframes list
                                if csv_saved is None:
                                    csv_saved = str(out_path)
                            else:
                                logger.info(f"⏭️ Skipped sheet '{sheet_name}' (no valid data)")
                                
                        except Exception as se:
                            logger.warning(f"⚠️ Could not process sheet {sheet_name}: {se}")
                
                # Fallback single sheet if nothing written
                if csv_saved is None:
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
                logger.warning(f"⚠️ Could not parse XLS workbook: {ce}")
                csv_saved = None

            # Track
            self.processed_weeks[week_key] = {
                'timestamp': datetime.now().isoformat(),
                'filename': filename,
                'csv_file': os.path.basename(csv_saved) if csv_saved else None,
                'url': url
            }
            self.save_processed_weeks()
            
            # Return both the file path and dataframes for parquet export
            return {
                'file_path': csv_saved or str(xls_path),
                'dataframes': processed_dataframes
            }
        except Exception as e:
            logger.error(f"❌ Supporting XLS download failed: {e}")
            return None

    def _process_sheet_by_type(self, sheet_name: str, df: pd.DataFrame) -> typing.Optional[pd.DataFrame]:
        """Process different sheet types with appropriate normalization."""
        try:
            # Remove banner rows
            df_clean = df.copy()
            
            # Drop rows that contain banner text
            banner_patterns = self._detect_banner_patterns(df_clean)
            
            for idx, row in df_clean.iterrows():
                row_text = ' '.join([str(x) for x in row.tolist() if pd.notna(x)]).lower()
                if any(pattern in row_text for pattern in banner_patterns):
                    df_clean = df_clean.drop(idx)
            
            if df_clean.empty:
                return None
                
            # Find header row dynamically
            header_row = self._detect_header_row(df_clean)
            
            # Set headers and remove header rows
            if header_row > 0:
                df_clean.columns = df_clean.iloc[header_row].astype(str).str.strip()
                df_clean = df_clean.iloc[header_row + 1:].reset_index(drop=True)
            else:
                df_clean.columns = df_clean.iloc[0].astype(str).str.strip()
                df_clean = df_clean.iloc[1:].reset_index(drop=True)
            
            # Clean column names
            df_clean.columns = [str(col).strip() for col in df_clean.columns]
            
            # Add sheet type identifier
            df_clean['Sheet_Type'] = sheet_name
            
            # Process based on sheet type - ONLY keep station-specific sheets
            if sheet_name == 'DC_Stations':
                return self._process_dc_stations_sheet(df_clean)
            elif sheet_name in ['Act_Inj_Gen_Stations', 'GS_Stations']:
                return self._process_generation_sheet(df_clean, sheet_name)
            elif 'station' in sheet_name.lower() and 'state' not in sheet_name.lower():
                # Generic processing for other station sheets
                return self._process_generic_sheet(df_clean, sheet_name)
            elif sheet_name in ['Normal_Rate', 'Reference_Rate', 'Contract_Rate']:
                # Include rate data but mark it clearly
                df_clean['Rate_Type'] = sheet_name.replace('_Rate', '')
                return self._process_rate_sheet(df_clean, sheet_name)
            else:
                # Skip state-level sheets - only want station data
                logger.info(f"⏭️ Skipping non-station sheet: {sheet_name}")
                return None
                
        except Exception as e:
            logger.warning(f"⚠️ Error processing sheet {sheet_name}: {e}")
            return None

    def _process_dc_stations_sheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process DC_Stations sheet - main station data."""
        try:
            # Ensure we have Stn_Name column
            if 'Stn_Name' not in df.columns:
                logger.warning("⚠️ DC_Stations sheet missing Stn_Name column")
                return None
            
            # Filter out rows without valid station names
            df = df[df['Stn_Name'].notna() & (df['Stn_Name'].astype(str).str.strip() != '')]
            
            # Add Station_Name column for consistency
            df['Station_Name'] = df['Stn_Name'].astype(str).str.strip()
            
            # Add date column if present
            if 'Stn_DC_Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Stn_DC_Date'], errors='coerce')
            
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ Error processing DC_Stations: {e}")
            return None

    def _process_rate_sheet(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Process rate sheets (Normal_Rate, Reference_Rate, Contract_Rate)."""
        try:
            # Add rate type column
            df['Rate_Type'] = sheet_name.replace('_Rate', '')
            
            # Handle dates dynamically
            date_col = self._find_date_column(df)
            if date_col:
                df['Date'] = pd.to_datetime(df[date_col], errors='coerce')
                logger.debug(f"📅 Using date column '{date_col}' for {sheet_name}")
            else:
                logger.warning(f"⚠️ No date column found in {sheet_name}")
            
            # Handle station/entity names dynamically
            name_col = self._find_station_column(df)
            if name_col:
                df['Station_Name'] = df[name_col].astype(str).str.strip()
                logger.debug(f"🏭 Using station column '{name_col}' for {sheet_name}")
            else:
                logger.warning(f"⚠️ No station column found in {sheet_name}")
            
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ Error processing {sheet_name}: {e}")
            return None

    def _process_frequency_sheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Frequency sheet."""
        try:
            if 'Frequency_Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Frequency_Date'], errors='coerce')
            return df
        except Exception as e:
            logger.warning(f"⚠️ Error processing Frequency: {e}")
            return None

    def _find_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """Dynamically find the best date column in the DataFrame."""
        # Priority order for date columns (most specific to most general)
        date_priority = [
            'Stn_Gen_Date', 'Stn_DC_Date', 'Stn_Dev_Date', 'Stn_Ref_Date', 
            'Stn_Contract_Date', 'Stn_Solar_Date', 'Stn_SRAS_Date', 'Stn_SRAS5_Date', 
            'Stn_Rras_Date', 'Seb_Sch_Date', 'Drawl_Date', 'Entity_Date', 
            'sch_date', 'Date', 'DATE', 'date'
        ]
        
        # First try priority list
        for col in date_priority:
            if col in df.columns:
                return col
        
        # If no priority column found, look for columns containing 'date' (case insensitive)
        date_columns = [col for col in df.columns if 'date' in str(col).lower()]
        if date_columns:
            return date_columns[0]  # Return first match
        
        return None

    def _find_station_column(self, df: pd.DataFrame) -> Optional[str]:
        """Dynamically find the best station/entity name column in the DataFrame."""
        # Priority order for station/entity columns
        station_priority = [
            'Stn_Name', 'Station_Name', 'Entity_Name', 'Area_Code', 
            'Seb_Name', 'State_Name', 'NAME', 'Name', 'name'
        ]
        
        # First try priority list
        for col in station_priority:
            if col in df.columns:
                return col
        
        # If no priority column found, look for columns containing 'name' or 'stn' (case insensitive)
        name_columns = [col for col in df.columns if any(keyword in str(col).lower() for keyword in ['name', 'stn', 'station', 'entity'])]
        if name_columns:
            return name_columns[0]  # Return first match
        
        return None

    def _detect_sheet_type(self, filename: str, df: pd.DataFrame) -> Optional[str]:
        """Dynamically detect sheet type from filename and content analysis"""
        try:
            filename_lower = filename.lower()
            
            # First, try filename-based detection with more flexible patterns
            sheet_patterns = {
                'DC_Stations': ['dc_station', 'dc_stations', 'dcstation', 'dcstations'],
                'GS_Stations': ['gs_station', 'gs_stations', 'gsstation', 'gsstations'],
                'Act_Inj_Gen_Stations': ['act_inj_gen', 'actinjgen', 'actual_injection', 'actualinjection'],
                'DSA_Week': ['dsa_week', 'dsaweek', 'dsa_', 'week_'],
                'Rate_Data': ['rate', 'normal_rate', 'reference_rate', 'contract_rate', 'hpdam'],
                'Deviation_Charges': ['deviation', 'deviation_charge', 'ui_charge'],
                'SRAS': ['sras', 'secondary_reserve'],
                'Solar_Availability': ['solar', 'availability', 'solar_avail'],
                'Frequency': ['freq', 'frequency', 'hz'],
                'Other_Station': ['station', 'entity']
            }
            
            # Check filename patterns
            for sheet_type, patterns in sheet_patterns.items():
                if any(pattern in filename_lower for pattern in patterns):
                    # Additional validation for station types
                    if sheet_type in ['DC_Stations', 'GS_Stations', 'Act_Inj_Gen_Stations', 'Other_Station']:
                        if 'state' in filename_lower:
                            continue  # Skip state-level data
                    return sheet_type
            
            # If filename doesn't match, try content-based detection
            if not df.empty:
                # Look for characteristic columns
                columns_lower = [str(col).lower() for col in df.columns]
                
                # Check for DC-specific columns
                if any('dc' in col for col in columns_lower) and any('stn' in col for col in columns_lower):
                    return 'DC_Stations'
                
                # Check for generation-specific columns
                if any('gen' in col for col in columns_lower) and any('inj' in col for col in columns_lower):
                    return 'Act_Inj_Gen_Stations'
                
                # Check for rate-specific columns
                if any('rate' in col for col in columns_lower) or any('hpdam' in col for col in columns_lower):
                    return 'Rate_Data'
                
                # Check for deviation-specific columns
                if any('deviation' in col for col in columns_lower) or any('ui' in col for col in columns_lower):
                    return 'Deviation_Charges'
                
                # Check for frequency-specific columns
                if any('freq' in col for col in columns_lower) or any('hz' in col for col in columns_lower):
                    return 'Frequency'
                
                # Check for station data (has station names but not state-level)
                if any('stn' in col for col in columns_lower) and not any('state' in col for col in columns_lower):
                    return 'Other_Station'
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error detecting sheet type for {filename}: {e}")
            return None

    def _detect_banner_patterns(self, df: pd.DataFrame) -> list:
        """Dynamically detect banner patterns from the data"""
        try:
            banner_patterns = []
            
            # Common banner patterns
            common_patterns = [
                'northern regional power committee',
                '----------',
                '==========',
                '*****',
                'power committee',
                'regional power',
                'load dispatch',
                'commercial data',
                'dsa data',
                'supporting data'
            ]
            
            # Check first few rows for banner-like content
            for i in range(min(5, len(df))):
                row_text = ' '.join([str(x) for x in df.iloc[i].tolist() if pd.notna(x)]).lower()
                
                # Look for patterns that suggest banner rows
                if any(pattern in row_text for pattern in common_patterns):
                    banner_patterns.extend([pattern for pattern in common_patterns if pattern in row_text])
                
                # Look for rows that are mostly dashes, equals, or asterisks
                if re.match(r'^[\s\-=*]+$', row_text.strip()):
                    banner_patterns.append(row_text.strip())
                
                # Look for rows that contain mostly organizational text
                if any(org_word in row_text for org_word in ['committee', 'regional', 'power', 'dispatch', 'commercial']):
                    if len(row_text.split()) <= 10:  # Short organizational phrases
                        banner_patterns.append(row_text.strip())
            
            # Remove duplicates and return
            return list(set(banner_patterns))
            
        except Exception as e:
            logger.warning(f"⚠️ Error detecting banner patterns: {e}")
            return ['northern regional power committee', '----------']  # Fallback

    def _detect_header_row(self, df: pd.DataFrame) -> int:
        """Dynamically detect the header row in the DataFrame"""
        try:
            if df.empty:
                return 0
            
            # Header keywords to look for
            header_keywords = [
                'stn_name', 'station_name', 'entity_name', 'date', 'time', 'block',
                'freq', 'frequency', 'actual', 'schedule', 'deviation', 'rate',
                'dc', 'gen', 'inj', 'sras', 'ui', 'hpdam', 'constituent'
            ]
            
            best_row = 0
            best_score = 0
            
            # Check first 10 rows for header-like content
            for i in range(min(10, len(df))):
                row_vals = [str(x).strip().lower() for x in df.iloc[i].tolist() if pd.notna(x)]
                
                # Score based on header keyword matches
                score = 0
                for val in row_vals:
                    for keyword in header_keywords:
                        if keyword in val:
                            score += 1
                
                # Bonus for rows with multiple header-like values
                if score > 2:
                    score += len([v for v in row_vals if any(kw in v for kw in header_keywords)])
                
                # Penalty for rows that look like data (contain numbers)
                numeric_count = sum(1 for val in row_vals if re.match(r'^\d+\.?\d*$', val))
                if numeric_count > len(row_vals) * 0.5:  # More than 50% numbers
                    score = max(0, score - 2)
                
                if score > best_score:
                    best_score = score
                    best_row = i
            
            # If no good header found, default to first row
            if best_score == 0:
                return 0
            
            logger.debug(f"📋 Detected header row {best_row} with score {best_score}")
            return best_row
            
        except Exception as e:
            logger.warning(f"⚠️ Error detecting header row: {e}")
            return 0

    def _process_generation_sheet(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Process generation-related sheets."""
        try:
            # Handle station names dynamically
            station_col = self._find_station_column(df)
            if station_col:
                df['Station_Name'] = df[station_col].astype(str).str.strip()
                logger.debug(f"🏭 Using station column '{station_col}' for {sheet_name}")
            else:
                logger.warning(f"⚠️ No station column found in {sheet_name}")
            
            # Handle dates dynamically
            date_col = self._find_date_column(df)
            if date_col:
                df['Date'] = pd.to_datetime(df[date_col], errors='coerce')
                logger.debug(f"📅 Using date column '{date_col}' for {sheet_name}")
            else:
                logger.warning(f"⚠️ No date column found in {sheet_name}")
            
            return df
        except Exception as e:
            logger.warning(f"⚠️ Error processing {sheet_name}: {e}")
            return None

    def _process_state_sheet(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Process state-related sheets."""
        try:
            # Handle state names dynamically
            state_col = self._find_station_column(df)  # Reuse the same function for state names
            if state_col:
                df['State_Name'] = df[state_col].astype(str).str.strip()
                logger.debug(f"🏛️ Using state column '{state_col}' for {sheet_name}")
            else:
                logger.warning(f"⚠️ No state column found in {sheet_name}")
            
            # Handle dates dynamically
            date_col = self._find_date_column(df)
            if date_col:
                df['Date'] = pd.to_datetime(df[date_col], errors='coerce')
                logger.debug(f"📅 Using date column '{date_col}' for {sheet_name}")
            else:
                logger.warning(f"⚠️ No date column found in {sheet_name}")
            
            return df
        except Exception as e:
            logger.warning(f"⚠️ Error processing {sheet_name}: {e}")
            return None

    def _process_generic_sheet(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Generic processing for other sheet types."""
        try:
            # Try to identify station/entity columns
            for col in df.columns:
                if 'stn' in str(col).lower() or 'entity' in str(col).lower():
                    df['Station_Name'] = df[col].astype(str).str.strip()
                    break
            
            # Try to identify date columns
            for col in df.columns:
                if 'date' in str(col).lower():
                    df['Date'] = pd.to_datetime(df[col], errors='coerce')
                    break
            
            return df
        except Exception as e:
            logger.warning(f"⚠️ Error processing generic sheet {sheet_name}: {e}")
            return None

    def run_extraction(self):
        """Main extraction process (supporting .xls only)"""
        if self.csv_only:
            logger.info("🚀 Starting NRLDC DSA extraction (CSV-only mode)...")
            items = self.get_csv_links()
            downloaded = []
            for item in items[:15]:  # keep it bounded
                res = self.download_supporting_csv(item)
                if res:
                    downloaded.append(res)
                else:
                    logger.info(f"⏭️ Skipped/failed CSV: {item['filename']}")
        else:
            logger.info("🚀 Starting NRLDC DSA extraction (supporting .xls only)...")
            # Prefer constructing URLs from the live DSA page tokens
            items = self.parse_weeks_from_dsa_page()
            if not items:
                # Fallback to past-7-days generator (may 404 if not present under 2021-22)
                items = self.generate_supporting_urls()
                logger.info(f"📅 Generated {len(items)} week URLs under 2021-22 (fallback)")

            downloaded = []
            all_dataframes = []  # Collect all dataframes for parquet export
            
            # Group items by week_key to test multiple filename patterns per week
            week_groups = {}
            for item in items[:50]:  # Increased limit since we're being smarter about testing
                week_key = item['week_key']
                if week_key not in week_groups:
                    week_groups[week_key] = []
                week_groups[week_key].append(item)
            
            downloaded = []
            all_dataframes = []  # Collect all dataframes for parquet export
            
            # Process each week group, finding the best working filename
            for week_key, week_items in list(week_groups.items())[:10]:  # Limit to 10 weeks
                logger.info(f"🔍 Testing {len(week_items)} filename patterns for week {week_key}")
                
                # Find the working filename for this week
                working_item = self._find_working_filename(week_items)
                if working_item:
                    res = self.download_supporting_xls(working_item)
                    if res:
                        downloaded.append(res)
                        # Extract dataframes from the result if available
                        if isinstance(res, dict) and 'dataframes' in res:
                            all_dataframes.extend(res['dataframes'])
                else:
                    logger.info(f"⏭️ Skipped/failed: {item['filename']}")

            # Export parquet files if we have dataframes
            if all_dataframes:
                try:
                    logger.info(f"🔄 Combining {len(all_dataframes)} dataframes for parquet export...")
                    import pandas as pd
                    combined_df = pd.concat(all_dataframes, ignore_index=True)
                    logger.info(f"📊 Combined dataframe has {len(combined_df)} rows")
                    
                    # Export parquet files using the existing function
                    self._export_partitioned_to_s3(combined_df)
                    logger.info("✅ Parquet files exported successfully")
                except Exception as e:
                    logger.warning(f"⚠️ Parquet export failed: {e}")

        # No master dataset creation needed
        logger.info(f"🎉 Supporting extraction complete. Files: {len(downloaded)}")
        return downloaded

    def run_position_based_extraction(self, target_position: str = "supp_files", max_weeks: int = 10):
        """
        Run extraction using position-based file discovery while preserving all existing mapping logic
        
        Args:
            target_position: How to identify files ('supp_files', 'last_column', 'column_3', etc.)
            max_weeks: Maximum number of weeks to process
        """
        logger.info(f"🚀 Starting NRLDC position-based extraction (position: {target_position})")
        
        # Step 1: Extract week URLs from DSA page using position-based discovery
        week_urls = self.extract_week_urls_from_dsa()
        if not week_urls:
            logger.error("❌ No week URLs found")
            return []
        
        # Step 2: Process each week page
        results = []
        processed_weeks = 0
        all_dataframes = []  # Collect all dataframes for parquet export
        
        for week_info in week_urls[:max_weeks]:
            logger.info(f"📅 Processing week: {week_info['week_text']}")
            
            # Step 3: Fetch the week page and extract files using position-based discovery
            file_links = self.fetch_week_page_and_extract_files(week_info, target_position)
            
            if not file_links:
                logger.warning(f"⚠️ No files found for week: {week_info['week_text']}")
                continue
            
            # Step 4: Process each file using existing download logic
            for file_info in file_links:
                # Convert file_info to the format expected by existing download_supporting_xls method
                item = {
                    'url': file_info['url'],
                    'filename': file_info['filename'],
                    'week_key': f"{week_info['week_value']}_{file_info['filename']}",
                    'week_text': week_info['week_text'],
                    'position': file_info.get('position', 'unknown'),
                    'row_context': file_info.get('row_context', '')
                }
                
                # Use existing download logic (preserves all mapping and processing)
                try:
                    res = self.download_supporting_xls(item)
                    if res:
                        results.append(res)
                        # Extract dataframes from the result if available
                        if isinstance(res, dict) and 'dataframes' in res:
                            all_dataframes.extend(res['dataframes'])
                        logger.info(f"✅ Downloaded: {file_info['filename']} (position: {file_info.get('position', 'unknown')})")
                    else:
                        logger.info(f"⏭️ Skipped/failed: {file_info['filename']}")
                        results.append({
                            'filename': file_info['filename'],
                            'action': 'failed',
                            'position': file_info.get('position', 'unknown'),
                            'week': week_info['week_text']
                        })
                except Exception as e:
                    logger.error(f"❌ Error processing {file_info['filename']}: {e}")
                    results.append({
                        'filename': file_info['filename'],
                        'action': 'error',
                        'position': file_info.get('position', 'unknown'),
                        'week': week_info['week_text'],
                        'error': str(e)
                    })
            
            processed_weeks += 1
        
        # Step 5: Export parquet files if we have dataframes (preserve existing logic)
        if all_dataframes:
            try:
                logger.info(f"🔄 Combining {len(all_dataframes)} dataframes for parquet export...")
                combined_df = pd.concat(all_dataframes, ignore_index=True)
                self._export_partitioned_to_s3(combined_df)
                logger.info("✅ Parquet export completed")
            except Exception as e:
                logger.warning(f"⚠️ Parquet export failed: {e}")
        
        # Step 6: Log results
        self._log_position_results(results, target_position)
        logger.info(f"🎉 Position-based extraction complete. Files: {len(results)}")
        return results

    def _log_position_results(self, results: List[Dict], target_position: str):
        """Log position-based extraction results"""
        logger.info(f"\n📊 POSITION-BASED EXTRACTION RESULTS ({target_position}):")
        logger.info("=" * 100)
        
        # Group by action
        by_action = {}
        for result in results:
            action = result.get('action', 'unknown')
            if action not in by_action:
                by_action[action] = []
            by_action[action].append(result)
        
        for action, action_results in by_action.items():
            logger.info(f"\n{action.upper()}: {len(action_results)} files")
            logger.info("-" * 50)
            for result in action_results[:5]:  # Show first 5 files
                filename = result.get('filename', 'unknown')
                position = result.get('position', 'unknown')
                week = result.get('week', 'unknown')
                logger.info(f"  {filename} | Position: {position} | Week: {week}")
            
            if len(action_results) > 5:
                logger.info(f"  ... and {len(action_results) - 5} more files")
        
        logger.info("=" * 100)
        total_files = len(results)
        logger.info(f"Total files processed: {total_files}")

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
