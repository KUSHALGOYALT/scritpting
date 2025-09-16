#!/usr/bin/env python3
"""
NERPC Dynamic Extractor - Downloads actual NERPC data files
Enhanced with past 7 days extraction, update handling, and master dataset creation
Handles both regular and revised data patterns
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
from typing import List, Dict, Optional, Tuple
import urllib.parse

# Add common module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'common'))
from auto_s3_upload import AutoS3Uploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NERPCDynamicExtractor:
    def __init__(self):
        self.base_url = "https://nerpc.gov.in"
        self.data_page_url = "https://nerpc.gov.in/?page_id=5823"
        
        # Initialize S3 uploader
        self.s3_uploader = AutoS3Uploader()
        
        # Session for maintaining cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Track processed files to avoid duplicates
        self.processed_files = set()
        self.load_processed_files()

    def load_processed_files(self):
        """Load list of already processed files from S3"""
        # For now, we'll skip file tracking to avoid local storage
        # In production, this could be stored in S3 or a database
        self.processed_files = set()
    def save_processed_files(self):
        """Save list of processed files to S3"""
        # For now, we'll skip file tracking to avoid local storage
        # In production, this could be stored in S3 or a database
        pass

    def get_past_7_days_weeks(self) -> List[Tuple[datetime, datetime]]:
        """Get the past 7 days broken into weeks"""
        today = datetime.now()
        weeks = []
        
        # Go back 7 days
        for i in range(7):
            date = today - timedelta(days=i)
            # Get start of week (Monday) and end of week (Sunday)
            start_of_week = date - timedelta(days=date.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            weeks.append((start_of_week, end_of_week))
        
        return weeks

    def extract_data_links_from_page(self) -> List[Dict]:
        """Extract data file links from the NERPC data page using BeautifulSoup"""
        try:
            logger.info(f"🔍 Fetching NERPC data page: {self.data_page_url}")
            response = self.session.get(self.data_page_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            data_links = []
            
            # Step 1: Find the Data File column index dynamically
            data_file_column_index = self._find_data_file_column_index(soup)
            if data_file_column_index is None:
                logger.warning("⚠️ Could not find 'Data File' column, trying fallback method...")
                return self._extract_data_links_fallback(soup)
            
            logger.info(f"✅ Found 'Data File' column at index: {data_file_column_index}")
            
            # Step 2: Extract ZIP file links from the Data File column
            tables = soup.find_all('table')
            
            for table_idx, table in enumerate(tables):
                logger.info(f"📋 Processing table {table_idx + 1}...")
                
                # Get all rows in the table body
                tbody = table.find('tbody') or table
                rows = tbody.find_all('tr')
                
                for row_idx, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    
                    # Check if this row has enough columns
                    if len(cells) > data_file_column_index:
                        # Extract duration information from first column
                        duration_cell = cells[0].get_text(strip=True) if len(cells) > 0 else ""
                        
                        # Get the Data File cell
                        data_file_cell = cells[data_file_column_index]
                        
                        # Find all links in the data file cell
                        links = data_file_cell.find_all('a', href=True)
                        
                        for link in links:
                            href = link.get('href', '').strip()
                            link_text = link.get_text(strip=True)
                            
                            # Check if it's a ZIP file (by extension or text)
                            is_zip = (
                                href.lower().endswith('.zip') or 
                                link_text.lower().endswith('.zip') or
                                '.zip' in href.lower() or
                                '.zip' in link_text.lower() or
                                'zip' in link_text.lower()
                            )
                            
                            if is_zip:
                                # Build full URL if relative
                                if href.startswith('http'):
                                    full_url = href
                                else:
                                    full_url = urllib.parse.urljoin(self.base_url, href)
                                
                                # Extract filename from URL or link text
                                if link_text and link_text.lower().endswith('.zip'):
                                    filename = link_text
                                else:
                                    filename = os.path.basename(urllib.parse.urlparse(full_url).path)
                                    if not filename.endswith('.zip'):
                                        filename += '.zip'
                                
                                # Determine if it's a revised file
                                is_revised = any(keyword in filename.upper() for keyword in ['R1', 'REV1', 'REVISED', 'DSMR1'])
                                
                                data_links.append({
                                    'url': full_url,
                                    'filename': filename,
                                    'duration': duration_cell,
                                    'is_revised': is_revised,
                                    'link_text': link_text,
                                    'source': 'nerpc_page',
                                    'table_index': table_idx,
                                    'row_index': row_idx
                                })
                                
                                logger.info(f"📎 Found data link: {filename} ({'Revised' if is_revised else 'Regular'})")
            
            logger.info(f"📊 Found {len(data_links)} data file links using BeautifulSoup")
            return data_links
            
        except Exception as e:
            logger.error(f"❌ Error extracting data links: {e}")
            return []

    def _find_data_file_column_index(self, soup: BeautifulSoup) -> Optional[int]:
        """Find the column index for the 'Data File' header"""
        try:
            logger.info("🔍 Searching for 'Data File' column header...")
            
            # Look for table headers containing 'Data File'
            tables = soup.find_all('table')
            
            for table in tables:
                # Find header row
                header_row = table.find('tr')
                if header_row:
                    headers = header_row.find_all(['th', 'td'])
                    for i, header in enumerate(headers):
                        header_text = header.get_text(strip=True).lower()
                        if 'data file' in header_text:
                            logger.info(f"✅ Found 'Data File' column in table at index {i}: '{header.get_text(strip=True)}'")
                            return i
            
            logger.warning("⚠️ No 'Data File' column found in any table")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error finding data file column: {e}")
            return None

    def _extract_data_links_fallback(self, soup: BeautifulSoup) -> List[Dict]:
        """Fallback method to extract data links if Data File column is not found"""
        try:
            logger.info("🔄 Using fallback method to extract data links...")
            data_links = []
            
            # Look for the table containing DSM/SRAS/TRAS data (original logic)
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 4:  # Duration, DSM, SRAS/TRAS, Data File columns
                        
                        # Extract duration information
                        duration_cell = cells[0].get_text(strip=True)
                        
                        # Look for data file links in the last column
                        data_file_cell = cells[-1]  # Last column should be Data File
                        links = data_file_cell.find_all('a', href=True)
                        
                        for link in links:
                            href = link.get('href', '')
                            link_text = link.get_text(strip=True)
                            
                            if href and ('zip' in href.lower() or 'data' in link_text.lower()):
                                # Build full URL if relative
                                if href.startswith('http'):
                                    full_url = href
                                else:
                                    full_url = urllib.parse.urljoin(self.base_url, href)
                                
                                # Extract filename from URL
                                filename = os.path.basename(urllib.parse.urlparse(full_url).path)
                                
                                # Determine if it's a revised file
                                is_revised = 'DSMR1' in filename or 'revised' in link_text.lower()
                                
                                data_links.append({
                                    'url': full_url,
                                    'filename': filename,
                                    'duration': duration_cell,
                                    'is_revised': is_revised,
                                    'link_text': link_text,
                                    'source': 'nerpc_page_fallback'
                                })
                                
                                logger.info(f"📎 Found data link (fallback): {filename} ({'Revised' if is_revised else 'Regular'})")
            
            logger.info(f"📊 Found {len(data_links)} data file links using fallback method")
            return data_links
            
        except Exception as e:
            logger.error(f"❌ Error in fallback extraction: {e}")
            return []

    def extract_station_info_from_data(self, file_info: Dict, filename: str) -> Dict:
        """Extract station information from the data file"""
        try:
            # Read the CSV to analyze station data
            df = pd.read_csv(file_info['local_csv'])
            
            # Try to identify station names from various possible columns
            station_columns = ['Station_Name', 'Station', 'STATION', 'STN', 'Station Name', 'Entity', 'Entity_Name']
            station_name = None
            
            for col in station_columns:
                if col in df.columns and not df[col].isna().all():
                    # Get the most common station name
                    station_name = df[col].mode().iloc[0] if not df[col].mode().empty else None
                    if station_name:
                        break
            
            # If no station column found, try to extract from sheet name
            if not station_name:
                sheet_name = file_info.get('original_name', '')
                # Extract station name from sheet name (e.g., "DOYANG", "KOPILI", etc.)
                if '(' in sheet_name and 'Sheet:' in sheet_name:
                    station_name = sheet_name.split('Sheet:')[-1].strip()
                elif '_' in sheet_name:
                    # Try to extract from filename patterns
                    parts = sheet_name.split('_')
                    for part in parts:
                        if part.isupper() and len(part) > 2:
                            station_name = part
                            break
            
            # If still no station name, use a default based on data type
            if not station_name:
                if 'DSM' in filename:
                    station_name = 'NERPC_DSM'
                elif 'SRAS' in filename:
                    station_name = 'NERPC_SRAS'
                elif 'TRAS' in filename:
                    station_name = 'NERPC_TRAS'
                elif 'SCUC' in filename:
                    station_name = 'NERPC_SCUC'
                else:
                    station_name = 'NERPC_UNKNOWN'
            
            # Normalize station name
            station_name = self.normalize_station_name(station_name)
            
            # Extract date information
            date_columns = ['Date', 'DATE', 'Date_Time', 'DateTime', '__date__']
            date_value = None
            
            for col in date_columns:
                if col in df.columns and not df[col].isna().all():
                    try:
                        # Try to parse the first valid date
                        date_series = pd.to_datetime(df[col], errors='coerce')
                        date_value = date_series.dropna().iloc[0] if not date_series.dropna().empty else None
                        if date_value:
                            break
                    except:
                        continue
            
            # If no date found, try to extract from filename
            if not date_value:
                date_patterns = [
                    r'(\d{2})\.(\d{2})\.(\d{4})',  # DD.MM.YYYY
                    r'(\d{2})-(\d{2})-(\d{4})',   # DD-MM-YYYY
                    r'(\d{4})-(\d{2})-(\d{2})',   # YYYY-MM-DD
                ]
                
                for pattern in date_patterns:
                    matches = re.findall(pattern, filename)
                    if matches:
                        try:
                            if pattern == r'(\d{4})-(\d{2})-(\d{2})':
                                date_value = datetime.strptime(f"{matches[0][0]}-{matches[0][1]}-{matches[0][2]}", '%Y-%m-%d')
                            else:
                                date_value = datetime.strptime(f"{matches[0][2]}-{matches[0][1]}-{matches[0][0]}", '%Y-%m-%d')
                            break
                        except:
                            continue
            
            # Default to current date if no date found
            if not date_value:
                date_value = datetime.now()
            
            return {
                'station_name': station_name,
                'date': date_value,
                'year': date_value.year,
                'month': f"{date_value.month:02d}",
                'data_type': self.extract_data_type(filename, file_info.get('original_name', ''))
            }
            
        except Exception as e:
            logger.warning(f"⚠️ Error extracting station info: {e}")
            return {
                'station_name': 'NERPC_UNKNOWN',
                'date': datetime.now(),
                'year': datetime.now().year,
                'month': f"{datetime.now().month:02d}",
                'data_type': 'UNKNOWN'
            }

    def normalize_station_name(self, station_name: str) -> str:
        """Normalize station name according to common mapping rules"""
        if not station_name:
            return 'NERPC_UNKNOWN'
        
        # Apply normalization rules from common mapping.json
        normalized = str(station_name).upper().strip()
        normalized = normalized.replace(' ', '_').replace('/', '_').replace('-', '_')
        # Strip non-word chars and collapse underscores
        normalized = re.sub(r'[^A-Z0-9_]', '', normalized)
        normalized = re.sub(r'_+', '_', normalized).strip('_')
        
        # Remove common prefixes/suffixes
        normalized = re.sub(r'^NERPC_', '', normalized)
        normalized = re.sub(r'_[0-9]+$', '', normalized)  # Remove trailing numbers
        
        return f"NERPC_{normalized}" if not normalized.startswith('NERPC_') else normalized

    def extract_data_type(self, filename: str, sheet_name: str) -> str:
        """Extract data type from filename and sheet name"""
        text = f"{filename} {sheet_name}".upper()
        
        if 'DSM' in text:
            return 'DSM'
        elif 'SRAS' in text:
            return 'SRAS'
        elif 'TRAS' in text:
            return 'TRAS'
        elif 'SCUC' in text:
            return 'SCUC'
        elif 'FREQUENCY' in text:
            return 'FREQUENCY'
        else:
            return 'GENERAL'

    def upload_to_organized_s3(self, file_info: Dict, filename: str, link_info: Dict) -> List[str]:
        """Upload file to S3 with organized structure:
        Raw:     dsm_data/raw/NERPC/{YEAR}/{MONTH}/W{WEEK}/{FILENAME}
        Parquet: dsm_data/parquet/NERPC/{STATION}/{YEAR}/{MONTH}/{FILENAME}
        """
        try:
            # Check if this is a consolidated station file
            if 'station_name' in file_info and 'consolidated' in file_info.get('original_name', '').lower():
                # Handle consolidated station files
                station_name = file_info['station_name']
                data_types = file_info.get('data_types', ['CONSOLIDATED'])
                
                # Extract date from filename or use current date
                date_str = self._extract_date_from_filename(filename)
                year = date_str.year
                month = date_str.month
                
                # Create organized filename for consolidated data
                data_types_str = '_'.join(sorted(data_types))
                data_type = f"CONSOLIDATED_{data_types_str}"
                base_filename = f"NERPC_{station_name}_CONSOLIDATED_{data_types_str}_{year}_{month:02d}"
                csv_filename = f"{base_filename}.csv"
                parquet_filename = f"{base_filename}.parquet"
                
                # Create S3 keys with new structure
                from datetime import datetime as _dt
                _week = _dt.now().isocalendar().week
                # Extract clean station name (remove NERPC_ prefix)
                clean_station_name = station_name.replace('NERPC_', '') if station_name.startswith('NERPC_') else station_name
                csv_key = f"dsm_data/raw/NERPC/{year}/{month:02d}/{csv_filename}"
                parquet_key = f"dsm_data/parquet/NERPC/{clean_station_name}/{year}/{month:02d}/{parquet_filename}"
        
            else:
                # Handle regular files (legacy support)
                station_info = self.extract_station_info_from_data(file_info, filename)
            station_name = station_info['station_name']
            # Ensure numeric year/month for formatting
            year = int(station_info['year']) if isinstance(station_info['year'], (str, bytes)) else station_info['year']
            month = int(station_info['month']) if isinstance(station_info['month'], (str, bytes)) else station_info['month']
            data_type = station_info['data_type']
            
            # Create organized filename
            base_filename = f"{station_name}_{data_type}_{year}_{month}"
            csv_filename = f"{base_filename}.csv"
            parquet_filename = f"{base_filename}.parquet"
            
                # Create S3 keys with new structure
            from datetime import datetime as _dt
            _week = _dt.now().isocalendar().week
            # Extract clean station name (remove NERPC_ prefix)
            clean_station_name = station_name.replace('NERPC_', '') if station_name.startswith('NERPC_') else station_name
            csv_key = f"dsm_data/raw/NERPC/{year}/{month:02d}/{csv_filename}"
            parquet_key = f"dsm_data/parquet/NERPC/{clean_station_name}/{year}/{month:02d}/{parquet_filename}"
            
            s3_results = []
            
            # Upload CSV
            try:
                csv_success = self.upload_file_to_s3(file_info['local_csv'], csv_key, 'text/csv')
                if csv_success:
                    s3_results.append(f"s3://{self.s3_uploader.bucket_name}/{csv_key}")
                    logger.info(f"✅ Uploaded CSV: {csv_key}")
            except Exception as e:
                logger.warning(f"⚠️ CSV upload failed: {e}")
            
            # Upload Parquet
            try:
                parquet_success = self.upload_file_to_s3(file_info['local_parquet'], parquet_key, 'application/octet-stream')
                if parquet_success:
                    s3_results.append(f"s3://{self.s3_uploader.bucket_name}/{parquet_key}")
                    logger.info(f"✅ Uploaded Parquet: {parquet_key}")
            except Exception as e:
                logger.warning(f"⚠️ Parquet upload failed: {e}")
            
            return s3_results
            
        except Exception as e:
            logger.error(f"❌ Error in organized S3 upload: {e}")
            return []

    def upload_file_to_s3(self, local_file_path: str, s3_key: str, content_type: str) -> bool:
        """Upload a single file to S3 and clean up temporary file"""
        try:
            # Check if file already exists
            try:
                self.s3_uploader.s3_client.head_object(Bucket=self.s3_uploader.bucket_name, Key=s3_key)
                logger.info(f"⏭️ File already exists in S3: {s3_key}")
                # Clean up temp file even if it already exists
                if local_file_path and os.path.exists(local_file_path):
                    os.unlink(local_file_path)
                return True
            except:
                pass  # File doesn't exist, proceed with upload
            
            # Upload file
            with open(local_file_path, 'rb') as f:
                self.s3_uploader.s3_client.put_object(
                    Bucket=self.s3_uploader.bucket_name,
                    Key=s3_key,
                    Body=f.read(),
                    ContentType=content_type
                )
            
            # Clean up temporary file after successful upload
            if local_file_path and os.path.exists(local_file_path):
                os.unlink(local_file_path)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 upload failed for {s3_key}: {e}")
            # Clean up temp file even if upload failed
            if local_file_path and os.path.exists(local_file_path):
                os.unlink(local_file_path)
            return False

    def clean_dataframe_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean dataframe to avoid parquet conversion issues"""
        try:
            # Make a copy to avoid modifying the original
            df_clean = df.copy()
            
            for col in df_clean.columns:
                try:
                    # Special handling for problematic columns to avoid parquet conversion issues
                    problematic_columns = [
                        'Block', 'Incentives', 'Variable Charges', 'SRAS Net', 'SRAS Up', 'SRAS Down', 
                        'SRAS Mileage', 'Detailed Deviation Report', 'Detailed Deviation Report   .1',
                        'Detailed Deviation Report   .2', 'Detailed Deviation Report   .3', 'Detailed Deviation Report   .4',
                        'Detailed Deviation Report   .5', 'Detailed Deviation Report   .6', 'Detailed Deviation Report   .7',
                        'Detailed Deviation Report   .8', 'Detailed Deviation Report   .9', 'Detailed Deviation Report   .10',
                        'Detailed Deviation Report   .11', 'Detailed Deviation Report   .12', 'Detailed Deviation Report   .13',
                        'Detailed Deviation Report   .14', 'Detailed Deviation Report   .15', 'Detailed Deviation Report   .16',
                        'Detailed Deviation Report   .17'
                    ]
                    if col in problematic_columns:
                        # Convert to string and clean problematic values
                        df_clean[col] = df_clean[col].astype(str)
                        df_clean[col] = df_clean[col].replace(['nan', 'NaN', 'None', 'null', ''], '')
                        df_clean[col] = df_clean[col].fillna('')
                        continue
                    
                    # For all other columns, handle mixed data types more robustly
                    if df_clean[col].dtype == 'object':
                        # Check if column has mixed data types (numeric and string)
                        has_numeric = False
                        has_string = False
                        
                        for val in df_clean[col].dropna().head(100):  # Check first 100 non-null values
                            try:
                                float(val)
                                has_numeric = True
                            except (ValueError, TypeError):
                                has_string = True
                        
                        # If column has mixed types, convert everything to string
                        if has_numeric and has_string:
                            df_clean[col] = df_clean[col].astype(str)
                        else:
                            # Try to convert to numeric first, fallback to string if fails
                            try:
                                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                                # If conversion successful but has NaN, convert back to string
                                if df_clean[col].isna().any():
                                    df_clean[col] = df_clean[col].astype(str)
                            except:
                                df_clean[col] = df_clean[col].astype(str)
                    else:
                        # For non-object columns, convert to string to ensure consistency
                        df_clean[col] = df_clean[col].astype(str)
                    
                    # Handle NaN values and clean up
                    df_clean[col] = df_clean[col].fillna('')
                    df_clean[col] = df_clean[col].replace(['nan', 'NaN', 'None', 'null', '<NA>'], '')
                    
                except Exception as e:
                    logger.debug(f"⚠️ Could not clean column {col}: {e}")
                    # If all else fails, convert to string
                    df_clean[col] = df_clean[col].astype(str)
                    df_clean[col] = df_clean[col].fillna('')
                    df_clean[col] = df_clean[col].replace(['nan', 'NaN', 'None', 'null', '<NA>'], '')
            
            # Remove any completely empty rows
            df_clean = df_clean.dropna(how='all')
            
            # Final check: ensure all columns are string type for parquet compatibility
            for col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str)
            
            return df_clean
            
        except Exception as e:
            logger.warning(f"⚠️ Error cleaning dataframe: {e}")
            # Fallback: convert everything to string
            df_fallback = df.copy()
            df_fallback = df_fallback.fillna('')
            return df_fallback.astype(str)

    def _consolidate_station_data_from_dataframe(self, df: pd.DataFrame, source_file: str, zip_filename: str, station_data_consolidated: Dict):
        """Consolidate station data from a DataFrame into the station_data_consolidated dictionary"""
        try:
            if df.empty:
                return
            
            # Extract station name from DataFrame
            station_name = self._extract_station_name_from_dataframe(df, source_file)
            if not station_name:
                logger.warning(f"⚠️ Could not extract station name from {source_file}")
                return
            
            # Normalize station name
            station_name = self.normalize_station_name(station_name)
            
            # Extract data type from source file
            data_type = self.extract_data_type(source_file, source_file)
            
            # Initialize station data if not exists
            if station_name not in station_data_consolidated:
                station_data_consolidated[station_name] = {
                    'dataframes': [],
                    'metadata': {
                        'station_name': station_name,
                        'source_files': [],
                        'data_types': set(),
                        'total_rows': 0,
                        'date_range': {'min': None, 'max': None}
                    }
                }
            
            # Add metadata
            station_data_consolidated[station_name]['metadata']['source_files'].append({
                'file': source_file,
                'zip_file': zip_filename,
                'rows': len(df),
                'columns': list(df.columns),
                'data_type': data_type
            })
            station_data_consolidated[station_name]['metadata']['data_types'].add(data_type)
            station_data_consolidated[station_name]['metadata']['total_rows'] += len(df)
            
            # Add DataFrame to consolidation
            station_data_consolidated[station_name]['dataframes'].append(df)
            
            logger.debug(f"📊 Consolidated {len(df)} rows for station {station_name} from {source_file}")
            
        except Exception as e:
            logger.warning(f"⚠️ Error consolidating station data from {source_file}: {e}")
    
    def _extract_station_name_from_dataframe(self, df: pd.DataFrame, source_file: str) -> Optional[str]:
        """Extract station name from DataFrame"""
        try:
            # Method 1: Extract from source file name (sheet names) - PRIORITY
            if 'Sheet:' in source_file:
                sheet_name = source_file.split('Sheet:')[-1].strip()
                logger.debug(f"🔍 Extracting station name from sheet: {sheet_name}")
                
                # Clean up the sheet name
                if sheet_name:
                    # Handle different sheet name patterns
                    clean_name = sheet_name.replace('_', ' ').replace('-', ' ').strip()
                    # Split by common separators and take the first meaningful part
                    parts = clean_name.split()
                    if parts:
                        station_name = parts[0].upper()
                        # Filter out generic names
                        if station_name not in ['FREQUENCY', 'TOTAL', 'SUMMARY', 'DATA', 'FILE']:
                            logger.debug(f"✅ Extracted station name: {station_name}")
                            return station_name
            
            # Method 2: Look for Entity column (most common in NERPC data)
            if 'Entity' in df.columns and not df['Entity'].isna().all():
                entities = df['Entity'].dropna().unique()
                if len(entities) > 0:
                    # Return the most common entity
                    entity_counts = df['Entity'].value_counts()
                    station_name = entity_counts.index[0]
                    logger.debug(f"✅ Extracted station name from Entity column: {station_name}")
                    return station_name
            
            # Method 3: Look for other station-related columns
            station_columns = ['Station_Name', 'Station', 'STATION', 'STN', 'Station Name', 'Entity_Name', 'Buyer', 'Seller', 'Unit', 'UNIT']
            for col in station_columns:
                if col in df.columns and not df[col].isna().all():
                    entities = df[col].dropna().unique()
                    if len(entities) > 0:
                        # Filter out generic values
                        filtered_entities = [e for e in entities if str(e).strip() and 
                                           str(e).upper() not in ['NAN', 'NONE', 'NULL', '', '0', '1', '2', '3', '4', '5']]
                        if filtered_entities:
                            station_name = filtered_entities[0]
                            logger.debug(f"✅ Extracted station name from {col} column: {station_name}")
                            return station_name
            
            # Method 4: Look for station names in any column that might contain them
            for col in df.columns:
                if df[col].dtype == 'object':  # String columns
                    unique_values = df[col].dropna().unique()
                    # Look for values that look like station names
                    for value in unique_values:
                        value_str = str(value).strip().upper()
                        if (len(value_str) >= 3 and 
                            value_str.isalpha() and 
                            value_str not in ['NAN', 'NONE', 'NULL', 'DATE', 'TIME', 'BLOCK', 'HOUR', 'MINUTE']):
                            logger.debug(f"✅ Extracted station name from {col} column: {value_str}")
                            return value_str
            
            # Method 4: Extract from filename patterns (dynamic approach)
            filename_lower = source_file.lower()
            
            # Look for common patterns in filenames
            # Pattern 1: Extract from sheet names (most common)
            if 'sheet:' in filename_lower:
                sheet_part = filename_lower.split('sheet:')[-1].strip()
                # Clean up the sheet name
                sheet_name = sheet_part.split('.')[0].split('_')[0].strip()
                if sheet_name and len(sheet_name) > 2:  # Valid sheet name
                    return sheet_name.upper()
            
            # Pattern 2: Look for station names in the filename
            # Extract potential station names from filename
            import re
            
            # Look for capitalized words that might be station names
            potential_stations = re.findall(r'\b[A-Z][A-Z0-9_-]{2,}\b', source_file)
            if potential_stations:
                # Filter out common non-station words
                exclude_words = {'DATA', 'FILE', 'ZIP', 'XLSX', 'CSV', 'DSM', 'SRAS', 'TRAS', 'SCUC', 'WEEK', 'MONTH', 'YEAR'}
                station_candidates = [s for s in potential_stations if s not in exclude_words]
                if station_candidates:
                    return station_candidates[0]
            
            # Pattern 3: Look for patterns like "StationName_" or "_StationName"
            station_pattern = r'[_-]([A-Z][A-Z0-9_-]{2,})[_-]'
            matches = re.findall(station_pattern, source_file)
            if matches:
                return matches[0]
            
            # Pattern 4: Extract from directory-like structures
            if '/' in source_file or '\\' in source_file:
                path_parts = re.split(r'[/\\]', source_file)
                for part in reversed(path_parts):  # Check from end
                    if len(part) > 2 and not part.lower().startswith(('data', 'file', 'dsm')):
                        # Clean up the part
                        clean_part = re.sub(r'[^A-Za-z0-9_-]', '', part)
                        if len(clean_part) > 2:
                            return clean_part.upper()
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error extracting station name from DataFrame: {e}")
            return None
    
    def _create_consolidated_station_files(self, station_data_consolidated: Dict, zip_filename: str) -> List[Dict]:
        """Create consolidated files for each station"""
        consolidated_files = []
        
        try:
            for station_name, station_data in station_data_consolidated.items():
                logger.info(f"🔄 Creating consolidated file for station: {station_name}")
                
                # Combine all DataFrames for this station
                combined_df = self._combine_station_dataframes(station_data['dataframes'])
                
                if combined_df.empty:
                    logger.warning(f"⚠️ No data to consolidate for station {station_name}")
                    continue
                
                # Add metadata columns
                combined_df = self._add_station_metadata(combined_df, station_data['metadata'])
                
                # Create filename
                safe_station_name = station_name.replace('/', '_').replace('\\', '_').replace(':', '_')
                safe_zip_name = zip_filename.replace('.zip', '').replace('/', '_').replace('\\', '_')
                
                # Create temporary files for upload
                csv_filename = f"NERPC_{safe_station_name}_{safe_zip_name}_consolidated.csv"
                parquet_filename = f"NERPC_{safe_station_name}_{safe_zip_name}_consolidated.parquet"
                
                # Use temporary files
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as csv_file:
                    combined_df.to_csv(csv_file.name, index=False)
                    csv_path = csv_file.name
                
                parquet_path = None
                try:
                    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as parquet_file:
                        combined_df.to_parquet(parquet_file.name, index=False)
                        parquet_path = parquet_file.name
                except Exception as parquet_error:
                    logger.warning(f"⚠️ Could not create parquet file for {station_name}: {parquet_error}")
                
                consolidated_files.append({
                    'original_name': f"Consolidated data for {station_name}",
                    'local_csv': csv_path,
                    'local_parquet': parquet_path,
                    'rows': len(combined_df),
                    'columns': list(combined_df.columns),
                    'station_name': station_name,
                    'data_types': list(station_data['metadata']['data_types']),
                    'source_files_count': len(station_data['metadata']['source_files'])
                })
                
                logger.info(f"✅ Created consolidated file for {station_name}: {csv_filename} ({len(combined_df)} rows)")
            
            return consolidated_files
            
        except Exception as e:
            logger.error(f"❌ Error creating consolidated station files: {e}")
            return []
    
    def _combine_station_dataframes(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """Combine multiple DataFrames for a station"""
        try:
            if not dataframes:
                return pd.DataFrame()
            
            if len(dataframes) == 1:
                return dataframes[0].copy()
            
            # Find common columns
            all_columns = set()
            for df in dataframes:
                all_columns.update(df.columns)
            
            # Create a unified schema
            unified_columns = sorted(list(all_columns))
            
            # Align all DataFrames to the same schema
            aligned_dfs = []
            for df in dataframes:
                aligned_df = df.reindex(columns=unified_columns)
                aligned_dfs.append(aligned_df)
            
            # Combine all DataFrames
            combined_df = pd.concat(aligned_dfs, ignore_index=True, sort=False)
            
            # Clean the combined DataFrame to avoid parquet conversion issues
            combined_df = self.clean_dataframe_for_parquet(combined_df)
            
            # Remove duplicates based on key columns if they exist
            key_columns = ['Date', 'date', 'Entity', 'entity', 'Block', 'block']
            existing_key_columns = [col for col in key_columns if col in combined_df.columns]
            
            if existing_key_columns:
                combined_df = combined_df.drop_duplicates(subset=existing_key_columns, keep='first')
            
            return combined_df
            
        except Exception as e:
            logger.error(f"❌ Error combining station DataFrames: {e}")
            return pd.DataFrame()
    
    def _add_station_metadata(self, df: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """Add metadata columns to the DataFrame"""
        try:
            # Add station metadata
            df['station_name'] = metadata['station_name']
            df['data_types'] = ','.join(sorted(metadata['data_types']))
            df['source_files_count'] = len(metadata['source_files'])
            df['total_rows'] = metadata['total_rows']
            
            # Add processing timestamp
            df['consolidated_at'] = datetime.now().isoformat()
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error adding station metadata: {e}")
            return df
    
    def _extract_date_from_filename(self, filename: str) -> datetime:
        """Extract date from filename, fallback to current date"""
        try:
            # Try to extract date from filename patterns
            import re
            
            # Pattern 1: Data_File_25.08.2025to31.08.2025
            date_pattern = r'(\d{1,2})\.(\d{1,2})\.(\d{4})'
            match = re.search(date_pattern, filename)
            if match:
                day, month, year = match.groups()
                return datetime(int(year), int(month), int(day))
            
            # Pattern 2: 25-Aug-25 to 31-Aug-25
            date_pattern2 = r'(\d{1,2})-([A-Za-z]{3})-(\d{2,4})'
            match = re.search(date_pattern2, filename)
            if match:
                day, month_str, year = match.groups()
                month_map = {
                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                }
                month_num = month_map.get(month_str, 8)  # Default to August
                year_int = int(year) if len(year) == 4 else 2000 + int(year)
                return datetime(year_int, month_num, int(day))
            
            # Fallback to current date
            return datetime.now()
            
        except Exception as e:
            logger.warning(f"⚠️ Could not extract date from filename {filename}: {e}")
            return datetime.now()

    def normalize_energy_units(self, df: pd.DataFrame) -> (pd.DataFrame, dict):
        """Convert any kWh columns to MWh and rename columns accordingly.

        Returns:
            (normalized_df, mapping) where mapping is {original_col: new_col or 'scaled_only'}
        """
        try:
            normalized = df.copy()
            column_mapping = {}

            for col in list(normalized.columns):
                new_col = col
                col_lower = str(col).lower()

                # Detect kWh in column name (common variations)
                if 'kwh' in col_lower or '(kwh)' in col_lower or ' kw h' in col_lower or 'kw-h' in col_lower:
                    # Scale values to MWh if numeric
                    try:
                        normalized[col] = pd.to_numeric(normalized[col], errors='coerce') / 1000.0
                    except Exception:
                        pass

                    # Rename column: replace kWh -> MWh (case-insensitive variations)
                    new_col = (
                        col.replace('kWh', 'MWh')
                           .replace('KWH', 'MWH')
                           .replace('KWh', 'MWh')
                           .replace('(kWh)', '(MWh)')
                           .replace('(KWH)', '(MWH)')
                    )
                    if new_col != col:
                        normalized.rename(columns={col: new_col}, inplace=True)
                        column_mapping[col] = new_col
                    else:
                        column_mapping[col] = 'scaled_only'

                # Explicit MWH labels remain unchanged
                elif 'mwh' in col_lower or '(mwh)' in col_lower:
                    continue

            return normalized, column_mapping

        except Exception:
            # On error, return original df with empty mapping
            return df, {}

    def is_file_recent(self, filename: str, duration_text: str) -> bool:
        """Check if the file is from the past 7 days or recent enough to be relevant"""
        try:
            # Extract dates from filename or duration text
            date_patterns = [
                r'(\d{2})\.(\d{2})\.(\d{4})',  # DD.MM.YYYY
                r'(\d{2})-(\d{2})-(\d{4})',   # DD-MM-YYYY
                r'(\d{4})-(\d{2})-(\d{2})',   # YYYY-MM-DD
                r'(\d{2})\.(\d{2})\.(\d{2})',  # DD.MM.YY (2-digit year)
            ]
            
            text_to_check = f"{filename} {duration_text}"
            logger.debug(f"🔍 Checking dates in: {text_to_check}")
            
            for pattern in date_patterns:
                matches = re.findall(pattern, text_to_check)
                for match in matches:
                    try:
                        if len(match) == 3:
                            if pattern == r'(\d{4})-(\d{2})-(\d{2})':
                                # YYYY-MM-DD format
                                date_str = f"{match[0]}-{match[1]}-{match[2]}"
                                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                            elif pattern == r'(\d{2})\.(\d{2})\.(\d{2})':
                                # DD.MM.YY format (2-digit year)
                                year = int(match[2])
                                # Assume 20xx for years 00-99
                                full_year = 2000 + year if year < 100 else year
                                date_str = f"{full_year}-{match[1]}-{match[0]}"
                                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                            else:
                                # DD.MM.YYYY or DD-MM-YYYY format
                                date_str = f"{match[2]}-{match[1]}-{match[0]}"
                                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                            
                            # Check if within past 7 days OR if it's from 2025 (recent data)
                            days_diff = (datetime.now() - file_date).days
                            is_2025_data = file_date.year == 2025
                            
                            if 0 <= days_diff <= 7 or is_2025_data:
                                logger.info(f"📅 File {filename} is recent ({days_diff} days old, year: {file_date.year})")
                                return True
                            else:
                                logger.debug(f"⏰ File {filename} is too old ({days_diff} days old, year: {file_date.year})")
                    except ValueError as ve:
                        logger.debug(f"⚠️ Date parsing error for {match}: {ve}")
                        continue
            
            # If no date found, check if it looks like recent data based on filename patterns
            if any(keyword in filename.lower() for keyword in ['2025', 'recent', 'latest']):
                logger.info(f"📅 File {filename} appears to be recent based on filename")
                return True
            
            logger.debug(f"⏰ No recent date found for {filename}")
            return False
            
        except Exception as e:
            logger.debug(f"⚠️ Could not parse date from {filename}: {e}")
            return False

    def download_and_process_zip(self, link_info: Dict) -> Optional[Dict]:
        """Download and process a ZIP file from NERPC with station consolidation"""
        try:
            url = link_info['url']
            filename = link_info['filename']
            
            logger.info(f"⬇️ Downloading: {filename}")
            
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            # Process ZIP file with station consolidation
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                # Dictionary to store all data by station
                station_data_consolidated = {}
                all_extracted_files = []
                
                # List all files in the ZIP for debugging
                logger.info(f"📦 ZIP contents: {[f.filename for f in zip_file.filelist]}")
                
                for zip_info in zip_file.filelist:
                    if not zip_info.is_dir():
                        file_ext = os.path.splitext(zip_info.filename)[1].lower()
                        logger.info(f"🔍 Processing file: {zip_info.filename} (extension: {file_ext})")
                        
                        # Handle different file types
                        if file_ext in ['.csv', '.xlsx', '.xls']:
                            try:
                                # Read file content
                                file_content = zip_file.read(zip_info.filename)
                                
                                # Process based on file type
                                if file_ext == '.csv':
                                    csv_df = pd.read_csv(io.BytesIO(file_content))
                                    self._consolidate_station_data_from_dataframe(
                                        csv_df, zip_info.filename, filename, station_data_consolidated
                                    )
                                elif file_ext in ['.xlsx', '.xls']:
                                    # For Excel files, read all sheets and consolidate by station
                                    excel_file = pd.ExcelFile(io.BytesIO(file_content))
                                    for sheet_name in excel_file.sheet_names:
                                        try:
                                            # Read Excel with more flexible data type handling
                                            csv_df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name)
                                            
                                            # Clean and convert data types to avoid parquet conversion issues
                                            csv_df = self.clean_dataframe_for_parquet(csv_df)
                                            # Normalize units (kWh -> MWh) and capture column mapping
                                            csv_df, unit_mapping = self.normalize_energy_units(csv_df)
                                            
                                            # Consolidate data by station
                                            self._consolidate_station_data_from_dataframe(
                                                csv_df, f"{zip_info.filename} (Sheet: {sheet_name})", filename, station_data_consolidated
                                            )
                                            
                                            logger.info(f"✅ Processed sheet: {sheet_name} from {zip_info.filename} ({len(csv_df)} rows)")
                                            
                                        except Exception as e:
                                            logger.warning(f"⚠️ Could not process sheet {sheet_name} in {zip_info.filename}: {e}")
                                            continue
                                
                            except Exception as e:
                                logger.warning(f"⚠️ Could not process {zip_info.filename}: {e}")
                                continue
                        else:
                            logger.info(f"⏭️ Skipping non-data file: {zip_info.filename}")
                            continue
                
            # Create consolidated station files
            if station_data_consolidated:
                logger.info(f"📊 Creating consolidated files for {len(station_data_consolidated)} stations")
                consolidated_files = self._create_consolidated_station_files(
                    station_data_consolidated, filename
                )
                all_extracted_files.extend(consolidated_files)
                
                # Upload consolidated station files to S3
                s3_results = []
                for file_info in consolidated_files:
                    try:
                        # Upload with organized S3 structure
                        upload_results = self.upload_to_organized_s3(file_info, filename, link_info)
                        s3_results.extend(upload_results)
                    except Exception as e:
                        logger.warning(f"⚠️ S3 upload failed for {file_info['local_parquet']}: {e}")
                    
                    return {
                        'filename': filename,
                        'url': url,
                    'extracted_files': all_extracted_files,
                        's3_urls': s3_results,
                        'is_revised': link_info.get('is_revised', False),
                        'duration': link_info.get('duration', ''),
                        'processed_at': datetime.now().isoformat(),
                    'stations_consolidated': list(station_data_consolidated.keys()),
                    'total_stations': len(station_data_consolidated)
                    }
                else:
                    logger.warning(f"⚠️ No station data found in {filename}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Error processing {filename}: {e}")
            return None


    def run_extraction(self) -> Dict:
        """Main extraction method"""
        try:
            logger.info("🚀 Starting NERPC data extraction...")
            
            # Extract data links from the page
            data_links = self.extract_data_links_from_page()
            
            if not data_links:
                logger.warning("⚠️ No data links found")
                return {'status': 'no_data', 'files_processed': 0}
            
            # Filter for recent files (past 7 days) and limit to first 3 files for testing
            recent_links = []
            for link in data_links:
                if self.is_file_recent(link['filename'], link.get('duration', '')):
                    recent_links.append(link)
                    # Limit to first 3 files to avoid long processing
                    if len(recent_links) >= 3:
                        logger.info(f"📊 Limiting to first 3 recent files for processing")
                        break
                else:
                    logger.debug(f"⏰ Skipping old file: {link['filename']}")
            
            logger.info(f"📅 Found {len(recent_links)} recent files to process")
            
            # Process each file
            processed_files = []
            for link_info in recent_links:
                filename = link_info['filename']
                
                # Skip if already processed
                if filename in self.processed_files:
                    logger.info(f"⏭️ Skipping already processed file: {filename}")
                    continue
                
                # Download and process
                result = self.download_and_process_zip(link_info)
                if result:
                    processed_files.append(result)
                    self.processed_files.add(filename)
                    logger.info(f"✅ Successfully processed: {filename}")
                else:
                    logger.warning(f"⚠️ Failed to process: {filename}")
            
            # Save processed files list
            self.save_processed_files()
            
            logger.info(f"🎉 Extraction completed! Processed {len(processed_files)} files")
            
            return {
                'status': 'success',
                'files_processed': len(processed_files),
                'processed_files': processed_files
            }
            
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            return {'status': 'error', 'error': str(e), 'files_processed': 0}

def main():
    """Main function to run the NERPC extractor"""
    try:
        extractor = NERPCDynamicExtractor()
        result = extractor.run_extraction()
        
        if result['status'] == 'success':
            print(f"✅ Successfully processed {result['files_processed']} files")
            if result.get('master_dataset'):
                print(f"📊 Master dataset created: {result['master_dataset']}")
        else:
            print(f"❌ Extraction failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    main()
