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
import tempfile

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from energy_data_extractors.common.auto_s3_upload import AutoS3Uploader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def extract_zip_contents(zip_path: Path):
    """Extract ZIP contents and return list of extracted file paths"""
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
    except Exception as e:
        logger.error(f"Failed to extract ZIP contents: {e}")
        return []

def read_data_file(file_path: Path):
    """Read data file based on its extension"""
    try:
        if file_path.suffix.lower() == '.csv':
            return pd.read_csv(file_path)
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            return pd.read_excel(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_path.suffix}")
            return None
    except Exception as e:
        logger.error(f"Failed to read data file {file_path}: {e}")
        return None

def extract_station_info(df, filename):
    """Extract station information from dataframe and filename"""
    station_info = {
        'station_name': 'Unknown_Station',
        'station_type': 'commercial',
        'region': 'SRPC'
    }
    
    # Try to extract station name from filename
    filename_lower = filename.lower()
    
    # Extract station name from filename patterns like:
    # commercial_dev2022_kudgi1.csv -> kudgi1
    # commercial_px_rstps.csv -> rstps
    # commercial_dev2022_simhadri1.csv -> simhadri1
    
    # Remove common prefixes and suffixes
    clean_filename = filename_lower
    clean_filename = clean_filename.replace('commercial_dev2022_', '')
    clean_filename = clean_filename.replace('commercial_px_', '')
    clean_filename = clean_filename.replace('commercial_pxsold_', '')
    clean_filename = clean_filename.replace('commercial_sch_', '')
    clean_filename = clean_filename.replace('commercial_urs_', '')
    clean_filename = clean_filename.replace('commercial_ent_', '')
    clean_filename = clean_filename.replace('commercial_postfacto_', '')
    clean_filename = clean_filename.replace('commercial_entonbar_', '')
    clean_filename = clean_filename.replace('commercial_', '')
    clean_filename = clean_filename.replace('.csv', '')
    
    # If we still have a meaningful name (not just generic terms)
    if clean_filename and len(clean_filename) > 2:
        # Check if it's a state name (these are usually full words)
        state_names = ['andhrapradesh', 'karnataka', 'kerala', 'tamilnadu', 'telangana', 
                      'goa', 'pondichery', 'orrissa', 'uttarakhand', 'jk', 'ndmc']
        
        if clean_filename not in state_names:
            station_info['station_name'] = f"SRPC_{clean_filename.upper()}"
            return station_info
    
    # Fallback: Look for specific station patterns in filename
    station_patterns = [
        'kudgi', 'rstps', 'simhadri', 'tstpp', 'talcher', 'nlc', 'ntpl', 'ntpc',
        'vallur', 'lkppl', 'mepl', 'sepl', 'sgpl', 'tpcil', 'mal_', 'serentica',
        'greeninfra', 'jsw', 'tata', 'adani', 'orange', 'mytrah', 'spring',
        'sprng', 'girel', 'grtjipl', 'ilfs', 'ircon', 'ostro', 'parm', 'ren',
        'rsopl', 'rsrpl', 'saupl', 'seil', 'vena', 'yar', 'zrepl', 'arpspl',
        'azpw', 'azu', 'atb', 'ath', 'atk', 'avdsol', 'ayana', 'betam',
        'coastal', 'frtmfin', 'frtmsol', 'greenko', 'kleio', 'kredl',
        'res_pvg', 'amplpvg', 'ampltumk', 'adyah', 'amgreen', 'andanika9'
    ]
    
    for pattern in station_patterns:
        if pattern in filename_lower:
            # Extract the full station name after the pattern
            start_idx = filename_lower.find(pattern)
            end_idx = start_idx + len(pattern)
            
            # Look for additional characters that might be part of station name
            remaining = filename_lower[end_idx:]
            if remaining and remaining[0] in '0123456789':
                # Include numbers after the pattern
                station_name = pattern + remaining[0]
                station_info['station_name'] = f"SRPC_{station_name.upper()}"
                return station_info
            else:
                station_info['station_name'] = f"SRPC_{pattern.upper()}"
                return station_info
    
    # Try to extract from dataframe columns if available
    if not df.empty:
        # Look for station-related columns
        station_columns = [col for col in df.columns if any(word in col.lower() 
                          for word in ['station', 'plant', 'unit', 'name', 'location', 'entity'])]
        
        if station_columns:
            # Use first non-null value from first station column
            for col in station_columns:
                non_null_values = df[col].dropna().astype(str).unique()
                if len(non_null_values) > 0:
                    station_info['station_name'] = f"SRPC_{non_null_values[0].upper()}"
                    break
    
    return station_info

def normalize_dataframe(df, station_info):
    """Normalize dataframe for consistent processing"""
    # Add metadata columns
    df['__station_name__'] = station_info['station_name']
    df['__station_type__'] = station_info['station_type']
    df['__region__'] = station_info['region']
    df['__data_source__'] = station_info['data_source']
    df['__file_type__'] = station_info['file_type']
    df['__filename__'] = station_info['filename']
    df['__date__'] = station_info['date']
    df['__year__'] = station_info['year']
    
    # Clean column names
    df.columns = [str(col).strip().replace(' ', '_') for col in df.columns]
    
    return df

def consolidate_station_data(station_data_consolidated, normalized_df, station_info):
    """Consolidate data by station"""
    station_name = station_info['station_name']
    
    if station_name not in station_data_consolidated:
        station_data_consolidated[station_name] = {
            'dataframes': [],
            'station_info': station_info,
            'total_rows': 0
        }
    
    station_data_consolidated[station_name]['dataframes'].append(normalized_df)
    station_data_consolidated[station_name]['total_rows'] += len(normalized_df)

def is_multi_entity_file(filename, df):
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

def process_multi_entity_file(df, filename, date, year, station_data_consolidated):
    """Split a multi-entity DataFrame into per-entity slices and consolidate each slice."""
    processed = 0
    name = filename.lower()

    def clean_entity(value):
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
                .map(clean_entity)
                .dropna()
                .unique()
            )
            # Filter out header-like markers
            entities = [e for e in entities if not any(tok in e.lower() for tok in ['states/ut', 'entity', 'total amount to the pool'])]
            
            for entity in entities:
                try:
                    slice_df = df[df[col].map(lambda x: clean_entity(x) == entity)].copy()
                    if slice_df.empty:
                        continue
                        
                    station_name = canonicalize_station_name(entity)
                    station_info = {
                        'station_name': station_name,
                        'filename': filename,
                        'date': date,
                        'year': year,
                        'data_type': infer_data_type_from_filename(filename),
                        'data_source': 'SRPC',
                        'file_type': 'commercial'
                    }
                    
                    normalized_df = normalize_dataframe(slice_df, station_info)
                    consolidate_station_data(station_data_consolidated, normalized_df, station_info)
                    processed += 1
                    logger.info(f'  📊 Processed entity: {entity} -> {station_name}')
                    
                except Exception as e:
                    logger.warning(f"⚠️ Failed processing entity '{entity}' in {filename}: {e}")
            return processed

    # If no explicit entity column, fallback: treat as single-entity
    station_info = extract_station_info(df, filename)
    station_info['filename'] = filename
    station_info['date'] = date
    station_info['year'] = year
    station_info['data_source'] = 'SRPC'
    station_info['file_type'] = 'commercial'
    
    normalized_df = normalize_dataframe(df, station_info)
    consolidate_station_data(station_data_consolidated, normalized_df, station_info)
    return 1

def canonicalize_station_name(name):
    """Normalize station/entity name to a canonical uppercase underscore form."""
    try:
        import re
        cleaned = re.sub(r"[^A-Za-z0-9]+", "_", name.upper()).strip("_")
        return cleaned or 'UNKNOWN_STATION'
    except Exception:
        return 'UNKNOWN_STATION'

def infer_data_type_from_filename(filename):
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
            ('gas', 'GAS')
        ]
        
        for pattern, data_type in data_type_patterns:
            if pattern in name:
                return data_type
                
        return 'COMMERCIAL'
    except Exception:
        return 'COMMERCIAL'

def upload_consolidated_station_data(station_data_consolidated, year, month, s3_uploader):
    """Upload consolidated station data to S3"""
    for station_name, station_data in station_data_consolidated.items():
        try:
            # Combine all dataframes for this station
            combined_df = pd.concat(station_data['dataframes'], ignore_index=True)
            
            # Convert to parquet and upload
            tmp_pq = Path(tempfile.mktemp(suffix='.parquet'))
            combined_df.to_parquet(tmp_pq, index=False)
            
            # Create parquet filename
            pq_filename = f'SRPC_Commercial_{station_name}_{year}_{month:02d}_consolidated.parquet'
            pq_key = f'dsm_data/parquet/SRPC/{station_name}/{year}/{month:02d}/{pq_filename}'
            
            s3_uploader.s3_client.upload_file(str(tmp_pq), s3_uploader.bucket_name, pq_key)
            logger.info(f'📤 Uploaded consolidated parquet to s3://{pq_key}')
            
            tmp_pq.unlink()
            
        except Exception as e:
            logger.error(f'❌ Failed to upload consolidated data for {station_name}: {e}')

def download_srpc_commercial_file():
    """Download and process the SRPC commercial data file"""
    s3_uploader = AutoS3Uploader()
    
    # The specific file URL from the Data File column
    url = 'https://www.srpc.kar.nic.in/website/2025/commercial/110825-240825.zip'
    filename = '110825-240825.zip'
    
    logger.info(f'📥 Downloading SRPC commercial file: {filename}')
    
    try:
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
                logger.warning(f'⚠️ Attempt {attempt + 1} failed: {e}. Retrying...')
                import time
                time.sleep(5)
        
        logger.info(f'✅ Downloaded {filename} ({len(response.content)} bytes)')
        
        # Extract date from filename (110825-240825 means Aug 11-24, 2025)
        # Parse the date range: 110825-240825 (MMDDYY-MMDDYY format)
        date_parts = filename.replace('.zip', '').split('-')
        start_date = date_parts[0]  # 110825
        end_date = date_parts[1]    # 240825
        
        # Parse dates: MMDDYY format
        start_month = int(start_date[:2])  # 11
        start_day = int(start_date[2:4])   # 08
        start_year = 2000 + int(start_date[4:6])  # 25 -> 2025
        
        end_month = int(end_date[:2])      # 24
        end_day = int(end_date[2:4])       # 08
        end_year = 2000 + int(end_date[4:6])  # 25 -> 2025
        
        logger.info(f'📅 Date range: {start_year}-{start_month:02d}-{start_day:02d} to {end_year}-{end_month:02d}-{end_day:02d}')
        
        # Use the start date for S3 organization
        year = start_year
        month = start_month
        
        # Upload raw ZIP file to S3
        raw_key = f'dsm_data/raw/SRPC/{year}/{month:02d}/{filename}'
        
        try:
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name
            
            s3_uploader.s3_client.upload_file(temp_file_path, s3_uploader.bucket_name, raw_key)
            logger.info(f'📤 Uploaded raw ZIP to s3://{raw_key}')
            
            os.unlink(temp_file_path)
        except Exception as e:
            logger.error(f'❌ Failed to upload raw ZIP: {e}')
            return
        
        # Process ZIP file using SRPC extractor logic
        logger.info(f'📦 Processing ZIP file: {filename}')
        
        try:
            # Save ZIP to temporary file for processing (like SRPC extractor does)
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
                temp_zip.write(response.content)
                zip_path = Path(temp_zip.name)
            
            # Extract ZIP contents (like SRPC extractor)
            extracted_files = extract_zip_contents(zip_path)
            if not extracted_files:
                logger.warning(f'No files extracted from {filename}')
                return
            
            logger.info(f'📋 Extracted {len(extracted_files)} files from ZIP')
            
            processed_count = 0
            station_data_consolidated = {}
            
            # Process each extracted file (like SRPC extractor)
            for file_path in extracted_files:
                # Handle nested ZIP files
                if file_path.suffix.lower() == '.zip':
                    logger.info(f'📦 Processing nested ZIP: {file_path.name}')
                    nested_extracted = extract_zip_contents(file_path)
                    if nested_extracted:
                        logger.info(f'📋 Extracted {len(nested_extracted)} files from nested ZIP')
                        extracted_files.extend(nested_extracted)
                    continue
                
                if file_path.suffix.lower() in ['.csv', '.xls', '.xlsx']:
                    logger.info(f'📄 Processing: {file_path.name}')
                    
                    try:
                        # Read data file
                        df = read_data_file(file_path)
                        if df is None or df.empty:
                            logger.warning(f'⚠️ Empty or invalid data in {file_path.name}')
                            continue
                        
                        logger.info(f'📊 Loaded {len(df)} rows from {file_path.name}')
                        
                        # Upload raw file to S3
                        raw_filename = f'{filename.replace(".zip", "")}_{file_path.name}'
                        raw_key = f'dsm_data/raw/SRPC/{year}/{month:02d}/{raw_filename}'
                        
                        s3_uploader.s3_client.upload_file(str(file_path), s3_uploader.bucket_name, raw_key)
                        logger.info(f'📤 Uploaded raw file to s3://{raw_key}')
                        
                        # Check if this file contains multiple entities (like SRPC extractor)
                        if is_multi_entity_file(file_path.name, df):
                            logger.info(f'📊 Multi-entity file detected: {file_path.name}')
                            entity_count = process_multi_entity_file(
                                df=df,
                                filename=file_path.name,
                                date=datetime(year, month, start_day),
                                year=str(year),
                                station_data_consolidated=station_data_consolidated
                            )
                            processed_count += entity_count
                            logger.info(f'✅ Processed {entity_count} entities from {file_path.name}')
                        else:
                            # Single entity file processing
                            station_info = extract_station_info(df, file_path.name)
                            station_info['filename'] = file_path.name
                            station_info['date'] = datetime(year, month, start_day)
                            station_info['year'] = str(year)
                            station_info['data_source'] = 'SRPC'
                            station_info['file_type'] = 'commercial'
                            station_info['date_range'] = f'{start_year}-{start_month:02d}-{start_day:02d}_to_{end_year}-{end_month:02d}-{end_day:02d}'
                            
                            # Normalize dataframe (like SRPC extractor)
                            normalized_df = normalize_dataframe(df, station_info)
                            
                            # Consolidate by station (like SRPC extractor)
                            consolidate_station_data(station_data_consolidated, normalized_df, station_info)
                            
                            processed_count += 1
                            logger.info(f'✅ Processed {file_path.name} -> {station_info["station_name"]}')
                        
                    except Exception as e:
                        logger.error(f'❌ Failed to process {file_path.name}: {e}')
            
            # Upload consolidated station data (like SRPC extractor)
            if station_data_consolidated:
                logger.info(f'📤 Uploading consolidated data for {len(station_data_consolidated)} stations...')
                upload_consolidated_station_data(station_data_consolidated, year, month, s3_uploader)
            
            # Cleanup
            if zip_path.exists():
                zip_path.unlink()
            
            logger.info(f'✅ Successfully processed {processed_count} data files from {filename}')
                            
        except Exception as e:
            logger.error(f'❌ Failed to process ZIP file: {e}')
            
    except Exception as e:
        logger.error(f'❌ Failed to download {filename}: {e}')

if __name__ == "__main__":
    download_srpc_commercial_file()
