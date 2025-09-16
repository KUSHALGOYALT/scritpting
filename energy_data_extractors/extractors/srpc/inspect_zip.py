import boto3
import zipfile
import io
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from energy_data_extractors.common.auto_s3_upload import AutoS3Uploader

s3_uploader = AutoS3Uploader()
bucket_name = s3_uploader.bucket_name

# Download the ZIP file from S3 to inspect it
raw_key = 'dsm_data/raw/SRPC/2025/11/110825-240825.zip'

try:
    # Download from S3
    s3_client = s3_uploader.s3_client
    response = s3_client.get_object(Bucket=bucket_name, Key=raw_key)
    zip_content = response['Body'].read()
    
    print(f'Downloaded ZIP from S3: {len(zip_content)} bytes')
    
    # Inspect ZIP contents
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
        file_list = zip_file.namelist()
        print(f'\nFiles in main ZIP ({len(file_list)} total):')
        
        for i, file_name in enumerate(file_list, 1):
            file_info = zip_file.getinfo(file_name)
            print(f'  {i}. {file_name} ({file_info.file_size} bytes)')
            
            # If it's a ZIP file, inspect its contents
            if file_name.endswith('.zip'):
                print(f'     This is a nested ZIP file')
                try:
                    with zip_file.open(file_name) as nested_zip_file:
                        nested_content = nested_zip_file.read()
                        with zipfile.ZipFile(io.BytesIO(nested_content)) as nested_zip:
                            nested_files = nested_zip.namelist()
                            print(f'     Nested ZIP contains {len(nested_files)} files:')
                            for nested_file in nested_files:
                                print(f'       - {nested_file}')
                                
                            # Show preview of first CSV if any
                            csv_files = [f for f in nested_files if f.endswith('.csv')]
                            if csv_files:
                                print(f'     CSV files found: {csv_files}')
                                # Try to read first CSV
                                first_csv = csv_files[0]
                                with nested_zip.open(first_csv) as csv_file:
                                    csv_content = csv_file.read(500)  # First 500 bytes
                                    print(f'     Preview of {first_csv}:')
                                    print(csv_content.decode('utf-8', errors='ignore'))
                                    
                except Exception as e:
                    print(f'     Error reading nested ZIP: {e}')
                    
except Exception as e:
    print(f'Error: {e}')
