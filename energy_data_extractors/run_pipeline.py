#!/usr/bin/env python3
import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Local imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'common'))
from energy_data_extractors.common.auto_s3_upload import AutoS3Uploader

# Extractors
from energy_data_extractors.extractors.erldc.erldc_extractor import ERLDCDynamicExtractor
from energy_data_extractors.extractors.wrpc.wrpc_extractor import WRPCDynamicExtractor
from energy_data_extractors.extractors.srpc.srpc_extractor import SRPCExtractor
from energy_data_extractors.extractors.nerpc.nerpc_extractor import NERPCDynamicExtractor

# Builders
from energy_data_extractors.tools.common_station_builder import build_common_files as build_wrpc_erldc_common
from energy_data_extractors.tools.overall_common_builder import build_overall_common as build_overall


def load_common_mapping() -> dict:
    mapping_file = Path('energy_data_extractors/master_data/common_mapping.json')
    if not mapping_file.exists():
        mapping_file = Path(os.path.dirname(__file__)) / 'master_data' / 'common_mapping.json'
    with open(mapping_file, 'r') as f:
        return json.load(f)


def run_extractors():
    # Run ERLDC, WRPC, SRPC, and NERPC extractors (they handle their own downloads and local outputs)
    try:
        er = ERLDCDynamicExtractor()
        er.run()
    except Exception as e:
        print(f"WARN: ERLDC extractor failed/skipped: {e}")
    try:
        wr = WRPCDynamicExtractor()
        wr.run()
    except Exception as e:
        print(f"WARN: WRPC extractor failed/skipped: {e}")
    try:
        sr = SRPCExtractor()
        sr.extract_past_7_days()
    except Exception as e:
        print(f"WARN: SRPC extractor failed/skipped: {e}")
    try:
        nr = NERPCDynamicExtractor()
        nr.run_extraction()
    except Exception as e:
        print(f"WARN: NERPC extractor failed/skipped: {e}")


def upload_overall_to_s3():
    cfg = load_common_mapping()
    s3_cfg = cfg.get('s3_storage', {})
    raw_prefix_tpl = s3_cfg.get('raw_prefix', 'dsm_data/raw/{STATION}/{YEAR}/{MONTH}/{FILENAME}')
    pq_prefix_tpl = s3_cfg.get('parquet_prefix', 'dsm_data/parquet/{STATION}/{YEAR}/{MONTH}/{FILENAME}')

    uploader = AutoS3Uploader()
    base = Path('local_data/overall_common')
    if not base.exists():
        print('No overall_common directory found. Skipping S3 upload.')
        return

    uploaded = 0
    skipped = 0
    failed = 0

    for station_dir in sorted([d for d in base.iterdir() if d.is_dir()]):
        station = station_dir.name
        for f in station_dir.iterdir():
            if not f.is_file():
                continue
            ext = f.suffix.lower()
            ts = datetime.now()
            year = ts.year
            month = ts.month
            filename = f.name
            if ext == '.csv':
                key = raw_prefix_tpl.format(STATION=station, YEAR=year, MONTH=f"{month:02d}", FILENAME=filename)
            elif ext == '.parquet':
                key = pq_prefix_tpl.format(STATION=station, YEAR=year, MONTH=f"{month:02d}", FILENAME=filename)
            else:
                continue
            # Dedup via head_object (AutoS3Uploader exposes s3_client & bucket_name)
            try:
                uploader.s3_client.head_object(Bucket=uploader.bucket_name, Key=key)
                print(f"SKIP exists: s3://{uploader.bucket_name}/{key}")
                skipped += 1
                continue
            except Exception:
                pass
            try:
                uploader.auto_upload_file(str(f), original_filename=key)
                print(f"UPLOADED: s3://{uploader.bucket_name}/{key}")
                uploaded += 1
            except Exception as e:
                print(f"FAIL upload {f}: {e}")
                failed += 1

    print(json.dumps({'uploaded': uploaded, 'skipped': skipped, 'failed': failed}, indent=2))


def main():
    print('STEP 1: Run ERLDC/WRPC/SRPC/NERPC extractors')
    run_extractors()

    print('STEP 2: Build station-common from WRPC/ERLDC')
    build_wrpc_erldc_common()

    print('STEP 3: Build overall per-station (including NRLDC/SRPC if matching)')
    build_overall()

    print('STEP 4: Upload overall per-station files to S3 (station-first)')
    upload_overall_to_s3()

    print('DONE')


if __name__ == '__main__':
    main()


