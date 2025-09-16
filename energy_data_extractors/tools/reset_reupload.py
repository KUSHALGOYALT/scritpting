#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from datetime import datetime
import argparse
import json

import boto3
import pandas as pd

# Ensure repo root on path
# Add repository root (parent of 'energy_data_extractors') to sys.path
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from energy_data_extractors.common.auto_s3_upload import AutoS3Uploader
from energy_data_extractors.run_pipeline import run_extractors
from energy_data_extractors.tools.common_station_builder import build_common_files as build_wrpc_erldc_common
from energy_data_extractors.tools.overall_common_builder import build_overall_common as build_overall


def delete_s3_prefix(bucket: str, prefix: str, region: str | None = None) -> dict:
    session = boto3.session.Session(region_name=region)
    s3 = session.resource('s3')
    bucket_obj = s3.Bucket(bucket)

    deleted = 0
    to_delete = bucket_obj.objects.filter(Prefix=prefix)
    batch = []
    for obj in to_delete:
        batch.append({'Key': obj.key})
        if len(batch) == 1000:
            bucket_obj.delete_objects(Delete={'Objects': batch})
            deleted += len(batch)
            batch = []
    if batch:
        bucket_obj.delete_objects(Delete={'Objects': batch})
        deleted += len(batch)
    return {"deleted": deleted, "prefix": prefix}


def combine_overall_common(output_dir: Path) -> dict:
    if not output_dir.exists():
        return {"combined_rows": 0, "files": 0, "output": None}

    frames: list[pd.DataFrame] = []
    files = 0
    for station_dir in sorted([d for d in output_dir.iterdir() if d.is_dir()]):
        for f in station_dir.glob('*.csv'):
            try:
                df = pd.read_csv(f)
                frames.append(df)
                files += 1
            except Exception:
                continue

    if not frames:
        return {"combined_rows": 0, "files": 0, "output": None}

    combined = pd.concat(frames, ignore_index=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    combined_csv = output_dir / 'common_all_stations.csv'
    combined_parquet = output_dir / 'common_all_stations.parquet'

    # Write CSV
    combined.to_csv(combined_csv, index=False)
    # Write Parquet (coerce non-bytes to strings where needed)
    try:
        combined.to_parquet(combined_parquet, index=False, engine='pyarrow')
    except Exception:
        for col in combined.select_dtypes(include=['object']).columns:
            combined[col] = combined[col].astype(str)
        combined.to_parquet(combined_parquet, index=False, engine='pyarrow')

    return {"combined_rows": len(combined), "files": files, "output": str(combined_csv)}


def main():
    parser = argparse.ArgumentParser(description='Delete S3 dsm_data and re-upload all station data, then build a local common file.')
    parser.add_argument('--dry-run', action='store_true', help='Show actions without executing S3 deletions')
    parser.add_argument('--confirm', action='store_true', help='Required to actually delete S3 data')
    parser.add_argument('--region', default=os.getenv('AWS_REGION', None), help='AWS region override')
    args = parser.parse_args()

    uploader = AutoS3Uploader()
    if not getattr(uploader, 's3_client', None) or not getattr(uploader, 'bucket_name', None):
        print('ERROR: S3 client or bucket not initialized. Check .env or AWS credentials.')
        sys.exit(1)

    bucket = uploader.bucket_name
    region = args.region

    print(f"Bucket: {bucket}")
    prefixes = ['dsm_data/raw/', 'dsm_data/parquet/']

    summary = {"deleted": []}
    if args.dry_run:
        for p in prefixes:
            print(f"DRY-RUN: Would delete s3://{bucket}/{p}*")
    else:
        if not args.confirm:
            print('Refusing to delete without --confirm. Re-run with --confirm to proceed.')
            sys.exit(2)
        for p in prefixes:
            print(f"Deleting prefix: s3://{bucket}/{p}*")
            res = delete_s3_prefix(bucket, p, region=region)
            summary["deleted"].append(res)

    # Run full pipeline: extractors -> builders -> upload
    print('STEP 1: Run extractors')
    run_extractors()

    print('STEP 2: Build station-common from WRPC/ERLDC')
    build_wrpc_erldc_common()

    print('STEP 3: Build overall per-station (including NRLDC/SRPC/NERPC if matching)')
    build_overall()

    # Build combined common file locally
    print('STEP 4: Build combined common file for all stations locally')
    overall_dir = REPO_ROOT / 'local_data' / 'overall_common'
    combine_res = combine_overall_common(overall_dir)

    result = {
        "s3": summary,
        "combined": combine_res,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()


