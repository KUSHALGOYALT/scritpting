#!/usr/bin/env python3
import argparse
import sys
import os

# Ensure imports work when running from repo root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXTRACTORS_DIR = os.path.join(BASE_DIR, 'extractors')
sys.path.append(EXTRACTORS_DIR)

# Add each region subdirectory to sys.path for region helper imports
for region_dir in ['nerpc', 'nrldc', 'erldc', 'srpc', 'wrpc']:
    sys.path.append(os.path.join(EXTRACTORS_DIR, region_dir))

# Region-specific imports
from nerpc_extractor import NERPCDynamicExtractor  # in extractors/nerpc
from nrldc_extractor import NRLDCWorkingDSAExtractor  # in extractors/nrldc
from erldc_extractor import ERLDCDynamicExtractor  # in extractors/erldc
from srpc_extractor import SRPCExtractor  # in extractors/srpc
from wrpc_extractor import WRPCDynamicExtractor  # in extractors/wrpc


def run_nerpc():
    extractor = NERPCDynamicExtractor()
    return extractor.run_extraction()


def run_nrldc():
    extractor = NRLDCWorkingDSAExtractor()
    return extractor.run_extraction()


def run_erldc():
    extractor = ERLDCDynamicExtractor()
    return extractor.run_extraction()


def run_srpc():
    extractor = SRPCExtractor()
    return extractor.extract_past_7_days()


def run_wrpc():
    extractor = WRPCDynamicExtractor()
    return extractor.run_extraction()


def main():
    parser = argparse.ArgumentParser(description='Run DSM extractors by region')
    parser.add_argument('--regions', nargs='*', default=['ALL'],
                        help='Regions to run: NERPC, NRLDC, ERLDC, SRPC, WRPC or ALL')
    args = parser.parse_args()

    selected = [r.upper() for r in args.regions]
    if 'ALL' in selected:
        selected = ['NERPC', 'NRLDC', 'ERLDC', 'SRPC', 'WRPC']

    runners = {
        'NERPC': run_nerpc,
        'NRLDC': run_nrldc,
        'ERLDC': run_erldc,
        'SRPC': run_srpc,
        'WRPC': run_wrpc,
    }

    summary = {}
    for region in selected:
        if region not in runners:
            print(f"Skipping unknown region: {region}")
            continue
        print(f"\n=== Running {region} extractor ===")
        try:
            result = runners[region]()
            summary[region] = 'ok'
            print(f"=== {region} completed ===")
        except Exception as e:
            summary[region] = f"error: {e}"
            print(f"ERROR in {region}: {e}")

    print("\nSummary:")
    for k, v in summary.items():
        print(f"- {k}: {v}")


if __name__ == '__main__':
    main()
