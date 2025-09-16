#!/usr/bin/env python3
import os
import re
import json
from glob import glob
from pathlib import Path
from typing import Dict, List

import pandas as pd


def canonicalize_station_name(name: str) -> str:
    if name is None:
        return ''
    s = str(name).strip()
    s = s.replace('/', '_').replace('\\', '_')
    s = re.sub(r"\s+", "_", s)
    return s.upper()


def drop_unnamed(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.str.startswith('Unnamed')]


def load_common_station(station_dir: Path) -> pd.DataFrame:
    csvs = sorted(station_dir.glob('*_COMMON.csv'))
    frames: List[pd.DataFrame] = []
    for p in csvs:
        try:
            df = pd.read_csv(p, low_memory=False)
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def find_nrldc_files_for_station(nrldc_dir: Path, station: str) -> List[Path]:
    # NRLDC files often start with NRLDC_{STATION}_*
    pattern = f"NRLDC_{station}_*.csv"
    return [Path(p) for p in glob(str(nrldc_dir / pattern))]

def find_srpc_files_for_station(srpc_dir: Path, station: str) -> List[Path]:
    # SRPC files often start with SRPC_{STATION}_*
    pattern = f"SRPC_{station}_*.csv"
    return [Path(p) for p in glob(str(srpc_dir / pattern))]


def load_nrldc_station(nrldc_dir: Path, station: str) -> pd.DataFrame:
    files = find_nrldc_files_for_station(nrldc_dir, station)
    frames: List[pd.DataFrame] = []
    for p in files:
        try:
            df = pd.read_csv(p, low_memory=False)
            df = drop_unnamed(df)
            df['Station_Name'] = station
            if 'Region' not in df.columns:
                df['Region'] = 'NRLDC'
            if 'Data_Source' not in df.columns:
                df['Data_Source'] = 'NRLDC'
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

def load_srpc_station(srpc_dir: Path, station: str) -> pd.DataFrame:
    files = find_srpc_files_for_station(srpc_dir, station)
    frames: List[pd.DataFrame] = []
    for p in files:
        try:
            df = pd.read_csv(p, low_memory=False)
            df = drop_unnamed(df)
            df['Station_Name'] = station
            if 'Region' not in df.columns:
                df['Region'] = 'SRPC'
            if 'Data_Source' not in df.columns:
                df['Data_Source'] = 'SRPC'
            frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def build_overall_common():
    base = Path('local_data')
    common_dir = base / 'common'
    nrldc_dir = base / 'NRLDC'
    srpc_dir = base / 'SRPC'
    out_dir = base / 'overall_common'
    out_dir.mkdir(parents=True, exist_ok=True)

    if not common_dir.exists():
        print(json.dumps({'error': 'local_data/common not found. Run common_station_builder first.'}))
        return

    stations = [d.name for d in common_dir.iterdir() if d.is_dir()]

    summary = []
    for station in stations:
        station_common_dir = common_dir / station
        df_common = load_common_station(station_common_dir)
        df_nrldc = load_nrldc_station(nrldc_dir, station) if nrldc_dir.exists() else pd.DataFrame()
        df_srpc = load_srpc_station(srpc_dir, station) if srpc_dir.exists() else pd.DataFrame()

        frames = []
        if not df_common.empty:
            # Add origin tag for clarity
            if 'Origin' not in df_common.columns:
                df_common['Origin'] = df_common.get('Region', 'WRPC/ERLDC/SRPC')
            frames.append(df_common)
        if not df_nrldc.empty:
            if 'Origin' not in df_nrldc.columns:
                df_nrldc['Origin'] = 'NRLDC'
            frames.append(df_nrldc)
        if not df_srpc.empty:
            if 'Origin' not in df_srpc.columns:
                df_srpc['Origin'] = 'SRPC'
            frames.append(df_srpc)

        if not frames:
            continue

        combined = pd.concat(frames, ignore_index=True, sort=False)

        # Sort if possible
        sort_cols = [c for c in ['Date', 'Time', 'Block', 'Sheet_Type'] if c in combined.columns]
        if sort_cols:
            try:
                combined = combined.sort_values(sort_cols)
            except Exception:
                pass

        station_dir = out_dir / station
        station_dir.mkdir(parents=True, exist_ok=True)
        csv_out = station_dir / f"{station}_OVERALL_COMMON.csv"
        pq_out = station_dir / f"{station}_OVERALL_COMMON.parquet"
        combined.to_csv(csv_out, index=False)
        try:
            combined.to_parquet(pq_out, index=False)
        except Exception:
            pass

        summary.append({
            'station': station, 
            'rows': len(combined), 
            'has_wrpc_erldc_srpc': int(not df_common.empty), 
            'has_nrldc': int(not df_nrldc.empty),
            'has_srpc': int(not df_srpc.empty)
        })

    print(json.dumps({'stations': len(summary), 'details': summary[:20]}, indent=2))


if __name__ == '__main__':
    build_overall_common()


