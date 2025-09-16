#!/usr/bin/env python3
import os
import re
import json
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


def normalize_columns(df: pd.DataFrame, source: str) -> pd.DataFrame:
    mapping: Dict[str, str] = {
        # common
        'Date': 'Date',
        'Time': 'Time',
        'Block': 'Block',
        'Station_Name': 'Station_Name',
        'Processing_Date': 'Processing_Date',
        'Region': 'Region',
        'Sheet_Name': 'Sheet_Name',
        'Source_File': 'Source_File',
        # frequency
        'Freq(Hz)': 'Freq_Hz',
        'Freq (Hz)': 'Freq_Hz',
        # energy
        'Actual (MWH)': 'Actual_MWh',
        'Schedule (MWH)': 'Schedule_MWh',
        'SRAS (MWH)': 'SRAS_MWh',
        'Deviation(MWH)': 'Deviation_MWh',
        'Deviation (MWH)': 'Deviation_MWh',
        'Deviation (%)': 'Deviation_Pct',
        'DSM Payable (Rs.)': 'DSM_Payable_Rs',
        'DSM Receivable (Rs.)': 'DSM_Receivable_Rs',
        # rates
        'Normal Rate (p/Kwh)': 'Normal_Rate_p_per_kWh',
        'Normal DSM Rate \nApplicable (p/KWH)': 'Normal_Rate_p_per_kWh',
        'Reference DSM Rate \\n+Applicable (p/KWH)': 'Reference_DSM_Rate_p_per_kWh',
        'Wt. Avg. Hybrid Rate (p/Kwh)': 'Wt_Avg_Hybrid_Rate_p_per_kWh',
        'Wt.Avg. DSM Rate (Hybrid Gen) \\n+Applicable (p/KWH)': 'Wt_Avg_Hybrid_Rate_p_per_kWh',
        'Variable DSM Rate (ISGS) \nApplicable (p/KWH)': 'Variable_DSM_Rate_ISGS_p_per_kWh',
        'Contract Rate (RE Gen) \nApplicable (Rs./MWH)': 'Contract_Rate_RE_Rs_per_MWh',
        # wrpc-specific
        'HPDAM Ref. Rate (p/Kwh)': 'HPDAM_Ref_Rate_p_per_kWh',
        'HPDAM Normal Rate (p/Kwh)': 'HPDAM_Normal_Rate_p_per_kWh',
        'Constituents': 'Constituents',
    }

    # Apply mapping where possible
    new_cols: List[str] = []
    for c in df.columns:
        if c in mapping:
            new_cols.append(mapping[c])
        else:
            # sanitize exotic whitespace to compare
            c_norm = c.replace('\r', '').replace('\n', ' ').strip()
            found = None
            for k, v in mapping.items():
                k_norm = k.replace('\\n', ' ').replace('\n', ' ').strip()
                if c_norm == k_norm:
                    found = v
                    break
            new_cols.append(found if found else c)
    df.columns = new_cols

    # Enrich
    df['Data_Source'] = source
    if 'Region' not in df.columns:
        df['Region'] = source
    return df


def build_common_files():
    base = Path('local_data')
    wrpc_dir = base / 'WRPC'
    erldc_dir = base / 'ERLDC'
    srpc_dir = base / 'SRPC'
    out_dir = base / 'common'
    out_dir.mkdir(parents=True, exist_ok=True)

    station_to_frames: Dict[str, List[pd.DataFrame]] = {}

    # Ingest WRPC
    if wrpc_dir.exists():
        for csv_path in sorted(wrpc_dir.glob('*.csv')):
            try:
                df = pd.read_csv(csv_path, low_memory=False)
                df = drop_unnamed(df)
                df = normalize_columns(df, 'WRPC')
                # ensure station
                if 'Station_Name' in df.columns:
                    station = canonicalize_station_name(df['Station_Name'].iloc[0])
                else:
                    station = canonicalize_station_name(csv_path.stem.split('_')[1])
                df['Station_Name'] = station
                station_to_frames.setdefault(station, []).append(df)
            except Exception:
                continue

    # Ingest ERLDC
    if erldc_dir.exists():
        for csv_path in sorted(erldc_dir.glob('*.csv')):
            try:
                df = pd.read_csv(csv_path, low_memory=False)
                df = drop_unnamed(df)
                df = normalize_columns(df, 'ERLDC')
                if 'Station_Name' in df.columns:
                    station = canonicalize_station_name(df['Station_Name'].iloc[0])
                else:
                    # attempt from filename
                    parts = csv_path.stem.split('_')
                    station = canonicalize_station_name(parts[1] if len(parts) > 1 else csv_path.stem)
                df['Station_Name'] = station
                station_to_frames.setdefault(station, []).append(df)
            except Exception:
                continue

    # Ingest SRPC
    if srpc_dir.exists():
        for csv_path in sorted(srpc_dir.glob('*.csv')):
            try:
                df = pd.read_csv(csv_path, low_memory=False)
                df = drop_unnamed(df)
                df = normalize_columns(df, 'SRPC')
                if 'Station_Name' in df.columns:
                    station = canonicalize_station_name(df['Station_Name'].iloc[0])
                else:
                    # attempt from filename
                    parts = csv_path.stem.split('_')
                    station = canonicalize_station_name(parts[1] if len(parts) > 1 else csv_path.stem)
                df['Station_Name'] = station
                station_to_frames.setdefault(station, []).append(df)
            except Exception:
                continue

    # Write per-station common files
    summary = []
    for station, frames in station_to_frames.items():
        try:
            combined = pd.concat(frames, ignore_index=True, sort=False)
            # sort for readability
            sort_cols = [c for c in ['Date', 'Time', 'Block'] if c in combined.columns]
            if sort_cols:
                combined = combined.sort_values(sort_cols)
            # output
            station_dir = out_dir / station
            station_dir.mkdir(parents=True, exist_ok=True)
            csv_out = station_dir / f"{station}_COMMON.csv"
            pq_out = station_dir / f"{station}_COMMON.parquet"
            combined.to_csv(csv_out, index=False)
            try:
                combined.to_parquet(pq_out, index=False)
            except Exception:
                pass
            summary.append({'station': station, 'rows': len(combined), 'files': len(frames)})
        except Exception:
            continue

    print(json.dumps({
        'stations': len(summary),
        'total_rows': int(sum(s['rows'] for s in summary)),
        'details': summary[:20]
    }, indent=2))


if __name__ == '__main__':
    build_common_files()


