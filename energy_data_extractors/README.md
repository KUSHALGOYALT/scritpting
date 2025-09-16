# Energy Data Extractors

Unified, cloud-native extractors for NERPC, NRLDC, ERLDC, SRPC, and WRPC. Raw files and per-station parquet outputs are uploaded directly to S3 using a standardized layout. No local/master datasets or regional remapping re-uploads are required.

## ✅ Key points
- S3-only outputs (no local persistence, no master CSVs)
- Standard S3 layout:
  - Raw: `dsm_data/raw/REGION/{year}/{month}/{filename}`
  - Parquet: `dsm_data/parquet/REGION/{station}/{year}/{month}/{file}.parquet`
- Robust discovery per region (HTML scraping, HEAD checks, or pattern generation)
- Station-aware processing; multi-entity files are split per station
- Duplicate/update-aware (skips or overwrites as appropriate)

## 📁 Project structure
```
energy_data_extractors/
├── README.md
├── run_all_extractors.py            # Unified runner
├── common/
│   ├── auto_s3_upload.py            # S3 upload utilities
│   └── parquet_processor.py         # Parquet helpers (used by regions as needed)
└── extractors/
    ├── nerpc/
    │   └── nerpc_extractor.py       # NERPCDynamicExtractor
    ├── nrldc/
    │   └── nrldc_extractor.py       # NRLDCWorkingDSAExtractor
    ├── erldc/
    │   └── erldc_extractor.py       # ERLDCDynamicExtractor
    ├── srpc/
    │   └── srpc_extractor.py        # SRPCExtractor
    └── wrpc/
        └── wrpc_extractor.py        # WRPCDynamicExtractor
   ```

## 🚀 Usage
From the repo root:

- Run all regions:
```
python energy_data_extractors/run_all_extractors.py
```

- Run selected regions:
```
python energy_data_extractors/run_all_extractors.py --regions NERPC ERLDC
```
Regions: `NERPC`, `NRLDC`, `ERLDC`, `SRPC`, `WRPC` (or `ALL`).

## 🔐 Configuration
- AWS credentials are expected to be available in the environment (or default profile) for `boto3`/S3 to work via `common/auto_s3_upload.py`.
- Optional env vars (if implemented in your environment):
  - `S3_ENABLED` (default true)
  - `S3_BUCKET`, `S3_REGION`

## 📦 Region extraction overview
- NERPC: BeautifulSoup to find "Data File" column links; ZIPs processed; per-station parquet.
- NRLDC: Flexible weekly filename discovery + revision handling; XLS processed; per-station parquet.
- ERLDC: XLS/XLSX discovery; per-sheet processing with robust type handling; per-station parquet.
- SRPC: URL pattern generation + HEAD checks for ZIPs; multi-entity split; per-station parquet.
- WRPC: ZIPs with many CSVs; consolidated then partitioned per station; parquet uploaded.

## 🗂️ Outputs
Directly to S3 (no local or master datasets):
- Raw: `dsm_data/raw/REGION/{year}/{month}/{filename}`
- Parquet: `dsm_data/parquet/REGION/{station}/{year}/{month}/{file}.parquet`

## 🛠️ Troubleshooting
- Ensure AWS credentials are valid and have write access to the S3 bucket.
- Network/SSL errors: some sites require `verify=False` (already handled where needed).
- If a site layout changes, region-specific discovery may need tweaks.

## 📄 License
MIT
