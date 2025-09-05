# Regional Energy Data Extraction & File Mapping Report

**Generated on:** September 4, 2025  
**Report for:** Manager Review  
**System:** NRLDC & ERLDC Data Extraction with Region Mapping

---

## Executive Summary

This report provides a comprehensive overview of the energy data extraction system with regional mapping capabilities for both Northern Regional Load Dispatch Centre (NRLDC) and Eastern Regional Load Dispatch Centre (ERLDC) data sources.

---

## 1. NRLDC (Northern Regional) Data Extraction

### ✅ Status: **OPERATIONAL**

### Regional Coverage:
- **Total Stations Mapped:** 120 power stations
- **States Covered:** 9 states
- **Regional Groups:** 6 distinct geographical zones

### State-wise Distribution:
| State | Records | Percentage |
|-------|---------|------------|
| Uttar Pradesh | 504 | 25.1% |
| Himachal Pradesh | 413 | 20.6% |
| Rajasthan | 210 | 10.5% |
| Uttarakhand | 154 | 7.7% |
| Jammu & Kashmir | 140 | 7.0% |
| Madhya Pradesh | 112 | 5.6% |
| Haryana | 28 | 1.4% |
| Multi-State | 28 | 1.4% |
| Unknown | 424 | 21.1% |

### Regional Group Distribution:
| Regional Group | Records | Coverage Area |
|----------------|---------|---------------|
| Northern Hills | 707 | Himachal Pradesh, J&K, Uttarakhand |
| Northern Plains | 532 | Uttar Pradesh, Haryana |
| Western | 210 | Rajasthan |
| Central | 112 | Madhya Pradesh |
| Multi-State | 28 | Cross-state entities |
| Unknown | 424 | Unmapped stations |

### File Structure:
```
master_data/NRLDC/
├── NRLDC_Master_Dataset_20250904_161607.csv (1.1 MB)
├── NRLDC_Summary.json (513 bytes)
└── processed_weeks.json (1.2 KB)
```

---

## 2. ERLDC (Eastern Regional) Data Extraction

### ⚠️ Status: **CONFIGURED - AWAITING DATA**

### Regional Coverage (Configured):
- **Total Stations Mapped:** 38 power stations
- **States Covered:** 5 states
- **Regional Groups:** 5 distinct geographical zones

### Configured State Mapping:
| State | Stations | Regional Group |
|-------|----------|----------------|
| Bihar | 8 stations | Eastern Plains |
| West Bengal | 12 stations | Eastern Plains |
| Jharkhand | 7 stations | Eastern Plateau |
| Odisha | 9 stations | Eastern Coastal |
| Sikkim | 1 station | Eastern Hills |
| Multi-State | 1 entity | Multi-State |

### Regional Group Configuration:
| Regional Group | States Covered | Characteristics |
|----------------|----------------|-----------------|
| Eastern Plains | Bihar, West Bengal | Flat terrain, high population density |
| Eastern Plateau | Jharkhand | Mineral-rich plateau region |
| Eastern Coastal | Odisha | Coastal areas, ports |
| Eastern Hills | Sikkim | Mountainous terrain |
| Multi-State | Cross-boundary | DVC and similar entities |

---

## 3. NRLDC (Northern Regional Load Dispatch Centre)

**Status**: ✅ OPERATIONAL  
**Last Updated**: 2025-09-04 17:25:27  
**Data Coverage**: July 29 - August 17, 2025  
**Mapping Coverage**: 99.8% (Near 100% coverage achieved!)  

### Current Data Status
- **Total Records**: 2,013
- **Unique Stations**: 120
- **States Covered**: 22 states + 10 regional groups
- **File Format**: Supporting_files.xls → CSV conversion
- **Master Dataset**: `NRLDC_Master_Dataset_20250904_172527.csv`

### Regional Coverage
- **Northern Hills**: Himachal Pradesh, Jammu & Kashmir, Uttarakhand (707 records)
- **Northern Plains**: Uttar Pradesh, Haryana (532 records)
- **Western**: Rajasthan, Gujarat (294 records)
- **Central**: Madhya Pradesh, Chhattisgarh, Maharashtra (231 records)
- **Southern**: Karnataka, Tamil Nadu, Andhra Pradesh, Telangana (119 records)
- **Eastern**: West Bengal, Bihar, Odisha, Assam (77 records)
- **North Eastern**: Arunachal Pradesh (14 records)
- **Cross-Border**: Bhutan (7 records)
- **Multi-State**: Cross-regional stations (28 records)
- **Unknown**: Only 4 records remaining unmapped

### Data Quality
- ✅ Automated XLS to CSV conversion
- ✅ Week-based file organization
- ✅ Duplicate handling and file replacement
- ✅ Enhanced region mapping with 50+ additional stations
- ✅ 99.8% mapping coverage achieved
| Multi-State | Cross-boundary | NTPC, NHPC entities |

### File Structure:
```
master_data/WRPC/
├── WRPC_MASTER_DATASET_20250904_165806.csv (104 KB)
└── WRPC_MASTER_SUMMARY.json (629 bytes)
```

---

## 4. Technical Implementation

### Region Mapping System:
- **NRLDC Region Mapper:** 47 stations mapped across 6 regional groups
- **ERLDC Region Mapper:** 38 stations mapped across 5 regional groups
- **WRPC Region Mapper:** 42 stations mapped across 5 regional groups
- **Automatic Enrichment:** All extracted data includes region information
- **Fallback Handling:** Unknown stations marked for manual review

### Data Processing Pipeline:
1. **Source Extraction:** Download from official websites
2. **Format Conversion:** Excel to CSV with region enrichment
3. **Master Dataset Creation:** Consolidated regional data
4. **Summary Generation:** Statistical breakdowns by region

### File Naming Convention:
```
{REGION}_Master_Dataset_{YYYYMMDD}_{HHMMSS}.csv
{REGION}_Summary.json
{REGION}_Region_Summary.json
```

---

## 4. Data Quality & Coverage

### NRLDC Data Quality:
- ✅ **2,013 total records** processed
- ✅ **79% stations mapped** to specific regions
- ⚠️ **21% unmapped stations** require manual review
- ✅ **Complete geographical coverage** of Northern region

### ERLDC Data Quality:
- ✅ **Region mapper ready** for 38 Eastern stations
- ✅ **Complete state coverage** of Eastern region
- ⚠️ **Data extraction pending** due to website access issues
- ✅ **Fallback mechanisms** implemented

---

## 5. Regional Analysis Capabilities

### Available Analytics:
1. **State-wise Power Generation Analysis**
2. **Regional Load Distribution Studies**
3. **Cross-regional Comparative Analysis**
4. **Geographical Energy Flow Mapping**
5. **Regional Deviation Pattern Analysis**

### Business Intelligence Features:
- Real-time regional summaries
- Automated weekly data updates
- Historical trend analysis by region
- State-wise performance metrics

---

## 6. File Organization for Manager Review

### Current File Structure:
```
energy_data_extractors/
├── master_data/
│   ├── NRLDC/
│   │   ├── NRLDC_Master_Dataset_20250904_161607.csv
│   │   ├── NRLDC_Summary.json
│   │   └── processed_weeks.json
│   ├── ERLDC/
│   │   └── [Pending data extraction]
│   └── WRPC/
│       ├── WRPC_MASTER_DATASET_20250904_165806.csv
│       └── WRPC_MASTER_SUMMARY.json
├── extractors/
│   ├── nrldc/
│   │   ├── nrldc_extractor.py
│   │   └── nrldc_region_mapper.py
│   ├── erldc/
│   │   ├── erldc_extractor.py
│   │   ├── erldc_direct_extractor.py
│   │   └── erldc_region_mapper.py
│   └── wrpc/
│       ├── wrpc_extractor.py
│       └── wrpc_region_mapper.py
└── REGIONAL_FILE_MAPPING_REPORT.md (this file)
```

---

## 7. Recommendations

### Immediate Actions:
1. **Review unmapped NRLDC stations** (21% of data)
2. **Resolve ERLDC data access** issues
3. **Implement automated scheduling** for daily extractions
4. **Set up monitoring alerts** for extraction failures

### Strategic Improvements:
1. **Expand region mapping** to include sub-regional classifications
2. **Implement data validation** rules for quality assurance
3. **Create dashboard** for real-time regional monitoring
4. **Establish backup data sources** for reliability

---

## 8. Contact & Support

**System Administrator:** Energy Data Team  
**Last Updated:** September 4, 2025  
**Next Review:** Weekly (every Monday)  
**Documentation:** Available in project repository

---

*This report is automatically generated and updated with each data extraction cycle.*
