# Energy Data Extractors

Enhanced energy data extraction system for NRLDC, ERLDC, and WRPC regions with intelligent data management and master dataset creation.

## 🚀 Enhanced Features

### **Past 7 Days Extraction**
- Automatically calculates and processes data for the past 7 days
- Smart week boundary detection (Monday to Sunday)
- Efficient date range processing

### **Update Handling**
- Tracks processed weeks to avoid duplicates
- Automatically detects and handles updates for existing weeks
- Maintains processing history with timestamps

### **Individual Master Datasets**
- Each region creates its own master dataset
- Combines all processed data with metadata
- Generates comprehensive summaries

### **Global Master Dataset**
- Combines data from all regions
- Cross-region analysis capabilities
- Unified data structure

## 📁 Project Structure

```
energy_data_extractors/
├── README.md                           # This file
├── requirements.txt                    # Python dependencies
├── run_extractors.py                  # Main runner script
├── test_enhanced_extractors.py        # Test script for enhanced functionality
├── common/                            # Shared utilities
│   ├── auto_s3_upload.py            # S3 upload functionality
│   └── parquet_processor.py         # Data processing utilities
└── extractors/                        # Core extractors
    ├── nrldc/
    │   └── nrldc_extractor.py       # NRLDC data extractor
    ├── erldc/
    │   └── erldc_extractor.py       # ERLDC data extractor
    └── wrpc/
        └── wrpc_extractor.py        # WRPC data extractor
```

## 🔧 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd energy_data_extractors
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation**
   ```bash
   python test_enhanced_extractors.py
   ```

## 🚀 Usage

### **Run All Extractors**
```bash
python run_extractors.py
```

### **Run Specific Region**
```bash
python run_extractors.py --region NRLDC
python run_extractors.py --region ERLDC
python run_extractors.py --region WRPC
```

### **Check Status**
```bash
python run_extractors.py --status
```

### **Run Tests**
```bash
python test_enhanced_extractors.py
```

## 📊 Data Flow

### **1. Past 7 Days Processing**
```
Current Date → Calculate Week Boundaries → Process Each Week → Update Detection
```

### **2. Update Handling**
```
Week Already Processed? → Check Timestamp → Update if Newer → Skip if Same
```

### **3. Master Dataset Creation**
```
Individual Files → Combine Data → Add Metadata → Create Summary → Save Master Dataset
```

### **4. Global Integration**
```
Regional Master Datasets → Combine → Cross-Region Analysis → Global Master Dataset
```

## 🔍 Key Components

### **NRLDC Extractor**
- **Data Source**: NRLDC website (http://164.100.60.165)
- **Data Type**: DSA (Day Ahead Schedule) data
- **Format**: PDF → CSV conversion with realistic data generation
- **Entities**: 6 states (Delhi, Haryana, Punjab, Rajasthan, UP, Uttarakhand)

### **ERLDC Extractor**
- **Data Source**: ERPC website (https://erpc.gov.in)
- **Data Type**: Dynamic discovery of .xlsx files
- **Format**: Excel files with smart pattern recognition
- **Features**: Fast scanning with early stopping

### **WRPC Extractor**
- **Data Source**: WRPC API (https://www.wrpc.gov.in/api/TopMenu/342)
- **Data Type**: ZIP files containing CSV data
- **Format**: ZIP extraction with CSV processing
- **Features**: API-based data discovery

## 📈 Data Structure

### **Standard Columns**
- `Date`: Date in YYYY-MM-DD format
- `Time`: Time in HH:MM format (15-minute blocks)
- `Block`: Block number (1-96 per day)
- `Freq(Hz)`: Frequency variation around 50Hz
- `Constituents`: Entity name
- `Actual (MWH)`: Actual energy values
- `Schedule (MWH)`: Scheduled energy values
- `SRAS (MWH)`: Secondary Reserve Ancillary Service
- `Deviation(MWH)`: Deviation from schedule
- `Deviation (%)`: Percentage deviation
- `DSM Payable (Rs.)`: DSM charges payable
- `DSM Receivable (Rs.)`: DSM charges receivable
- `Normal Rate (p/Kwh)`: Normal energy rate
- `Region`: Region identifier
- `Entity Code`: Entity code
- `Week`: Week information
- `Processing_Date`: Data processing timestamp

### **Metadata Columns**
- `Source_File`: Original file name
- `Master_Dataset_Created`: Master dataset creation timestamp
- `Total_Records`: Total record count
- `Global_Region`: Region identifier in global dataset
- `Global_Source_File`: Source file in global context

## 🗂️ Output Structure

### **Local Data**
```
local_data/
├── NRLDC/                    # NRLDC raw data files
├── ERLDC/                    # ERLDC raw data files
└── WRPC/                     # WRPC raw data files
```

### **Master Data**
```
master_data/
├── NRLDC/                    # NRLDC master datasets
│   ├── NRLDC_Master_Dataset_*.csv
│   ├── NRLDC_Summary.json
│   └── processed_weeks.json
├── ERLDC/                    # ERLDC master datasets
│   ├── ERLDC_Master_Dataset_*.csv
│   ├── ERLDC_Summary.json
│   └── processed_weeks.json
├── WRPC/                     # WRPC master datasets
│   ├── WRPC_Master_Dataset_*.csv
│   ├── WRPC_Summary.json
│   └── processed_weeks.json
└── GLOBAL/                   # Global master datasets
    ├── GLOBAL_Master_Dataset_*.csv
    └── GLOBAL_Summary.json
```

## 🔄 Update Process

### **Weekly Updates**
1. **Detection**: System detects new weeks or updates
2. **Download**: Downloads new/updated data files
3. **Processing**: Converts and processes data
4. **Integration**: Updates master datasets
5. **Tracking**: Records processing history

### **Duplicate Prevention**
- Tracks processed weeks with timestamps
- Skips already processed weeks
- Updates existing weeks when newer data available

## 📊 Data Quality Features

### **Validation**
- Date range validation
- Data type checking
- Missing value handling
- Outlier detection

### **Enrichment**
- Metadata addition
- Processing timestamps
- Source file tracking
- Region identification

## 🚀 Performance Features

### **Optimization**
- Smart early stopping
- Priority-based processing
- Efficient file handling
- Memory management

### **Scalability**
- Modular architecture
- Configurable processing
- Resource monitoring
- Error handling

## 🧪 Testing

### **Test Coverage**
- Past 7 days calculation
- Processed weeks tracking
- Master dataset creation
- Week update handling

### **Run Tests**
```bash
python test_enhanced_extractors.py
```

## 🔧 Configuration

### **Environment Variables**
- `S3_ENABLED`: Enable/disable S3 uploads
- `S3_BUCKET`: S3 bucket name
- `S3_REGION`: AWS region

### **Logging**
- Configurable log levels
- Structured logging format
- Error tracking and reporting

## 📈 Monitoring

### **Status Checking**
```bash
python run_extractors.py --status
```

### **Metrics**
- Files processed
- Records generated
- Processing time
- Error rates

## 🚨 Troubleshooting

### **Common Issues**
1. **Network Errors**: Check internet connectivity
2. **File Permissions**: Ensure write access to directories
3. **Dependencies**: Verify all packages installed
4. **Data Sources**: Check website accessibility

### **Debug Mode**
```bash
export LOG_LEVEL=DEBUG
python run_extractors.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the test results

## 🔮 Future Enhancements

- Real-time data streaming
- Advanced analytics integration
- Machine learning capabilities
- API endpoints for data access
- Dashboard for monitoring
- Automated scheduling
- Cloud deployment options
