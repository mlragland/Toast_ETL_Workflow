# 🍴 Toast ETL Pipeline

A modernized, scalable ETL pipeline for processing Toast POS data from SFTP to BigQuery.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Google Cloud SDK
- Access to Toast SFTP server
- Google Cloud Project with BigQuery and Storage enabled

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd Toast_ETL_Workflow

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration
```

### Basic Usage
```bash
# Run full ETL pipeline for yesterday's data
python main.py

# Run for specific date
python main.py --date 20241210

# Run only extraction
python main.py --extract-only --date 20241210

# Run with debug logging
python main.py --debug
```

## 📁 Project Structure

```
Toast_ETL_Workflow/
├── src/
│   ├── config/           # Configuration management
│   │   ├── settings.py   # Environment-based settings
│   │   └── file_config.py # File transformation configurations
│   ├── extractors/       # Data extraction modules
│   │   └── sftp_extractor.py # SFTP data extraction
│   ├── transformers/     # Data transformation modules
│   │   ├── base_transformer.py # Base transformation class
│   │   └── csv_transformer.py  # CSV data transformations
│   ├── loaders/          # Data loading modules
│   │   ├── gcs_loader.py     # Google Cloud Storage loader
│   │   └── bigquery_loader.py # BigQuery loader
│   └── utils/            # Utility functions
│       ├── logging_utils.py # Logging configuration
│       ├── retry_utils.py   # Retry mechanisms
│       └── time_utils.py    # Time conversion utilities
├── tests/               # Test suite
│   ├── unit/           # Unit tests
│   └── integration/    # Integration tests
├── docs/               # Documentation
├── infrastructure/     # Infrastructure as Code
├── main.py            # Main entry point
├── requirements.txt   # Python dependencies
└── README.md         # This file
```

## 🔧 Configuration

### Environment Variables
Create a `.env` file with the following variables:

```bash
# SFTP Configuration
SFTP_USER=LoveExportUser
SFTP_SERVER=s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com
SSH_KEY_PATH=~/.ssh/toast_ssh

# Google Cloud Configuration
GCP_PROJECT_ID=toast-analytics-444116
GCS_BUCKET_NAME=toast-raw-data
BIGQUERY_DATASET=toast_analytics

# Processing Configuration
ENVIRONMENT=development
DEBUG=true
MAX_RETRY_ATTEMPTS=3
```

## 📊 Data Processing

### Supported Data Files
The pipeline processes the following Toast POS CSV files:

1. **AllItemsReport.csv** - Menu items and sales data
2. **CheckDetails.csv** - Customer check information  
3. **CashEntries.csv** - Cash drawer transactions
4. **ItemSelectionDetails.csv** - Detailed item selections
5. **KitchenTimings.csv** - Kitchen performance metrics
6. **OrderDetails.csv** - Order summary information
7. **PaymentDetails.csv** - Payment transaction details

### Data Transformations
- Column name standardization (snake_case)
- Date/time format standardization
- Data type conversions
- Missing value handling
- Business rule validation

## 🔄 Pipeline Phases

### 1. Extract
- Downloads files from Toast SFTP server
- Validates SSH connection and file availability
- Implements retry logic for network failures

### 2. Transform
- Applies standardized column mappings
- Converts date/time formats
- Handles data type conversions
- Adds processing metadata

### 3. Load
- Uploads cleaned data to Google Cloud Storage
- Loads data into BigQuery tables
- Validates data quality and completeness

## 🧪 Testing

```bash
# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/

# Run all tests with coverage
pytest --cov=src --cov-report=html
```

## 📝 Logging

The pipeline uses structured logging with both console and Google Cloud Logging:

- **Console Logging**: Always enabled for local development
- **Cloud Logging**: Enabled in staging/production environments
- **Log Levels**: DEBUG, INFO, WARNING, ERROR
- **Correlation IDs**: Track requests across the pipeline

## 🔒 Security

- SSH key-based SFTP authentication
- Google Cloud IAM for resource access
- Secrets management via environment variables
- Data encryption in transit and at rest

## 🚨 Error Handling

- Comprehensive exception handling
- Exponential backoff retry logic
- Circuit breaker patterns
- Graceful degradation strategies

## 📈 Monitoring

- Pipeline execution metrics
- Data quality monitoring
- Performance tracking
- Alerting via Pub/Sub

## 🔄 Deployment

See the [Development Plan Checklist](development_plan_checklist.md) for detailed deployment steps.

### Quick Deploy (Development)
```bash
# Set environment
export ENVIRONMENT=development

# Run pipeline
python main.py --debug
```

## 🤝 Contributing

1. Follow the development checklist in `development_plan_checklist.md`
2. Write tests for new functionality
3. Update documentation
4. Follow Python coding standards (Black, Flake8)

## 📞 Support

For issues and questions:
- Check the troubleshooting guide in `/docs`
- Review logs in Google Cloud Logging
- Contact the data engineering team

## 🏗️ Architecture

This modernized pipeline replaces the legacy monolithic script with:

- **Modular Design**: Separate extraction, transformation, and loading components
- **Cloud-Native**: Built for Google Cloud Platform
- **Scalable**: Handles increasing data volumes
- **Reliable**: Comprehensive error handling and retry logic
- **Observable**: Extensive logging and monitoring
- **Testable**: Unit and integration test coverage

## 📚 Legacy Migration

The original `manual_etl_script.py` has been refactored into this modular architecture while maintaining all existing functionality and improving:

- Code maintainability
- Error handling
- Performance
- Monitoring
- Testing capabilities

---

**Next Steps**: See `development_plan_checklist.md` for the complete modernization roadmap. 