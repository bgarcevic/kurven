# Kurven

Open source framework for data collection from Danish supermarkets. This project provides a scalable pipeline for extracting product, pricing, and catalog data from multiple retail chains using modern data engineering tools.

## 🚀 Features

- **Multi-Chain Support**: Framework designed to support multiple Danish supermarket chains
- **Modular Architecture**: Easy to extend with new supermarket integrations
- **Automated Data Extraction**: Extract product catalogs, pricing, and store information
- **Data Pipeline**: Built with dlt (data loading tool) for robust, incremental data loading
- **DuckDB Storage**: Fast, analytical database storage with SQL capabilities
- **Danish Market Focus**: Specialized for the Danish retail ecosystem

## 📊 Data Collection Scope

- **Product Information**: Names, descriptions, pricing, categories
- **Store Data**: Department structures, category mappings, availability
- **Pricing Data**: Current prices, promotions, and historical tracking
- **Catalog Structure**: Complete product catalog organization

## 🛠️ Technology Stack

- **Python 3.12+**: Core programming language
- **dlt**: Data loading tool for pipeline orchestration
- **DuckDB**: Embedded analytical database for data storage
- **REST APIs & Web Scraping**: Multiple integration methods
- **Modular Pipeline Design**: Easy to add new data sources

## 📁 Project Structure

```
kurven/
├── extract/                    # Data extraction pipelines
│   ├── rema_pipeline.py       # REMA 1000 extraction pipeline
│   ├── pyproject.toml         # Python project configuration
│   ├── uv.lock               # Dependency lock file
│   ├── README.md             # Pipeline documentation
│   ├── .python-version       # Python version specification
│   └── .dlt/                 # dlt configuration
│       └── config.toml       # Pipeline configuration
├── data/                     # Generated data directory (created after run)
│   └── *.duckdb             # DuckDB databases for each chain
├── README.md                 # Project overview
└── LICENSE                   # Project license
```

## 🚀 Quick Start

### Prerequisites

- Python 3.12 or higher
- uv package manager (recommended) or pip

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd kurven
   ```

2. **Navigate to the extract directory:**
   ```bash
   cd extract
   ```

3. **Install dependencies:**
   ```bash
   # Using uv (recommended)
   uv sync

   # Or using pip
   pip install -e .
   ```

### Usage

1. **Run a data extraction pipeline:**
   ```bash
   cd extract
   python rema_pipeline.py
   ```

2. **The pipeline will:**
   - Connect to the supermarket's APIs or websites
   - Extract product and pricing data
   - Store structured data in DuckDB

3. **Query the extracted data:**
   ```python
   import duckdb

   # Connect to the database
   conn = duckdb.connect('data/rema.duckdb')

   # Query products
   result = conn.execute("""
       SELECT *
       FROM rest_api_data_rema.products
       LIMIT 10
   """).fetchall()

   print(result)
   ```

## 🔧 Configuration

The pipeline uses dlt's configuration system. Key settings can be modified in `extract/.dlt/config.toml`:

```toml
[runtime]
log_level = "INFO"  # Set to "DEBUG" for detailed logging
```

## 🏪 Adding New Supermarket Chains

The framework is designed to be extensible. To add support for a new Danish supermarket:

1. **Create a new pipeline file** (e.g., `coop_pipeline.py`, `netto_pipeline.py`)
2. **Implement the data source function** following the pattern in `rema_pipeline.py`
3. **Define the API endpoints and data extraction logic**
4. **Configure the dlt pipeline settings**

### Current Integrations

- **REMA 1000**: Complete integration with catalog API and Algolia search
- *Future chains can be added following the same pattern*

## 📊 Data Schema

The extracted data typically includes:

- **departments**: Store department and category structure
- **products**: Individual product details and pricing
- **pricing_history**: Historical price tracking (when available)
- **store_info**: Store location and operational data

## 🤝 Contributing

We welcome contributions to expand Danish supermarket coverage!

### Adding a New Chain

1. **Research the target supermarket's data sources**
2. **Identify available APIs or web scraping opportunities**
3. **Create a new pipeline module**
4. **Test the integration thoroughly**
5. **Submit a pull request**

### Development Guidelines

```bash
# Install in development mode
cd extract
uv sync --dev

# Run with debug logging
DLT_LOG_LEVEL=DEBUG python your_pipeline.py
```

## ⚖️ License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.

## 🔄 Future Enhancements

- **Additional Chains**: Aldi, Lidl, Coop, Netto, Føtex, and others
- **Data Quality**: Validation and cleansing pipelines
- **Web Dashboard**: User interface for data exploration
- **API Access**: RESTful API for accessing processed data
- **Price History**: Comprehensive historical price tracking
- **Comparison Tools**: Price comparison across chains
- **Data Enrichment**: Enhance product data with external sources

## 📈 Use Cases

- **Price Comparison**: Compare prices across Danish supermarket chains
- **Market Research**: Analyze product availability and pricing trends
- **Consumer Tools**: Build applications for smart shopping
- **Academic Research**: Study Danish retail market dynamics
- **Business Intelligence**: Retail market analysis and insights
