# Databús Python SDK

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-black)](https://github.com/psf/black)

Python SDK and command-line toolkit for GTFS data processing, validation, and analysis. Provides programmatic access to Databús APIs, GTFS manipulation utilities, data conversion tools, and automated testing frameworks for transit data workflows and research applications.

## Features

### 🚌 GTFS Data Processing
- Load and manipulate GTFS feeds from ZIP files or directories
- Filter feeds by geographic bounds or date ranges
- Export processed feeds to various formats
- Statistical analysis and reporting

### ✅ Data Validation
- Comprehensive GTFS specification compliance checking
- Custom validation rules and quality metrics
- Detailed validation reports with scoring
- Integration with standard validation tools

### 🌐 API Integration
- Full access to Databús API endpoints
- Automatic feed discovery and metadata retrieval
- Bulk download and synchronization capabilities
- Rate limiting and retry mechanisms

### 🛠️ Command-Line Tools
- Intuitive CLI for common operations
- Rich output formatting and progress indicators
- Batch processing and automation support
- Integration with shell scripts and workflows

## Installation

### Using uv (recommended)

```bash
# Install from PyPI (when published)
uv pip install databus

# Install from source
git clone https://github.com/fabianabarca/databus-py.git
cd databus-py
uv pip install -e .
```

### Using pip

```bash
# Install from PyPI (when published)
pip install databus

# Install from source
git clone https://github.com/fabianabarca/databus-py.git
cd databus-py
pip install -e .
```

## Quick Start

### Python API

```python
from databus import DatabusClient, GTFSProcessor, GTFSValidator

# Connect to Databús API
client = DatabusClient("https://api.databus.cr")
feeds = client.get_feeds(country="CR")

# Process a GTFS feed
processor = GTFSProcessor("costa_rica_gtfs.zip")
processor.load_feed()

# Get feed statistics
stats = processor.get_feed_stats()
print(f"Routes: {stats['routes']}, Stops: {stats['stops']}")

# Validate the feed
validator = GTFSValidator(processor)
report = validator.validate()
print(f"Validation score: {report.score}/100")

# Filter by geographic area
san_jose_area = processor.filter_by_bounding_box(
    9.8, -84.2, 10.1, -83.9
)
san_jose_area.export_to_zip("san_jose_gtfs.zip")
```

### Command Line Interface

```bash
# List available feeds
databus api feeds --country CR

# Download a feed
databus api download costa-rica-gtfs

# Get feed information
databus gtfs info costa_rica_gtfs.zip

# Validate a feed
databus gtfs validate costa_rica_gtfs.zip

# Filter feed by bounding box
databus gtfs filter costa_rica_gtfs.zip san_jose.zip \
    --bbox "-84.2,9.8,-83.9,10.1"

# Filter by date range
databus gtfs filter costa_rica_gtfs.zip current_service.zip \
    --dates "2024-01-01,2024-12-31"
```

## Documentation

### Core Classes

#### DatabusClient

The main interface for interacting with Databús APIs:

```python
client = DatabusClient(
    base_url="https://api.databus.cr",
    api_key="your_api_key",  # Optional
    timeout=30
)

# Discover feeds
feeds = client.get_feeds()
costarica_feeds = client.get_feeds(country="CR")

# Get detailed feed information
feed = client.get_feed("costa-rica-gtfs")

# Access GTFS data
agencies = client.get_agencies("costa-rica-gtfs")
routes = client.get_routes("costa-rica-gtfs")
stops = client.get_stops("costa-rica-gtfs")

# Download feeds
client.download_feed("costa-rica-gtfs", "costa_rica.zip")
```

#### GTFSProcessor

Load, manipulate, and analyze GTFS feeds:

```python
processor = GTFSProcessor("feed.zip")
processor.load_feed()

# Access GTFS tables as DataFrames
routes = processor.get_routes()
stops = processor.get_stops(as_geodataframe=True)
trips = processor.get_trips(route_id="route_1")

# Get comprehensive statistics
stats = processor.get_feed_stats()
route_stats = processor.get_route_stats("route_1")

# Filter and transform
filtered = processor.filter_by_bounding_box(
    min_lat=9.8, min_lon=-84.2,
    max_lat=10.1, max_lon=-83.9
)
date_filtered = processor.filter_by_dates(
    "2024-01-01", "2024-12-31"
)

# Export results
processor.export_to_zip("processed_feed.zip")
```

#### GTFSValidator

Validate GTFS feeds for compliance and quality:

```python
validator = GTFSValidator(processor)
report = validator.validate()

print(f"Status: {report.status}")
print(f"Score: {report.score}/100")
print(f"Errors: {len(report.errors)}")
print(f"Warnings: {len(report.warnings)}")

# Access detailed issues
for error in report.errors:
    print(f"Error: {error['message']}")

# Save report
with open("validation_report.json", "w") as f:
    f.write(report.to_json())
```

### Configuration

Configure the library using environment variables or configuration files:

```bash
# Environment variables
export DATABUS_API_URL="https://api.databus.cr"
export DATABUS_API_KEY="your_api_key"
export DATABUS_LOG_LEVEL="INFO"
```

Or create a configuration file at `~/.databus/config.json`:

```json
{
  "api": {
    "base_url": "https://api.databus.cr",
    "api_key": "your_api_key",
    "timeout": 30
  },
  "logging": {
    "level": "INFO"
  },
  "processing": {
    "chunk_size": 10000
  }
}
```

## Development

### Setup Development Environment

```bash
git clone https://github.com/fabianabarca/databus-py.git
cd databus-py

# Install with development dependencies
uv pip install -e ".[dev,test]"

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=databus --cov-report=html

# Run specific test file
pytest tests/test_gtfs_processor.py
```

### Code Quality

```bash
# Format code
black src/databus tests/

# Sort imports
isort src/databus tests/

# Lint code
flake8 src/databus tests/

# Type checking
mypy src/databus
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and development process.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built on top of [gtfs-kit](https://github.com/mrcagney/gtfs_kit) for GTFS processing
- Uses [pandas](https://pandas.pydata.org/) and [geopandas](https://geopandas.org/) for data manipulation
- CLI powered by [click](https://click.palletsprojects.com/) and [rich](https://rich.readthedocs.io/)
- Validation framework inspired by [gtfs-validator](https://github.com/MobilityData/gtfs-validator)

## Related Projects

- [Databús](https://github.com/fabianabarca/databus) - The main Databús platform
- [GTFS Specification](https://gtfs.org/) - General Transit Feed Specification
- [OpenMobilityData](https://transitland.org/) - Global transit data platform
