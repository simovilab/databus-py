"""Pytest configuration and shared fixtures."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import gtfs_kit as gk

from databus.api import DatabusClient
from databus.gtfs import GTFSProcessor
from databus.utils.config import Config


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    config = Config()
    config._config = {
        "api": {
            "base_url": "https://api.test.databus.cr",
            "api_key": "test_key",
            "timeout": 10,
            "max_retries": 1,
        },
        "logging": {
            "level": "DEBUG",
            "format": "%(levelname)s - %(message)s",
        },
        "processing": {
            "chunk_size": 1000,
            "temp_dir": None,
        },
        "validation": {
            "strict_mode": True,
            "custom_rules_dir": None,
        }
    }
    return config


@pytest.fixture
def mock_api_client(mock_config):
    """Create a mock API client for testing."""
    client = DatabusClient(
        base_url=mock_config.get("api.base_url"),
        api_key=mock_config.get("api.api_key"),
        timeout=mock_config.get("api.timeout"),
    )
    return client


@pytest.fixture
def sample_gtfs_data():
    """Create sample GTFS data for testing."""
    data = {
        "agency": pd.DataFrame([
            {
                "agency_id": "agency_1",
                "agency_name": "Test Transit Agency",
                "agency_url": "https://test-transit.com",
                "agency_timezone": "America/Costa_Rica",
            }
        ]),
        "routes": pd.DataFrame([
            {
                "route_id": "route_1",
                "agency_id": "agency_1",
                "route_short_name": "R1",
                "route_long_name": "Test Route 1",
                "route_type": 3,
                "route_color": "FF0000",
                "route_text_color": "FFFFFF",
            },
            {
                "route_id": "route_2", 
                "agency_id": "agency_1",
                "route_short_name": "R2",
                "route_long_name": "Test Route 2",
                "route_type": 3,
            }
        ]),
        "stops": pd.DataFrame([
            {
                "stop_id": "stop_1",
                "stop_name": "Test Stop 1",
                "stop_lat": 9.9281,
                "stop_lon": -84.0907,
            },
            {
                "stop_id": "stop_2",
                "stop_name": "Test Stop 2", 
                "stop_lat": 9.9350,
                "stop_lon": -84.0830,
            },
            {
                "stop_id": "stop_3",
                "stop_name": "Test Stop 3",
                "stop_lat": 9.9420,
                "stop_lon": -84.0750,
            }
        ]),
        "trips": pd.DataFrame([
            {
                "route_id": "route_1",
                "service_id": "service_1",
                "trip_id": "trip_1",
                "trip_headsign": "Downtown",
                "direction_id": 0,
            },
            {
                "route_id": "route_1",
                "service_id": "service_1", 
                "trip_id": "trip_2",
                "trip_headsign": "Uptown",
                "direction_id": 1,
            }
        ]),
        "stop_times": pd.DataFrame([
            {
                "trip_id": "trip_1",
                "arrival_time": "08:00:00",
                "departure_time": "08:00:00",
                "stop_id": "stop_1",
                "stop_sequence": 1,
            },
            {
                "trip_id": "trip_1",
                "arrival_time": "08:05:00",
                "departure_time": "08:05:00",
                "stop_id": "stop_2",
                "stop_sequence": 2,
            },
            {
                "trip_id": "trip_2",
                "arrival_time": "08:10:00", 
                "departure_time": "08:10:00",
                "stop_id": "stop_2",
                "stop_sequence": 1,
            },
            {
                "trip_id": "trip_2",
                "arrival_time": "08:15:00",
                "departure_time": "08:15:00",
                "stop_id": "stop_1", 
                "stop_sequence": 2,
            }
        ]),
        "calendar": pd.DataFrame([
            {
                "service_id": "service_1",
                "monday": 1,
                "tuesday": 1,
                "wednesday": 1,
                "thursday": 1,
                "friday": 1,
                "saturday": 1,
                "sunday": 0,
                "start_date": "20240101",
                "end_date": "20241231",
            }
        ]),
    }
    return data


@pytest.fixture
def sample_gtfs_feed(sample_gtfs_data, temp_dir):
    """Create a mock GTFS feed object for testing."""
    # Create a mock feed object that resembles gtfs_kit feed structure
    feed = Mock()
    
    # Add all the dataframes as attributes
    for table_name, df in sample_gtfs_data.items():
        setattr(feed, table_name, df)
    
    # Add some missing optional tables as None
    feed.shapes = None
    feed.calendar_dates = None
    feed.fare_attributes = None
    feed.fare_rules = None
    feed.frequencies = None
    feed.transfers = None
    
    return feed


@pytest.fixture
def sample_gtfs_zip(sample_gtfs_data, temp_dir):
    """Create a sample GTFS ZIP file for testing."""
    import zipfile
    
    zip_path = temp_dir / "sample_feed.zip"
    
    with zipfile.ZipFile(zip_path, 'w') as zf:
        for table_name, df in sample_gtfs_data.items():
            csv_content = df.to_csv(index=False)
            zf.writestr(f"{table_name}.txt", csv_content)
    
    return zip_path


@pytest.fixture
def mock_gtfs_processor(sample_gtfs_feed):
    """Create a mock GTFS processor for testing."""
    processor = Mock(spec=GTFSProcessor)
    processor.feed = sample_gtfs_feed
    processor._is_loaded = True
    processor.feed_path = Path("test_feed.zip")
    
    # Mock methods
    processor.get_agencies.return_value = sample_gtfs_feed.agency
    processor.get_routes.return_value = sample_gtfs_feed.routes
    processor.get_stops.return_value = sample_gtfs_feed.stops
    processor.get_trips.return_value = sample_gtfs_feed.trips
    processor.get_stop_times.return_value = sample_gtfs_feed.stop_times
    
    return processor


@pytest.fixture
def api_responses():
    """Sample API response data for testing."""
    return {
        "feeds": {
            "feeds": [
                {
                    "id": "costa-rica-gtfs",
                    "name": "Costa Rica GTFS",
                    "description": "National transit feed for Costa Rica",
                    "country_code": "CR",
                    "region": "Central America",
                    "operator": "COSEVI",
                    "url": "https://gtfs.costa-rica.cr",
                    "download_url": "https://api.databus.cr/feeds/costa-rica-gtfs/download",
                    "file_size": 2048576,
                    "version": "2024.1",
                    "status": "active",
                }
            ]
        },
        "feed_detail": {
            "id": "costa-rica-gtfs",
            "name": "Costa Rica GTFS", 
            "description": "National transit feed for Costa Rica",
            "country_code": "CR",
            "region": "Central America",
            "operator": "COSEVI",
            "url": "https://gtfs.costa-rica.cr",
            "download_url": "https://api.databus.cr/feeds/costa-rica-gtfs/download",
            "file_size": 2048576,
            "version": "2024.1",
            "status": "active",
        },
        "agencies": {
            "agencies": [
                {
                    "agency_id": "COSEVI",
                    "agency_name": "Consejo de Seguridad Vial",
                    "agency_url": "https://www.cosevi.go.cr",
                    "agency_timezone": "America/Costa_Rica",
                }
            ]
        }
    }
