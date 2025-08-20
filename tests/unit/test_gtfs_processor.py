"""Unit tests for GTFSProcessor class."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from pathlib import Path

from databus.gtfs import GTFSProcessor
from databus.utils.exceptions import GTFSProcessingError


class TestGTFSProcessor:
    """Test cases for GTFSProcessor class."""
    
    def test_init_without_path(self):
        """Test initialization without feed path."""
        processor = GTFSProcessor()
        assert processor.feed_path is None
        assert processor.feed is None
        assert processor._is_loaded is False
    
    def test_init_with_path(self):
        """Test initialization with feed path."""
        feed_path = "test_feed.zip"
        processor = GTFSProcessor(feed_path)
        assert processor.feed_path == Path(feed_path)
        assert processor.feed is None
        assert processor._is_loaded is False
    
    @patch('databus.gtfs.processor.gk.read_gtfs')
    def test_load_feed_success(self, mock_read_gtfs, sample_gtfs_feed):
        """Test successful feed loading."""
        mock_read_gtfs.return_value = sample_gtfs_feed
        
        processor = GTFSProcessor()
        processor.load_feed("test_feed.zip")
        
        assert processor._is_loaded is True
        assert processor.feed == sample_gtfs_feed
        assert processor.feed_path == Path("test_feed.zip")
        mock_read_gtfs.assert_called_once_with("test_feed.zip")
    
    def test_load_feed_no_path(self):
        """Test loading feed without providing path."""
        processor = GTFSProcessor()
        
        with pytest.raises(GTFSProcessingError, match="No feed path provided"):
            processor.load_feed()
    
    def test_load_feed_nonexistent_path(self):
        """Test loading feed with non-existent path."""
        processor = GTFSProcessor()
        
        with pytest.raises(GTFSProcessingError, match="Feed path does not exist"):
            processor.load_feed("nonexistent.zip")
    
    @patch('databus.gtfs.processor.gk.read_gtfs')
    def test_load_feed_gtfs_error(self, mock_read_gtfs):
        """Test handling of gtfs-kit errors during loading."""
        mock_read_gtfs.side_effect = Exception("Invalid GTFS file")
        
        processor = GTFSProcessor()
        
        with pytest.raises(GTFSProcessingError, match="Failed to load GTFS feed"):
            processor.load_feed("invalid_feed.zip")
    
    def test_ensure_loaded_not_loaded(self):
        """Test _ensure_loaded when no feed is loaded."""
        processor = GTFSProcessor()
        
        with pytest.raises(GTFSProcessingError, match="No GTFS feed loaded"):
            processor._ensure_loaded()
    
    def test_get_agencies(self, mock_gtfs_processor):
        """Test getting agencies dataframe.""" 
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        agencies = processor.get_agencies()
        assert isinstance(agencies, pd.DataFrame)
        assert "agency_name" in agencies.columns
    
    def test_get_routes_no_filter(self, mock_gtfs_processor):
        """Test getting all routes."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        routes = processor.get_routes()
        assert isinstance(routes, pd.DataFrame)
        assert len(routes) == 2
        assert "route_id" in routes.columns
    
    def test_get_routes_with_agency_filter(self, mock_gtfs_processor):
        """Test getting routes filtered by agency."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        routes = processor.get_routes(agency_id="agency_1")
        assert isinstance(routes, pd.DataFrame)
        assert all(routes["agency_id"] == "agency_1")
    
    def test_get_stops_dataframe(self, mock_gtfs_processor):
        """Test getting stops as DataFrame."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        stops = processor.get_stops(as_geodataframe=False)
        assert isinstance(stops, pd.DataFrame)
        assert "stop_lat" in stops.columns
        assert "stop_lon" in stops.columns
    
    @patch('databus.gtfs.processor.gpd.GeoDataFrame')
    @patch('databus.gtfs.processor.Point')
    def test_get_stops_geodataframe(self, mock_point, mock_geodataframe, mock_gtfs_processor):
        """Test getting stops as GeoDataFrame."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        # Mock Point creation
        mock_point.return_value = Mock()
        mock_geodataframe.return_value = Mock()
        
        stops = processor.get_stops(as_geodataframe=True)
        
        # Verify Point was called for each stop
        assert mock_point.call_count == len(mock_gtfs_processor.feed.stops)
        mock_geodataframe.assert_called_once()
    
    def test_get_trips_no_filter(self, mock_gtfs_processor):
        """Test getting all trips."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        trips = processor.get_trips()
        assert isinstance(trips, pd.DataFrame)
        assert len(trips) == 2
        assert "trip_id" in trips.columns
    
    def test_get_trips_with_route_filter(self, mock_gtfs_processor):
        """Test getting trips filtered by route."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        trips = processor.get_trips(route_id="route_1")
        assert isinstance(trips, pd.DataFrame)
        assert all(trips["route_id"] == "route_1")
    
    def test_get_stop_times_no_filter(self, mock_gtfs_processor):
        """Test getting all stop times."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        stop_times = processor.get_stop_times()
        assert isinstance(stop_times, pd.DataFrame)
        assert len(stop_times) == 4
        assert "trip_id" in stop_times.columns
    
    def test_get_stop_times_with_trip_filter(self, mock_gtfs_processor):
        """Test getting stop times filtered by trip."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        stop_times = processor.get_stop_times(trip_id="trip_1")
        assert isinstance(stop_times, pd.DataFrame)
        assert all(stop_times["trip_id"] == "trip_1")
    
    def test_get_shapes_empty(self, mock_gtfs_processor):
        """Test getting shapes when none exist."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        shapes = processor.get_shapes()
        assert isinstance(shapes, pd.DataFrame)
        assert shapes.empty
    
    def test_get_feed_stats(self, mock_gtfs_processor):
        """Test getting comprehensive feed statistics."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        stats = processor.get_feed_stats()
        
        assert isinstance(stats, dict)
        assert "agencies" in stats
        assert "routes" in stats
        assert "stops" in stats
        assert "trips" in stats
        assert "stop_times" in stats
        assert stats["agencies"] == 1
        assert stats["routes"] == 2
        assert stats["stops"] == 3
        assert stats["trips"] == 2
        assert stats["stop_times"] == 4
    
    def test_get_route_stats_valid_route(self, mock_gtfs_processor):
        """Test getting statistics for a valid route."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        route_stats = processor.get_route_stats("route_1")
        
        assert isinstance(route_stats, dict)
        assert route_stats["route_id"] == "route_1"
        assert route_stats["total_trips"] == 2
        assert "unique_stops" in route_stats
        assert "directions" in route_stats
    
    def test_get_route_stats_invalid_route(self, mock_gtfs_processor):
        """Test getting statistics for non-existent route."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        with pytest.raises(GTFSProcessingError, match="Route nonexistent not found"):
            processor.get_route_stats("nonexistent")
    
    @patch('databus.gtfs.processor.gk.filter_by_bounding_box')
    def test_filter_by_bounding_box(self, mock_filter, mock_gtfs_processor):
        """Test filtering feed by bounding box."""
        mock_filter.return_value = mock_gtfs_processor.feed
        
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        filtered = processor.filter_by_bounding_box(9.9, -84.1, 10.0, -84.0)
        
        assert isinstance(filtered, GTFSProcessor)
        assert filtered._is_loaded is True
        mock_filter.assert_called_once_with(
            mock_gtfs_processor.feed, -84.1, 9.9, -84.0, 10.0
        )
    
    @patch('databus.gtfs.processor.gk.filter_by_dates')
    def test_filter_by_dates(self, mock_filter, mock_gtfs_processor):
        """Test filtering feed by date range."""
        mock_filter.return_value = mock_gtfs_processor.feed
        
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        filtered = processor.filter_by_dates("2024-01-01", "2024-12-31")
        
        assert isinstance(filtered, GTFSProcessor)
        assert filtered._is_loaded is True
        mock_filter.assert_called_once_with(
            mock_gtfs_processor.feed, "20240101", "20241231"
        )
    
    @patch('databus.gtfs.processor.gk.write_gtfs')
    def test_export_to_zip(self, mock_write, mock_gtfs_processor, temp_dir):
        """Test exporting feed to ZIP file."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        output_path = temp_dir / "exported_feed.zip"
        result_path = processor.export_to_zip(output_path)
        
        assert result_path == output_path
        mock_write.assert_called_once_with(mock_gtfs_processor.feed, str(output_path))
    
    def test_to_dict(self, mock_gtfs_processor):
        """Test converting feed to dictionary."""
        processor = GTFSProcessor()
        processor.feed = mock_gtfs_processor.feed
        processor._is_loaded = True
        
        result = processor.to_dict()
        
        assert isinstance(result, dict)
        assert "agency" in result
        assert "routes" in result
        assert "stops" in result
        assert isinstance(result["agency"], pd.DataFrame)
    
    def test_methods_require_loaded_feed(self):
        """Test that methods require a loaded feed."""
        processor = GTFSProcessor()
        
        with pytest.raises(GTFSProcessingError, match="No GTFS feed loaded"):
            processor.get_agencies()
        
        with pytest.raises(GTFSProcessingError, match="No GTFS feed loaded"):
            processor.get_routes()
        
        with pytest.raises(GTFSProcessingError, match="No GTFS feed loaded"):
            processor.get_feed_stats()
