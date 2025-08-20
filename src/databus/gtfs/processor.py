"""GTFS data processor for loading, manipulating, and analyzing transit data."""

import logging
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import tempfile
import os

import pandas as pd
import gtfs_kit as gk
import geopandas as gpd
from shapely.geometry import Point

from ..utils.exceptions import GTFSProcessingError, GTFSValidationError


logger = logging.getLogger(__name__)


class GTFSProcessor:
    """Processor for GTFS data manipulation and analysis.
    
    Provides methods for loading, processing, and analyzing GTFS feeds
    using gtfs-kit and pandas for efficient data manipulation.
    
    Args:
        feed_path: Path to GTFS feed (ZIP file or directory)
        
    Example:
        >>> processor = GTFSProcessor("costa_rica_gtfs.zip")
        >>> routes = processor.get_routes()
        >>> stats = processor.get_feed_stats()
    """
    
    def __init__(self, feed_path: Optional[Union[str, Path]] = None):
        self.feed_path = Path(feed_path) if feed_path else None
        self.feed = None
        self._is_loaded = False
        
    def load_feed(self, feed_path: Optional[Union[str, Path]] = None) -> None:
        """Load GTFS feed from file or directory.
        
        Args:
            feed_path: Path to GTFS feed (overrides constructor path)
        """
        path = Path(feed_path) if feed_path else self.feed_path
        if not path:
            raise GTFSProcessingError("No feed path provided")
        
        if not path.exists():
            raise GTFSProcessingError(f"Feed path does not exist: {path}")
        
        try:
            logger.info(f"Loading GTFS feed from {path}")
            self.feed = gk.read_gtfs(str(path))
            self.feed_path = path
            self._is_loaded = True
            logger.info("GTFS feed loaded successfully")
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to load GTFS feed: {e}")
    
    def _ensure_loaded(self) -> None:
        """Ensure feed is loaded."""
        if not self._is_loaded or self.feed is None:
            raise GTFSProcessingError("No GTFS feed loaded. Call load_feed() first.")
    
    def get_agencies(self) -> pd.DataFrame:
        """Get agencies dataframe."""
        self._ensure_loaded()
        return self.feed.agency
    
    def get_routes(self, agency_id: Optional[str] = None) -> pd.DataFrame:
        """Get routes dataframe, optionally filtered by agency.
        
        Args:
            agency_id: Filter routes by agency ID
            
        Returns:
            Routes dataframe
        """
        self._ensure_loaded()
        routes = self.feed.routes
        
        if agency_id:
            routes = routes[routes['agency_id'] == agency_id]
        
        return routes
    
    def get_stops(self, as_geodataframe: bool = False) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Get stops dataframe, optionally as GeoDataFrame.
        
        Args:
            as_geodataframe: Return as GeoDataFrame with Point geometries
            
        Returns:
            Stops dataframe or GeoDataFrame
        """
        self._ensure_loaded()
        stops = self.feed.stops
        
        if as_geodataframe and 'stop_lat' in stops.columns and 'stop_lon' in stops.columns:
            # Create Point geometries from lat/lon
            geometry = [Point(lon, lat) for lon, lat in zip(stops['stop_lon'], stops['stop_lat'])]
            stops_gdf = gpd.GeoDataFrame(stops, geometry=geometry, crs="EPSG:4326")
            return stops_gdf
        
        return stops
    
    def get_trips(self, route_id: Optional[str] = None) -> pd.DataFrame:
        """Get trips dataframe, optionally filtered by route.
        
        Args:
            route_id: Filter trips by route ID
            
        Returns:
            Trips dataframe
        """
        self._ensure_loaded()
        trips = self.feed.trips
        
        if route_id:
            trips = trips[trips['route_id'] == route_id]
        
        return trips
    
    def get_stop_times(self, trip_id: Optional[str] = None) -> pd.DataFrame:
        """Get stop times dataframe, optionally filtered by trip.
        
        Args:
            trip_id: Filter stop times by trip ID
            
        Returns:
            Stop times dataframe
        """
        self._ensure_loaded()
        stop_times = self.feed.stop_times
        
        if trip_id:
            stop_times = stop_times[stop_times['trip_id'] == trip_id]
        
        return stop_times
    
    def get_shapes(self, as_geodataframe: bool = False) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Get shapes dataframe, optionally as GeoDataFrame.
        
        Args:
            as_geodataframe: Return as GeoDataFrame with LineString geometries
            
        Returns:
            Shapes dataframe or GeoDataFrame
        """
        self._ensure_loaded()
        
        if self.feed.shapes is None:
            return pd.DataFrame()
        
        shapes = self.feed.shapes
        
        if as_geodataframe:
            # Convert shapes to LineString geometries
            shapes_gdf = gk.shapes_to_linestrings(self.feed)
            return shapes_gdf
        
        return shapes
    
    def get_feed_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the GTFS feed.
        
        Returns:
            Dictionary with feed statistics
        """
        self._ensure_loaded()
        
        stats = {
            'agencies': len(self.feed.agency) if self.feed.agency is not None else 0,
            'routes': len(self.feed.routes) if self.feed.routes is not None else 0,
            'stops': len(self.feed.stops) if self.feed.stops is not None else 0,
            'trips': len(self.feed.trips) if self.feed.trips is not None else 0,
            'stop_times': len(self.feed.stop_times) if self.feed.stop_times is not None else 0,
            'shapes': len(self.feed.shapes) if self.feed.shapes is not None else 0,
            'calendar': len(self.feed.calendar) if self.feed.calendar is not None else 0,
            'calendar_dates': len(self.feed.calendar_dates) if self.feed.calendar_dates is not None else 0,
        }
        
        # Add route type breakdown
        if self.feed.routes is not None:
            route_types = self.feed.routes['route_type'].value_counts().to_dict()
            stats['routes_by_type'] = route_types
        
        # Add date range
        if self.feed.calendar is not None and not self.feed.calendar.empty:
            start_dates = pd.to_datetime(self.feed.calendar['start_date'], format='%Y%m%d')
            end_dates = pd.to_datetime(self.feed.calendar['end_date'], format='%Y%m%d')
            stats['service_period'] = {
                'start': start_dates.min().strftime('%Y-%m-%d'),
                'end': end_dates.max().strftime('%Y-%m-%d')
            }
        
        return stats
    
    def get_route_stats(self, route_id: str) -> Dict[str, Any]:
        """Get statistics for a specific route.
        
        Args:
            route_id: Route ID to analyze
            
        Returns:
            Dictionary with route statistics
        """
        self._ensure_loaded()
        
        # Get route info
        route = self.feed.routes[self.feed.routes['route_id'] == route_id]
        if route.empty:
            raise GTFSProcessingError(f"Route {route_id} not found")
        
        # Get trips for this route
        trips = self.get_trips(route_id)
        
        # Get stops for this route
        trip_ids = trips['trip_id'].tolist()
        stop_times = self.feed.stop_times[self.feed.stop_times['trip_id'].isin(trip_ids)]
        unique_stops = stop_times['stop_id'].nunique()
        
        # Get directions
        directions = trips['direction_id'].nunique() if 'direction_id' in trips.columns else 1
        
        stats = {
            'route_id': route_id,
            'route_name': route.iloc[0]['route_long_name'] if 'route_long_name' in route.columns else None,
            'route_short_name': route.iloc[0]['route_short_name'] if 'route_short_name' in route.columns else None,
            'route_type': route.iloc[0]['route_type'],
            'total_trips': len(trips),
            'unique_stops': unique_stops,
            'directions': directions,
            'total_stop_times': len(stop_times),
        }
        
        return stats
    
    def filter_by_bounding_box(
        self,
        min_lat: float,
        min_lon: float, 
        max_lat: float,
        max_lon: float
    ) -> 'GTFSProcessor':
        """Filter GTFS feed by geographic bounding box.
        
        Args:
            min_lat: Minimum latitude
            min_lon: Minimum longitude
            max_lat: Maximum latitude
            max_lon: Maximum longitude
            
        Returns:
            New GTFSProcessor instance with filtered feed
        """
        self._ensure_loaded()
        
        try:
            # Filter feed by bounding box using gtfs-kit
            filtered_feed = gk.filter_by_bounding_box(
                self.feed,
                min_lon, min_lat, max_lon, max_lat
            )
            
            # Create new processor with filtered feed
            new_processor = GTFSProcessor()
            new_processor.feed = filtered_feed
            new_processor._is_loaded = True
            
            return new_processor
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to filter by bounding box: {e}")
    
    def filter_by_dates(self, start_date: str, end_date: str) -> 'GTFSProcessor':
        """Filter GTFS feed by date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            New GTFSProcessor instance with filtered feed
        """
        self._ensure_loaded()
        
        try:
            # Filter feed by dates using gtfs-kit
            filtered_feed = gk.filter_by_dates(
                self.feed,
                start_date.replace('-', ''),
                end_date.replace('-', '')
            )
            
            # Create new processor with filtered feed
            new_processor = GTFSProcessor()
            new_processor.feed = filtered_feed
            new_processor._is_loaded = True
            
            return new_processor
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to filter by dates: {e}")
    
    def export_to_zip(self, output_path: Union[str, Path]) -> Path:
        """Export processed GTFS feed to ZIP file.
        
        Args:
            output_path: Path for output ZIP file
            
        Returns:
            Path to created ZIP file
        """
        self._ensure_loaded()
        
        output_path = Path(output_path)
        
        try:
            # Use gtfs-kit to write GTFS to zip
            gk.write_gtfs(self.feed, str(output_path))
            logger.info(f"GTFS feed exported to {output_path}")
            return output_path
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to export GTFS feed: {e}")
    
    def to_dict(self) -> Dict[str, pd.DataFrame]:
        """Convert GTFS feed to dictionary of DataFrames.
        
        Returns:
            Dictionary mapping table names to DataFrames
        """
        self._ensure_loaded()
        
        tables = {}
        for attr_name in dir(self.feed):
            if not attr_name.startswith('_'):
                attr_value = getattr(self.feed, attr_name)
                if isinstance(attr_value, pd.DataFrame) and not attr_value.empty:
                    tables[attr_name] = attr_value
        
        return tables
