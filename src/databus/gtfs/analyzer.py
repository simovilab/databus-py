"""GTFS data analyzer for advanced analysis and insights."""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import math

import pandas as pd
import numpy as np

from ..utils.exceptions import GTFSProcessingError
from ..utils.helpers import calculate_distance, parse_gtfs_time


logger = logging.getLogger(__name__)


class GTFSAnalyzer:
    """Analyzer for advanced GTFS data analysis and insights.
    
    Provides methods for:
    - Service frequency analysis
    - Coverage analysis
    - Performance metrics
    - Accessibility analysis
    """
    
    def __init__(self, processor):
        """Initialize analyzer with GTFS processor.
        
        Args:
            processor: GTFSProcessor instance with loaded feed
        """
        from .processor import GTFSProcessor
        self.processor = processor
    
    def analyze_service_frequency(self, time_window: int = 3600) -> Dict[str, Any]:
        """Analyze service frequency by route and time period.
        
        Args:
            time_window: Time window in seconds (default: 1 hour)
            
        Returns:
            Dictionary with frequency analysis results
        """
        try:
            stop_times = self.processor.get_stop_times()
            trips = self.processor.get_trips()
            routes = self.processor.get_routes()
            
            # Join stop times with trips to get route info
            trip_routes = stop_times.merge(trips[['trip_id', 'route_id']], on='trip_id')
            
            # Parse arrival times
            trip_routes['arrival_seconds'] = trip_routes['arrival_time'].apply(
                lambda x: parse_gtfs_time(x).total_seconds() if parse_gtfs_time(x) else None
            )
            
            # Filter out invalid times
            trip_routes = trip_routes.dropna(subset=['arrival_seconds'])
            
            # Group by route and calculate frequency metrics
            frequency_analysis = {}
            
            for route_id in trip_routes['route_id'].unique():
                route_data = trip_routes[trip_routes['route_id'] == route_id]
                route_info = routes[routes['route_id'] == route_id].iloc[0]
                
                # Get first stop of each trip (trip start times)
                trip_starts = route_data.groupby('trip_id')['arrival_seconds'].min()
                
                # Calculate headways (time between consecutive trips)
                headways = trip_starts.sort_values().diff().dropna()
                
                frequency_analysis[route_id] = {
                    'route_name': route_info.get('route_long_name', route_info.get('route_short_name', route_id)),
                    'total_trips': len(trip_starts),
                    'average_headway_minutes': headways.mean() / 60 if not headways.empty else None,
                    'min_headway_minutes': headways.min() / 60 if not headways.empty else None,
                    'max_headway_minutes': headways.max() / 60 if not headways.empty else None,
                    'trips_per_hour': len(trip_starts) * (time_window / (24 * 3600)) if not trip_starts.empty else 0,
                }
            
            return {
                'by_route': frequency_analysis,
                'overall': {
                    'total_routes_analyzed': len(frequency_analysis),
                    'average_trips_per_route': np.mean([r['total_trips'] for r in frequency_analysis.values()]),
                    'time_window_hours': time_window / 3600,
                }
            }
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to analyze service frequency: {e}")
    
    def analyze_stop_coverage(self, buffer_distance: float = 400) -> Dict[str, Any]:
        """Analyze stop coverage and accessibility.
        
        Args:
            buffer_distance: Buffer distance in meters for coverage analysis
            
        Returns:
            Dictionary with coverage analysis results
        """
        try:
            stops = self.processor.get_stops()
            
            if stops.empty:
                return {'error': 'No stops found in feed'}
            
            # Calculate stop density
            lat_range = stops['stop_lat'].max() - stops['stop_lat'].min()
            lon_range = stops['stop_lon'].max() - stops['stop_lon'].min()
            
            # Approximate area in km² (rough calculation)
            area_km2 = lat_range * lon_range * 111 * 111 * math.cos(math.radians(stops['stop_lat'].mean()))
            
            # Calculate distances between stops
            distances = []
            stop_coords = stops[['stop_lat', 'stop_lon']].values
            
            for i in range(min(100, len(stop_coords))):  # Sample first 100 stops for performance
                for j in range(i + 1, min(100, len(stop_coords))):
                    dist = calculate_distance(
                        stop_coords[i][0], stop_coords[i][1],
                        stop_coords[j][0], stop_coords[j][1]
                    )
                    distances.append(dist * 1000)  # Convert to meters
            
            return {
                'total_stops': len(stops),
                'stop_density_per_km2': len(stops) / area_km2 if area_km2 > 0 else 0,
                'coverage_area_km2': area_km2,
                'buffer_distance_m': buffer_distance,
                'average_stop_distance_m': np.mean(distances) if distances else 0,
                'min_stop_distance_m': min(distances) if distances else 0,
                'max_stop_distance_m': max(distances) if distances else 0,
                'bounding_box': {
                    'min_lat': stops['stop_lat'].min(),
                    'max_lat': stops['stop_lat'].max(),
                    'min_lon': stops['stop_lon'].min(),
                    'max_lon': stops['stop_lon'].max(),
                }
            }
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to analyze stop coverage: {e}")
    
    def analyze_route_efficiency(self) -> Dict[str, Any]:
        """Analyze route efficiency metrics.
        
        Returns:
            Dictionary with efficiency analysis results
        """
        try:
            routes = self.processor.get_routes()
            trips = self.processor.get_trips()
            stop_times = self.processor.get_stop_times()
            stops = self.processor.get_stops()
            
            efficiency_analysis = {}
            
            for _, route in routes.iterrows():
                route_id = route['route_id']
                route_trips = trips[trips['route_id'] == route_id]
                
                if route_trips.empty:
                    continue
                
                # Get stop times for this route's trips
                trip_ids = route_trips['trip_id'].tolist()
                route_stop_times = stop_times[stop_times['trip_id'].isin(trip_ids)]
                
                if route_stop_times.empty:
                    continue
                
                # Calculate route metrics
                unique_stops = route_stop_times['stop_id'].nunique()
                total_trips = len(route_trips)
                
                # Calculate average trip duration
                trip_durations = []
                for trip_id in trip_ids[:10]:  # Sample first 10 trips for performance
                    trip_stops = route_stop_times[route_stop_times['trip_id'] == trip_id].sort_values('stop_sequence')
                    
                    if len(trip_stops) >= 2:
                        start_time = parse_gtfs_time(trip_stops.iloc[0]['departure_time'])
                        end_time = parse_gtfs_time(trip_stops.iloc[-1]['arrival_time'])
                        
                        if start_time and end_time:
                            duration = (end_time - start_time).total_seconds() / 60  # minutes
                            trip_durations.append(duration)
                
                # Calculate route length (approximate using straight-line distances)
                route_stops_ids = route_stop_times['stop_id'].unique()
                route_stops_coords = stops[stops['stop_id'].isin(route_stops_ids)]
                
                route_length_km = 0
                if len(route_stops_coords) >= 2:
                    coords = route_stops_coords[['stop_lat', 'stop_lon']].values
                    for i in range(len(coords) - 1):
                        dist = calculate_distance(
                            coords[i][0], coords[i][1],
                            coords[i + 1][0], coords[i + 1][1]
                        )
                        route_length_km += dist
                
                efficiency_analysis[route_id] = {
                    'route_name': route.get('route_long_name', route.get('route_short_name', route_id)),
                    'route_type': route.get('route_type'),
                    'unique_stops': unique_stops,
                    'total_trips': total_trips,
                    'average_trip_duration_minutes': np.mean(trip_durations) if trip_durations else None,
                    'approximate_length_km': route_length_km,
                    'stops_per_km': unique_stops / route_length_km if route_length_km > 0 else None,
                }
            
            return {
                'by_route': efficiency_analysis,
                'summary': {
                    'total_routes_analyzed': len(efficiency_analysis),
                    'average_stops_per_route': np.mean([r['unique_stops'] for r in efficiency_analysis.values()]),
                    'average_trips_per_route': np.mean([r['total_trips'] for r in efficiency_analysis.values()]),
                }
            }
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to analyze route efficiency: {e}")
    
    def analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyze temporal service patterns.
        
        Returns:
            Dictionary with temporal analysis results
        """
        try:
            stop_times = self.processor.get_stop_times()
            calendar = self.processor.feed.calendar
            
            # Parse departure times
            stop_times['departure_seconds'] = stop_times['departure_time'].apply(
                lambda x: parse_gtfs_time(x).total_seconds() if parse_gtfs_time(x) else None
            )
            
            stop_times = stop_times.dropna(subset=['departure_seconds'])
            
            # Convert to hours
            stop_times['departure_hour'] = stop_times['departure_seconds'] / 3600
            
            # Analyze hourly patterns
            hourly_trips = stop_times.groupby(stop_times['departure_hour'].astype(int)).size()
            
            # Find peak hours
            peak_hour = hourly_trips.idxmax()
            off_peak_hour = hourly_trips.idxmin()
            
            # Service span
            service_start = stop_times['departure_hour'].min()
            service_end = stop_times['departure_hour'].max()
            
            temporal_analysis = {
                'service_span_hours': service_end - service_start,
                'earliest_service_hour': service_start,
                'latest_service_hour': service_end,
                'peak_hour': peak_hour,
                'peak_hour_trips': hourly_trips[peak_hour],
                'off_peak_hour': off_peak_hour,
                'off_peak_hour_trips': hourly_trips[off_peak_hour],
                'hourly_distribution': hourly_trips.to_dict(),
            }
            
            # Analyze weekly patterns if calendar data is available
            if calendar is not None and not calendar.empty:
                weekday_columns = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                weekday_service = {}
                
                for i, day in enumerate(weekday_columns):
                    if day in calendar.columns:
                        services_on_day = calendar[calendar[day] == 1]
                        weekday_service[day] = len(services_on_day)
                
                temporal_analysis['weekly_patterns'] = weekday_service
            
            return temporal_analysis
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to analyze temporal patterns: {e}")
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate a comprehensive analysis report.
        
        Returns:
            Dictionary with all analysis results
        """
        logger.info("Generating comprehensive GTFS analysis report")
        
        try:
            report = {
                'feed_stats': self.processor.get_feed_stats(),
                'frequency_analysis': self.analyze_service_frequency(),
                'coverage_analysis': self.analyze_stop_coverage(),
                'efficiency_analysis': self.analyze_route_efficiency(),
                'temporal_patterns': self.analyze_temporal_patterns(),
                'generated_at': datetime.now().isoformat(),
            }
            
            logger.info("Comprehensive analysis report generated successfully")
            return report
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to generate comprehensive report: {e}")
    
    def find_service_gaps(self, min_headway_minutes: int = 60) -> Dict[str, Any]:
        """Find potential service gaps in the schedule.
        
        Args:
            min_headway_minutes: Minimum time gap to consider as a service gap
            
        Returns:
            Dictionary with service gap analysis
        """
        try:
            stop_times = self.processor.get_stop_times()
            trips = self.processor.get_trips()
            
            # Join to get route information
            trip_routes = stop_times.merge(trips[['trip_id', 'route_id']], on='trip_id')
            
            # Parse times
            trip_routes['departure_seconds'] = trip_routes['departure_time'].apply(
                lambda x: parse_gtfs_time(x).total_seconds() if parse_gtfs_time(x) else None
            )
            
            trip_routes = trip_routes.dropna(subset=['departure_seconds'])
            
            service_gaps = {}
            
            for route_id in trip_routes['route_id'].unique():
                route_data = trip_routes[trip_routes['route_id'] == route_id]
                
                # Get all departure times for this route
                departures = route_data['departure_seconds'].sort_values()
                gaps = departures.diff()
                
                # Find gaps larger than threshold
                large_gaps = gaps[gaps > min_headway_minutes * 60]
                
                if not large_gaps.empty:
                    service_gaps[route_id] = {
                        'gaps_found': len(large_gaps),
                        'largest_gap_minutes': large_gaps.max() / 60,
                        'average_gap_minutes': large_gaps.mean() / 60,
                        'gap_times': [
                            {
                                'start_time': seconds_to_time(departures.iloc[i-1]),
                                'end_time': seconds_to_time(departures.iloc[i]),
                                'duration_minutes': gap / 60
                            }
                            for i, gap in enumerate(large_gaps) if i < 5  # Show first 5 gaps
                        ]
                    }
            
            return {
                'threshold_minutes': min_headway_minutes,
                'routes_with_gaps': len(service_gaps),
                'gaps_by_route': service_gaps
            }
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to find service gaps: {e}")


def seconds_to_time(seconds: float) -> str:
    """Convert seconds to HH:MM:SS format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
