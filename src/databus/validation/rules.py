"""Standard GTFS validation rules."""

from typing import Dict, List, Any
import pandas as pd

from .models import ValidationRule


class StandardRules:
    """Collection of standard GTFS validation rules."""
    
    @staticmethod
    def get_all_rules() -> List[ValidationRule]:
        """Get all standard validation rules.
        
        Returns:
            List of ValidationRule instances
        """
        return [
            StandardRules.required_files_rule(),
            StandardRules.required_fields_rule(),
            StandardRules.data_types_rule(),
            StandardRules.foreign_keys_rule(),
            StandardRules.coordinate_validity_rule(),
            StandardRules.service_dates_rule(),
            StandardRules.stop_times_sequence_rule(),
            StandardRules.route_names_rule(),
            StandardRules.duplicate_ids_rule(),
            StandardRules.speed_validation_rule(),
        ]
    
    @staticmethod
    def required_files_rule() -> ValidationRule:
        """Rule to check for required GTFS files."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            required_files = ['agency', 'routes', 'stops', 'trips', 'stop_times']
            
            for file_name in required_files:
                table = getattr(feed, file_name, None)
                if table is None or table.empty:
                    issues.append({
                        'message': f"Required file {file_name}.txt is missing or empty",
                        'details': {'file': file_name}
                    })
            return issues
        
        return ValidationRule(
            name="required_files",
            description="Check for required GTFS files",
            validate_func=validate,
            severity="error",
            category="structure"
        )
    
    @staticmethod
    def required_fields_rule() -> ValidationRule:
        """Rule to check for required fields in each file."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            required_fields = {
                'agency': ['agency_name', 'agency_url', 'agency_timezone'],
                'routes': ['route_id', 'route_type'],
                'stops': ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
                'trips': ['route_id', 'service_id', 'trip_id'],
                'stop_times': ['trip_id', 'stop_id', 'stop_sequence']
            }
            
            for table_name, fields in required_fields.items():
                table = getattr(feed, table_name, None)
                if table is not None and not table.empty:
                    for field in fields:
                        if field not in table.columns:
                            issues.append({
                                'message': f"Required field '{field}' missing in {table_name}.txt",
                                'details': {'file': table_name, 'field': field}
                            })
            return issues
        
        return ValidationRule(
            name="required_fields",
            description="Check for required fields in each file",
            validate_func=validate,
            severity="error",
            category="structure"
        )
    
    @staticmethod
    def data_types_rule() -> ValidationRule:
        """Rule to validate data types for key fields."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            # Check stop coordinates
            if hasattr(feed, 'stops') and feed.stops is not None:
                stops = feed.stops
                if 'stop_lat' in stops.columns and 'stop_lon' in stops.columns:
                    try:
                        pd.to_numeric(stops['stop_lat'], errors='raise')
                        pd.to_numeric(stops['stop_lon'], errors='raise')
                    except (ValueError, TypeError):
                        issues.append({
                            'message': "Stop coordinates must be numeric",
                            'details': {'file': 'stops', 'fields': ['stop_lat', 'stop_lon']}
                        })
            
            # Check route type
            if hasattr(feed, 'routes') and feed.routes is not None:
                routes = feed.routes
                if 'route_type' in routes.columns:
                    try:
                        pd.to_numeric(routes['route_type'], errors='raise')
                    except (ValueError, TypeError):
                        issues.append({
                            'message': "Route type must be numeric",
                            'details': {'file': 'routes', 'field': 'route_type'}
                        })
            
            return issues
        
        return ValidationRule(
            name="data_types",
            description="Check data type compliance",
            validate_func=validate,
            severity="error",
            category="data_quality"
        )
    
    @staticmethod
    def foreign_keys_rule() -> ValidationRule:
        """Rule to validate foreign key relationships."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            # Check route_id in trips references routes
            if (hasattr(feed, 'routes') and hasattr(feed, 'trips') and 
                feed.routes is not None and feed.trips is not None):
                
                route_ids = set(feed.routes['route_id'].unique())
                trip_route_ids = set(feed.trips['route_id'].unique())
                missing_routes = trip_route_ids - route_ids
                
                if missing_routes:
                    issues.append({
                        'message': f"Trips reference non-existent routes",
                        'details': {'missing_route_ids': list(missing_routes)[:10]}
                    })
            
            # Check stop_id in stop_times references stops
            if (hasattr(feed, 'stops') and hasattr(feed, 'stop_times') and 
                feed.stops is not None and feed.stop_times is not None):
                
                stop_ids = set(feed.stops['stop_id'].unique())
                stop_time_stop_ids = set(feed.stop_times['stop_id'].unique())
                missing_stops = stop_time_stop_ids - stop_ids
                
                if missing_stops:
                    issues.append({
                        'message': f"Stop times reference non-existent stops",
                        'details': {'missing_stop_ids': list(missing_stops)[:10]}
                    })
            
            return issues
        
        return ValidationRule(
            name="foreign_keys",
            description="Check foreign key relationships",
            validate_func=validate,
            severity="error",
            category="referential_integrity"
        )
    
    @staticmethod
    def coordinate_validity_rule() -> ValidationRule:
        """Rule to validate coordinate ranges."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            if hasattr(feed, 'stops') and feed.stops is not None:
                stops = feed.stops
                if 'stop_lat' in stops.columns and 'stop_lon' in stops.columns:
                    # Check latitude range
                    invalid_lats = stops[(stops['stop_lat'] < -90) | (stops['stop_lat'] > 90)]
                    if not invalid_lats.empty:
                        issues.append({
                            'message': f"Invalid latitudes found: {len(invalid_lats)} stops",
                            'details': {'count': len(invalid_lats)}
                        })
                    
                    # Check longitude range  
                    invalid_lons = stops[(stops['stop_lon'] < -180) | (stops['stop_lon'] > 180)]
                    if not invalid_lons.empty:
                        issues.append({
                            'message': f"Invalid longitudes found: {len(invalid_lons)} stops",
                            'details': {'count': len(invalid_lons)}
                        })
            
            return issues
        
        return ValidationRule(
            name="coordinate_validity",
            description="Check coordinate validity",
            validate_func=validate,
            severity="error",
            category="geographic"
        )
    
    @staticmethod
    def service_dates_rule() -> ValidationRule:
        """Rule to validate service date ranges."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            if hasattr(feed, 'calendar') and feed.calendar is not None and not feed.calendar.empty:
                calendar = feed.calendar
                
                try:
                    start_dates = pd.to_datetime(calendar['start_date'], format='%Y%m%d')
                    end_dates = pd.to_datetime(calendar['end_date'], format='%Y%m%d')
                    
                    now = pd.Timestamp.now()
                    
                    # Check for past service periods
                    past_services = calendar[end_dates < (now - pd.Timedelta(days=30))]
                    if not past_services.empty:
                        issues.append({
                            'message': f"Service periods ending more than 30 days ago: {len(past_services)}",
                            'details': {'count': len(past_services)}
                        })
                    
                    # Check for unreasonably long service periods
                    long_services = calendar[(end_dates - start_dates).dt.days > 730]  # 2 years
                    if not long_services.empty:
                        issues.append({
                            'message': f"Service periods longer than 2 years: {len(long_services)}",
                            'details': {'count': len(long_services)}
                        })
                        
                except Exception as e:
                    issues.append({
                        'message': f"Error validating service dates: {e}",
                        'details': {}
                    })
            
            return issues
        
        return ValidationRule(
            name="service_dates",
            description="Check service date ranges",
            validate_func=validate,
            severity="warning",
            category="temporal"
        )
    
    @staticmethod
    def stop_times_sequence_rule() -> ValidationRule:
        """Rule to validate stop time sequences."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            if hasattr(feed, 'stop_times') and feed.stop_times is not None:
                stop_times = feed.stop_times
                
                # Check for duplicate stop sequences within trips
                duplicates = stop_times.groupby(['trip_id', 'stop_sequence']).size()
                duplicates = duplicates[duplicates > 1]
                
                if not duplicates.empty:
                    issues.append({
                        'message': f"Duplicate stop sequences found in {len(duplicates)} cases",
                        'details': {'count': len(duplicates)}
                    })
                
                # Check for missing stop sequences
                for trip_id in stop_times['trip_id'].unique()[:100]:  # Check first 100 trips
                    trip_stops = stop_times[stop_times['trip_id'] == trip_id]['stop_sequence'].sort_values()
                    expected_sequence = list(range(1, len(trip_stops) + 1))
                    if list(trip_stops) != expected_sequence:
                        issues.append({
                            'message': f"Non-sequential stop sequences in trip {trip_id}",
                            'details': {'trip_id': trip_id}
                        })
                        break  # Only report one example
            
            return issues
        
        return ValidationRule(
            name="stop_times_sequence",
            description="Check stop time sequences",
            validate_func=validate,
            severity="warning",
            category="sequence"
        )
    
    @staticmethod
    def route_names_rule() -> ValidationRule:
        """Rule to validate route naming consistency."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            if hasattr(feed, 'routes') and feed.routes is not None:
                routes = feed.routes
                
                # Check for routes with neither short nor long name
                no_name = routes[
                    (routes.get('route_short_name', '').astype(str).str.strip() == '') &
                    (routes.get('route_long_name', '').astype(str).str.strip() == '')
                ]
                
                if not no_name.empty:
                    issues.append({
                        'message': f"Routes without names: {len(no_name)}",
                        'details': {
                            'count': len(no_name), 
                            'route_ids': no_name['route_id'].tolist()[:5]
                        }
                    })
            
            return issues
        
        return ValidationRule(
            name="route_names",
            description="Check route naming consistency",
            validate_func=validate,
            severity="info",
            category="naming"
        )
    
    @staticmethod
    def duplicate_ids_rule() -> ValidationRule:
        """Rule to check for duplicate IDs."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            # Check for duplicate route IDs
            if hasattr(feed, 'routes') and feed.routes is not None:
                routes = feed.routes
                if 'route_id' in routes.columns:
                    duplicates = routes[routes['route_id'].duplicated()]
                    if not duplicates.empty:
                        issues.append({
                            'message': f"Duplicate route IDs found: {len(duplicates)}",
                            'details': {'count': len(duplicates)}
                        })
            
            # Check for duplicate stop IDs
            if hasattr(feed, 'stops') and feed.stops is not None:
                stops = feed.stops
                if 'stop_id' in stops.columns:
                    duplicates = stops[stops['stop_id'].duplicated()]
                    if not duplicates.empty:
                        issues.append({
                            'message': f"Duplicate stop IDs found: {len(duplicates)}",
                            'details': {'count': len(duplicates)}
                        })
            
            return issues
        
        return ValidationRule(
            name="duplicate_ids",
            description="Check for duplicate IDs",
            validate_func=validate,
            severity="error",
            category="uniqueness"
        )
    
    @staticmethod
    def speed_validation_rule() -> ValidationRule:
        """Rule to validate travel speeds between stops."""
        def validate(feed) -> List[Dict[str, Any]]:
            issues = []
            
            # This would require more complex logic to calculate distances and times
            # For now, just a placeholder that checks if shapes exist for speed calculation
            if hasattr(feed, 'shapes') and hasattr(feed, 'stop_times'):
                if feed.shapes is None or feed.shapes.empty:
                    issues.append({
                        'message': "No shapes available for speed validation",
                        'details': {'recommendation': 'Add shapes.txt for better validation'}
                    })
            
            return issues
        
        return ValidationRule(
            name="speed_validation",
            description="Validate travel speeds between stops",
            validate_func=validate,
            severity="info",
            category="performance"
        )
