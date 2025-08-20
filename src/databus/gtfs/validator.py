"""GTFS validator for validating transit feed compliance and quality."""

import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

import gtfs_kit as gk
import pandas as pd

from ..utils.exceptions import GTFSValidationError
from ..validation import ValidationReport, ValidationRule


logger = logging.getLogger(__name__)


class GTFSValidator:
    """Validator for GTFS feeds using gtfs-kit and custom validation rules.
    
    Provides comprehensive validation of GTFS feeds including:
    - Standard GTFS specification compliance
    - Data quality checks
    - Custom validation rules
    - Performance analysis
    
    Args:
        processor: GTFSProcessor instance with loaded feed
        
    Example:
        >>> from databus.gtfs import GTFSProcessor
        >>> processor = GTFSProcessor("feed.zip")
        >>> processor.load_feed()
        >>> validator = GTFSValidator(processor)
        >>> report = validator.validate()
        >>> print(f"Validation score: {report.score}")
    """
    
    def __init__(self, processor=None):
        from .processor import GTFSProcessor
        self.processor = processor
        self._validation_rules = []
        self._setup_default_rules()
    
    def _setup_default_rules(self) -> None:
        """Set up default validation rules."""
        # Add standard GTFS validation rules
        self._validation_rules = [
            ValidationRule(
                "required_files",
                "Check for required GTFS files",
                self._validate_required_files,
                severity="error"
            ),
            ValidationRule(
                "required_fields",
                "Check for required fields in each file",
                self._validate_required_fields,
                severity="error"
            ),
            ValidationRule(
                "data_types",
                "Check data type compliance",
                self._validate_data_types,
                severity="error"
            ),
            ValidationRule(
                "foreign_keys",
                "Check foreign key relationships",
                self._validate_foreign_keys,
                severity="error"
            ),
            ValidationRule(
                "coordinate_validity",
                "Check coordinate validity",
                self._validate_coordinates,
                severity="error"
            ),
            ValidationRule(
                "service_dates",
                "Check service date ranges",
                self._validate_service_dates,
                severity="warning"
            ),
            ValidationRule(
                "stop_times_sequence",
                "Check stop time sequences",
                self._validate_stop_times_sequence,
                severity="warning"
            ),
            ValidationRule(
                "route_names",
                "Check route naming consistency",
                self._validate_route_names,
                severity="info"
            ),
        ]
    
    def add_custom_rule(self, rule: ValidationRule) -> None:
        """Add a custom validation rule.
        
        Args:
            rule: Custom ValidationRule instance
        """
        self._validation_rules.append(rule)
    
    def validate(self, processor=None) -> ValidationReport:
        """Run comprehensive validation on GTFS feed.
        
        Args:
            processor: GTFSProcessor instance (overrides constructor processor)
            
        Returns:
            ValidationReport with results
        """
        if processor:
            self.processor = processor
            
        if not self.processor or not self.processor._is_loaded:
            raise GTFSValidationError("No loaded GTFS processor provided")
        
        logger.info("Starting GTFS validation")
        
        errors = []
        warnings = []
        notices = []
        
        # Run each validation rule
        for rule in self._validation_rules:
            try:
                logger.debug(f"Running validation rule: {rule.name}")
                issues = rule.validate_func(self.processor.feed)
                
                for issue in issues:
                    issue_dict = {
                        'rule': rule.name,
                        'message': issue.get('message', rule.description),
                        'details': issue.get('details', {}),
                        'severity': rule.severity
                    }
                    
                    if rule.severity == "error":
                        errors.append(issue_dict)
                    elif rule.severity == "warning":
                        warnings.append(issue_dict)
                    else:
                        notices.append(issue_dict)
                        
            except Exception as e:
                logger.error(f"Error running validation rule {rule.name}: {e}")
                errors.append({
                    'rule': rule.name,
                    'message': f"Validation rule failed: {e}",
                    'details': {},
                    'severity': 'error'
                })
        
        # Calculate validation score
        total_issues = len(errors) + len(warnings) + len(notices)
        error_weight = 10
        warning_weight = 3
        notice_weight = 1
        
        total_weight = len(errors) * error_weight + len(warnings) * warning_weight + len(notices) * notice_weight
        max_possible_weight = len(self._validation_rules) * error_weight
        
        if max_possible_weight > 0:
            score = max(0, 100 - (total_weight / max_possible_weight * 100))
        else:
            score = 100.0
        
        # Determine overall status
        if errors:
            status = "invalid"
        elif warnings:
            status = "valid_with_warnings"
        else:
            status = "valid"
        
        report = ValidationReport(
            status=status,
            score=score,
            errors=errors,
            warnings=warnings,
            notices=notices,
            feed_path=str(self.processor.feed_path) if self.processor.feed_path else None
        )
        
        logger.info(f"Validation completed. Status: {status}, Score: {score:.1f}")
        return report
    
    def _validate_required_files(self, feed) -> List[Dict[str, Any]]:
        """Validate that required GTFS files are present."""
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
    
    def _validate_required_fields(self, feed) -> List[Dict[str, Any]]:
        """Validate that required fields are present in each file."""
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
    
    def _validate_data_types(self, feed) -> List[Dict[str, Any]]:
        """Validate data types for key fields."""
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
    
    def _validate_foreign_keys(self, feed) -> List[Dict[str, Any]]:
        """Validate foreign key relationships."""
        issues = []
        
        # Check route_id in trips references routes
        if (hasattr(feed, 'routes') and hasattr(feed, 'trips') and 
            feed.routes is not None and feed.trips is not None):
            
            route_ids = set(feed.routes['route_id'].unique())
            trip_route_ids = set(feed.trips['route_id'].unique())
            missing_routes = trip_route_ids - route_ids
            
            if missing_routes:
                issues.append({
                    'message': f"Trips reference non-existent routes: {list(missing_routes)[:5]}",
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
                    'message': f"Stop times reference non-existent stops: {list(missing_stops)[:5]}",
                    'details': {'missing_stop_ids': list(missing_stops)[:10]}
                })
        
        return issues
    
    def _validate_coordinates(self, feed) -> List[Dict[str, Any]]:
        """Validate coordinate ranges."""
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
    
    def _validate_service_dates(self, feed) -> List[Dict[str, Any]]:
        """Validate service date ranges."""
        issues = []
        
        if hasattr(feed, 'calendar') and feed.calendar is not None and not feed.calendar.empty:
            calendar = feed.calendar
            
            # Check for past service periods
            try:
                start_dates = pd.to_datetime(calendar['start_date'], format='%Y%m%d')
                end_dates = pd.to_datetime(calendar['end_date'], format='%Y%m%d')
                
                now = pd.Timestamp.now()
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
    
    def _validate_stop_times_sequence(self, feed) -> List[Dict[str, Any]]:
        """Validate stop time sequences."""
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
        
        return issues
    
    def _validate_route_names(self, feed) -> List[Dict[str, Any]]:
        """Validate route naming consistency."""
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
                    'details': {'count': len(no_name), 'route_ids': no_name['route_id'].tolist()[:5]}
                })
        
        return issues
