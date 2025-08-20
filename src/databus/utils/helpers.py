"""Helper functions and utilities for databus operations."""

import math
import re
from datetime import datetime, timedelta
from typing import Tuple, Union, Optional


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format.
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Formatted size string (e.g., "1.5 MB", "2.3 GB")
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    
    return f"{s} {size_names[i]}"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string (e.g., "2m 30s", "1h 15m")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    
    if minutes < 60:
        if remaining_seconds > 0:
            return f"{minutes}m {remaining_seconds:.1f}s"
        return f"{minutes}m"
    
    hours = minutes // 60
    remaining_minutes = minutes % 60
    
    if remaining_minutes > 0:
        return f"{hours}h {remaining_minutes}m"
    return f"{hours}h"


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points using Haversine formula.
    
    Args:
        lat1: Latitude of first point
        lon1: Longitude of first point
        lat2: Latitude of second point
        lon2: Longitude of second point
        
    Returns:
        Distance in kilometers
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Earth's radius in kilometers
    r = 6371
    
    return c * r


def parse_gtfs_time(time_str: str) -> Optional[timedelta]:
    """Parse GTFS time format (HH:MM:SS) to timedelta.
    
    GTFS allows times beyond 24:00:00 to represent times after midnight.
    
    Args:
        time_str: Time string in HH:MM:SS format
        
    Returns:
        timedelta object or None if parsing fails
    """
    if not time_str or time_str.strip() == '':
        return None
    
    try:
        # Match HH:MM:SS format
        match = re.match(r'^(\d{1,2}):(\d{2}):(\d{2})$', time_str.strip())
        if not match:
            return None
        
        hours, minutes, seconds = map(int, match.groups())
        
        # Validate ranges (allowing hours > 23 for GTFS)
        if minutes >= 60 or seconds >= 60:
            return None
        
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)
        
    except (ValueError, AttributeError):
        return None


def format_gtfs_time(td: timedelta) -> str:
    """Format timedelta to GTFS time format (HH:MM:SS).
    
    Args:
        td: timedelta object
        
    Returns:
        Time string in HH:MM:SS format
    """
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def validate_coordinate(lat: float, lon: float) -> Tuple[bool, str]:
    """Validate geographic coordinates.
    
    Args:
        lat: Latitude
        lon: Longitude
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return False, "Coordinates must be numeric"
    
    if lat < -90 or lat > 90:
        return False, f"Latitude {lat} is out of range [-90, 90]"
    
    if lon < -180 or lon > 180:
        return False, f"Longitude {lon} is out of range [-180, 180]"
    
    return True, ""


def parse_gtfs_color(color_str: str) -> Optional[str]:
    """Parse and validate GTFS color format.
    
    Args:
        color_str: Color string (6-digit hex without #)
        
    Returns:
        Validated color string or None
    """
    if not color_str:
        return None
    
    # Remove # if present
    color = color_str.strip().lstrip('#')
    
    # Validate 6-digit hex
    if re.match(r'^[0-9A-Fa-f]{6}$', color):
        return color.upper()
    
    return None


def clean_gtfs_text(text: str, max_length: Optional[int] = None) -> str:
    """Clean and validate GTFS text fields.
    
    Args:
        text: Input text
        max_length: Maximum allowed length
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    # Remove excessive whitespace and normalize
    cleaned = ' '.join(text.strip().split())
    
    # Truncate if necessary
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].strip()
    
    return cleaned


def get_route_type_name(route_type: int) -> str:
    """Get human-readable name for GTFS route type.
    
    Args:
        route_type: GTFS route type code
        
    Returns:
        Route type name
    """
    route_types = {
        0: "Tram, Streetcar, Light rail",
        1: "Subway, Metro",
        2: "Rail",
        3: "Bus", 
        4: "Ferry",
        5: "Cable tram",
        6: "Aerial lift, suspended cable car",
        7: "Funicular",
        11: "Trolleybus",
        12: "Monorail",
    }
    
    return route_types.get(route_type, f"Unknown ({route_type})")


def generate_unique_id(prefix: str = "", length: int = 8) -> str:
    """Generate a unique identifier.
    
    Args:
        prefix: Optional prefix for the ID
        length: Length of random part
        
    Returns:
        Unique identifier string
    """
    import random
    import string
    
    random_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    
    if prefix:
        return f"{prefix}_{random_part}"
    return random_part


def safe_divide(numerator: Union[int, float], denominator: Union[int, float]) -> float:
    """Safely divide two numbers, handling division by zero.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        
    Returns:
        Division result or 0.0 if denominator is zero
    """
    if denominator == 0:
        return 0.0
    return numerator / denominator


def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to specified length with optional suffix.
    
    Args:
        text: Input text
        max_length: Maximum length including suffix
        suffix: Suffix to add when truncating
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    
    if len(suffix) >= max_length:
        return text[:max_length]
    
    return text[:max_length - len(suffix)] + suffix
