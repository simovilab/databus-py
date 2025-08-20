"""Unit tests for helper functions."""

import pytest
from datetime import timedelta

from databus.utils.helpers import (
    format_file_size,
    format_duration,
    calculate_distance,
    parse_gtfs_time,
    format_gtfs_time,
    validate_coordinate,
    parse_gtfs_color,
    clean_gtfs_text,
    get_route_type_name,
    generate_unique_id,
    safe_divide,
    truncate_text,
)


class TestHelperFunctions:
    """Test cases for utility helper functions."""
    
    def test_format_file_size(self):
        """Test file size formatting."""
        assert format_file_size(0) == "0 B"
        assert format_file_size(1024) == "1.0 KB"
        assert format_file_size(1048576) == "1.0 MB"
        assert format_file_size(1073741824) == "1.0 GB"
        assert format_file_size(1536) == "1.5 KB"
        assert format_file_size(2621440) == "2.5 MB"
    
    def test_format_duration(self):
        """Test duration formatting."""
        assert format_duration(30.5) == "30.5s"
        assert format_duration(60) == "1m"
        assert format_duration(90.5) == "1m 30.5s"
        assert format_duration(3600) == "1h"
        assert format_duration(3660) == "1h 1m"
        assert format_duration(7200) == "2h"
    
    def test_calculate_distance(self):
        """Test distance calculation using Haversine formula."""
        # Test same point
        assert calculate_distance(0, 0, 0, 0) == 0
        
        # Test known distance (approximately)
        # Distance between San José and Cartago, Costa Rica
        san_jose_lat, san_jose_lon = 9.9281, -84.0907
        cartago_lat, cartago_lon = 9.8644, -83.9173
        
        distance = calculate_distance(san_jose_lat, san_jose_lon, cartago_lat, cartago_lon)
        assert 15 < distance < 25  # Approximately 20 km
    
    def test_parse_gtfs_time_valid(self):
        """Test parsing valid GTFS time formats."""
        assert parse_gtfs_time("08:30:00") == timedelta(hours=8, minutes=30)
        assert parse_gtfs_time("12:00:00") == timedelta(hours=12)
        assert parse_gtfs_time("23:59:59") == timedelta(hours=23, minutes=59, seconds=59)
        assert parse_gtfs_time("25:30:00") == timedelta(hours=25, minutes=30)  # GTFS allows > 24h
    
    def test_parse_gtfs_time_invalid(self):
        """Test parsing invalid GTFS time formats."""
        assert parse_gtfs_time("") is None
        assert parse_gtfs_time("  ") is None
        assert parse_gtfs_time("invalid") is None
        assert parse_gtfs_time("8:30:00") is None  # Missing leading zero not supported
        assert parse_gtfs_time("08:60:00") is None  # Invalid minutes
        assert parse_gtfs_time("08:30:60") is None  # Invalid seconds
    
    def test_format_gtfs_time(self):
        """Test formatting timedelta to GTFS time format."""
        assert format_gtfs_time(timedelta(hours=8, minutes=30)) == "08:30:00"
        assert format_gtfs_time(timedelta(hours=12)) == "12:00:00"
        assert format_gtfs_time(timedelta(hours=25, minutes=30)) == "25:30:00"
        assert format_gtfs_time(timedelta(seconds=3661)) == "01:01:01"
    
    def test_validate_coordinate_valid(self):
        """Test validation of valid coordinates."""
        is_valid, error = validate_coordinate(9.9281, -84.0907)
        assert is_valid is True
        assert error == ""
        
        # Test edge cases
        is_valid, error = validate_coordinate(90, 180)
        assert is_valid is True
        
        is_valid, error = validate_coordinate(-90, -180)
        assert is_valid is True
    
    def test_validate_coordinate_invalid(self):
        """Test validation of invalid coordinates."""
        # Invalid latitude
        is_valid, error = validate_coordinate(91, 0)
        assert is_valid is False
        assert "Latitude" in error
        
        is_valid, error = validate_coordinate(-91, 0)
        assert is_valid is False
        assert "Latitude" in error
        
        # Invalid longitude
        is_valid, error = validate_coordinate(0, 181)
        assert is_valid is False
        assert "Longitude" in error
        
        is_valid, error = validate_coordinate(0, -181)
        assert is_valid is False
        assert "Longitude" in error
        
        # Non-numeric coordinates
        is_valid, error = validate_coordinate("invalid", 0)
        assert is_valid is False
        assert "numeric" in error
    
    def test_parse_gtfs_color_valid(self):
        """Test parsing valid GTFS color formats."""
        assert parse_gtfs_color("FF0000") == "FF0000"
        assert parse_gtfs_color("ff0000") == "FF0000"  # Converted to uppercase
        assert parse_gtfs_color("#FF0000") == "FF0000"  # Hash removed
        assert parse_gtfs_color("  FF0000  ") == "FF0000"  # Whitespace stripped
    
    def test_parse_gtfs_color_invalid(self):
        """Test parsing invalid GTFS color formats."""
        assert parse_gtfs_color("") is None
        assert parse_gtfs_color("FF00") is None  # Too short
        assert parse_gtfs_color("FF0000FF") is None  # Too long
        assert parse_gtfs_color("GG0000") is None  # Invalid hex
        assert parse_gtfs_color("FF00ZZ") is None  # Invalid hex
    
    def test_clean_gtfs_text(self):
        """Test cleaning and validating GTFS text fields."""
        assert clean_gtfs_text("") == ""
        assert clean_gtfs_text("  Hello   World  ") == "Hello World"
        assert clean_gtfs_text("Text\\nWith\\nNewlines") == "Text With Newlines"
        
        # Test max length
        long_text = "This is a very long text that should be truncated"
        assert len(clean_gtfs_text(long_text, max_length=20)) <= 20
    
    def test_get_route_type_name(self):
        """Test getting human-readable route type names."""
        assert get_route_type_name(0) == "Tram, Streetcar, Light rail"
        assert get_route_type_name(1) == "Subway, Metro"
        assert get_route_type_name(3) == "Bus"
        assert get_route_type_name(999) == "Unknown (999)"
    
    def test_generate_unique_id(self):
        """Test unique ID generation."""
        # Test default parameters
        id1 = generate_unique_id()
        id2 = generate_unique_id()
        assert id1 != id2
        assert len(id1) == 8
        
        # Test with prefix
        id_with_prefix = generate_unique_id(prefix="test")
        assert id_with_prefix.startswith("test_")
        assert len(id_with_prefix) == 13  # "test_" + 8 chars
        
        # Test custom length
        id_custom_length = generate_unique_id(length=12)
        assert len(id_custom_length) == 12
    
    def test_safe_divide(self):
        """Test safe division with zero handling."""
        assert safe_divide(10, 2) == 5.0
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(0, 5) == 0.0
        assert safe_divide(-10, 2) == -5.0
    
    def test_truncate_text(self):
        """Test text truncation."""
        text = "This is a long text"
        
        # No truncation needed
        assert truncate_text(text, 50) == text
        
        # Truncation with default suffix
        truncated = truncate_text(text, 10)
        assert len(truncated) == 10
        assert truncated.endswith("...")
        
        # Truncation with custom suffix
        truncated_custom = truncate_text(text, 10, suffix="[...]")
        assert len(truncated_custom) == 10
        assert truncated_custom.endswith("[...]")
        
        # Edge case: suffix longer than max length
        short_truncated = truncate_text(text, 5, suffix="[...]")
        assert len(short_truncated) == 5
        assert not short_truncated.endswith("[...]")
