"""Pydantic models for Databús API data structures."""

from datetime import datetime, date
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator


class Feed(BaseModel):
    """Represents a GTFS feed in the Databús system."""
    
    id: str = Field(..., description="Feed identifier")
    name: str = Field(..., description="Human-readable feed name")
    description: Optional[str] = Field(None, description="Feed description")
    country_code: str = Field(..., description="ISO country code")
    region: Optional[str] = Field(None, description="Region or state")
    city: Optional[str] = Field(None, description="Primary city served")
    operator: Optional[str] = Field(None, description="Transit operator name")
    url: Optional[str] = Field(None, description="Official feed URL")
    download_url: Optional[str] = Field(None, description="Download URL")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    file_size: Optional[int] = Field(None, description="Feed file size in bytes")
    version: Optional[str] = Field(None, description="Feed version")
    status: str = Field(default="active", description="Feed status")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Feed":
        """Create Feed instance from dictionary."""
        return cls(**data)


class Agency(BaseModel):
    """Represents a transit agency from GTFS."""
    
    agency_id: Optional[str] = Field(None, description="Agency ID")
    agency_name: str = Field(..., description="Agency name")
    agency_url: str = Field(..., description="Agency URL")
    agency_timezone: str = Field(..., description="Agency timezone")
    agency_lang: Optional[str] = Field(None, description="Agency language")
    agency_phone: Optional[str] = Field(None, description="Agency phone")
    agency_fare_url: Optional[str] = Field(None, description="Fare information URL")
    agency_email: Optional[str] = Field(None, description="Agency email")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Agency":
        """Create Agency instance from dictionary."""
        return cls(**data)


class Route(BaseModel):
    """Represents a transit route from GTFS."""
    
    route_id: str = Field(..., description="Route ID")
    agency_id: Optional[str] = Field(None, description="Agency ID")
    route_short_name: Optional[str] = Field(None, description="Short route name")
    route_long_name: Optional[str] = Field(None, description="Long route name")
    route_desc: Optional[str] = Field(None, description="Route description")
    route_type: int = Field(..., description="GTFS route type")
    route_url: Optional[str] = Field(None, description="Route URL")
    route_color: Optional[str] = Field(None, description="Route color")
    route_text_color: Optional[str] = Field(None, description="Route text color")
    route_sort_order: Optional[int] = Field(None, description="Route sort order")
    
    @validator('route_type')
    def validate_route_type(cls, v):
        """Validate GTFS route type."""
        valid_types = {0, 1, 2, 3, 4, 5, 6, 7, 11, 12}
        if v not in valid_types:
            raise ValueError(f"Invalid route type: {v}")
        return v
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Route":
        """Create Route instance from dictionary."""
        return cls(**data)


class Stop(BaseModel):
    """Represents a transit stop from GTFS."""
    
    stop_id: str = Field(..., description="Stop ID")
    stop_code: Optional[str] = Field(None, description="Stop code")
    stop_name: str = Field(..., description="Stop name")
    stop_desc: Optional[str] = Field(None, description="Stop description")
    stop_lat: float = Field(..., description="Stop latitude", ge=-90, le=90)
    stop_lon: float = Field(..., description="Stop longitude", ge=-180, le=180)
    zone_id: Optional[str] = Field(None, description="Zone ID")
    stop_url: Optional[str] = Field(None, description="Stop URL")
    location_type: Optional[int] = Field(None, description="Location type")
    parent_station: Optional[str] = Field(None, description="Parent station ID")
    stop_timezone: Optional[str] = Field(None, description="Stop timezone")
    wheelchair_boarding: Optional[int] = Field(None, description="Wheelchair accessibility")
    platform_code: Optional[str] = Field(None, description="Platform code")
    
    @validator('location_type')
    def validate_location_type(cls, v):
        """Validate GTFS location type."""
        if v is not None and v not in {0, 1, 2, 3, 4}:
            raise ValueError(f"Invalid location type: {v}")
        return v
    
    @validator('wheelchair_boarding')
    def validate_wheelchair_boarding(cls, v):
        """Validate wheelchair boarding value."""
        if v is not None and v not in {0, 1, 2}:
            raise ValueError(f"Invalid wheelchair boarding value: {v}")
        return v
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Stop":
        """Create Stop instance from dictionary."""
        return cls(**data)


class Trip(BaseModel):
    """Represents a transit trip from GTFS."""
    
    route_id: str = Field(..., description="Route ID")
    service_id: str = Field(..., description="Service ID")
    trip_id: str = Field(..., description="Trip ID")
    trip_headsign: Optional[str] = Field(None, description="Trip headsign")
    trip_short_name: Optional[str] = Field(None, description="Trip short name")
    direction_id: Optional[int] = Field(None, description="Direction ID")
    block_id: Optional[str] = Field(None, description="Block ID")
    shape_id: Optional[str] = Field(None, description="Shape ID")
    wheelchair_accessible: Optional[int] = Field(None, description="Wheelchair accessibility")
    bikes_allowed: Optional[int] = Field(None, description="Bikes allowed")
    
    @validator('direction_id')
    def validate_direction_id(cls, v):
        """Validate direction ID."""
        if v is not None and v not in {0, 1}:
            raise ValueError(f"Invalid direction ID: {v}")
        return v
    
    @validator('wheelchair_accessible')
    def validate_wheelchair_accessible(cls, v):
        """Validate wheelchair accessibility value."""
        if v is not None and v not in {0, 1, 2}:
            raise ValueError(f"Invalid wheelchair accessible value: {v}")
        return v
    
    @validator('bikes_allowed')
    def validate_bikes_allowed(cls, v):
        """Validate bikes allowed value."""
        if v is not None and v not in {0, 1, 2}:
            raise ValueError(f"Invalid bikes allowed value: {v}")
        return v
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Trip":
        """Create Trip instance from dictionary."""
        return cls(**data)


class ValidationResult(BaseModel):
    """Represents a validation result from the API."""
    
    status: str = Field(..., description="Validation status")
    score: Optional[float] = Field(None, description="Validation score", ge=0, le=100)
    errors: List[Dict[str, Any]] = Field(default_factory=list, description="Validation errors")
    warnings: List[Dict[str, Any]] = Field(default_factory=list, description="Validation warnings")
    notices: List[Dict[str, Any]] = Field(default_factory=list, description="Validation notices")
    created_at: datetime = Field(..., description="Validation timestamp")
    feed_id: str = Field(..., description="Feed identifier")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValidationResult":
        """Create ValidationResult instance from dictionary."""
        return cls(**data)
