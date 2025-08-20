"""Custom exceptions for the databus package."""


class DatabusError(Exception):
    """Base exception class for all databus-related errors."""
    pass


class DatabusAPIError(DatabusError):
    """Exception raised for API-related errors."""
    pass


class DatabusConnectionError(DatabusError):
    """Exception raised for connection-related errors."""
    pass


class GTFSProcessingError(DatabusError):
    """Exception raised during GTFS data processing."""
    pass


class GTFSValidationError(DatabusError):
    """Exception raised during GTFS validation."""
    pass


class ConfigurationError(DatabusError):
    """Exception raised for configuration-related errors."""
    pass


class DataFormatError(DatabusError):
    """Exception raised for data format-related errors."""
    pass
