"""Databús Python SDK and command-line toolkit.

A comprehensive toolkit for GTFS data processing, validation, and analysis.
Provides programmatic access to Databús APIs, GTFS manipulation utilities,
data conversion tools, and automated testing frameworks for transit data
workflows and research applications.
"""

__version__ = "0.1.0"
__author__ = "Fabián Abarca"
__email__ = "ensinergia@gmail.com"

# Core imports for easy access
from .api import DatabusClient
from .gtfs import GTFSProcessor, GTFSValidator
from .validation import ValidationReport, ValidationRule

__all__ = [
    "DatabusClient",
    "GTFSProcessor",
    "GTFSValidator",
    "ValidationReport",
    "ValidationRule",
]
