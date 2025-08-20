"""Databús API client for programmatic access to transit data APIs."""

import logging
from typing import Dict, List, Optional, Union, Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import Feed, Agency, Route, Stop, Trip
from ..utils.exceptions import DatabusAPIError, DatabusConnectionError


logger = logging.getLogger(__name__)


class DatabusClient:
    """Client for interacting with Databús APIs.
    
    Provides methods for accessing GTFS feeds, agencies, routes, stops,
    and trips through the Databús API endpoints.
    
    Args:
        base_url: Base URL for the Databús API
        api_key: Optional API key for authenticated requests
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        
    Example:
        >>> client = DatabusClient("https://api.databus.cr")
        >>> feeds = client.get_feeds()
        >>> costa_rica_feed = client.get_feed("costa-rica-gtfs")
    """
    
    def __init__(
        self,
        base_url: str = "https://api.databus.cr",
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        headers = {
            "User-Agent": "databus-python-sdk/0.1.0",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            
        self.session.headers.update(headers)
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to API endpoint."""
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.ConnectionError as e:
            raise DatabusConnectionError(f"Failed to connect to {url}: {e}")
        except requests.exceptions.Timeout as e:
            raise DatabusConnectionError(f"Request timed out: {e}")
        except requests.exceptions.HTTPError as e:
            raise DatabusAPIError(f"API request failed: {e}")
        except requests.exceptions.RequestException as e:
            raise DatabusAPIError(f"Request failed: {e}")
    
    def get_feeds(self, country: Optional[str] = None) -> List[Feed]:
        """Get list of available GTFS feeds.
        
        Args:
            country: Filter feeds by country code (e.g., 'CR', 'GT')
            
        Returns:
            List of Feed objects
        """
        params = {}
        if country:
            params["country"] = country
            
        data = self._make_request("GET", "/feeds", params=params)
        return [Feed.from_dict(feed_data) for feed_data in data.get("feeds", [])]
    
    def get_feed(self, feed_id: str) -> Feed:
        """Get specific GTFS feed by ID.
        
        Args:
            feed_id: Feed identifier
            
        Returns:
            Feed object
        """
        data = self._make_request("GET", f"/feeds/{feed_id}")
        return Feed.from_dict(data)
    
    def get_agencies(self, feed_id: str) -> List[Agency]:
        """Get agencies for a specific feed.
        
        Args:
            feed_id: Feed identifier
            
        Returns:
            List of Agency objects
        """
        data = self._make_request("GET", f"/feeds/{feed_id}/agencies")
        return [Agency.from_dict(agency_data) for agency_data in data.get("agencies", [])]
    
    def get_routes(
        self,
        feed_id: str,
        agency_id: Optional[str] = None,
        route_type: Optional[int] = None,
    ) -> List[Route]:
        """Get routes for a specific feed.
        
        Args:
            feed_id: Feed identifier
            agency_id: Filter by agency ID
            route_type: Filter by GTFS route type
            
        Returns:
            List of Route objects
        """
        params = {}
        if agency_id:
            params["agency_id"] = agency_id
        if route_type is not None:
            params["route_type"] = route_type
            
        data = self._make_request("GET", f"/feeds/{feed_id}/routes", params=params)
        return [Route.from_dict(route_data) for route_data in data.get("routes", [])]
    
    def get_stops(
        self,
        feed_id: str,
        bbox: Optional[List[float]] = None,
        route_id: Optional[str] = None,
    ) -> List[Stop]:
        """Get stops for a specific feed.
        
        Args:
            feed_id: Feed identifier
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            route_id: Filter by route ID
            
        Returns:
            List of Stop objects
        """
        params = {}
        if bbox:
            params["bbox"] = ",".join(map(str, bbox))
        if route_id:
            params["route_id"] = route_id
            
        data = self._make_request("GET", f"/feeds/{feed_id}/stops", params=params)
        return [Stop.from_dict(stop_data) for stop_data in data.get("stops", [])]
    
    def get_trips(
        self,
        feed_id: str,
        route_id: Optional[str] = None,
        service_id: Optional[str] = None,
    ) -> List[Trip]:
        """Get trips for a specific feed.
        
        Args:
            feed_id: Feed identifier
            route_id: Filter by route ID
            service_id: Filter by service ID
            
        Returns:
            List of Trip objects
        """
        params = {}
        if route_id:
            params["route_id"] = route_id
        if service_id:
            params["service_id"] = service_id
            
        data = self._make_request("GET", f"/feeds/{feed_id}/trips", params=params)
        return [Trip.from_dict(trip_data) for trip_data in data.get("trips", [])]
        
    def download_feed(self, feed_id: str, output_path: str) -> str:
        """Download GTFS feed as ZIP file.
        
        Args:
            feed_id: Feed identifier
            output_path: Path to save the downloaded file
            
        Returns:
            Path to downloaded file
        """
        url = urljoin(self.base_url + "/", f"/feeds/{feed_id}/download")
        
        try:
            response = self.session.get(url, stream=True, timeout=self.timeout)
            response.raise_for_status()
            
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            logger.info(f"Downloaded feed {feed_id} to {output_path}")
            return output_path
            
        except requests.exceptions.RequestException as e:
            raise DatabusAPIError(f"Failed to download feed: {e}")
