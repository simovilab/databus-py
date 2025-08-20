"""Unit tests for DatabusClient class."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
import json

from databus.api import DatabusClient, Feed, Agency, Route, Stop, Trip
from databus.utils.exceptions import DatabusAPIError, DatabusConnectionError


class TestDatabusClient:
    """Test cases for DatabusClient class."""
    
    def test_init_default_params(self):
        """Test initialization with default parameters."""
        client = DatabusClient()
        
        assert client.base_url == "https://api.databus.cr"
        assert client.api_key is None
        assert client.timeout == 30
        assert "User-Agent" in client.session.headers
        assert "databus-python-sdk" in client.session.headers["User-Agent"]
    
    def test_init_custom_params(self):
        """Test initialization with custom parameters."""
        client = DatabusClient(
            base_url="https://custom.api.com",
            api_key="test_key",
            timeout=60,
            max_retries=5
        )
        
        assert client.base_url == "https://custom.api.com"
        assert client.api_key == "test_key"
        assert client.timeout == 60
        assert "Authorization" in client.session.headers
        assert client.session.headers["Authorization"] == "Bearer test_key"
    
    @patch('requests.Session.request')
    def test_make_request_success(self, mock_request):
        """Test successful API request."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"result": "success"}
        mock_request.return_value = mock_response
        
        client = DatabusClient()
        result = client._make_request("GET", "/test")
        
        assert result == {"result": "success"}
        mock_request.assert_called_once()
        mock_response.raise_for_status.assert_called_once()
    
    @patch('requests.Session.request')
    def test_make_request_connection_error(self, mock_request):
        """Test connection error handling."""
        mock_request.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        client = DatabusClient()
        
        with pytest.raises(DatabusConnectionError, match="Failed to connect"):
            client._make_request("GET", "/test")
    
    @patch('requests.Session.request')
    def test_make_request_timeout_error(self, mock_request):
        """Test timeout error handling."""
        mock_request.side_effect = requests.exceptions.Timeout("Request timed out")
        
        client = DatabusClient()
        
        with pytest.raises(DatabusConnectionError, match="Request timed out"):
            client._make_request("GET", "/test")
    
    @patch('requests.Session.request')
    def test_make_request_http_error(self, mock_request):
        """Test HTTP error handling."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_request.return_value = mock_response
        
        client = DatabusClient()
        
        with pytest.raises(DatabusAPIError, match="API request failed"):
            client._make_request("GET", "/test")
    
    @patch.object(DatabusClient, '_make_request')
    def test_get_feeds_no_filter(self, mock_request, api_responses):
        """Test getting all feeds without filter."""
        mock_request.return_value = api_responses["feeds"]
        
        client = DatabusClient()
        feeds = client.get_feeds()
        
        assert len(feeds) == 1
        assert isinstance(feeds[0], Feed)
        assert feeds[0].id == "costa-rica-gtfs"
        assert feeds[0].country_code == "CR"
        mock_request.assert_called_once_with("GET", "/feeds", params={})\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_feeds_with_country_filter(self, mock_request, api_responses):\n        \"\"\"Test getting feeds filtered by country.\"\"\"\n        mock_request.return_value = api_responses[\"feeds\"]\n        \n        client = DatabusClient()\n        feeds = client.get_feeds(country=\"CR\")\n        \n        assert len(feeds) == 1\n        mock_request.assert_called_once_with(\"GET\", \"/feeds\", params={\"country\": \"CR\"})\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_feed(self, mock_request, api_responses):\n        \"\"\"Test getting specific feed by ID.\"\"\"\n        mock_request.return_value = api_responses[\"feed_detail\"]\n        \n        client = DatabusClient()\n        feed = client.get_feed(\"costa-rica-gtfs\")\n        \n        assert isinstance(feed, Feed)\n        assert feed.id == \"costa-rica-gtfs\"\n        assert feed.name == \"Costa Rica GTFS\"\n        mock_request.assert_called_once_with(\"GET\", \"/feeds/costa-rica-gtfs\")\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_agencies(self, mock_request, api_responses):\n        \"\"\"Test getting agencies for a feed.\"\"\"\n        mock_request.return_value = api_responses[\"agencies\"]\n        \n        client = DatabusClient()\n        agencies = client.get_agencies(\"costa-rica-gtfs\")\n        \n        assert len(agencies) == 1\n        assert isinstance(agencies[0], Agency)\n        assert agencies[0].agency_id == \"COSEVI\"\n        mock_request.assert_called_once_with(\"GET\", \"/feeds/costa-rica-gtfs/agencies\")\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_routes_no_filter(self, mock_request):\n        \"\"\"Test getting routes without filter.\"\"\"\n        mock_response = {\n            \"routes\": [\n                {\n                    \"route_id\": \"route_1\",\n                    \"route_type\": 3,\n                    \"route_short_name\": \"R1\",\n                    \"route_long_name\": \"Test Route\"\n                }\n            ]\n        }\n        mock_request.return_value = mock_response\n        \n        client = DatabusClient()\n        routes = client.get_routes(\"costa-rica-gtfs\")\n        \n        assert len(routes) == 1\n        assert isinstance(routes[0], Route)\n        assert routes[0].route_id == \"route_1\"\n        mock_request.assert_called_once_with(\n            \"GET\", \"/feeds/costa-rica-gtfs/routes\", params={}\n        )\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_routes_with_filters(self, mock_request):\n        \"\"\"Test getting routes with agency and type filters.\"\"\"\n        mock_response = {\"routes\": []}\n        mock_request.return_value = mock_response\n        \n        client = DatabusClient()\n        routes = client.get_routes(\n            \"costa-rica-gtfs\", \n            agency_id=\"COSEVI\", \n            route_type=3\n        )\n        \n        assert len(routes) == 0\n        mock_request.assert_called_once_with(\n            \"GET\", \n            \"/feeds/costa-rica-gtfs/routes\", \n            params={\"agency_id\": \"COSEVI\", \"route_type\": 3}\n        )\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_stops_no_filter(self, mock_request):\n        \"\"\"Test getting stops without filter.\"\"\"\n        mock_response = {\n            \"stops\": [\n                {\n                    \"stop_id\": \"stop_1\",\n                    \"stop_name\": \"Test Stop\",\n                    \"stop_lat\": 9.9281,\n                    \"stop_lon\": -84.0907\n                }\n            ]\n        }\n        mock_request.return_value = mock_response\n        \n        client = DatabusClient()\n        stops = client.get_stops(\"costa-rica-gtfs\")\n        \n        assert len(stops) == 1\n        assert isinstance(stops[0], Stop)\n        assert stops[0].stop_id == \"stop_1\"\n        mock_request.assert_called_once_with(\n            \"GET\", \"/feeds/costa-rica-gtfs/stops\", params={}\n        )\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_stops_with_bbox(self, mock_request):\n        \"\"\"Test getting stops with bounding box filter.\"\"\"\n        mock_response = {\"stops\": []}\n        mock_request.return_value = mock_response\n        \n        client = DatabusClient()\n        bbox = [-84.2, 9.8, -83.9, 10.1]\n        stops = client.get_stops(\"costa-rica-gtfs\", bbox=bbox)\n        \n        assert len(stops) == 0\n        mock_request.assert_called_once_with(\n            \"GET\", \n            \"/feeds/costa-rica-gtfs/stops\", \n            params={\"bbox\": \"-84.2,9.8,-83.9,10.1\"}\n        )\n    \n    @patch.object(DatabusClient, '_make_request')\n    def test_get_trips(self, mock_request):\n        \"\"\"Test getting trips.\"\"\"\n        mock_response = {\n            \"trips\": [\n                {\n                    \"route_id\": \"route_1\",\n                    \"service_id\": \"service_1\",\n                    \"trip_id\": \"trip_1\",\n                    \"trip_headsign\": \"Downtown\"\n                }\n            ]\n        }\n        mock_request.return_value = mock_response\n        \n        client = DatabusClient()\n        trips = client.get_trips(\"costa-rica-gtfs\")\n        \n        assert len(trips) == 1\n        assert isinstance(trips[0], Trip)\n        assert trips[0].trip_id == \"trip_1\"\n        mock_request.assert_called_once_with(\n            \"GET\", \"/feeds/costa-rica-gtfs/trips\", params={}\n        )\n    \n    @patch('requests.Session.get')\n    def test_download_feed_success(self, mock_get, temp_dir):\n        \"\"\"Test successful feed download.\"\"\"\n        # Mock response with file content\n        mock_response = Mock()\n        mock_response.raise_for_status.return_value = None\n        mock_response.iter_content.return_value = [b\"fake gtfs data\"]\n        mock_get.return_value = mock_response\n        \n        client = DatabusClient()\n        output_path = temp_dir / \"downloaded_feed.zip\"\n        result_path = client.download_feed(\"costa-rica-gtfs\", str(output_path))\n        \n        assert result_path == str(output_path)\n        assert output_path.exists()\n        mock_get.assert_called_once()\n        mock_response.raise_for_status.assert_called_once()\n    \n    @patch('requests.Session.get')\n    def test_download_feed_request_error(self, mock_get):\n        \"\"\"Test download feed with request error.\"\"\"\n        mock_get.side_effect = requests.exceptions.RequestException(\"Download failed\")\n        \n        client = DatabusClient()\n        \n        with pytest.raises(DatabusAPIError, match=\"Failed to download feed\"):\n            client.download_feed(\"costa-rica-gtfs\", \"output.zip\")\n    \n    def test_url_construction(self):\n        \"\"\"Test URL construction for different endpoints.\"\"\"\n        client = DatabusClient(base_url=\"https://api.test.com\")\n        \n        with patch.object(client, '_make_request') as mock_request:\n            # Test that URLs are constructed correctly\n            client.get_feeds()\n            args = mock_request.call_args\n            # The _make_request should be called with the endpoint\n            assert args[0] == (\"GET\", \"/feeds\")\n    \n    def test_base_url_trailing_slash(self):\n        \"\"\"Test that trailing slash is removed from base URL.\"\"\"\n        client = DatabusClient(base_url=\"https://api.test.com/\")\n        assert client.base_url == \"https://api.test.com\""
