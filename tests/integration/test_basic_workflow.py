"""Integration tests for basic workflow."""

import pytest
from unittest.mock import patch
from pathlib import Path

from databus.gtfs import GTFSProcessor, GTFSValidator
from databus.api import DatabusClient


class TestBasicWorkflow:
    """Integration tests for common usage patterns."""
    
    @pytest.mark.integration
    def test_gtfs_processing_workflow(self, sample_gtfs_zip):
        """Test complete GTFS processing workflow."""
        # This test uses a real GTFS ZIP file created by the fixture
        processor = GTFSProcessor(str(sample_gtfs_zip))
        processor.load_feed()
        
        # Test basic operations work together
        stats = processor.get_feed_stats()
        assert stats['agencies'] > 0
        assert stats['routes'] > 0
        assert stats['stops'] > 0
        
        # Test validation workflow
        validator = GTFSValidator(processor)
        report = validator.validate()
        
        assert report.status in ['valid', 'valid_with_warnings', 'invalid']
        assert 0 <= report.score <= 100
        
        # Test filtering workflow  
        routes = processor.get_routes()
        if not routes.empty:
            route_id = routes.iloc[0]['route_id']
            trips = processor.get_trips(route_id=route_id)
            assert not trips.empty
            assert all(trips['route_id'] == route_id)
    
    @pytest.mark.integration
    @patch('databus.api.client.requests.Session.request')
    def test_api_workflow(self, mock_request, api_responses):
        """Test API client workflow."""
        # Mock successful API responses
        mock_response = lambda data: type('MockResponse', (), {
            'raise_for_status': lambda: None,
            'json': lambda: data
        })()
        
        mock_request.side_effect = [
            mock_response(api_responses['feeds']),
            mock_response(api_responses['feed_detail']),
            mock_response(api_responses['agencies']),
        ]
        
        client = DatabusClient()
        
        # Test workflow: discover feeds -> get details -> get agencies
        feeds = client.get_feeds()
        assert len(feeds) > 0
        
        feed_id = feeds[0].id
        feed_detail = client.get_feed(feed_id)
        assert feed_detail.id == feed_id
        
        agencies = client.get_agencies(feed_id)
        assert len(agencies) > 0
        
        # Verify all requests were made
        assert mock_request.call_count == 3
    
    @pytest.mark.integration 
    def test_configuration_workflow(self, temp_dir):
        """Test configuration management workflow."""
        from databus.utils.config import Config
        
        config = Config()
        
        # Test setting and getting values
        config.set('api.timeout', 45)
        assert config.get('api.timeout') == 45
        
        # Test saving configuration
        config_file = temp_dir / 'test_config.json'
        config.save_to_file(config_file)
        
        assert config_file.exists()
        
        # Test loading configuration
        new_config = Config(config_file)
        assert new_config.get('api.timeout') == 45
    
    @pytest.mark.integration
    def test_cli_integration_mock(self):
        """Test that CLI components can be imported and initialized."""
        # This is a minimal test to ensure CLI imports work
        from databus.cli import main
        assert callable(main)
        
        # Test that the main CLI group can be created
        from databus.cli.main import main as cli_main
        import click
        
        # Test the CLI context
        with click.testing.CliRunner() as runner:
            result = runner.invoke(cli_main, ['--help'])
            assert result.exit_code == 0
            assert 'Databús Python SDK' in result.output
