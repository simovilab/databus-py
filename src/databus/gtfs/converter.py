"""GTFS format converter for transforming between different formats."""

import logging
from pathlib import Path
from typing import Dict, Any, Union, Optional
import tempfile

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString

from ..utils.exceptions import GTFSProcessingError


logger = logging.getLogger(__name__)


class GTFSConverter:
    """Converter for transforming GTFS data between different formats.
    
    Supports conversion to various formats including:
    - GeoJSON
    - Shapefile
    - Parquet
    - CSV
    - Excel
    """
    
    def __init__(self, processor):
        """Initialize converter with GTFS processor.
        
        Args:
            processor: GTFSProcessor instance with loaded feed
        """
        from .processor import GTFSProcessor
        self.processor = processor
    
    def to_geojson(self, output_dir: Union[str, Path], include_shapes: bool = True) -> Dict[str, Path]:
        """Convert GTFS to GeoJSON format.
        
        Args:
            output_dir: Directory to save GeoJSON files
            include_shapes: Whether to include route shapes
            
        Returns:
            Dictionary mapping layer names to file paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        files = {}
        
        try:
            # Convert stops to GeoJSON
            stops_gdf = self.processor.get_stops(as_geodataframe=True)
            if not stops_gdf.empty:
                stops_path = output_dir / "stops.geojson"
                stops_gdf.to_file(stops_path, driver="GeoJSON")
                files["stops"] = stops_path
                logger.info(f"Exported {len(stops_gdf)} stops to {stops_path}")
            
            # Convert shapes to GeoJSON if available
            if include_shapes:
                shapes_gdf = self.processor.get_shapes(as_geodataframe=True)
                if not shapes_gdf.empty:
                    shapes_path = output_dir / "shapes.geojson"
                    shapes_gdf.to_file(shapes_path, driver="GeoJSON")
                    files["shapes"] = shapes_path
                    logger.info(f"Exported shapes to {shapes_path}")
            
            return files
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to convert to GeoJSON: {e}")
    
    def to_parquet(self, output_dir: Union[str, Path]) -> Dict[str, Path]:
        """Convert GTFS to Parquet format.
        
        Args:
            output_dir: Directory to save Parquet files
            
        Returns:
            Dictionary mapping table names to file paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        files = {}
        tables = self.processor.to_dict()
        
        try:
            for table_name, df in tables.items():
                if not df.empty:
                    parquet_path = output_dir / f"{table_name}.parquet"
                    df.to_parquet(parquet_path, index=False)
                    files[table_name] = parquet_path
                    logger.info(f"Exported {table_name} ({len(df)} rows) to {parquet_path}")
            
            return files
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to convert to Parquet: {e}")
    
    def to_excel(self, output_path: Union[str, Path]) -> Path:
        """Convert GTFS to Excel format with multiple sheets.
        
        Args:
            output_path: Path for output Excel file
            
        Returns:
            Path to created Excel file
        """
        output_path = Path(output_path)
        tables = self.processor.to_dict()
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for table_name, df in tables.items():
                    if not df.empty:
                        # Excel sheet names are limited to 31 characters
                        sheet_name = table_name[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        logger.info(f"Added {table_name} sheet with {len(df)} rows")
            
            logger.info(f"Exported GTFS to Excel: {output_path}")
            return output_path
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to convert to Excel: {e}")
    
    def to_csv(self, output_dir: Union[str, Path]) -> Dict[str, Path]:
        """Convert GTFS to CSV format (same as original but cleaned).
        
        Args:
            output_dir: Directory to save CSV files
            
        Returns:
            Dictionary mapping table names to file paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        files = {}
        tables = self.processor.to_dict()
        
        try:
            for table_name, df in tables.items():
                if not df.empty:
                    csv_path = output_dir / f"{table_name}.txt"
                    df.to_csv(csv_path, index=False)
                    files[table_name] = csv_path
                    logger.info(f"Exported {table_name} ({len(df)} rows) to {csv_path}")
            
            return files
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to convert to CSV: {e}")
    
    def to_spatial_formats(
        self, 
        output_dir: Union[str, Path],
        formats: list = ['geojson', 'shapefile']
    ) -> Dict[str, Dict[str, Path]]:
        """Convert spatial GTFS data to multiple spatial formats.
        
        Args:
            output_dir: Directory to save files
            formats: List of formats to export ('geojson', 'shapefile', 'gpkg')
            
        Returns:
            Nested dictionary with format and layer names mapping to paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results = {}
        
        # Get spatial data
        stops_gdf = self.processor.get_stops(as_geodataframe=True)
        shapes_gdf = self.processor.get_shapes(as_geodataframe=True)
        
        for fmt in formats:
            format_dir = output_dir / fmt
            format_dir.mkdir(exist_ok=True)
            results[fmt] = {}
            
            if fmt == 'geojson':
                driver = "GeoJSON"
                ext = ".geojson"
            elif fmt == 'shapefile':
                driver = "ESRI Shapefile"
                ext = ".shp"
            elif fmt == 'gpkg':
                driver = "GPKG"
                ext = ".gpkg"
            else:
                logger.warning(f"Unsupported format: {fmt}")
                continue
            
            try:
                # Export stops
                if not stops_gdf.empty:
                    stops_path = format_dir / f"stops{ext}"
                    stops_gdf.to_file(stops_path, driver=driver)
                    results[fmt]['stops'] = stops_path
                
                # Export shapes
                if not shapes_gdf.empty:
                    shapes_path = format_dir / f"shapes{ext}"
                    shapes_gdf.to_file(shapes_path, driver=driver)
                    results[fmt]['shapes'] = shapes_path
                
                logger.info(f"Exported spatial data to {fmt} format in {format_dir}")
                
            except Exception as e:
                logger.error(f"Failed to export to {fmt}: {e}")
        
        return results
    
    def create_summary_report(self, output_path: Union[str, Path]) -> Path:
        """Create a summary report of the GTFS feed.
        
        Args:
            output_path: Path for output report file
            
        Returns:
            Path to created report file
        """
        output_path = Path(output_path)
        
        try:
            stats = self.processor.get_feed_stats()
            
            # Create summary report content
            report_lines = [
                f"# GTFS Feed Summary Report",
                f"",
                f"Generated from: {self.processor.feed_path}",
                f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"",
                f"## Feed Statistics",
                f"",
                f"- **Agencies**: {stats.get('agencies', 0)}",
                f"- **Routes**: {stats.get('routes', 0)}",
                f"- **Stops**: {stats.get('stops', 0)}",
                f"- **Trips**: {stats.get('trips', 0)}",
                f"- **Stop Times**: {stats.get('stop_times', 0)}",
                f"- **Shapes**: {stats.get('shapes', 0)}",
                f"- **Calendar Entries**: {stats.get('calendar', 0)}",
                f"",
            ]
            
            # Add route type breakdown
            if 'routes_by_type' in stats:
                from ..utils.helpers import get_route_type_name
                report_lines.extend([
                    f"## Routes by Type",
                    f"",
                ])
                for route_type, count in stats['routes_by_type'].items():
                    type_name = get_route_type_name(route_type)
                    report_lines.append(f"- **{type_name}**: {count}")
                report_lines.append("")
            
            # Add service period
            if 'service_period' in stats:
                report_lines.extend([
                    f"## Service Period",
                    f"",
                    f"- **Start Date**: {stats['service_period']['start']}",
                    f"- **End Date**: {stats['service_period']['end']}",
                    f"",
                ])
            
            # Write report
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            
            logger.info(f"Created summary report: {output_path}")
            return output_path
            
        except Exception as e:
            raise GTFSProcessingError(f"Failed to create summary report: {e}")
