"""Main CLI entry point for databus."""

import logging
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table
from rich.progress import track

from .. import __version__
from ..api import DatabusClient
from ..gtfs import GTFSProcessor, GTFSValidator
from ..utils.config import config
from ..utils.exceptions import DatabusError
from ..utils.helpers import format_file_size


console = Console()
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format=config.get("logging.format"),
        handlers=[logging.StreamHandler()]
    )


@click.group()
@click.version_option(version=__version__, prog_name="databus")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.option("--config", "config_file", type=click.Path(exists=True), help="Configuration file path")
@click.pass_context
def main(ctx: click.Context, verbose: bool, config_file: Optional[str]) -> None:
    """Databús Python SDK and command-line toolkit.
    
    A comprehensive toolkit for GTFS data processing, validation, and analysis.
    Provides programmatic access to Databús APIs, GTFS manipulation utilities,
    data conversion tools, and automated testing frameworks.
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    
    setup_logging(verbose)
    
    if config_file:
        # Load custom config file
        # This would require updating the Config class to reload
        pass


@main.group()
def api() -> None:
    """Commands for interacting with Databús APIs."""
    pass


@api.command()
@click.option("--country", help="Filter feeds by country code")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), 
              default="table", help="Output format")
def feeds(country: Optional[str], output_format: str) -> None:
    """List available GTFS feeds."""
    try:
        client = DatabusClient(
            base_url=config.get("api.base_url"),
            api_key=config.get("api.api_key"),
            timeout=config.get("api.timeout")
        )
        
        with console.status("Fetching feeds..."):
            feed_list = client.get_feeds(country=country)
        
        if output_format == "json":
            import json
            feed_data = [feed.dict() for feed in feed_list]
            console.print(json.dumps(feed_data, indent=2, default=str))
        else:
            # Display as table
            table = Table(title="GTFS Feeds")
            table.add_column("ID", style="cyan")
            table.add_column("Name", style="bold")
            table.add_column("Country", style="green")
            table.add_column("Operator")
            table.add_column("Size", justify="right")
            table.add_column("Updated", style="dim")
            
            for feed in feed_list:
                size_str = format_file_size(feed.file_size) if feed.file_size else "N/A"
                updated_str = feed.last_updated.strftime("%Y-%m-%d") if feed.last_updated else "N/A"
                
                table.add_row(
                    feed.id,
                    feed.name,
                    feed.country_code,
                    feed.operator or "N/A",
                    size_str,
                    updated_str
                )
            
            console.print(table)
            console.print(f"\nTotal feeds: {len(feed_list)}")
            
    except DatabusError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@api.command()
@click.argument("feed_id")
@click.option("--output", "-o", type=click.Path(), help="Output file path")
def download(feed_id: str, output: Optional[str]) -> None:
    """Download a GTFS feed."""
    try:
        client = DatabusClient(
            base_url=config.get("api.base_url"),
            api_key=config.get("api.api_key"),
            timeout=config.get("api.timeout")
        )
        
        if not output:
            output = f"{feed_id}.zip"
        
        output_path = Path(output)
        
        with console.status(f"Downloading feed {feed_id}..."):
            downloaded_path = client.download_feed(feed_id, str(output_path))
        
        console.print(f"[green]✓[/green] Downloaded feed to: {downloaded_path}")
        
    except DatabusError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@main.group()
def gtfs() -> None:
    """Commands for GTFS data processing and analysis."""
    pass


@gtfs.command()
@click.argument("feed_path", type=click.Path(exists=True))
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), 
              default="table", help="Output format")
def info(feed_path: str, output_format: str) -> None:
    """Display information about a GTFS feed."""
    try:
        processor = GTFSProcessor(feed_path)
        
        with console.status("Loading GTFS feed..."):
            processor.load_feed()
            stats = processor.get_feed_stats()
        
        if output_format == "json":
            import json
            console.print(json.dumps(stats, indent=2, default=str))
        else:
            # Display as table
            table = Table(title=f"GTFS Feed Info: {Path(feed_path).name}")
            table.add_column("Metric", style="bold")
            table.add_column("Count", justify="right", style="cyan")
            
            # Basic counts
            table.add_row("Agencies", str(stats.get("agencies", 0)))
            table.add_row("Routes", str(stats.get("routes", 0)))
            table.add_row("Stops", str(stats.get("stops", 0)))
            table.add_row("Trips", str(stats.get("trips", 0)))
            table.add_row("Stop Times", str(stats.get("stop_times", 0)))
            table.add_row("Shapes", str(stats.get("shapes", 0)))
            
            console.print(table)
            
            # Route types breakdown
            if "routes_by_type" in stats:
                route_table = Table(title="Routes by Type")
                route_table.add_column("Route Type", style="bold")
                route_table.add_column("Count", justify="right", style="cyan")
                
                from ..utils.helpers import get_route_type_name
                for route_type, count in stats["routes_by_type"].items():
                    route_table.add_row(get_route_type_name(route_type), str(count))
                
                console.print()
                console.print(route_table)
            
            # Service period
            if "service_period" in stats:
                console.print()
                console.print(f"[bold]Service Period:[/bold] {stats['service_period']['start']} to {stats['service_period']['end']}")
        
    except DatabusError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@gtfs.command()
@click.argument("feed_path", type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), help="Output file path for report")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), 
              default="table", help="Output format")
def validate(feed_path: str, output: Optional[str], output_format: str) -> None:
    """Validate a GTFS feed."""
    try:
        processor = GTFSProcessor(feed_path)
        
        with console.status("Loading GTFS feed..."):
            processor.load_feed()
        
        validator = GTFSValidator(processor)
        
        with console.status("Validating GTFS feed..."):
            report = validator.validate()
        
        if output_format == "json":
            report_json = report.to_json()
            if output:
                with open(output, 'w') as f:
                    f.write(report_json)
                console.print(f"[green]✓[/green] Validation report saved to: {output}")
            else:
                console.print(report_json)
        else:
            # Display summary table
            summary_table = Table(title=f"Validation Report: {Path(feed_path).name}")
            summary_table.add_column("Status", style="bold")
            summary_table.add_column("Score", justify="right")
            summary_table.add_column("Errors", justify="right", style="red")
            summary_table.add_column("Warnings", justify="right", style="yellow")
            summary_table.add_column("Notices", justify="right", style="blue")
            
            status_style = "green" if report.status == "valid" else "yellow" if report.status == "valid_with_warnings" else "red"
            
            summary_table.add_row(
                f"[{status_style}]{report.status.upper()}[/{status_style}]",
                f"{report.score:.1f}/100",
                str(len(report.errors)),
                str(len(report.warnings)),
                str(len(report.notices))
            )
            
            console.print(summary_table)
            
            # Show issues if any
            if report.errors:
                console.print("\n[red]Errors:[/red]")
                for error in report.errors[:5]:  # Show first 5
                    console.print(f"  • {error['message']}")
                if len(report.errors) > 5:
                    console.print(f"  ... and {len(report.errors) - 5} more errors")
            
            if report.warnings and output_format == "table":
                console.print("\n[yellow]Warnings:[/yellow]")
                for warning in report.warnings[:3]:  # Show first 3
                    console.print(f"  • {warning['message']}")
                if len(report.warnings) > 3:
                    console.print(f"  ... and {len(report.warnings) - 3} more warnings")
        
    except DatabusError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@gtfs.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
@click.option("--bbox", help="Bounding box as 'min_lon,min_lat,max_lon,max_lat'")
@click.option("--dates", help="Date range as 'start_date,end_date' (YYYY-MM-DD)")
def filter(input_path: str, output_path: str, bbox: Optional[str], dates: Optional[str]) -> None:
    """Filter GTFS feed by geographic bounds or date range."""
    try:
        processor = GTFSProcessor(input_path)
        
        with console.status("Loading GTFS feed..."):
            processor.load_feed()
        
        filtered_processor = processor
        
        if bbox:
            try:
                coords = list(map(float, bbox.split(',')))
                if len(coords) != 4:
                    raise ValueError("Bounding box must have 4 coordinates")
                
                min_lon, min_lat, max_lon, max_lat = coords
                with console.status("Filtering by bounding box..."):
                    filtered_processor = processor.filter_by_bounding_box(min_lat, min_lon, max_lat, max_lon)
                    
            except ValueError as e:
                console.print(f"[red]Invalid bounding box format: {e}[/red]")
                sys.exit(1)
        
        if dates:
            try:
                date_parts = dates.split(',')
                if len(date_parts) != 2:
                    raise ValueError("Date range must have start and end date")
                
                start_date, end_date = date_parts
                with console.status("Filtering by date range..."):
                    filtered_processor = filtered_processor.filter_by_dates(start_date, end_date)
                    
            except ValueError as e:
                console.print(f"[red]Invalid date range format: {e}[/red]")
                sys.exit(1)
        
        with console.status("Exporting filtered feed..."):
            output_file = filtered_processor.export_to_zip(output_path)
        
        console.print(f"[green]✓[/green] Filtered feed exported to: {output_file}")
        
    except DatabusError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@main.command()
def config_show() -> None:
    """Show current configuration."""
    import json
    console.print(json.dumps(config.to_dict(), indent=2))


if __name__ == "__main__":
    main()
