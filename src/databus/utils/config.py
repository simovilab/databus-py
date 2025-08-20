"""Configuration management for databus."""

import os
from typing import Dict, Any, Optional
from pathlib import Path
import json
import logging

from .exceptions import ConfigurationError


logger = logging.getLogger(__name__)


class Config:
    """Configuration manager for databus settings.
    
    Handles configuration loading from environment variables, config files,
    and provides default values.
    """
    
    DEFAULT_CONFIG = {
        "api": {
            "base_url": "https://api.databus.cr",
            "timeout": 30,
            "max_retries": 3,
            "api_key": None,
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        },
        "processing": {
            "max_memory_usage": "1GB",
            "chunk_size": 10000,
            "temp_dir": None,  # Will use system temp
        },
        "validation": {
            "strict_mode": False,
            "custom_rules_dir": None,
        }
    }
    
    def __init__(self, config_file: Optional[Path] = None):
        """Initialize configuration.
        
        Args:
            config_file: Optional path to configuration file
        """
        self._config = self.DEFAULT_CONFIG.copy()
        self._config_file = config_file
        
        # Load configuration from various sources
        self._load_from_file()
        self._load_from_environment()
    
    def _load_from_file(self) -> None:
        """Load configuration from file."""
        config_paths = []
        
        if self._config_file:
            config_paths.append(self._config_file)
        
        # Default config file locations
        config_paths.extend([
            Path.home() / ".databus" / "config.json",
            Path.cwd() / "databus.json",
            Path.cwd() / ".databusrc",
        ])
        
        for config_path in config_paths:
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        file_config = json.load(f)
                    
                    self._merge_config(file_config)
                    logger.info(f"Loaded configuration from {config_path}")
                    break
                    
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(f"Failed to load config from {config_path}: {e}")
    
    def _load_from_environment(self) -> None:
        """Load configuration from environment variables."""
        env_mappings = {
            "DATABUS_API_URL": ["api", "base_url"],
            "DATABUS_API_KEY": ["api", "api_key"],
            "DATABUS_API_TIMEOUT": ["api", "timeout"],
            "DATABUS_LOG_LEVEL": ["logging", "level"],
            "DATABUS_TEMP_DIR": ["processing", "temp_dir"],
            "DATABUS_STRICT_VALIDATION": ["validation", "strict_mode"],
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # Convert string values to appropriate types
                if env_var.endswith("_TIMEOUT"):
                    try:
                        value = int(value)
                    except ValueError:
                        continue
                elif env_var.endswith("_STRICT_VALIDATION"):
                    value = value.lower() in ('true', '1', 'yes', 'on')
                
                self._set_nested_value(config_path, value)
    
    def _merge_config(self, new_config: Dict[str, Any]) -> None:
        """Merge new configuration with existing."""
        def merge_dict(base: Dict[str, Any], update: Dict[str, Any]) -> None:
            for key, value in update.items():
                if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                    merge_dict(base[key], value)
                else:
                    base[key] = value
        
        merge_dict(self._config, new_config)
    
    def _set_nested_value(self, path: list, value: Any) -> None:
        """Set a nested configuration value."""
        config = self._config
        for key in path[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[path[-1]] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'api.base_url')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'api.timeout')
            value: Value to set
        """
        keys = key.split('.')
        self._set_nested_value(keys, value)
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration section."""
        return self._config.get("api", {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration section."""
        return self._config.get("logging", {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration section."""
        return self._config.get("processing", {})
    
    def get_validation_config(self) -> Dict[str, Any]:
        """Get validation configuration section."""
        return self._config.get("validation", {})
    
    def save_to_file(self, file_path: Optional[Path] = None) -> None:
        """Save current configuration to file.
        
        Args:
            file_path: Path to save configuration (defaults to user config)
        """
        if not file_path:
            config_dir = Path.home() / ".databus"
            config_dir.mkdir(exist_ok=True)
            file_path = config_dir / "config.json"
        
        try:
            with open(file_path, 'w') as f:
                json.dump(self._config, f, indent=2)
            logger.info(f"Configuration saved to {file_path}")
        except IOError as e:
            raise ConfigurationError(f"Failed to save configuration: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Get configuration as dictionary."""
        return self._config.copy()
    
    def __getitem__(self, key: str) -> Any:
        """Get configuration value using bracket notation."""
        return self.get(key)
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set configuration value using bracket notation."""
        self.set(key, value)


# Global configuration instance
config = Config()
