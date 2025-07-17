"""Configuration management for Borges.

This module handles loading configuration from YAML files and environment variables,
with support for variable interpolation and validation.
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator


class PathConfig(BaseModel):
    """Path configuration."""
    
    base_dir: Path = Field(default=Path("./data"))
    input_dir: Optional[Path] = None
    raw_dir: Optional[Path] = None
    processed_dir: Optional[Path] = None
    output_dir: Optional[Path] = None
    html_cache_dir: Optional[Path] = None
    favicon_cache_dir: Optional[Path] = None
    
    @validator("*", pre=False, always=True)
    def resolve_paths(cls, v, values, field):
        """Resolve paths relative to base_dir."""
        if v is None and field.name != "base_dir":
            # Set default paths based on base_dir
            base = values.get("base_dir", Path("./data"))
            if field.name == "input_dir":
                return base / "input"
            elif field.name == "raw_dir":
                return base / "raw"
            elif field.name == "processed_dir":
                return base / "processed"
            elif field.name == "output_dir":
                return base / "output"
            elif field.name == "html_cache_dir":
                return values.get("raw_dir", base / "raw") / "html_cache"
            elif field.name == "favicon_cache_dir":
                return values.get("raw_dir", base / "raw") / "favicon_cache"
        return v
    
    class Config:
        """Pydantic config."""
        
        arbitrary_types_allowed = True


class APIConfig(BaseModel):
    """API configuration."""
    
    class OpenAIConfig(BaseModel):
        """OpenAI API configuration."""
        
        api_key: str
        model: str = "gpt-4o-mini"
        temperature: float = 0.0
        max_retries: int = 3
        timeout: int = 30
        vision_model: str = "gpt-4o-mini"
    
    openai: OpenAIConfig


class ScrapingConfig(BaseModel):
    """Web scraping configuration."""
    
    class HTMLConfig(BaseModel):
        """HTML scraping configuration."""
        
        max_workers: int = 100
        timeout: int = 30
        user_agent: str = "Borges Network Analyzer 1.0"
        retry_attempts: int = 3
        delay_between_requests: float = 0.1
    
    class FaviconConfig(BaseModel):
        """Favicon scraping configuration."""
        
        max_workers: int = 50
        google_favicon_api: str = "https://t3.gstatic.com/faviconV2"
        api_params: Dict[str, Any] = {
            "client": "SOCIAL",
            "type": "FAVICON",
            "fallback_opts": "TYPE,SIZE,URL",
            "size": 16,
        }
        retry_attempts: int = 3
    
    html: HTMLConfig
    favicon: FaviconConfig


class ProcessingConfig(BaseModel):
    """Data processing configuration."""
    
    batch_size: int = 1000
    llm_batch_size: int = 10
    prompts: Dict[str, str] = {}


class OutputConfig(BaseModel):
    """Output configuration."""
    
    class FormatsConfig(BaseModel):
        """Output format configuration."""
        
        default: str = "parquet"
        supported: list[str] = ["parquet", "json", "csv"]
    
    formats: FormatsConfig
    file_patterns: Dict[str, str] = {}


class LoggingConfig(BaseModel):
    """Logging configuration."""
    
    level: str = "INFO"
    format: str = "structured"
    log_dir: Path = Path("./logs")
    modules: Dict[str, str] = {}


class PipelineConfig(BaseModel):
    """Pipeline configuration."""
    
    stages: Dict[str, bool] = {}
    checkpoint: Dict[str, Any] = {}
    error_handling: Dict[str, Any] = {}


class Config(BaseModel):
    """Main configuration class."""
    
    environment: str = "development"
    paths: PathConfig
    input_files: Dict[str, str] = {}
    api: APIConfig
    scraping: ScrapingConfig
    processing: ProcessingConfig
    output: OutputConfig
    logging: LoggingConfig
    pipeline: PipelineConfig
    performance: Dict[str, Any] = {}
    development: Dict[str, Any] = {}


def interpolate_env_vars(value: Any) -> Any:
    """Recursively interpolate environment variables in configuration values.
    
    Args:
        value: Configuration value to interpolate
        
    Returns:
        Interpolated value
    """
    if isinstance(value, str):
        # Match ${VAR} or ${VAR:default}
        pattern = re.compile(r'\$\{([^}:]+)(?::([^}]+))?\}')
        
        def replacer(match):
            var_name = match.group(1)
            default_value = match.group(2)
            return os.environ.get(var_name, default_value or f"${{{var_name}}}")
        
        return pattern.sub(replacer, value)
    elif isinstance(value, dict):
        return {k: interpolate_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [interpolate_env_vars(v) for v in value]
    else:
        return value


def interpolate_internal_vars(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Interpolate internal configuration variables (e.g., ${paths.base_dir}).
    
    Args:
        config_dict: Configuration dictionary
        
    Returns:
        Interpolated configuration dictionary
    """
    def get_nested_value(obj: Dict[str, Any], path: str) -> Any:
        """Get nested dictionary value using dot notation."""
        keys = path.split('.')
        value = obj
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
    
    def interpolate_value(value: Any, config: Dict[str, Any]) -> Any:
        """Recursively interpolate internal variables."""
        if isinstance(value, str):
            # Match ${paths.base_dir} style references
            pattern = re.compile(r'\$\{([^}]+)\}')
            
            def replacer(match):
                var_path = match.group(1)
                if not var_path.startswith(('OPENAI_', 'LOG_', 'ENV')):  # Skip env vars
                    nested_value = get_nested_value(config, var_path)
                    if nested_value is not None:
                        return str(nested_value)
                return match.group(0)
            
            return pattern.sub(replacer, value)
        elif isinstance(value, dict):
            return {k: interpolate_value(v, config) for k, v in value.items()}
        elif isinstance(value, list):
            return [interpolate_value(v, config) for v in value]
        else:
            return value
    
    # Perform multiple passes to handle nested interpolations
    for _ in range(3):
        config_dict = interpolate_value(config_dict, config_dict)
    
    return config_dict


def load_config(config_path: Optional[Union[str, Path]] = None) -> Config:
    """Load configuration from YAML file and environment variables.
    
    Args:
        config_path: Path to configuration file (defaults to config.yaml)
        
    Returns:
        Loaded configuration object
        
    Raises:
        FileNotFoundError: If configuration file not found
        ValueError: If configuration is invalid
    """
    # Load environment variables
    load_dotenv()
    
    # Determine config file path
    if config_path is None:
        config_path = os.environ.get("CONFIG_FILE", "config.yaml")
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Load YAML configuration
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
    
    # Interpolate environment variables first
    config_dict = interpolate_env_vars(config_dict)
    
    # Then interpolate internal variables
    config_dict = interpolate_internal_vars(config_dict)
    
    # Create configuration object
    try:
        config = Config(**config_dict)
    except Exception as e:
        raise ValueError(f"Invalid configuration: {e}")
    
    # Create directories if they don't exist
    for path_attr in ["base_dir", "input_dir", "raw_dir", "processed_dir", "output_dir", 
                      "html_cache_dir", "favicon_cache_dir"]:
        path = getattr(config.paths, path_attr)
        if path:
            path.mkdir(parents=True, exist_ok=True)
    
    # Create log directory
    config.logging.log_dir.mkdir(parents=True, exist_ok=True)
    
    return config


# Global configuration instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get the global configuration instance.
    
    Returns:
        Configuration object
        
    Raises:
        RuntimeError: If configuration not loaded
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: Config) -> None:
    """Set the global configuration instance.
    
    Args:
        config: Configuration object
    """
    global _config
    _config = config