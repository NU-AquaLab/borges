"""Tests for configuration management."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from borges.config import Config, interpolate_env_vars, interpolate_internal_vars, load_config


class TestConfigInterpolation:
    """Test configuration variable interpolation."""

    def test_env_var_interpolation(self):
        """Test environment variable interpolation."""
        os.environ["TEST_VAR"] = "test_value"
        os.environ["TEST_PATH"] = "/test/path"
        
        config = {
            "simple": "${TEST_VAR}",
            "with_default": "${MISSING_VAR:default_value}",
            "nested": {
                "path": "${TEST_PATH}/subdir",
                "list": ["${TEST_VAR}", "static"]
            }
        }
        
        result = interpolate_env_vars(config)
        
        assert result["simple"] == "test_value"
        assert result["with_default"] == "default_value"
        assert result["nested"]["path"] == "/test/path/subdir"
        assert result["nested"]["list"] == ["test_value", "static"]

    def test_internal_var_interpolation(self):
        """Test internal variable interpolation."""
        config = {
            "paths": {
                "base_dir": "/data",
                "input_dir": "${paths.base_dir}/input",
                "output_dir": "${paths.base_dir}/output"
            },
            "files": {
                "input": "${paths.input_dir}/data.json"
            }
        }
        
        result = interpolate_internal_vars(config)
        
        assert result["paths"]["input_dir"] == "/data/input"
        assert result["paths"]["output_dir"] == "/data/output"
        assert result["files"]["input"] == "/data/input/data.json"


class TestConfigLoading:
    """Test configuration loading."""

    def test_load_valid_config(self):
        """Test loading valid configuration."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            config_data = {
                "environment": "test",
                "paths": {
                    "base_dir": "./test_data"
                },
                "input_files": {
                    "peeringdb": "${paths.base_dir}/peeringdb.json"
                },
                "api": {
                    "openai": {
                        "api_key": "test_key",
                        "model": "gpt-4o-mini",
                        "temperature": 0.0,
                        "max_retries": 3,
                        "timeout": 30,
                        "vision_model": "gpt-4o-mini"
                    }
                },
                "scraping": {
                    "html": {
                        "max_workers": 50,
                        "timeout": 20,
                        "user_agent": "Test Agent",
                        "retry_attempts": 2,
                        "delay_between_requests": 0.5
                    },
                    "favicon": {
                        "max_workers": 25,
                        "google_favicon_api": "https://test.api",
                        "api_params": {},
                        "retry_attempts": 2
                    }
                },
                "processing": {
                    "batch_size": 500,
                    "llm_batch_size": 5,
                    "prompts": {}
                },
                "output": {
                    "formats": {
                        "default": "parquet",
                        "supported": ["parquet", "json"]
                    },
                    "file_patterns": {}
                },
                "logging": {
                    "level": "DEBUG",
                    "format": "simple",
                    "log_dir": "./logs",
                    "modules": {}
                },
                "pipeline": {
                    "stages": {"load_data": True},
                    "checkpoint": {"enabled": True},
                    "error_handling": {"continue_on_error": True}
                }
            }
            
            yaml.dump(config_data, f)
            config_path = f.name
        
        try:
            # Load config
            config = load_config(config_path)
            
            # Verify loaded correctly
            assert config.environment == "test"
            assert config.paths.base_dir == Path("./test_data")
            assert config.api.openai.api_key == "test_key"
            assert config.scraping.html.max_workers == 50
            
            # Verify interpolation worked
            assert str(config.input_files["peeringdb"]) == "./test_data/peeringdb.json"
            
        finally:
            os.unlink(config_path)
            # Clean up created directories
            if Path("./test_data").exists():
                import shutil
                shutil.rmtree("./test_data")
            if Path("./logs").exists():
                import shutil
                shutil.rmtree("./logs")

    def test_missing_config_file(self):
        """Test loading missing configuration file."""
        with pytest.raises(FileNotFoundError):
            load_config("/non/existent/config.yaml")

    def test_invalid_config(self):
        """Test loading invalid configuration."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            # Missing required fields
            config_data = {
                "environment": "test"
                # Missing required nested configs
            }
            
            yaml.dump(config_data, f)
            config_path = f.name
        
        try:
            with pytest.raises(ValueError):
                load_config(config_path)
        finally:
            os.unlink(config_path)