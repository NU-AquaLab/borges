"""Logging configuration and utilities."""

import logging
import sys
from pathlib import Path
from typing import Optional

import structlog
from structlog.stdlib import LoggerFactory

from ..config import get_config


def setup_logging(
    log_level: Optional[str] = None,
    log_dir: Optional[Path] = None,
    use_structured: bool = True
) -> None:
    """Set up logging configuration.

    Args:
        log_level: Log level (uses config if not provided)
        log_dir: Log directory (uses config if not provided)
        use_structured: Whether to use structured logging
    """
    config = get_config()

    # Use provided values or fall back to config
    log_level = log_level or config.logging.level
    log_dir = log_dir or config.logging.log_dir
    use_structured = use_structured and config.logging.format == "structured"

    # Create log directory
    log_dir.mkdir(parents=True, exist_ok=True)

    # Configure standard logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "borges.log")
        ]
    )

    if use_structured:
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.CallsiteParameterAdder(
                    parameters=[
                        structlog.processors.CallsiteParameter.FILENAME,
                        structlog.processors.CallsiteParameter.LINENO,
                    ]
                ),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=LoggerFactory(),
            cache_logger_on_first_use=True,
        )


def get_logger(name: str, use_structured: bool = True) -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name (usually __name__)
        use_structured: Whether to use structured logging

    Returns:
        Logger instance
    """
    config = get_config()
    use_structured = use_structured and config.logging.format == "structured"

    if use_structured:
        return structlog.get_logger(name)
    else:
        logger = logging.getLogger(name)

        # Apply module-specific log levels if configured
        module_levels = config.logging.modules
        for module_prefix, level in module_levels.items():
            if name.startswith(module_prefix):
                logger.setLevel(getattr(logging, level.upper()))
                break

        return logger


class LogContext:
    """Context manager for adding context to structured logs."""

    def __init__(self, logger, **context):
        """Initialize log context.

        Args:
            logger: Logger instance
            **context: Context variables to bind
        """
        self.logger = logger
        self.context = context

    def __enter__(self):
        """Enter context and bind variables."""
        if hasattr(self.logger, "bind"):
            self.bound_logger = self.logger.bind(**self.context)
            return self.bound_logger
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        pass


def log_execution_time(logger, operation_name: str):
    """Decorator to log execution time of functions.

    Args:
        logger: Logger instance
        operation_name: Name of the operation being timed
    """
    import time
    from functools import wraps

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time

                if hasattr(logger, "info"):
                    logger.info(
                        f"{operation_name} completed",
                        duration=duration,
                        status="success"
                    )

                return result

            except Exception as e:
                duration = time.time() - start_time

                if hasattr(logger, "error"):
                    logger.error(
                        f"{operation_name} failed",
                        duration=duration,
                        status="error",
                        error=str(e)
                    )
                raise

        return wrapper
    return decorator