"""HTTP client utilities with retry and rate limiting."""

import time
from typing import Any, Dict, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .logging import get_logger

# Lazy logger initialization to avoid loading config at import time
logger = None

def _get_logger():
    """Get logger instance lazily."""
    global logger
    if logger is None:
        logger = get_logger(__name__)
    return logger


class RateLimiter:
    """Simple rate limiter for HTTP requests."""

    def __init__(self, requests_per_second: float = 10.0):
        """Initialize rate limiter.

        Args:
            requests_per_second: Maximum requests per second
        """
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0

    def wait_if_needed(self) -> None:
        """Wait if necessary to respect rate limit."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.min_interval:
            sleep_time = self.min_interval - time_since_last_request
            time.sleep(sleep_time)

        self.last_request_time = time.time()


class HTTPClient:
    """HTTP client with retry logic and rate limiting."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        rate_limit: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None
    ):
        """Initialize HTTP client.

        Args:
            base_url: Base URL for requests
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries
            rate_limit: Requests per second limit
            headers: Default headers
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limiter = RateLimiter(rate_limit) if rate_limit else None

        # Initialize client
        self.client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers=headers or {},
            follow_redirects=True
        )

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> httpx.Response:
        """Make HTTP request with retry logic.

        Args:
            method: HTTP method
            url: URL to request
            **kwargs: Additional request arguments

        Returns:
            HTTP response
        """
        # Apply rate limiting
        if self.rate_limiter:
            self.rate_limiter.wait_if_needed()

        # Log request
        _get_logger().info(
            "Making HTTP request",
            method=method,
            url=url
        )

        try:
            response = self.client.request(method, url, **kwargs)
            response.raise_for_status()

            _get_logger().info(
                "HTTP request successful",
                method=method,
                url=url,
                status_code=response.status_code
            )

            return response

        except httpx.HTTPStatusError as e:
            _get_logger().error(
                "HTTP request failed",
                method=method,
                url=url,
                status_code=e.response.status_code,
                error=str(e)
            )
            raise

        except Exception as e:
            _get_logger().error(
                "HTTP request error",
                method=method,
                url=url,
                error=str(e)
            )
            raise

    def get(self, url: str, **kwargs) -> httpx.Response:
        """Make GET request.

        Args:
            url: URL to request
            **kwargs: Additional request arguments

        Returns:
            HTTP response
        """
        return self._make_request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> httpx.Response:
        """Make POST request.

        Args:
            url: URL to request
            **kwargs: Additional request arguments

        Returns:
            HTTP response
        """
        return self._make_request("POST", url, **kwargs)

    def get_json(self, url: str, **kwargs) -> Any:
        """Make GET request and return JSON.

        Args:
            url: URL to request
            **kwargs: Additional request arguments

        Returns:
            Parsed JSON response
        """
        response = self.get(url, **kwargs)
        return response.json()

    def post_json(self, url: str, json: Any, **kwargs) -> Any:
        """Make POST request with JSON data.

        Args:
            url: URL to request
            json: JSON data to send
            **kwargs: Additional request arguments

        Returns:
            Parsed JSON response
        """
        response = self.post(url, json=json, **kwargs)
        return response.json()

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def create_client(
    config_section: str = "scraping.html",
    **kwargs
) -> HTTPClient:
    """Create HTTP client from configuration.

    Args:
        config_section: Configuration section to use
        **kwargs: Override configuration values

    Returns:
        Configured HTTP client
    """
    from ..config import get_config

    config = get_config()

    # Get configuration section
    section = config
    for part in config_section.split("."):
        section = getattr(section, part)

    # Build client configuration
    client_config = {
        "timeout": getattr(section, "timeout", 30),
        "max_retries": getattr(section, "retry_attempts", 3),
        "headers": {"User-Agent": getattr(section, "user_agent", "Borges/1.0")}
    }

    # Apply overrides
    client_config.update(kwargs)

    return HTTPClient(**client_config)