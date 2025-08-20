"""Redirect-only scraping functionality."""

import pickle
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import get_config
from ..models import WebsiteInfo


class RedirectScraper:
    """Scrape only redirect information from websites (no HTML content)."""

    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize redirect scraper.

        Args:
            cache_dir: Directory to cache redirect data
        """
        config = get_config()
        self.config = config.scraping.html
        self.cache_dir = cache_dir or config.paths.html_cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.user_agent
        })

    def _get_cache_path(self, url: str) -> Path:
        """Get cache file path for URL.

        Args:
            url: URL to cache

        Returns:
            Path to cache file
        """
        # Replace slashes to create valid filename
        filename = url.replace("/", "_").replace(":", "_") + ".redirect"
        return self.cache_dir / filename

    def _is_cached(self, url: str) -> bool:
        """Check if URL is already cached.

        Args:
            url: URL to check

        Returns:
            True if cached
        """
        return self._get_cache_path(url).exists()

    def _load_from_cache(self, url: str) -> Optional[Tuple[str, List[str], str]]:
        """Load URL data from cache.

        Args:
            url: URL to load

        Returns:
            Cached data or None
        """
        cache_path = self._get_cache_path(url)
        if cache_path.exists():
            try:
                with open(cache_path, "rb") as f:
                    return pickle.load(f)
            except Exception:
                # Corrupted cache file
                cache_path.unlink()
        return None

    def _save_to_cache(self, url: str, data: Tuple[str, List[str], str]) -> None:
        """Save URL data to cache.

        Args:
            url: Original URL
            data: Tuple of (original_url, redirects, final_url)
        """
        cache_path = self._get_cache_path(url)
        with open(cache_path, "wb") as f:
            pickle.dump(data, f)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _fetch_redirects(self, url: str) -> Tuple[str, List[str], str]:
        """Fetch URL redirects without downloading content.

        Args:
            url: URL to fetch

        Returns:
            Tuple of (original_url, redirects, final_url)
        """
        # Use HEAD request to avoid downloading content
        response = self.session.head(
            url,
            timeout=self.config.timeout,
            allow_redirects=True
        )
        
        # If HEAD fails, fall back to GET with stream=True
        if response.status_code >= 400:
            response = self.session.get(
                url,
                timeout=self.config.timeout,
                allow_redirects=True,
                stream=True  # Don't download the body
            )
        
        response.raise_for_status()

        # Extract redirect chain
        redirects = [hist.url for hist in response.history]
        final_url = response.url

        return (url, redirects, final_url)

    def scrape_url(self, url: str, use_cache: bool = True) -> WebsiteInfo:
        """Scrape redirect information for a single URL.

        Args:
            url: URL to scrape
            use_cache: Whether to use cache

        Returns:
            Website information (without HTML content)
        """
        # Check cache first
        if use_cache:
            cached_data = self._load_from_cache(url)
            if cached_data:
                orig_url, redirects, final_url = cached_data
                return WebsiteInfo(
                    original_url=orig_url,
                    redirects=redirects,
                    final_url=final_url,
                    html_content=None  # No HTML content
                )

        # Fetch redirects
        try:
            data = self._fetch_redirects(url)
            orig_url, redirects, final_url = data

            # Save to cache
            if use_cache:
                self._save_to_cache(url, data)

            return WebsiteInfo(
                original_url=orig_url,
                redirects=redirects,
                final_url=final_url,
                html_content=None  # No HTML content
            )

        except Exception as e:
            return WebsiteInfo(
                original_url=url,
                error=str(e)
            )

    def scrape_urls(
        self,
        urls: List[str],
        max_workers: Optional[int] = None,
        use_cache: bool = True,
        progress_callback: Optional[callable] = None
    ) -> List[WebsiteInfo]:
        """Scrape redirect information for multiple URLs in parallel.

        Args:
            urls: List of URLs to scrape
            max_workers: Maximum parallel workers
            use_cache: Whether to use cache
            progress_callback: Callback for progress updates

        Returns:
            List of website information
        """
        if max_workers is None:
            max_workers = self.config.max_workers

        results = []

        # Filter out already cached URLs if using cache
        if use_cache:
            urls_to_fetch = [url for url in urls if not self._is_cached(url)]
            cached_urls = [url for url in urls if self._is_cached(url)]

            # Load cached results
            for url in cached_urls:
                result = self.scrape_url(url, use_cache=True)
                results.append(result)
                if progress_callback:
                    progress_callback(1)
        else:
            urls_to_fetch = urls

        # Fetch remaining URLs in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_url = {
                executor.submit(self.scrape_url, url, use_cache): url
                for url in urls_to_fetch
            }

            # Process completed tasks
            for future in as_completed(future_to_url):
                result = future.result()
                results.append(result)

                if progress_callback:
                    progress_callback(1)

                # Rate limiting
                time.sleep(self.config.delay_between_requests)

        return results

    def scrape_from_dataframe(
        self,
        df,
        url_column: str = "website",
        max_workers: Optional[int] = None
    ) -> Dict[str, WebsiteInfo]:
        """Scrape URLs from a DataFrame.

        Args:
            df: DataFrame containing URLs
            url_column: Column name with URLs
            max_workers: Maximum parallel workers

        Returns:
            Dictionary mapping URL to website info
        """
        # Get unique URLs
        urls = df[url_column].dropna().unique().tolist()

        # Scrape URLs
        results = self.scrape_urls(urls, max_workers=max_workers)

        # Create mapping
        url_to_info = {}
        for info in results:
            url_to_info[str(info.original_url)] = info

        return url_to_info

    def close(self):
        """Close the scraper session."""
        self.session.close()