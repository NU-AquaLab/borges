"""Favicon scraping functionality."""

import pickle
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from PIL import Image
from io import BytesIO
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import get_config


class FaviconScraper:
    """Scrape favicons from websites."""

    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize favicon scraper.

        Args:
            cache_dir: Directory to cache favicons
        """
        config = get_config()
        self.config = config.scraping.favicon
        self.cache_dir = cache_dir or config.paths.favicon_cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Session for connection pooling
        self.session = requests.Session()

    def _get_cache_path(self, url: str) -> Path:
        """Get cache file path for URL.

        Args:
            url: URL to cache

        Returns:
            Path to cache file
        """
        import hashlib
        
        # Create a hash-based filename to avoid filesystem length limits
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        
        # Extract domain for readability (but keep it short)
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            # Limit domain length and make it filesystem-safe
            if domain:
                domain = domain.replace(".", "_")[:50]  # Limit to 50 chars
                filename = f"{domain}_{url_hash}.favicon"
            else:
                filename = f"{url_hash}.favicon"
        except:
            filename = f"{url_hash}.favicon"
            
        return self.cache_dir / filename

    def _is_cached(self, url: str) -> bool:
        """Check if favicon is already cached.

        Args:
            url: URL to check

        Returns:
            True if cached
        """
        return self._get_cache_path(url).exists()

    def _load_from_cache(self, url: str) -> Optional[Tuple[str, bytes]]:
        """Load favicon data from cache.

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

    def _save_to_cache(self, url: str, data: Tuple[str, bytes]) -> None:
        """Save favicon data to cache.

        Args:
            url: Original URL
            data: Tuple of (url, favicon_bytes)
        """
        cache_path = self._get_cache_path(url)
        with open(cache_path, "wb") as f:
            pickle.dump(data, f)

    def _get_favicon_url(self, url: str) -> str:
        """Get Google favicon service URL.

        Args:
            url: Website URL

        Returns:
            Favicon service URL
        """
        params = self.config.api_params.copy()
        params['url'] = url

        # Build query string
        query_parts = []
        for key, value in params.items():
            if key == 'url':
                query_parts.append(f"{key}={value}")
            else:
                query_parts.append(f"{key}={value}")

        query_string = "&".join(query_parts)
        return f"{self.config.google_favicon_api}?{query_string}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _fetch_favicon(self, url: str) -> bytes:
        """Fetch favicon with retry logic.

        Args:
            url: Website URL

        Returns:
            Favicon bytes
        """
        favicon_url = self._get_favicon_url(url)
        response = self.session.get(favicon_url, timeout=30)
        response.raise_for_status()
        return response.content

    def scrape_favicon(self, url: str, use_cache: bool = True) -> Optional[bytes]:
        """Scrape favicon for a single URL.

        Args:
            url: Website URL
            use_cache: Whether to use cache

        Returns:
            Favicon bytes or None
        """
        # Check cache first
        if use_cache:
            cached_data = self._load_from_cache(url)
            if cached_data:
                return cached_data[1]

        # Fetch favicon
        try:
            favicon_data = self._fetch_favicon(url)

            # Validate it's an image
            try:
                img = Image.open(BytesIO(favicon_data))
                img.verify()
            except Exception:
                return None

            # Save to cache (only if favicon data is non-empty)
            if use_cache and favicon_data and len(favicon_data) > 0:
                self._save_to_cache(url, (url, favicon_data))

            return favicon_data

        except Exception:
            return None

    def scrape_favicons(
        self,
        urls: List[str],
        max_workers: Optional[int] = None,
        use_cache: bool = True,
        force_fresh: bool = False,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, bytes]:
        """Scrape favicons for multiple URLs in parallel.

        Args:
            urls: List of URLs
            max_workers: Maximum parallel workers
            use_cache: Whether to use cache
            force_fresh: Force fresh download even if cached
            progress_callback: Callback for progress updates

        Returns:
            Dictionary mapping URL to favicon bytes
        """
        if max_workers is None:
            max_workers = self.config.max_workers

        results = {}

        # Filter out already cached URLs if using cache and not forcing fresh
        if use_cache and not force_fresh:
            urls_to_fetch = [url for url in urls if not self._is_cached(url)]
            cached_urls = [url for url in urls if self._is_cached(url)]

            # Load cached results
            for url in cached_urls:
                favicon_data = self.scrape_favicon(url, use_cache=True)
                if favicon_data:
                    results[url] = favicon_data
                if progress_callback:
                    progress_callback(1)
        else:
            urls_to_fetch = urls

        # Fetch remaining favicons in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks (disable cache if force_fresh is True)
            effective_use_cache = use_cache and not force_fresh
            future_to_url = {
                executor.submit(self.scrape_favicon, url, effective_use_cache): url
                for url in urls_to_fetch
            }

            # Process completed tasks
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    favicon_data = future.result()
                    if favicon_data:
                        results[url] = favicon_data
                except Exception:
                    pass

                if progress_callback:
                    progress_callback(1)

                # Rate limiting
                time.sleep(0.1)

        return results

    def scrape_from_dataframe(
        self,
        df,
        url_column: str = "final_url",
        max_workers: Optional[int] = None
    ) -> Dict[str, bytes]:
        """Scrape favicons from URLs in a DataFrame.

        Args:
            df: DataFrame containing URLs
            url_column: Column name with URLs
            max_workers: Maximum parallel workers

        Returns:
            Dictionary mapping URL to favicon bytes
        """
        # Get unique URLs
        urls = df[url_column].dropna().unique().tolist()

        # Scrape favicons
        return self.scrape_favicons(urls, max_workers=max_workers)

    def close(self):
        """Close the scraper session."""
        self.session.close()