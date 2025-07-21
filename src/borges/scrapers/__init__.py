"""Borges web scraping utilities."""

from .favicon_scraper import FaviconScraper
from .html_scraper import HTMLScraper
from .redirect_scraper import RedirectScraper

__all__ = [
    "HTMLScraper",
    "FaviconScraper",
    "RedirectScraper",
]