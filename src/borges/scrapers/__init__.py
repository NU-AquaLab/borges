"""Borges web scraping utilities."""

from .favicon_scraper import FaviconScraper
from .html_scraper import HTMLScraper

__all__ = [
    "HTMLScraper",
    "FaviconScraper",
]