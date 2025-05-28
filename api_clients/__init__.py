# api_clients/__init__.py
from .base_client import APIClient, scrape_article_content, extract_S1_text_sections
from .finnhub_client import FinnhubClient
from .fmp_client import FinancialModelingPrepClient
from .alphavantage_client import AlphaVantageClient
from .eodhd_client import EODHDClient
from .sec_edgar_client import SECEDGARClient
from .gemini_client import GeminiAPIClient

__all__ = [
    "APIClient",
    "scrape_article_content",
    "extract_S1_text_sections",
    "FinnhubClient",
    "FinancialModelingPrepClient",
    "AlphaVantageClient",
    "EODHDClient",
    "SECEDGARClient",
    "GeminiAPIClient",
]