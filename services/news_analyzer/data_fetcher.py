# services/news_analyzer/data_fetcher.py
import time
from api_clients import scrape_article_content  # scrape_article_content is generic
from core.logging_setup import logger
from core.config import MAX_NEWS_ARTICLES_PER_QUERY  # Use default from config if not overridden


def fetch_market_news_from_api(analyzer_instance, category="general",
                               count_to_fetch_from_api=MAX_NEWS_ARTICLES_PER_QUERY):
    """Fetches market news from the Finnhub API."""
    logger.info(f"Fetching latest market news for category: {category} (max {count_to_fetch_from_api} from API)...")
    news_items = analyzer_instance.finnhub.get_market_news(
        category=category)  # Finnhub `get_market_news` doesn't have a count param in current client

    if news_items and isinstance(news_items, list):
        logger.info(f"Fetched {len(news_items)} news items from Finnhub.")
        # The Finnhub client's get_market_news might already limit, but we can slice here if needed
        return news_items[:count_to_fetch_from_api]
    else:
        logger.warning(f"Failed to fetch news or received unexpected format from Finnhub: {news_items}")
        return []


def scrape_news_article_content(news_url):
    """Scrapes content for a given news URL."""
    if not news_url:
        return None
    logger.info(f"Attempting to scrape full article for: {news_url}")
    full_article_text = scrape_article_content(news_url)  # This is the generic scraper
    time.sleep(1)  # Small delay after scraping
    if full_article_text:
        logger.info(f"Scraped ~{len(full_article_text)} chars for {news_url}")
    else:
        logger.warning(f"Failed to scrape full article for {news_url}.")
    return full_article_text