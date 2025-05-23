# news_analyzer.py
import time
from api_clients import FinnhubClient, GeminiAPIClient
from database import SessionLocal, get_db_session
from models import NewsEvent, NewsEventAnalysis, Stock  # To link news to stocks
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload
from datetime import datetime
from config import MAX_NEWS_ARTICLES_PER_QUERY


class NewsAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        self.db_session = next(get_db_session())

    def fetch_market_news(self, category="general", count=MAX_NEWS_ARTICLES_PER_QUERY):
        logger.info(f"Fetching latest market news for category: {category} (max {count})...")
        # Finnhub's /news endpoint doesn't have a 'count' param, it returns recent news.
        # We might need to paginate or filter if we need more than one batch.
        # For now, one call and take top 'count'.
        news_items = self.finnhub.get_market_news(category=category)

        if news_items and isinstance(news_items, list):
            logger.info(f"Fetched {len(news_items)} news items from Finnhub.")
            return news_items[:count]
        else:
            logger.error(f"Failed to fetch news or received unexpected format: {news_items}")
            return []

    def _get_or_create_news_event(self, news_item_from_api):
        # Assuming news_item_from_api is a dict from Finnhub like:
        # {'category': 'business', 'datetime': 1600000000, 'headline': '...',
        #  'id': 123, 'image': '...', 'related': 'AAPL', 'source': 'Reuters', 'summary': '...', 'url': '...'}

        source_url = news_item_from_api.get("url")
        if not source_url:
            logger.warning(f"News item missing URL, cannot use as unique ID: {news_item_from_api.get('headline')}")
            return None  # Cannot reliably deduplicate without a URL or persistent ID

        event = self.db_session.query(NewsEvent).filter_by(source_url=source_url).first()
        if event:
            logger.info(f"News event already processed: {source_url}")
            return event  # Already processed

        event_datetime = datetime.fromtimestamp(news_item_from_api.get("datetime")) if news_item_from_api.get(
            "datetime") else datetime.utcnow()

        event = NewsEvent(
            event_title=news_item_from_api.get("headline"),
            event_date=event_datetime,
            source_url=source_url,
            category=news_item_from_api.get("category"),
            # related_symbols can be stored if needed, news_item_from_api.get("related") often gives one symbol
        )
        self.db_session.add(event)
        try:
            self.db_session.commit()
            logger.info(f"Stored new news event: {event.event_title}")
            return event
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error storing news event {event.event_title}: {e}", exc_info=True)
            return None

    def analyze_single_news_item(self, news_item_data, existing_event_db_id=None):
        """
        Analyzes a single piece of news.
        news_item_data: dict from Finnhub API.
        existing_event_db_id: If the NewsEvent DB entry already exists.
        """
        if existing_event_db_id:
            news_event_db = self.db_session.query(NewsEvent).get(existing_event_db_id)
        else:  # Should have been created before calling this if it's new
            logger.error("analyze_single_news_item called without a valid news_event_db reference.")
            return None

        if not news_event_db:
            logger.error(f"News event with ID {existing_event_db_id} not found for analysis.")
            return None

        headline = news_event_db.event_title
        summary = news_item_data.get("summary", "")  # Finnhub summary
        news_content_for_analysis = f"Headline: {headline}\nSummary: {summary}"
        # In a more advanced system, you'd fetch full article content from news_event_db.source_url using a browsing tool.
        # For now, we use headline + API summary.

        logger.info(f"Analyzing news: {headline}")
        analysis_payload = {"key_news_snippets": {"headline": headline, "api_summary": summary}}

        # Step 1 & 2: Identify/Categorize & Scope/Relevance (partially done by Finnhub, Gemini can elaborate)
        prompt_scope = (
            f"News: \"{news_content_for_analysis}\"\n\n"
            f"1. Categorize this news more specifically (e.g., earnings, product launch, macroeconomic shift, regulatory change, M&A).\n"
            f"2. What is the potential scope (broad market, specific sectors, few companies) and direct relevance of this news?"
        )
        scope_relevance_text = self.gemini.generate_text(prompt_scope)
        analysis_payload["scope_relevance"] = scope_relevance_text
        # TODO: Parse category from scope_relevance_text if needed to refine NewsEvent.category

        # Step 3: Identify Potentially Affected Stocks/Sectors
        # Finnhub 'related' field gives one symbol. Gemini can find more.
        related_symbols_api = news_item_data.get("related", "")  # e.g., "AAPL" or "MSFT,GOOGL"

        prompt_affected = (
            f"News: \"{news_content_for_analysis}\"\n\n"
            f"Initial related symbol(s) from API: '{related_symbols_api}'.\n"
            f"Besides these, what other specific companies (by ticker symbol if possible) or sectors are likely to be "
            f"significantly affected by this news, and why briefly? "
            f"Think about direct competitors, key suppliers/customers, or companies in related industries."
        )
        affected_text = self.gemini.generate_text(prompt_affected)
        # This text needs parsing to extract tickers and sectors. For now, store as text.
        # A more robust solution would involve named entity recognition for tickers.
        analysis_payload["affected_stocks_sectors"] = {"text_analysis": affected_text,
                                                       "api_related": related_symbols_api}

        # Step 4: Analyze the Mechanism of Impact
        prompt_mechanism = (
            f"News: \"{news_content_for_analysis}\"\n\n"
            f"For the primary company/sector identified ({related_symbols_api} or as determined from context), "
            f"how will this news likely affect its fundamentals (revenue, costs, profitability, growth) or market perception? "
            f"Explain the mechanism."
        )
        analysis_payload["mechanism_of_impact"] = self.gemini.generate_text(prompt_mechanism)

        # Step 5 & 6: Estimate Timing, Duration, Magnitude, Direction
        prompt_timing_magnitude = (
            f"News: \"{news_content_for_analysis}\"\n\n"
            f"Estimate the likely timing (immediate, short-term, medium-term, long-term) and duration of the impact. "
            f"Also, estimate the potential magnitude (small, medium, large) and direction (positive, negative, neutral/mixed) of the impact "
            f"on the primary affected entities."
        )
        timing_magnitude_text = self.gemini.generate_text(prompt_timing_magnitude)
        # Simple split, Gemini might format differently
        parts_tm = timing_magnitude_text.split("Magnitude and Direction:")  # Heuristic split
        analysis_payload["estimated_timing"] = parts_tm[0].replace("Timing and Duration:", "").strip()
        if len(parts_tm) > 1:
            analysis_payload["estimated_magnitude_direction"] = parts_tm[1].strip()
        else:  # If split fails, put all text in timing
            analysis_payload["estimated_magnitude_direction"] = "See timing section or N/A"

        # Step 7: Countervailing Factors (General prompt)
        prompt_counter = (
            f"News: \"{news_content_for_analysis}\"\n\n"
            f"What are potential countervailing factors or broader market sentiments that might moderate or amplify the impact of this news?"
        )
        analysis_payload["countervailing_factors"] = self.gemini.generate_text(prompt_counter)

        # Summary for email
        prompt_summary_email = (
            f"News: \"{news_content_for_analysis}\"\n\n"
            f"Primary affected: {analysis_payload['affected_stocks_sectors']['text_analysis']}\n"
            f"Likely Impact: Mechanism: {analysis_payload['mechanism_of_impact'][:200]}... Direction/Magnitude: {analysis_payload.get('estimated_magnitude_direction', 'N/A')}\n\n"
            f"Provide a concise 2-3 sentence summary of this news and its most critical implication for an investor."
        )
        analysis_payload["summary_for_email"] = self.gemini.generate_text(prompt_summary_email)

        # Store analysis
        news_analysis_entry = NewsEventAnalysis(
            news_event_id=news_event_db.id,
            scope_relevance=analysis_payload.get("scope_relevance"),
            affected_stocks_sectors=analysis_payload.get("affected_stocks_sectors"),
            mechanism_of_impact=analysis_payload.get("mechanism_of_impact"),
            estimated_timing=analysis_payload.get("estimated_timing"),
            estimated_magnitude_direction=analysis_payload.get("estimated_magnitude_direction"),
            countervailing_factors=analysis_payload.get("countervailing_factors"),
            summary_for_email=analysis_payload.get("summary_for_email"),
            key_news_snippets=analysis_payload.get("key_news_snippets")
        )
        self.db_session.add(news_analysis_entry)
        news_event_db.processed_date = datetime.utcnow()

        try:
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved news: {headline}")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving news analysis for {headline}: {e}", exc_info=True)
            return None

        return news_analysis_entry

    def run_news_analysis_pipeline(self, category="general", count=5):
        """ Fetches news, checks if already processed, analyzes if new, saves."""
        fetched_news_items = self.fetch_market_news(category=category,
                                                    count=count * 2)  # Fetch more to allow for already processed
        if not fetched_news_items:
            logger.info("No news items fetched.")
            return []

        analyzed_news_results = []
        processed_count = 0

        for news_item in fetched_news_items:
            if processed_count >= count:
                break  # Reached desired number of *newly* analyzed items

            # Check if news event exists by URL
            news_event_db_entry = self.db_session.query(NewsEvent).filter_by(source_url=news_item.get("url")).options(
                joinedload(NewsEvent.analyses)).first()

            is_new_analysis_needed = False
            if news_event_db_entry:
                if not news_event_db_entry.analyses:  # Exists but not analyzed
                    logger.info(
                        f"News event '{news_item.get('headline')}' found in DB but not analyzed yet. Analyzing now.")
                    is_new_analysis_needed = True
                else:
                    logger.info(f"News event '{news_item.get('headline')}' already analyzed. Skipping.")
                    # Could add logic to re-analyze if old, but for now, skip if any analysis exists.
            else:  # News event not in DB at all
                logger.info(f"News event '{news_item.get('headline')}' is new. Storing and analyzing.")
                news_event_db_entry = self._get_or_create_news_event(news_item)  # This commits the NewsEvent
                if news_event_db_entry:
                    is_new_analysis_needed = True

            if is_new_analysis_needed and news_event_db_entry:
                # Re-acquire session if it was closed or became inactive
                if not self.db_session.is_active: self.db_session = next(get_db_session())  # type: ignore

                analysis_result = self.analyze_single_news_item(news_item, existing_event_db_id=news_event_db_entry.id)
                if analysis_result:
                    analyzed_news_results.append(analysis_result)
                    processed_count += 1
                time.sleep(2)  # Courtesy delay for Gemini API calls
            elif not news_event_db_entry and not is_new_analysis_needed:  # Failed to create entry
                logger.warning(f"Skipping news item due to failure in DB handling: {news_item.get('headline')}")

        logger.info(f"News analysis pipeline completed. Newly analyzed {len(analyzed_news_results)} items.")
        if self.db_session.is_active:  # type: ignore
            self.db_session.close()
        return analyzed_news_results


# Example usage:
if __name__ == '__main__':
    from database import init_db

    try:
        init_db()
        logger.info("Starting standalone news analysis pipeline test...")
        analyzer = NewsAnalyzer()
        # Analyze 2 new general news items
        results = analyzer.run_news_analysis_pipeline(category="general", count=2)
        if results:
            logger.info(f"Processed {len(results)} new news items.")
            for res in results:
                logger.info(f"News: {res.news_event.event_title}, Summary: {res.summary_for_email}")
        else:
            logger.info("No new news items were processed.")
    except Exception as e:
        logger.error(f"Error during news analysis test: {e}", exc_info=True)
