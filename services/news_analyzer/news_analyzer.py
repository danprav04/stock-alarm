# services/news_analyzer/news_analyzer.py
import time
from datetime import datetime, timezone, timedelta
from database import SessionLocal, get_db_session, NewsEventAnalysis
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError
from core.config import MAX_NEWS_ARTICLES_PER_QUERY, MAX_NEWS_TO_ANALYZE_PER_RUN

from api_clients import FinnhubClient, GeminiAPIClient  # No scrape_article_content directly needed here

# Import from submodules
from .data_fetcher import fetch_market_news_from_api
from .db_handler import (
    get_or_create_news_event_db_entry,
    _ensure_news_event_is_bound_to_session,
    _ensure_news_event_session_is_active
)
from .ai_analyzer import perform_ai_analysis_for_news_item


class NewsAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        # NewsAnalyzer manages its own DB session for the duration of its pipeline run or instance lifetime.
        self.db_session_generator = get_db_session()  # Store the generator
        self.db_session = next(self.db_session_generator)  # Get the initial session
        logger.debug("NewsAnalyzer initialized with a new DB session.")

    def get_new_db_session_generator(self):
        """Allows db_handler to get a new session generator if needed."""
        return get_db_session()

    def _close_session_if_active(self):
        """Closes the NewsAnalyzer's current DB session if it's active."""
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close()
                logger.debug("DB session closed in NewsAnalyzer.")
            except Exception as e_close:
                logger.warning(f"Error closing session in NewsAnalyzer: {e_close}")
        # db_session_generator does not need explicit closing here.
        # SessionLocal.remove() will be called by the context manager of get_db_session if it was used that way.
        # Or if SessionLocal() was directly used, it must be managed.
        # For now, we assume SessionLocal() created by get_db_session handles its own removal/closing when the generator is exhausted or via its yield finally block.

    def analyze_single_news_item_and_save(self, news_event_db_obj):
        """
        Orchestrates AI analysis for a single news item and saves the analysis.
        Uses the NewsAnalyzer's managed db_session.
        """
        if not news_event_db_obj:
            logger.error("analyze_single_news_item_and_save called with no NewsEvent DB object.")
            return None

        # Ensure the object is bound to the current session
        bound_news_event_db_obj = _ensure_news_event_is_bound_to_session(self, news_event_db_obj)
        if not bound_news_event_db_obj:
            logger.error(
                f"Failed to bind news event (ID: {news_event_db_obj.id if news_event_db_obj.id else 'N/A'}) to session. Cannot analyze.")
            return None

        news_event_to_analyze = bound_news_event_db_obj  # Use the (potentially merged/re-fetched) object

        analysis_payload = perform_ai_analysis_for_news_item(self, news_event_to_analyze)

        current_analysis_time = datetime.now(timezone.utc)
        news_analysis_entry = NewsEventAnalysis(
            news_event_id=news_event_to_analyze.id,
            analysis_date=current_analysis_time,
            **analysis_payload
        )

        self.db_session.add(news_analysis_entry)
        news_event_to_analyze.last_analyzed_date = current_analysis_time  # Update parent event

        try:
            self.db_session.commit()
            logger.info(
                f"Successfully analyzed and saved news: '{news_event_to_analyze.event_title[:70]}...' (Analysis ID: {news_analysis_entry.id})")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving news analysis for '{news_event_to_analyze.event_title[:70]}...': {e}",
                         exc_info=True)
            return None  # Indicate failure

        return news_analysis_entry

    def run_news_analysis_pipeline(self, category="general", count_to_fetch_from_api=MAX_NEWS_ARTICLES_PER_QUERY,
                                   count_to_analyze_this_run=MAX_NEWS_TO_ANALYZE_PER_RUN):
        try:
            _ensure_news_event_session_is_active(self,
                                                 f"Pipeline Start: Category {category}")  # Ensure session is good at start

            fetched_news_items_api = fetch_market_news_from_api(self, category=category,
                                                                count_to_fetch_from_api=count_to_fetch_from_api)
            if not fetched_news_items_api:
                logger.info("No news items fetched from API for analysis.")
                return []

            analyzed_news_results = []
            newly_analyzed_count_this_run = 0

            # Define how old an analysis can be before re-analysis is considered
            reanalyze_older_than_days = 2
            reanalyze_threshold_date = datetime.now(timezone.utc) - timedelta(days=reanalyze_older_than_days)

            for news_item_api_data in fetched_news_items_api:
                if newly_analyzed_count_this_run >= count_to_analyze_this_run:
                    logger.info(
                        f"Reached analysis limit of {count_to_analyze_this_run} new/re-analyzed items for this run.")
                    break

                try:
                    # Get or create the NewsEvent DB entry. This also handles scraping.
                    news_event_db = get_or_create_news_event_db_entry(self, news_item_api_data)
                    if not news_event_db:
                        logger.warning(
                            f"Skipping news item (could not get/create in DB): {news_item_api_data.get('headline')}")
                        continue

                    # Ensure this db object (which might be new or existing) is bound to the session correctly
                    news_event_db = _ensure_news_event_is_bound_to_session(self, news_event_db)
                    if not news_event_db:  # If binding failed critically
                        logger.error(
                            f"Critical: Failed to bind news event {news_item_api_data.get('headline')} to session. Skipping.")
                        continue

                    analysis_needed = False
                    latest_analysis = None

                    # Check existing analyses for this news event
                    # Query sorted by date to get the most recent one
                    if news_event_db.id:  # Ensure event has an ID (i.e., it's persisted or merged)
                        latest_analysis = self.db_session.query(NewsEventAnalysis) \
                            .filter(NewsEventAnalysis.news_event_id == news_event_db.id) \
                            .order_by(NewsEventAnalysis.analysis_date.desc()) \
                            .first()

                    if not latest_analysis:
                        analysis_needed = True
                        logger.info(f"News '{news_event_db.event_title[:50]}...' requires new analysis.")
                    elif latest_analysis.analysis_date < reanalyze_threshold_date:
                        analysis_needed = True
                        logger.info(
                            f"News '{news_event_db.event_title[:50]}...' requires re-analysis (last analyzed {latest_analysis.analysis_date}, older than {reanalyze_older_than_days} days).")
                    # Check if full text became available since last analysis (if last analysis was headline-only)
                    elif news_event_db.full_article_text and latest_analysis and latest_analysis.key_news_snippets:
                        source_type_used_last_time = latest_analysis.key_news_snippets.get("source_type_used", "")
                        if "full article" not in source_type_used_last_time.lower():
                            analysis_needed = True
                            logger.info(
                                f"News '{news_event_db.event_title[:50]}...' re-analyzing with newly available/confirmed full text (was: {source_type_used_last_time}).")
                    else:
                        logger.info(
                            f"News '{news_event_db.event_title[:50]}...' already recently analyzed with available text. Skipping.")

                    if analysis_needed:
                        analysis_result = self.analyze_single_news_item_and_save(news_event_db)
                        if analysis_result:
                            analyzed_news_results.append(analysis_result)
                            newly_analyzed_count_this_run += 1
                        time.sleep(3)  # Delay between AI calls if multiple items are analyzed

                except Exception as e_item:  # Catch errors for a single item processing
                    logger.error(
                        f"Failed to process or analyze news item '{news_item_api_data.get('headline')}': {e_item}",
                        exc_info=True)
                    _ensure_news_event_session_is_active(self,
                                                         f"Error Recovery for {news_item_api_data.get('headline')}")  # Ensure session is active for next item
                    if self.db_session and self.db_session.is_active:
                        self.db_session.rollback()  # Rollback any partial transaction for this item

            logger.info(
                f"News analysis pipeline completed. Newly analyzed/re-analyzed {newly_analyzed_count_this_run} items.")
            return analyzed_news_results

        finally:
            self._close_session_if_active()  # Close session at the end of the pipeline run