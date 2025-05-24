# news_analyzer.py
import time
from sqlalchemy import inspect as sa_inspect  # For checking instance state
from sqlalchemy.orm import joinedload
from datetime import datetime, timezone, timedelta

from api_clients import FinnhubClient, GeminiAPIClient, scrape_article_content
from database import SessionLocal, get_db_session
from models import NewsEvent, NewsEventAnalysis
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError
from config import (
    MAX_NEWS_ARTICLES_PER_QUERY, MAX_NEWS_TO_ANALYZE_PER_RUN,
    NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI, MAX_GEMINI_TEXT_LENGTH
)


class NewsAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        self.db_session = next(get_db_session())

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close()
                logger.debug("DB session closed in NewsAnalyzer.")
            except Exception as e_close:
                logger.warning(f"Error closing session in NewsAnalyzer: {e_close}")

    def fetch_market_news(self, category="general", count_to_fetch_from_api=MAX_NEWS_ARTICLES_PER_QUERY):
        logger.info(f"Fetching latest market news for category: {category} (max {count_to_fetch_from_api} from API)...")
        # Finnhub's /news endpoint doesn't have a 'count' param for items returned in one call, it returns recent news.
        # We might need to paginate or filter if we need more than one batch.
        # For now, one call and take top 'count_to_fetch_from_api'.
        news_items = self.finnhub.get_market_news(category=category)

        if news_items and isinstance(news_items, list):
            logger.info(f"Fetched {len(news_items)} news items from Finnhub.")
            return news_items[:count_to_fetch_from_api]
        else:
            logger.warning(f"Failed to fetch news or received unexpected format from Finnhub: {news_items}")
            return []

    def _get_or_create_news_event(self, news_item_from_api):
        self._ensure_news_event_session_active(news_item_from_api.get('headline', 'Unknown News'))

        source_url = news_item_from_api.get("url")
        if not source_url:
            logger.warning(f"News item missing URL, cannot process: {news_item_from_api.get('headline')}")
            return None  # Cannot reliably deduplicate or process without a URL

        event = self.db_session.query(NewsEvent).filter_by(source_url=source_url).first()

        full_article_text_scraped_now = None
        # Scrape only if event is new, or if it exists but full_article_text is missing
        if not event or (event and not event.full_article_text):
            logger.info(f"Attempting to scrape full article for: {source_url}")
            full_article_text_scraped_now = scrape_article_content(source_url)  # This is an API call (HTTP GET)
            time.sleep(1)  # Small delay after scraping
            if full_article_text_scraped_now:
                logger.info(f"Scraped ~{len(full_article_text_scraped_now)} chars for {source_url}")
            else:
                logger.warning(
                    f"Failed to scrape full article for {source_url}. Analysis will use summary if available.")

        current_time_utc = datetime.now(timezone.utc)
        if event:  # Event already exists in DB
            logger.debug(f"News event '{event.event_title[:70]}...' (URL: {source_url}) already in DB.")
            # If we just scraped text and it was missing, update the event
            if full_article_text_scraped_now and not event.full_article_text:
                logger.info(f"Updating existing event {event.id} with newly scraped full article text.")
                event.full_article_text = full_article_text_scraped_now
                event.processed_date = current_time_utc  # Update processed date as we've enhanced it
                try:
                    self.db_session.commit()
                except SQLAlchemyError as e:
                    self.db_session.rollback()
                    logger.error(f"Error updating full_article_text for existing event {source_url}: {e}")
            return event

        # Event is new, create it
        event_timestamp = news_item_from_api.get("datetime")
        event_datetime_utc = datetime.fromtimestamp(event_timestamp,
                                                    timezone.utc) if event_timestamp else current_time_utc

        new_event = NewsEvent(
            event_title=news_item_from_api.get("headline"),
            event_date=event_datetime_utc,
            source_url=source_url,
            source_name=news_item_from_api.get("source"),
            category=news_item_from_api.get("category"),
            full_article_text=full_article_text_scraped_now,  # Store scraped text
            processed_date=current_time_utc
        )
        self.db_session.add(new_event)
        try:
            self.db_session.commit()
            self.db_session.refresh(new_event)  # Get ID and other defaults loaded
            logger.info(f"Stored new news event: {new_event.event_title[:70]}... (ID: {new_event.id})")
            return new_event
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error storing new news event '{news_item_from_api.get('headline')}': {e}",
                         exc_info=True)
            return None

    def analyze_single_news_item(self, news_event_db):
        if not news_event_db:
            logger.error("analyze_single_news_item called with no NewsEvent DB object.")
            return None

        # Ensure the event object is bound to the current session
        news_event_db = self._ensure_news_event_is_bound(news_event_db)
        if not news_event_db:  # If binding failed
            return None

        headline = news_event_db.event_title
        content_for_analysis = news_event_db.full_article_text
        analysis_source_type = "full article"

        if not content_for_analysis:
            # Fallback to headline if no full article text (e.g. scraping failed or PDF)
            # A more robust system might use Finnhub summary if that was stored with NewsEvent.
            content_for_analysis = headline
            analysis_source_type = "headline only"
            logger.warning(f"No full article text for '{headline}'. Analyzing based on headline only.")

        # Truncate for Gemini if too long
        if len(content_for_analysis) > NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI:
            content_for_analysis = content_for_analysis[
                                   :NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI] + "\n... [CONTENT TRUNCATED FOR AI ANALYSIS] ..."
            logger.info(
                f"Truncated news content for '{headline}' to {NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI} chars for Gemini.")
            analysis_source_type += " (truncated)"

        logger.info(f"Analyzing news: '{headline[:70]}...' (using {analysis_source_type})")
        analysis_payload = {"key_news_snippets": {"headline": headline, "source_type_used": analysis_source_type}}

        # 1. Sentiment Analysis
        sentiment_context = f"News headline for context: {headline}"
        sentiment_response = self.gemini.analyze_sentiment_with_reasoning(content_for_analysis,
                                                                          context=sentiment_context)
        time.sleep(2)  # API courtesy
        if not sentiment_response.startswith("Error:"):
            try:  # Attempt to parse sentiment and reasoning
                parts = sentiment_response.split("Reasoning:", 1)
                if ":" in parts[0]:  # Expect "Sentiment: [Class]"
                    sentiment_class_text = parts[0].split(":", 1)[1].strip()
                    # Take first word, remove punctuation
                    analysis_payload["sentiment"] = sentiment_class_text.split(' ')[0].split('.')[0].split(',')[
                        0].strip()
                else:  # If no "Sentiment:" prefix, try to infer
                    analysis_payload["sentiment"] = parts[0].strip().split(' ')[0]  # First word as sentiment
                analysis_payload["sentiment_reasoning"] = parts[1].strip() if len(parts) > 1 else sentiment_response
            except Exception as e_parse_sent:
                logger.warning(
                    f"Could not robustly parse sentiment response for '{headline}': {sentiment_response}. Error: {e_parse_sent}. Storing raw.")
                analysis_payload["sentiment"] = "Error Parsing"
                analysis_payload["sentiment_reasoning"] = sentiment_response
        else:
            analysis_payload["sentiment"] = "AI Error"
            analysis_payload["sentiment_reasoning"] = sentiment_response

        # 2. Detailed Summary & Impact Analysis
        prompt_detailed_analysis = (
            f"News Headline: \"{headline}\"\n"
            f"News Content (may be truncated): \"{content_for_analysis}\"\n\n"
            f"Instructions for Analysis:\n"
            f"1. News Summary: Provide a comprehensive yet concise summary of this news article (3-5 key sentences).\n"
            f"2. Affected Entities: Identify specific companies (with ticker symbols if known and highly relevant) and/or specific industry sectors directly or significantly indirectly affected by this news. Explain why briefly for each.\n"
            f"3. Mechanism of Impact: For the primary affected entities, describe how this news will likely affect their fundamentals (e.g., revenue, costs, market share, customer sentiment) or market perception.\n"
            f"4. Estimated Timing & Duration: Estimate the likely timing (e.g., Immediate, Short-term <3mo, Medium-term 3-12mo, Long-term >1yr) and duration of the impact.\n"
            f"5. Estimated Magnitude & Direction: Estimate the potential magnitude (e.g., Low, Medium, High) and direction (e.g., Positive, Negative, Neutral/Mixed) of the impact on the primary affected entities.\n"
            f"6. Confidence Level: State your confidence (High, Medium, Low) in this overall impact assessment, briefly justifying it (e.g., based on clarity of news, directness of impact).\n"
            f"7. Investor Summary: Provide a final 2-sentence summary specifically for an investor, highlighting the most critical implication or takeaway.\n\n"
            f"Structure your response clearly with headings for each point (e.g., 'News Summary:', 'Affected Entities:', etc.)."
        )

        impact_analysis_response = self.gemini.generate_text(
            prompt_detailed_analysis[:MAX_GEMINI_TEXT_LENGTH])  # Ensure prompt length
        time.sleep(2)  # API courtesy

        if not impact_analysis_response.startswith("Error:"):
            analysis_payload["news_summary_detailed"] = self._parse_ai_section(impact_analysis_response,
                                                                               "News Summary:")
            analysis_payload["potential_impact_on_companies"] = self._parse_ai_section(impact_analysis_response,
                                                                                       ["Affected Entities:",
                                                                                        "Affected Companies:",
                                                                                        "Affected Stocks/Sectors:"])
            # If "Affected Sectors:" is a distinct section, try to get it too.
            sectors_text = self._parse_ai_section(impact_analysis_response, "Affected Sectors:")
            if sectors_text and not sectors_text.startswith("Section not found"):
                analysis_payload["potential_impact_on_sectors"] = sectors_text
            elif analysis_payload["potential_impact_on_companies"] and not analysis_payload.get(
                    "potential_impact_on_sectors"):  # If combined
                analysis_payload["potential_impact_on_sectors"] = analysis_payload["potential_impact_on_companies"]

            analysis_payload["mechanism_of_impact"] = self._parse_ai_section(impact_analysis_response,
                                                                             "Mechanism of Impact:")
            analysis_payload["estimated_timing_duration"] = self._parse_ai_section(impact_analysis_response,
                                                                                   ["Estimated Timing & Duration:",
                                                                                    "Estimated Timing:"])
            analysis_payload["estimated_magnitude_direction"] = self._parse_ai_section(impact_analysis_response, [
                "Estimated Magnitude & Direction:", "Estimated Magnitude/Direction:"])
            analysis_payload["confidence_of_assessment"] = self._parse_ai_section(impact_analysis_response,
                                                                                  "Confidence Level:")
            analysis_payload["summary_for_email"] = self._parse_ai_section(impact_analysis_response,
                                                                           ["Investor Summary:",
                                                                            "Final Summary for Investor:"])
        else:
            logger.error(
                f"Gemini failed to provide detailed impact analysis for '{headline}': {impact_analysis_response}")
            analysis_payload["news_summary_detailed"] = impact_analysis_response  # Store error message

        # Store analysis
        current_analysis_time = datetime.now(timezone.utc)
        news_analysis_entry = NewsEventAnalysis(
            news_event_id=news_event_db.id,
            analysis_date=current_analysis_time,
            sentiment=analysis_payload.get("sentiment"),
            sentiment_reasoning=analysis_payload.get("sentiment_reasoning"),
            news_summary_detailed=analysis_payload.get("news_summary_detailed"),
            potential_impact_on_companies=analysis_payload.get("potential_impact_on_companies"),
            potential_impact_on_sectors=analysis_payload.get("potential_impact_on_sectors"),
            mechanism_of_impact=analysis_payload.get("mechanism_of_impact"),
            estimated_timing_duration=analysis_payload.get("estimated_timing_duration"),
            estimated_magnitude_direction=analysis_payload.get("estimated_magnitude_direction"),
            confidence_of_assessment=analysis_payload.get("confidence_of_assessment"),
            summary_for_email=analysis_payload.get("summary_for_email"),
            key_news_snippets=analysis_payload.get("key_news_snippets")
            # TODO: Add logic to parse tickers/sectors from `potential_impact_on_companies` into `affected_stocks_explicit` JSON fields if needed.
        )
        self.db_session.add(news_analysis_entry)
        news_event_db.last_analyzed_date = current_analysis_time  # Update parent event

        try:
            self.db_session.commit()
            logger.info(
                f"Successfully analyzed and saved news: '{headline[:70]}...' (Analysis ID: {news_analysis_entry.id})")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving news analysis for '{headline[:70]}...': {e}", exc_info=True)
            return None

        return news_analysis_entry

    def _parse_ai_section(self, ai_text, section_header_keywords):
        # This helper is now more robust, moved from IPOAnalyzer to be reusable if needed
        # (though for now it's kept within each class for simplicity of single file changes)
        if not ai_text or ai_text.startswith("Error:"): return "AI Error or No Text"

        if isinstance(section_header_keywords, str):
            keywords_to_check = [section_header_keywords.lower().strip()]
        else:
            keywords_to_check = [k.lower().strip() for k in section_header_keywords]

        lines = ai_text.split('\n')
        capture = False
        section_content = []

        # Define a list of all potential headers that could terminate a section.
        # This helps in accurately capturing multi-line content for the current section.
        all_known_headers_lower_prefixes = [
            "news summary:", "affected entities:", "affected companies:", "affected stocks/sectors:",
            "mechanism of impact:", "estimated timing & duration:", "estimated timing:",
            "estimated magnitude & direction:", "estimated magnitude/direction:",
            "confidence level:", "investor summary:", "final summary for investor:"
        ]  # Add other general section headers from other analyzers if this becomes shared utility.

        for i, line_original in enumerate(lines):
            line_stripped_lower = line_original.strip().lower()

            matched_current_keyword = None
            for kw_lower in keywords_to_check:
                # Check if the line starts with the keyword (potentially followed by a colon)
                if line_stripped_lower.startswith(kw_lower + ":") or line_stripped_lower == kw_lower:
                    matched_current_keyword = kw_lower
                    break

            if matched_current_keyword:
                capture = True
                # Get content on the same line after the header
                content_on_header_line = line_original.strip()[len(matched_current_keyword):].strip()
                if content_on_header_line.startswith(":"):
                    content_on_header_line = content_on_header_line[1:].strip()
                if content_on_header_line:
                    section_content.append(content_on_header_line)
                continue  # Move to next line

            if capture:
                # Check if the current line starts a *different* known section
                is_another_known_header = False
                for known_header_prefix in all_known_headers_lower_prefixes:
                    if line_stripped_lower.startswith(
                            known_header_prefix) and known_header_prefix not in keywords_to_check:
                        is_another_known_header = True
                        break

                if is_another_known_header:
                    break  # End capture for the current section

                section_content.append(line_original)  # Append the original line to preserve formatting

        return "\n".join(section_content).strip() if section_content else "Section not found or empty."

    def run_news_analysis_pipeline(self, category="general", count_to_fetch_from_api=MAX_NEWS_ARTICLES_PER_QUERY,
                                   count_to_analyze_this_run=MAX_NEWS_TO_ANALYZE_PER_RUN):
        fetched_news_items_api = self.fetch_market_news(category=category,
                                                        count_to_fetch_from_api=count_to_fetch_from_api)
        if not fetched_news_items_api:
            logger.info("No news items fetched from API for analysis.")
            self._close_session_if_active()
            return []

        analyzed_news_results = []
        newly_analyzed_count_this_run = 0

        reanalyze_older_than_days = 2  # Re-analyze if analysis is older, or if full text was missing and now found
        reanalyze_threshold_date = datetime.now(timezone.utc) - timedelta(days=reanalyze_older_than_days)

        for news_item_api_data in fetched_news_items_api:
            if newly_analyzed_count_this_run >= count_to_analyze_this_run:
                logger.info(
                    f"Reached analysis limit of {count_to_analyze_this_run} new/re-analyzed items for this run.")
                break

            try:
                news_event_db = self._get_or_create_news_event(
                    news_item_api_data)  # Scrapes/stores full text if new or missing
                if not news_event_db:
                    logger.warning(
                        f"Skipping news item as it could not be fetched or created in DB: {news_item_api_data.get('headline')}")
                    continue

                news_event_db = self._ensure_news_event_is_bound(news_event_db)  # Ensure session attachment
                if not news_event_db: continue

                analysis_needed = False
                # Check if analysis exists and is recent enough
                latest_analysis = None
                if news_event_db.analyses:  # Relationship is a list
                    # Sort by analysis_date descending to get the most recent
                    sorted_analyses = sorted(news_event_db.analyses, key=lambda x: x.analysis_date, reverse=True)
                    if sorted_analyses:
                        latest_analysis = sorted_analyses[0]

                if not latest_analysis:
                    analysis_needed = True
                    logger.info(
                        f"News '{news_event_db.event_title[:50]}...' (ID: {news_event_db.id}) requires new analysis (never analyzed).")
                elif latest_analysis.analysis_date < reanalyze_threshold_date:
                    analysis_needed = True
                    logger.info(
                        f"News '{news_event_db.event_title[:50]}...' (ID: {news_event_db.id}) requires re-analysis (analysis older than {reanalyze_older_than_days} days).")
                elif not news_event_db.full_article_text and latest_analysis:
                    # If it was analyzed but full text was missing (e.g. scraping failed before)
                    # _get_or_create_news_event tries to scrape again. If it succeeds now, we should re-analyze.
                    # The check `if full_article_text_scraped_now and not event.full_article_text:` in _get_or_create_news_event
                    # handles updating the event. If event.full_article_text is now populated, re-analyze.
                    if news_event_db.full_article_text:  # Check if it was successfully populated in this run
                        analysis_needed = True
                        logger.info(
                            f"News '{news_event_db.event_title[:50]}...' (ID: {news_event_db.id}) re-analyzing with newly scraped full text.")
                    else:
                        logger.info(
                            f"News '{news_event_db.event_title[:50]}...' (ID: {news_event_db.id}) already analyzed, full text still unavailable. Skipping re-analysis.")

                if analysis_needed:
                    analysis_result = self.analyze_single_news_item(news_event_db)
                    if analysis_result:
                        analyzed_news_results.append(analysis_result)
                        newly_analyzed_count_this_run += 1
                    time.sleep(3)  # API courtesy delay after each full analysis cycle
                else:
                    logger.info(
                        f"News '{news_event_db.event_title[:50]}...' (ID: {news_event_db.id}) already recently analyzed with available text. Skipping.")

            except Exception as e:
                logger.error(f"Failed to process or analyze news item '{news_item_api_data.get('headline')}': {e}",
                             exc_info=True)
                # Ensure session robustness for the next item
                if self.db_session and not self.db_session.is_active:
                    self.db_session = next(get_db_session())
                elif self.db_session:
                    self.db_session.rollback()  # Rollback current transaction if error occurred within loop item

        logger.info(
            f"News analysis pipeline completed. Newly analyzed/re-analyzed {newly_analyzed_count_this_run} items.")
        self._close_session_if_active()
        return analyzed_news_results

    def _ensure_news_event_session_active(self, news_identifier_for_log):
        if not self.db_session.is_active:
            logger.warning(f"Session for News '{news_identifier_for_log}' was inactive. Re-establishing.")
            self._close_session_if_active()
            self.db_session = next(get_db_session())

    def _ensure_news_event_is_bound(self, news_event_db_obj):
        """Ensures the news_event_db_obj is bound to the current active session."""
        if not news_event_db_obj: return None  # Should not happen

        self._ensure_news_event_session_active(
            news_event_db_obj.event_title[:50] if news_event_db_obj.event_title else 'Unknown News')

        instance_state = sa_inspect(news_event_db_obj)
        if not instance_state.session or instance_state.session is not self.db_session:
            logger.warning(
                f"NewsEvent DB entry '{news_event_db_obj.event_title[:50]}...' (ID: {news_event_db_obj.id if instance_state.has_identity else 'Transient'}) is not bound to current session. Merging.")
            try:
                if not instance_state.has_identity and news_event_db_obj.id is None:
                    # Try to find by URL if it's a new object that might already exist in this session's view due to prior ops
                    existing_in_session = self.db_session.query(NewsEvent).filter_by(
                        source_url=news_event_db_obj.source_url).first()
                    if existing_in_session:
                        news_event_db_obj = existing_in_session
                        logger.info(
                            f"Replaced transient NewsEvent for '{news_event_db_obj.source_url}' with instance from current session.")
                        return news_event_db_obj

                merged_event = self.db_session.merge(news_event_db_obj)
                logger.info(
                    f"Successfully merged/re-associated NewsEvent '{merged_event.event_title[:50]}...' (ID: {merged_event.id}) into current session.")
                return merged_event
            except Exception as e_merge:
                logger.error(
                    f"Failed to merge NewsEvent '{news_event_db_obj.event_title[:50]}...' into session: {e_merge}. Re-fetching as fallback.",
                    exc_info=True)
                fallback_event = None
                if instance_state.has_identity and news_event_db_obj.id:
                    fallback_event = self.db_session.query(NewsEvent).get(news_event_db_obj.id)
                elif news_event_db_obj.source_url:  # Try by unique URL
                    fallback_event = self.db_session.query(NewsEvent).filter_by(
                        source_url=news_event_db_obj.source_url).first()

                if not fallback_event:
                    logger.critical(
                        f"CRITICAL: Failed to re-associate NewsEvent '{news_event_db_obj.event_title[:50]}...' with current session after merge failure and could not re-fetch.")
                    return None  # Indicate critical failure
                logger.info(
                    f"Successfully re-fetched NewsEvent '{fallback_event.event_title[:50]}...' after merge failure.")
                return fallback_event
        return news_event_db_obj


if __name__ == '__main__':
    from database import init_db

    # init_db() # Ensure DB is initialized with new NewsEvent/Analysis model fields if changed

    logger.info("Starting standalone news analysis pipeline test...")
    analyzer = NewsAnalyzer()
    # Analyze a few new general news items
    results = analyzer.run_news_analysis_pipeline(category="general", count_to_fetch_from_api=10,
                                                  count_to_analyze_this_run=3)
    if results:
        logger.info(f"Pipeline processed {len(results)} news items this run.")
        for res_idx, res in enumerate(results):
            if hasattr(res, 'news_event') and res.news_event:  # Check if result object is valid
                logger.info(f"--- Result {res_idx + 1} ---")
                logger.info(f"News: {res.news_event.event_title[:100]}...")
                logger.info(f"Source: {res.news_event.source_url}")
                logger.info(f"Sentiment: {res.sentiment} - Reasoning: {res.sentiment_reasoning[:100]}...")
                logger.info(f"Investor Summary: {res.summary_for_email}")
                logger.info(f"Full Article Scraped: {'Yes' if res.news_event.full_article_text else 'No'}")
                if res.news_event.full_article_text:
                    logger.debug(f"Full Article Snippet: {res.news_event.full_article_text[:200]}...")
            else:
                logger.warning(
                    f"Processed news result item missing 'news_event' attribute or news_event is None. Result: {res}")
    else:
        logger.info("No new news items were processed in this run.")