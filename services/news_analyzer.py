import time
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone, timedelta

from api_clients import FinnhubClient, GeminiAPIClient, scrape_article_content
from database import SessionLocal, get_db_session, NewsEvent, NewsEventAnalysis
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError
from core.config import (
    MAX_NEWS_ARTICLES_PER_QUERY, MAX_NEWS_TO_ANALYZE_PER_RUN,
    NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION
)


class NewsAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        # Each NewsAnalyzer instance gets its own session, managed carefully
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
        news_items = self.finnhub.get_market_news(category=category)
        if news_items and isinstance(news_items, list):
            logger.info(f"Fetched {len(news_items)} news items from Finnhub.")
            return news_items[:count_to_fetch_from_api]
        else:
            logger.warning(f"Failed to fetch news or received unexpected format from Finnhub: {news_items}")
            return []

    def _ensure_news_event_session_active(self, news_identifier_for_log="Unknown News"):
        if not self.db_session or not self.db_session.is_active:
            logger.warning(f"Session for News '{news_identifier_for_log}' was inactive/closed. Re-establishing.")
            self._close_session_if_active()  # Close any old one
            self.db_session = next(get_db_session())  # Get a fresh session

    def _ensure_news_event_is_bound(self, news_event_db_obj):
        if not news_event_db_obj: return None
        self._ensure_news_event_session_active(
            news_event_db_obj.event_title[:50] if news_event_db_obj.event_title else 'Unknown News')

        instance_state = sa_inspect(news_event_db_obj)
        if not instance_state.session or instance_state.session is not self.db_session:
            obj_id_log = news_event_db_obj.id if instance_state.has_identity else 'Transient'
            logger.warning(
                f"NewsEvent DB entry '{news_event_db_obj.event_title[:50]}...' (ID: {obj_id_log}) not bound to current session. Merging.")
            try:
                if not instance_state.has_identity and news_event_db_obj.id is None:  # Truly transient object
                    existing_in_session = self.db_session.query(NewsEvent).filter_by(
                        source_url=news_event_db_obj.source_url).first()
                    if existing_in_session:
                        news_event_db_obj = existing_in_session
                        logger.info(
                            f"Replaced transient NewsEvent for '{news_event_db_obj.source_url}' with instance from session.")
                        return news_event_db_obj
                # If it has an ID or was not found by URL, try merging
                merged_event = self.db_session.merge(news_event_db_obj)
                logger.info(
                    f"Successfully merged NewsEvent '{merged_event.event_title[:50]}...' (ID: {merged_event.id}) into session.")
                return merged_event
            except Exception as e_merge:
                logger.error(
                    f"Failed to merge NewsEvent '{news_event_db_obj.event_title[:50]}...' into session: {e_merge}. Re-fetching.",
                    exc_info=True)
                fallback_event = None
                if instance_state.has_identity and news_event_db_obj.id:  # If it had an ID
                    fallback_event = self.db_session.query(NewsEvent).get(news_event_db_obj.id)
                elif news_event_db_obj.source_url:  # If it had a URL
                    fallback_event = self.db_session.query(NewsEvent).filter_by(
                        source_url=news_event_db_obj.source_url).first()

                if not fallback_event:
                    logger.critical(
                        f"CRITICAL: Failed to re-associate NewsEvent '{news_event_db_obj.event_title[:50]}...' with session after merge failure.");
                    return None
                logger.info(
                    f"Successfully re-fetched NewsEvent '{fallback_event.event_title[:50]}...' after merge failure.")
                return fallback_event
        return news_event_db_obj

    def _get_or_create_news_event(self, news_item_from_api):
        self._ensure_news_event_session_active(news_item_from_api.get('headline', 'Unknown News'))
        source_url = news_item_from_api.get("url")
        if not source_url: logger.warning(
            f"News item missing URL, cannot process: {news_item_from_api.get('headline')}"); return None

        event = self.db_session.query(NewsEvent).filter_by(source_url=source_url).first()
        full_article_text_scraped_now = None
        if not event or (event and not event.full_article_text):
            logger.info(f"Attempting to scrape full article for: {source_url}")
            full_article_text_scraped_now = scrape_article_content(source_url);
            time.sleep(1)
            if full_article_text_scraped_now:
                logger.info(f"Scraped ~{len(full_article_text_scraped_now)} chars for {source_url}")
            else:
                logger.warning(
                    f"Failed to scrape full article for {source_url}. Analysis will use summary if available.")

        current_time_utc = datetime.now(timezone.utc)
        if event:
            logger.debug(f"News event '{event.event_title[:70]}...' (URL: {source_url}) already in DB.")
            if full_article_text_scraped_now and not event.full_article_text:
                logger.info(f"Updating existing event {event.id} with newly scraped full article text.")
                event.full_article_text = full_article_text_scraped_now
                event.processed_date = current_time_utc
                try:
                    self.db_session.commit()
                except SQLAlchemyError as e:
                    self.db_session.rollback(); logger.error(
                        f"Error updating full_article_text for existing event {source_url}: {e}")
            return event

        event_timestamp = news_item_from_api.get("datetime")
        event_datetime_utc = datetime.fromtimestamp(event_timestamp,
                                                    timezone.utc) if event_timestamp else current_time_utc
        new_event = NewsEvent(
            event_title=news_item_from_api.get("headline"), event_date=event_datetime_utc,
            source_url=source_url, source_name=news_item_from_api.get("source"),
            category=news_item_from_api.get("category"), full_article_text=full_article_text_scraped_now,
            processed_date=current_time_utc
        )
        self.db_session.add(new_event)
        try:
            self.db_session.commit();
            self.db_session.refresh(new_event)
            logger.info(f"Stored new news event: {new_event.event_title[:70]}... (ID: {new_event.id})")
            return new_event
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error storing new news event '{news_item_from_api.get('headline')}': {e}",
                         exc_info=True)
            return None

    def analyze_single_news_item(self, news_event_db):
        if not news_event_db: logger.error("analyze_single_news_item called with no NewsEvent DB object."); return None
        news_event_db = self._ensure_news_event_is_bound(news_event_db)  # Ensure bound to current session
        if not news_event_db: return None  # If binding failed critically

        headline = news_event_db.event_title
        content_for_analysis, analysis_source_type = news_event_db.full_article_text, "full article"
        if not content_for_analysis:
            content_for_analysis, analysis_source_type = headline, "headline only"
            logger.warning(f"No full article text for '{headline}'. Analyzing based on headline only.")
        if len(content_for_analysis) > NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION:
            content_for_analysis = content_for_analysis[
                                   :NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION] + "\n... [CONTENT TRUNCATED FOR AI ANALYSIS] ..."
            logger.info(
                f"Truncated news content for '{headline}' to {NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION} chars for Gemini.")
            analysis_source_type += " (truncated)"

        logger.info(f"Analyzing news: '{headline[:70]}...' (using {analysis_source_type})")
        analysis_payload = {"key_news_snippets": {"headline": headline, "source_type_used": analysis_source_type}}
        sentiment_response = self.gemini.analyze_sentiment_with_reasoning(content_for_analysis,
                                                                          context=f"News headline for context: {headline}");
        time.sleep(2)
        if not sentiment_response.startswith("Error:"):
            try:
                parts = sentiment_response.split("Reasoning:", 1)
                analysis_payload["sentiment"] = \
                parts[0].split(":", 1)[1].strip().split(' ')[0].split('.')[0].split(',')[0].strip() if ":" in parts[
                    0] else parts[0].strip().split(' ')[0]
                analysis_payload["sentiment_reasoning"] = parts[1].strip() if len(parts) > 1 else sentiment_response
            except Exception as e_parse_sent:
                logger.warning(
                    f"Could not parse sentiment response for '{headline}': {sentiment_response}. Error: {e_parse_sent}. Storing raw.")
                analysis_payload["sentiment"], analysis_payload[
                    "sentiment_reasoning"] = "Error Parsing", sentiment_response
        else:
            analysis_payload["sentiment"], analysis_payload["sentiment_reasoning"] = "AI Error", sentiment_response

        prompt_detailed_analysis = (
            f"News Headline: \"{headline}\"\nNews Content (may be truncated): \"{content_for_analysis}\"\n\n"
            f"Instructions for Analysis:\n"
            f"1. News Summary: Provide a comprehensive yet concise summary of this news article (3-5 key sentences).\n"
            f"2. Affected Entities: Identify specific companies (with ticker symbols if known and highly relevant) and/or specific industry sectors directly or significantly indirectly affected by this news. Explain why briefly for each.\n"
            f"3. Mechanism of Impact: For the primary affected entities, describe how this news will likely affect their fundamentals (e.g., revenue, costs, market share, customer sentiment) or market perception.\n"
            f"4. Estimated Timing & Duration: Estimate the likely timing (e.g., Immediate, Short-term <3mo, Medium-term 3-12mo, Long-term >1yr) and duration of the impact.\n"
            f"5. Estimated Magnitude & Direction: Estimate the potential magnitude (e.g., Low, Medium, High) and direction (e.g., Positive, Negative, Neutral/Mixed) of the impact on the primary affected entities.\n"
            f"6. Confidence Level: State your confidence (High, Medium, Low) in this overall impact assessment, briefly justifying it (e.g., based on clarity of news, directness of impact).\n"
            f"7. Investor Summary: Provide a final 2-sentence summary specifically for an investor, highlighting the most critical implication or takeaway.\n\n"
            f"Structure your response clearly with headings for each point (e.g., 'News Summary:', 'Affected Entities:', etc.).")
        impact_analysis_response = self.gemini.generate_text(prompt_detailed_analysis);
        time.sleep(2)
        if not impact_analysis_response.startswith("Error:"):
            analysis_payload["news_summary_detailed"] = self._parse_ai_section(impact_analysis_response,
                                                                               "News Summary:")
            analysis_payload["potential_impact_on_companies"] = self._parse_ai_section(impact_analysis_response,
                                                                                       ["Affected Entities:",
                                                                                        "Affected Companies:",
                                                                                        "Affected Stocks/Sectors:"])
            sectors_text = self._parse_ai_section(impact_analysis_response, "Affected Sectors:")
            if sectors_text and not sectors_text.startswith("Section not found"):
                analysis_payload["potential_impact_on_sectors"] = sectors_text
            elif analysis_payload["potential_impact_on_companies"] and not analysis_payload.get(
                "potential_impact_on_sectors"):
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
                f"Gemini failed to provide detailed impact analysis for '{headline}': {impact_analysis_response}");
            analysis_payload["news_summary_detailed"] = impact_analysis_response

        current_analysis_time = datetime.now(timezone.utc)
        news_analysis_entry = NewsEventAnalysis(news_event_id=news_event_db.id, analysis_date=current_analysis_time,
                                                **analysis_payload)
        self.db_session.add(news_analysis_entry)
        news_event_db.last_analyzed_date = current_analysis_time
        try:
            self.db_session.commit()
            logger.info(
                f"Successfully analyzed and saved news: '{headline[:70]}...' (Analysis ID: {news_analysis_entry.id})")
        except SQLAlchemyError as e:
            self.db_session.rollback();
            logger.error(f"Database error saving news analysis for '{headline[:70]}...': {e}", exc_info=True);
            return None
        return news_analysis_entry

    def _parse_ai_section(self, ai_text, section_header_keywords):
        if not ai_text or ai_text.startswith("Error:"): return "AI Error or No Text"
        keywords_to_check = [k.lower().strip() for k in (
            [section_header_keywords] if isinstance(section_header_keywords, str) else section_header_keywords)]
        lines, capture, section_content = ai_text.split('\n'), False, []
        all_known_headers_lower_prefixes = ["news summary:", "affected entities:", "affected companies:",
                                            "affected stocks/sectors:", "mechanism of impact:",
                                            "estimated timing & duration:", "estimated timing:",
                                            "estimated magnitude & direction:", "estimated magnitude/direction:",
                                            "confidence level:", "investor summary:", "final summary for investor:"]
        for line_original in lines:
            line_stripped_lower = line_original.strip().lower()
            matched_current_keyword = next((kw_lower for kw_lower in keywords_to_check if
                                            line_stripped_lower.startswith(
                                                kw_lower + ":") or line_stripped_lower == kw_lower), None)
            if matched_current_keyword:
                capture = True;
                content_on_header_line = line_original.strip()[len(matched_current_keyword):].strip()
                if content_on_header_line.startswith(":"): content_on_header_line = content_on_header_line[1:].strip()
                if content_on_header_line: section_content.append(content_on_header_line)
                continue
            if capture:
                is_another_known_header = any(
                    line_stripped_lower.startswith(known_header_prefix) for known_header_prefix in
                    all_known_headers_lower_prefixes if known_header_prefix not in keywords_to_check)
                if is_another_known_header: break
                section_content.append(line_original)
        return "\n".join(section_content).strip() if section_content else "Section not found or empty."

    def run_news_analysis_pipeline(self, category="general", count_to_fetch_from_api=MAX_NEWS_ARTICLES_PER_QUERY,
                                   count_to_analyze_this_run=MAX_NEWS_TO_ANALYZE_PER_RUN):
        try:
            fetched_news_items_api = self.fetch_market_news(category=category,
                                                            count_to_fetch_from_api=count_to_fetch_from_api)
            if not fetched_news_items_api: logger.info("No news items fetched from API for analysis."); return []
            analyzed_news_results, newly_analyzed_count_this_run = [], 0
            reanalyze_older_than_days, reanalyze_threshold_date = 2, datetime.now(timezone.utc) - timedelta(days=2)

            for news_item_api_data in fetched_news_items_api:
                if newly_analyzed_count_this_run >= count_to_analyze_this_run:
                    logger.info(
                        f"Reached analysis limit of {count_to_analyze_this_run} new/re-analyzed items for this run.");
                    break
                try:
                    news_event_db = self._get_or_create_news_event(news_item_api_data)
                    if not news_event_db: logger.warning(
                        f"Skipping news item (could not get/create in DB): {news_item_api_data.get('headline')}"); continue
                    news_event_db = self._ensure_news_event_is_bound(news_event_db)
                    if not news_event_db: continue

                    analysis_needed, latest_analysis = False, None
                    if news_event_db.analyses:  # Relationship is loaded by _ensure_news_event_is_bound if merged
                        sorted_analyses = sorted(news_event_db.analyses, key=lambda x: x.analysis_date, reverse=True)
                        if sorted_analyses: latest_analysis = sorted_analyses[0]

                    if not latest_analysis:
                        analysis_needed = True; logger.info(
                            f"News '{news_event_db.event_title[:50]}...' requires new analysis.")
                    elif latest_analysis.analysis_date < reanalyze_threshold_date:
                        analysis_needed = True; logger.info(
                            f"News '{news_event_db.event_title[:50]}...' requires re-analysis (older than {reanalyze_older_than_days} days).")
                    elif news_event_db.full_article_text and latest_analysis and (
                            not latest_analysis.key_news_snippets or "full article" not in latest_analysis.key_news_snippets.get(
                            "source_type_used", "")):
                        analysis_needed = True;
                        logger.info(
                            f"News '{news_event_db.event_title[:50]}...' re-analyzing with newly available/confirmed full text.")
                    else:
                        logger.info(
                            f"News '{news_event_db.event_title[:50]}...' already recently analyzed with available text. Skipping.")

                    if analysis_needed:
                        analysis_result = self.analyze_single_news_item(news_event_db)
                        if analysis_result: analyzed_news_results.append(
                            analysis_result); newly_analyzed_count_this_run += 1
                        time.sleep(3)
                except Exception as e:
                    logger.error(f"Failed to process or analyze news item '{news_item_api_data.get('headline')}': {e}",
                                 exc_info=True)
                    self._ensure_news_event_session_active(
                        news_item_api_data.get('headline'))  # Ensure session is active for next item
                    if self.db_session: self.db_session.rollback()  # Rollback any partial transaction for this item
            logger.info(
                f"News analysis pipeline completed. Newly analyzed/re-analyzed {newly_analyzed_count_this_run} items.")
            return analyzed_news_results
        finally:
            self._close_session_if_active()  # Close session at the end of the pipeline run


if __name__ == '__main__':
    from database import init_db

    # init_db()
    logger.info("Starting standalone news analysis pipeline test...")
    analyzer = NewsAnalyzer()
    results = analyzer.run_news_analysis_pipeline(category="general", count_to_fetch_from_api=10,
                                                  count_to_analyze_this_run=3)
    if results:
        logger.info(f"Pipeline processed {len(results)} news items this run.")
        for res_idx, res in enumerate(results):
            if hasattr(res, 'news_event') and res.news_event:
                logger.info(f"--- Result {res_idx + 1} ---")
                logger.info(f"News: {res.news_event.event_title[:100]}...")
                logger.info(f"Source: {res.news_event.source_url}")
                logger.info(f"Sentiment: {res.sentiment} - Reasoning: {res.sentiment_reasoning[:100]}...")
                logger.info(f"Investor Summary: {res.summary_for_email}")
                logger.info(f"Full Article Scraped: {'Yes' if res.news_event.full_article_text else 'No'}")
                if res.news_event.full_article_text: logger.debug(
                    f"Full Article Snippet: {res.news_event.full_article_text[:200]}...")
            else:
                logger.warning(f"Processed news result item missing 'news_event' or news_event is None. Result: {res}")
    else:
        logger.info("No new news items were processed in this run.")