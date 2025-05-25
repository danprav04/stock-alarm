# services/news_analyzer/db_handler.py
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone
from database import NewsEvent
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError
from .data_fetcher import scrape_news_article_content  # For scraping if needed during get/create


def _ensure_news_event_session_is_active(analyzer_instance, news_identifier_for_log="Unknown News"):
    """Ensures the NewsAnalyzer's DB session is active, re-establishing if necessary."""
    if not analyzer_instance.db_session or not analyzer_instance.db_session.is_active:
        logger.warning(f"Session for News '{news_identifier_for_log}' was inactive/closed. Re-establishing.")
        analyzer_instance._close_session_if_active()  # Call the NewsAnalyzer's close method
        analyzer_instance.db_session = next(
            analyzer_instance.get_new_db_session_generator())  # Get a fresh session via NewsAnalyzer's method
        logger.info(f"Re-established DB session for NewsAnalyzer processing '{news_identifier_for_log}'.")


def _ensure_news_event_is_bound_to_session(analyzer_instance, news_event_db_obj):
    """Ensures a NewsEvent DB object is bound to the NewsAnalyzer's current session."""
    if not news_event_db_obj:
        return None

    news_title_for_log = news_event_db_obj.event_title[:50] if news_event_db_obj.event_title else 'Unknown News'
    _ensure_news_event_session_is_active(analyzer_instance, news_title_for_log)

    instance_state = sa_inspect(news_event_db_obj)

    # If object is not in any session OR in a different session, merge it.
    if not instance_state.session or instance_state.session is not analyzer_instance.db_session:
        obj_id_log = news_event_db_obj.id if instance_state.has_identity else 'Transient'
        logger.warning(
            f"NewsEvent DB entry '{news_title_for_log}...' (ID: {obj_id_log}) not bound to current session. Merging."
        )
        try:
            # If it's a new object not yet persisted (no ID) but we found an existing one by URL, use that one.
            if not instance_state.has_identity and news_event_db_obj.id is None and news_event_db_obj.source_url:
                existing_in_session_by_url = analyzer_instance.db_session.query(NewsEvent).filter_by(
                    source_url=news_event_db_obj.source_url).first()
                if existing_in_session_by_url:
                    logger.info(
                        f"Replaced transient NewsEvent for '{news_event_db_obj.source_url}' with instance (ID: {existing_in_session_by_url.id}) from session during binding."
                    )
                    return existing_in_session_by_url  # Return the one already in session

            # Proceed with merge for objects with identity or new objects not found by URL
            merged_event = analyzer_instance.db_session.merge(news_event_db_obj)
            logger.info(
                f"Successfully merged NewsEvent '{merged_event.event_title[:50]}...' (ID: {merged_event.id}) into session."
            )
            return merged_event
        except Exception as e_merge:
            logger.error(
                f"Failed to merge NewsEvent '{news_title_for_log}...' into session: {e_merge}. Attempting re-fetch.",
                exc_info=True
            )
            # Fallback: Try to re-fetch the object using its ID or a unique key if merge fails
            fallback_event = None
            if instance_state.has_identity and news_event_db_obj.id:  # If it had an ID
                fallback_event = analyzer_instance.db_session.query(NewsEvent).get(news_event_db_obj.id)
            elif news_event_db_obj.source_url:  # If it had a URL (unique constraint)
                fallback_event = analyzer_instance.db_session.query(NewsEvent).filter_by(
                    source_url=news_event_db_obj.source_url).first()

            if not fallback_event:
                logger.critical(
                    f"CRITICAL: Failed to re-associate NewsEvent '{news_title_for_log}...' with session after merge failure and re-fetch attempt.");
                return None  # Critical failure
            logger.info(
                f"Successfully re-fetched NewsEvent '{fallback_event.event_title[:50]}...' (ID: {fallback_event.id}) into session after merge failure."
            )
            return fallback_event

    return news_event_db_obj  # Object is already bound correctly


def get_or_create_news_event_db_entry(analyzer_instance, news_item_from_api):
    """
    Gets an existing NewsEvent from the DB or creates a new one.
    Manages scraping of full article text if not already present.
    Uses the NewsAnalyzer's managed db_session.
    """
    session_log_id = news_item_from_api.get('headline', 'Unknown News API Item')
    _ensure_news_event_session_is_active(analyzer_instance, session_log_id)

    source_url = news_item_from_api.get("url")
    if not source_url:
        logger.warning(f"News item missing URL, cannot process: {news_item_from_api.get('headline')}")
        return None

    event = analyzer_instance.db_session.query(NewsEvent).filter_by(source_url=source_url).first()

    full_article_text_scraped_this_time = None
    # Scrape if event doesn't exist, or if it exists but has no full_article_text
    if not event or (event and not event.full_article_text):
        full_article_text_scraped_this_time = scrape_news_article_content(source_url)

    current_time_utc = datetime.now(timezone.utc)
    if event:
        logger.debug(f"News event '{event.event_title[:70]}...' (URL: {source_url}) already in DB.")
        # If we scraped text now and the existing event didn't have it, update the event
        if full_article_text_scraped_this_time and not event.full_article_text:
            logger.info(f"Updating existing event {event.id} with newly scraped full article text.")
            event.full_article_text = full_article_text_scraped_this_time
            event.processed_date = current_time_utc  # Update processed date as we added content
            try:
                analyzer_instance.db_session.commit()
            except SQLAlchemyError as e:
                analyzer_instance.db_session.rollback()
                logger.error(f"Error updating full_article_text for existing event {source_url}: {e}")
        return event  # Return existing event (possibly updated)

    # If event does not exist, create a new one
    event_timestamp = news_item_from_api.get("datetime")  # Finnhub provides UNIX timestamp
    event_datetime_utc = datetime.fromtimestamp(event_timestamp, timezone.utc) if event_timestamp else current_time_utc

    new_event = NewsEvent(
        event_title=news_item_from_api.get("headline"),
        event_date=event_datetime_utc,
        source_url=source_url,
        source_name=news_item_from_api.get("source"),
        category=news_item_from_api.get("category"),
        full_article_text=full_article_text_scraped_this_time,  # Use newly scraped text
        processed_date=current_time_utc
    )
    analyzer_instance.db_session.add(new_event)
    try:
        analyzer_instance.db_session.commit()
        analyzer_instance.db_session.refresh(new_event)  # Get ID and other defaults
        logger.info(f"Stored new news event: {new_event.event_title[:70]}... (ID: {new_event.id})")
        return new_event
    except SQLAlchemyError as e:
        analyzer_instance.db_session.rollback()
        logger.error(f"Database error storing new news event '{news_item_from_api.get('headline')}': {e}",
                     exc_info=True)
        # Attempt to find if it was created by a concurrent process due to unique constraint
        # This can happen if pipeline runs in parallel or if error handling is complex
        existing_after_error = analyzer_instance.db_session.query(NewsEvent).filter_by(source_url=source_url).first()
        if existing_after_error:
            logger.warning(
                f"Found existing event for {source_url} after commit error, likely due to race condition. Using existing.")
            return existing_after_error
        return None