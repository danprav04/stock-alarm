# services/ipo_analyzer/db_handler.py
import time
from database import IPO
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError


def get_or_create_ipo_db_entry(analyzer_instance, db_session, ipo_data_from_fetch):
    """Gets an existing IPO entry from the DB or creates a new one."""
    ipo_db_entry = None

    # Try to find by symbol first, as it's more likely to be unique if present
    if ipo_data_from_fetch.get("symbol"):
        ipo_db_entry = db_session.query(IPO).filter(IPO.symbol == ipo_data_from_fetch["symbol"]).first()

    # If not found by symbol, try by company name and IPO date string (original date string)
    if not ipo_db_entry and ipo_data_from_fetch.get("company_name") and ipo_data_from_fetch.get("ipo_date_str"):
        ipo_db_entry = db_session.query(IPO).filter(
            IPO.company_name == ipo_data_from_fetch["company_name"],
            IPO.ipo_date_str == ipo_data_from_fetch["ipo_date_str"]  # Match on the original string
        ).first()

    # Try to get CIK if not already available
    cik_to_store = ipo_data_from_fetch.get("cik")  # CIK might come from data fetch
    if not cik_to_store and ipo_data_from_fetch.get("symbol"):
        # If no CIK from fetched data, try to get it using symbol
        cik_to_store = analyzer_instance.sec_edgar.get_cik_by_ticker(ipo_data_from_fetch["symbol"])
        time.sleep(0.5)  # SEC EDGAR rate limiting
    elif not cik_to_store and ipo_db_entry and ipo_db_entry.symbol and not ipo_db_entry.cik:
        # If DB entry exists but has no CIK, try to get it
        cik_to_store = analyzer_instance.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol)
        time.sleep(0.5)

    if not ipo_db_entry:
        logger.info(f"IPO '{ipo_data_from_fetch.get('company_name')}' not found in DB, creating new entry.")
        ipo_db_entry = IPO(
            company_name=ipo_data_from_fetch.get("company_name"),
            symbol=ipo_data_from_fetch.get("symbol"),
            ipo_date_str=ipo_data_from_fetch.get("ipo_date_str"),
            ipo_date=ipo_data_from_fetch.get("ipo_date"),  # Parsed date
            expected_price_range_low=ipo_data_from_fetch.get("expected_price_range_low"),
            expected_price_range_high=ipo_data_from_fetch.get("expected_price_range_high"),
            offered_shares=ipo_data_from_fetch.get("offered_shares"),
            total_shares_value=ipo_data_from_fetch.get("total_shares_value"),
            exchange=ipo_data_from_fetch.get("exchange"),
            status=ipo_data_from_fetch.get("status"),
            cik=cik_to_store
        )
        db_session.add(ipo_db_entry)
        try:
            db_session.commit()
            db_session.refresh(ipo_db_entry)  # To get ID and other defaults
            logger.info(
                f"Created IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}, CIK: {ipo_db_entry.cik})")
        except SQLAlchemyError as e:
            db_session.rollback()
            logger.error(f"Error creating IPO DB entry for '{ipo_data_from_fetch.get('company_name')}': {e}",
                         exc_info=True)
            return None  # Return None if creation fails
    else:
        # IPO entry exists, update it if necessary
        updated = False
        fields_to_update = [
            "company_name", "symbol", "ipo_date_str", "ipo_date",
            "expected_price_range_low", "expected_price_range_high",
            "offered_shares", "total_shares_value", "exchange", "status"
        ]
        for field in fields_to_update:
            new_val = ipo_data_from_fetch.get(field)
            # Check if new_val is not None to avoid overwriting existing data with None
            # Also check if the value has actually changed
            if new_val is not None and getattr(ipo_db_entry, field) != new_val:
                setattr(ipo_db_entry, field, new_val)
                updated = True

        # Update CIK if a new one was found and it's different or was missing
        if cik_to_store and (ipo_db_entry.cik != cik_to_store or not ipo_db_entry.cik):
            ipo_db_entry.cik = cik_to_store
            updated = True

        if updated:
            try:
                db_session.commit()
                db_session.refresh(ipo_db_entry)
                logger.info(f"Updated IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}).")
            except SQLAlchemyError as e:
                db_session.rollback()
                logger.error(f"Error updating IPO DB entry for '{ipo_db_entry.company_name}': {e}", exc_info=True)
                # Potentially return the existing, un-updated entry or None depending on desired behavior

    return ipo_db_entry