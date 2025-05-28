# services/ipo_analyzer/data_fetcher.py
import time
from datetime import datetime, timedelta, timezone
from core.logging_setup import logger
from .helpers import parse_ipo_date_string
from sqlalchemy.exc import SQLAlchemyError


def fetch_upcoming_ipo_data(analyzer_instance):
    """Fetches upcoming IPO data from Finnhub."""
    logger.info("Fetching upcoming IPOs using Finnhub...")
    ipos_data_to_process = []
    today = datetime.now(timezone.utc)
    # Look back 60 days and forward 180 days for IPOs
    from_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
    to_date = (today + timedelta(days=180)).strftime('%Y-%m-%d')

    finnhub_response = analyzer_instance.finnhub.get_ipo_calendar(from_date=from_date, to_date=to_date)
    actual_ipo_list = []

    if finnhub_response and isinstance(finnhub_response, dict) and "ipoCalendar" in finnhub_response:
        actual_ipo_list = finnhub_response["ipoCalendar"]
        if not isinstance(actual_ipo_list, list):
            logger.warning(f"Finnhub response 'ipoCalendar' field is not a list. Found: {type(actual_ipo_list)}")
            actual_ipo_list = []  # Reset to empty list if not a list
        elif not actual_ipo_list:
            logger.info("Finnhub 'ipoCalendar' list is empty for the current period.")
    elif finnhub_response is None:  # Explicit check for None, indicating API failure handled by base_client
        logger.error("Failed to fetch IPOs from Finnhub (API call failed or returned None).")
    else:  # Other unexpected formats
        logger.info(f"No IPOs found or unexpected format from Finnhub. Response: {str(finnhub_response)[:200]}")

    if actual_ipo_list:  # Ensure it's a list and has items
        for ipo_api_data in actual_ipo_list:
            if not isinstance(ipo_api_data, dict):  # Skip if an item is not a dictionary
                logger.warning(f"Skipping non-dictionary item in Finnhub IPO calendar: {ipo_api_data}")
                continue

            price_range_raw = ipo_api_data.get("price")
            price_low, price_high = None, None
            if isinstance(price_range_raw, str) and price_range_raw.strip():  # e.g., "10.0-12.0" or "15.0"
                parts = price_range_raw.split('-', 1)
                try:
                    price_low = float(parts[0].strip())
                except:
                    pass  # pylint: disable=bare-except
                try:
                    price_high = float(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else price_low
                except:
                    price_high = price_low if price_low is not None else None  # pylint: disable=bare-except
            elif isinstance(price_range_raw, (float, int)):  # If it's just a number
                price_low = float(price_range_raw)
                price_high = float(price_range_raw)

            parsed_date = parse_ipo_date_string(ipo_api_data.get("date"))

            ipos_data_to_process.append({
                "company_name": ipo_api_data.get("name"),
                "symbol": ipo_api_data.get("symbol"),
                "ipo_date_str": ipo_api_data.get("date"),  # Original string for DB
                "ipo_date": parsed_date,  # Parsed date object
                "expected_price_range_low": price_low,
                "expected_price_range_high": price_high,
                "exchange": ipo_api_data.get("exchange"),
                "status": ipo_api_data.get("status"),
                "offered_shares": ipo_api_data.get("numberOfShares"),  # Finnhub field name
                "total_shares_value": ipo_api_data.get("totalSharesValue"),  # Finnhub field name
                "source_api": "Finnhub",  # To track where the data came from
                "raw_data": ipo_api_data  # Store the raw dict for snapshot
            })
        logger.info(f"Successfully parsed {len(ipos_data_to_process)} IPOs from Finnhub API response.")

    # Deduplicate IPOs based on a composite key (name, symbol, date string)
    # This is important if multiple sources are ever combined or if an API returns duplicates
    unique_ipos = []
    seen_keys = set()
    for ipo_info in ipos_data_to_process:
        # Normalize key parts for better matching
        key_name = ipo_info.get("company_name", "").strip().lower() if ipo_info.get(
            "company_name") else "unknown_company"
        key_symbol = ipo_info.get("symbol", "").strip().upper() if ipo_info.get(
            "symbol") else "NO_SYMBOL"  # Handle missing symbols
        key_date = ipo_info.get("ipo_date_str", "")  # Use original date string for uniqueness

        unique_tuple = (key_name, key_symbol, key_date)
        if unique_tuple not in seen_keys:
            unique_ipos.append(ipo_info)
            seen_keys.add(unique_tuple)

    logger.info(f"Total unique IPOs fetched after deduplication: {len(unique_ipos)}")
    return unique_ipos


def fetch_s1_filing_data(analyzer_instance, db_session, ipo_db_entry):
    """Fetches S-1 filing text for a given IPO DB entry."""
    if not ipo_db_entry:
        return None, None

    target_cik = ipo_db_entry.cik

    # If CIK is not in the DB entry, try to get it using the symbol
    if not target_cik:
        if ipo_db_entry.symbol:
            target_cik = analyzer_instance.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol)
            time.sleep(0.5)  # SEC EDGAR rate limiting
            if target_cik:
                ipo_db_entry.cik = target_cik  # Update DB entry with found CIK
                try:
                    db_session.commit()
                except SQLAlchemyError as e:  # Catch potential commit errors
                    db_session.rollback()
                    logger.error(f"Failed to update CIK for {ipo_db_entry.company_name}: {e}")
            else:
                logger.warning(f"No CIK found via symbol {ipo_db_entry.symbol} for IPO '{ipo_db_entry.company_name}'.")
                return None, None  # Cannot proceed without CIK
        else:
            logger.warning(f"No CIK or symbol available for IPO '{ipo_db_entry.company_name}'. Cannot fetch S-1.")
            return None, None  # Cannot proceed

    logger.info(f"Attempting to fetch S-1/F-1 for {ipo_db_entry.company_name} (CIK: {target_cik})")
    s1_url = None
    # Try common S-1 and F-1 forms (including amendments)
    for form_type in ["S-1", "S-1/A", "F-1", "F-1/A"]:
        s1_url = analyzer_instance.sec_edgar.get_filing_document_url(cik=target_cik, form_type=form_type)
        time.sleep(0.5)  # SEC EDGAR rate limiting
        if s1_url:
            logger.info(f"Found {form_type} URL for {ipo_db_entry.company_name}: {s1_url}")
            break

    if s1_url:
        # Update the s1_filing_url in the database if it's new or different
        if ipo_db_entry.s1_filing_url != s1_url:
            ipo_db_entry.s1_filing_url = s1_url
            try:
                db_session.commit()
            except SQLAlchemyError as e:
                db_session.rollback()
                logger.warning(f"Failed to update S1 filing URL for {ipo_db_entry.company_name} due to: {e}")

        filing_text = analyzer_instance.sec_edgar.get_filing_text(s1_url)
        if filing_text:
            logger.info(f"Fetched S-1/F-1 text (length: {len(filing_text)}) for {ipo_db_entry.company_name}")
            return filing_text, s1_url
        else:
            logger.warning(f"Failed to fetch S-1/F-1 text from {s1_url}")
    else:
        logger.warning(f"No S-1 or F-1 URL found for {ipo_db_entry.company_name} (CIK: {target_cik}).")

    return None, None  # Return None if no text or URL found