# services/ipo_analyzer/helpers.py
from dateutil import parser as date_parser
from core.logging_setup import logger

def parse_ipo_date_string(date_str):
    if not date_str:
        return None
    try:
        # date_parser is quite flexible
        return date_parser.parse(date_str).date()
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not parse IPO date string '{date_str}': {e}")
        return None