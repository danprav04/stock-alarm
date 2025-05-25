# services/stock_analyzer/helpers.py
import math
from core.logging_setup import logger

def safe_get_float(data_dict, key, default=None):
    if data_dict is None or not isinstance(data_dict, dict): return default
    val = data_dict.get(key)
    if val is None or val == "None" or val == "" or str(val).lower() == "n/a" or str(val).lower() == "-": return default
    try: return float(val)
    except (ValueError, TypeError): return default

def calculate_cagr(end_value, start_value, years):
    if start_value is None or end_value is None or not isinstance(years, (int, float)) or years <= 0: return None
    if start_value == 0: return None
    if (start_value < 0 and end_value > 0) or (start_value > 0 and end_value < 0): return None
    if start_value < 0 and end_value < 0: return None # Or handle appropriately if CAGR for negative values is desired
    if end_value == 0 and start_value > 0: return -1.0 # Total loss
    try:
        return ((float(end_value) / float(start_value)) ** (1 / float(years))) - 1
    except (ValueError, TypeError, ZeroDivisionError): # ZeroDivisionError for years = 0, though already checked
        return None


def calculate_growth(current_value, previous_value):
    if previous_value is None or current_value is None: return None
    try:
        current_value_float = float(current_value)
        previous_value_float = float(previous_value)
        if previous_value_float == 0:
            return None if current_value_float == 0 else (float('inf') if current_value_float > 0 else float('-inf'))
        return (current_value_float - previous_value_float) / abs(previous_value_float)
    except (ValueError, TypeError):
        return None

def get_value_from_statement_list(data_list, field, year_offset=0, report_date_for_log=None):
    if data_list and isinstance(data_list, list) and len(data_list) > year_offset:
        report = data_list[year_offset]
        if report and isinstance(report, dict):
            val = safe_get_float(report, field)
            # Optional: detailed logging if value is None
            # if val is None:
            #     date_info = report_date_for_log or report.get('date', 'Unknown Date')
            #     logger.debug(f"Field '{field}' not found or invalid in report for {date_info} (offset {year_offset}).")
            return val
    return None

def get_finnhub_concept_value(finnhub_quarterly_reports_data, report_section_key, concept_names_list, quarter_offset=0):
    if not finnhub_quarterly_reports_data or len(finnhub_quarterly_reports_data) <= quarter_offset: return None
    report_data = finnhub_quarterly_reports_data[quarter_offset]
    if 'report' not in report_data or report_section_key not in report_data['report']: return None
    section_items = report_data['report'][report_section_key]
    if not section_items: return None
    for item in section_items:
        if item.get('concept') in concept_names_list or item.get('label') in concept_names_list:
            return safe_get_float(item, 'value')
    return None

def get_alphavantage_value(av_quarterly_reports, field_name, quarter_offset_from_latest=0):
    if not av_quarterly_reports or len(av_quarterly_reports) <= quarter_offset_from_latest: return None
    report = av_quarterly_reports[quarter_offset_from_latest]
    return safe_get_float(report, field_name)

def get_fmp_value(fmp_quarterly_reports, field_name, quarter_offset_from_latest=0):
    if not fmp_quarterly_reports or len(fmp_quarterly_reports) <= quarter_offset_from_latest: return None
    report = fmp_quarterly_reports[quarter_offset_from_latest]
    return safe_get_float(report, field_name)