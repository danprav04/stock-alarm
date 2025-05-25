# services/stock_analyzer/data_fetcher.py
import time
from core.logging_setup import logger
from core.config import STOCK_FINANCIAL_YEARS

def fetch_financial_statements_data(analyzer_instance):
    """Fetches all necessary financial statements and stores them in analyzer_instance._financial_data_cache."""
    ticker = analyzer_instance.ticker
    logger.info(f"Fetching financial statements for {ticker}...")
    statements_cache = {
        "fmp_income_annual": [], "fmp_balance_annual": [], "fmp_cashflow_annual": [],
        "fmp_income_quarterly": [],
        "finnhub_financials_quarterly_reported": {"data": []},
        "alphavantage_income_quarterly": {"quarterlyReports": []},
        "alphavantage_balance_quarterly": {"quarterlyReports": []},
        "alphavantage_cashflow_quarterly": {"quarterlyReports": []}
    }
    try:
        # FMP Annuals
        statements_cache["fmp_income_annual"] = analyzer_instance.fmp.get_financial_statements(ticker, "income-statement", "annual", STOCK_FINANCIAL_YEARS) or []
        time.sleep(1.5)
        statements_cache["fmp_balance_annual"] = analyzer_instance.fmp.get_financial_statements(ticker, "balance-sheet-statement", "annual", STOCK_FINANCIAL_YEARS) or []
        time.sleep(1.5)
        statements_cache["fmp_cashflow_annual"] = analyzer_instance.fmp.get_financial_statements(ticker, "cash-flow-statement", "annual", STOCK_FINANCIAL_YEARS) or []
        time.sleep(1.5)
        logger.info(f"FMP Annuals for {ticker}: Income({len(statements_cache['fmp_income_annual'])}), Balance({len(statements_cache['fmp_balance_annual'])}), Cashflow({len(statements_cache['fmp_cashflow_annual'])}).")

        # FMP Quarterlies
        statements_cache["fmp_income_quarterly"] = analyzer_instance.fmp.get_financial_statements(ticker, "income-statement", "quarter", 8) or []
        time.sleep(1.5)
        logger.info(f"FMP Quarterly Income for {ticker}: {len(statements_cache['fmp_income_quarterly'])} records.")

        # Finnhub Quarterlies
        fh_q_data = analyzer_instance.finnhub.get_financials_reported(ticker, freq="quarterly", count=8)
        time.sleep(1.5)
        if fh_q_data and isinstance(fh_q_data, dict) and fh_q_data.get("data"):
            statements_cache["finnhub_financials_quarterly_reported"] = fh_q_data
            logger.info(f"Fetched {len(fh_q_data['data'])} quarterly reports from Finnhub for {ticker}.")
        else:
            logger.warning(f"Finnhub quarterly financials reported data missing or malformed for {ticker}.")

        # Alpha Vantage Quarterlies
        av_income_q = analyzer_instance.alphavantage.get_income_statement_quarterly(ticker)
        time.sleep(15) # Alpha Vantage free tier has strict rate limits
        if av_income_q and isinstance(av_income_q, dict) and av_income_q.get("quarterlyReports"):
            statements_cache["alphavantage_income_quarterly"] = av_income_q
            logger.info(f"Fetched {len(av_income_q['quarterlyReports'])} quarterly income reports from Alpha Vantage for {ticker}.")
        else:
            logger.warning(f"Alpha Vantage quarterly income reports missing or malformed for {ticker}.")

        av_balance_q = analyzer_instance.alphavantage.get_balance_sheet_quarterly(ticker)
        time.sleep(15)
        if av_balance_q and isinstance(av_balance_q, dict) and av_balance_q.get("quarterlyReports"):
            statements_cache["alphavantage_balance_quarterly"] = av_balance_q
            logger.info(f"Fetched {len(av_balance_q['quarterlyReports'])} quarterly balance reports from Alpha Vantage for {ticker}.")
        else:
            logger.warning(f"Alpha Vantage quarterly balance reports missing or malformed for {ticker}.")

        av_cashflow_q = analyzer_instance.alphavantage.get_cash_flow_quarterly(ticker)
        time.sleep(15)
        if av_cashflow_q and isinstance(av_cashflow_q, dict) and av_cashflow_q.get("quarterlyReports"):
            statements_cache["alphavantage_cashflow_quarterly"] = av_cashflow_q
            logger.info(f"Fetched {len(av_cashflow_q['quarterlyReports'])} quarterly cash flow reports from Alpha Vantage for {ticker}.")
        else:
            logger.warning(f"Alpha Vantage quarterly cash flow reports missing or malformed for {ticker}.")

    except Exception as e:
        logger.warning(f"Error during financial statements fetch for {ticker}: {e}.", exc_info=True)

    analyzer_instance._financial_data_cache['financial_statements'] = statements_cache


def fetch_key_metrics_and_profile_data(analyzer_instance):
    """Fetches key metrics and profile data, storing them in analyzer_instance._financial_data_cache."""
    ticker = analyzer_instance.ticker
    logger.info(f"Fetching key metrics and profile for {ticker}.")

    # FMP Key Metrics (Annual & Quarterly)
    analyzer_instance._financial_data_cache['key_metrics_annual_fmp'] = analyzer_instance.fmp.get_key_metrics(ticker, "annual", STOCK_FINANCIAL_YEARS + 2) or []
    time.sleep(1.5)
    key_metrics_quarterly_fmp = analyzer_instance.fmp.get_key_metrics(ticker, "quarterly", 8)
    time.sleep(1.5)
    analyzer_instance._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp if key_metrics_quarterly_fmp is not None else []

    # Finnhub Basic Financials
    analyzer_instance._financial_data_cache['basic_financials_finnhub'] = analyzer_instance.finnhub.get_basic_financials(ticker) or {}
    time.sleep(1.5)

    # FMP Profile (if not already fetched during _get_or_create_stock_entry)
    if 'profile_fmp' not in analyzer_instance._financial_data_cache or not analyzer_instance._financial_data_cache.get('profile_fmp'):
        profile_fmp_list = analyzer_instance.fmp.get_company_profile(ticker)
        time.sleep(1.5)
        analyzer_instance._financial_data_cache['profile_fmp'] = profile_fmp_list[0] if profile_fmp_list and isinstance(profile_fmp_list, list) and profile_fmp_list[0] else {}

    logger.info(f"FMP KM Annual for {ticker}: {len(analyzer_instance._financial_data_cache['key_metrics_annual_fmp'])}. "
                f"FMP KM Quarterly for {ticker}: {len(analyzer_instance._financial_data_cache['key_metrics_quarterly_fmp'])}. "
                f"Finnhub Basic Financials for {ticker}: {'OK' if analyzer_instance._financial_data_cache.get('basic_financials_finnhub', {}).get('metric') else 'Data missing'}.")