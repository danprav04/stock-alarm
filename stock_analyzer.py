# stock_analyzer.py
import pandas as pd
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone, timedelta
import math  # For DCF calculations
import time  # For API courtesy delays
import warnings  # For filtering warnings
from bs4 import XMLParsedAsHTMLWarning  # For filtering specific BS4 warning
import re  # For parsing AI response

# Filter the XMLParsedAsHTMLWarning from BeautifulSoup
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from api_clients import (
    FinnhubClient, FinancialModelingPrepClient, AlphaVantageClient,
    EODHDClient, GeminiAPIClient, SECEDGARClient, extract_S1_text_sections
)
from database import SessionLocal, get_db_session
from models import Stock, StockAnalysis
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError
from config import (
    STOCK_FINANCIAL_YEARS, DEFAULT_DISCOUNT_RATE,
    DEFAULT_PERPETUAL_GROWTH_RATE, DEFAULT_FCF_PROJECTION_YEARS,
    TEN_K_KEY_SECTIONS, MAX_10K_SECTION_LENGTH_FOR_GEMINI,
    MAX_GEMINI_TEXT_LENGTH
)


# Helper function to safely get a numeric value from a dictionary
def safe_get_float(data_dict, key, default=None):
    val = data_dict.get(key)
    if val is None or val == "None" or val == "": return default  # Added empty string check
    try:
        return float(val)
    except (ValueError, TypeError):
        # logger.debug(f"safe_get_float: Could not convert key '{key}' value '{val}' to float. Returning default.")
        return default


# Helper function for CAGR calculation
def calculate_cagr(end_value, start_value, years):
    if start_value is None or end_value is None or not isinstance(years, (int, float)) or years <= 0: return None
    if start_value == 0: return None  # Avoid division by zero; growth is infinite or undefined
    # Handle negative values carefully: CAGR is typically for positive values.
    # If both are negative, it can be calculated but interpretation is complex.
    # If one is negative and other positive, CAGR is usually not meaningful.
    if start_value < 0 and end_value < 0:  # Both negative
        # Treat as positive for calculation, then adjust sign if needed (though CAGR is a rate)
        # This specific formula handles common cases for financial CAGR
        return ((float(end_value) / float(start_value)) ** (1 / float(years))) - 1
    if start_value < 0 or end_value < 0:  # One is negative, other positive/zero
        return None
    if end_value == 0 and start_value > 0:  # Value dropped to zero
        return -1.0  # Represents a 100% loss
    return ((float(end_value) / float(start_value)) ** (1 / float(years))) - 1


# Helper function for simple growth (YoY, QoQ)
def calculate_growth(current_value, previous_value):
    if previous_value is None or current_value is None: return None
    if float(previous_value) == 0:
        return None  # Or handle as infinite growth if current_value is positive
    try:
        return (float(current_value) - float(previous_value)) / abs(float(previous_value))
    except (ValueError, TypeError):
        return None


# Helper function to get value from a list of statement dicts (FMP style)
def get_value_from_statement_list(data_list, field, year_offset=0, report_date_for_log=None):
    if data_list and isinstance(data_list, list) and len(data_list) > year_offset:
        report = data_list[year_offset]
        if report and isinstance(report, dict):
            val = safe_get_float(report, field)
            if val is None:
                date_info = report_date_for_log or report.get('date', 'Unknown Date')
                # logger.debug(f"Field '{field}' not found or is None in FMP statement list for offset {year_offset} (Date: {date_info}).")
            return val
    # logger.debug(f"Could not get FMP statement field '{field}' for offset {year_offset}. Data list length {len(data_list) if data_list else 'N/A'}.")
    return None


# Helper function to get a specific concept value from Finnhub's reported financials
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


# Helper function to get value from Alpha Vantage quarterly reports (list sorted oldest to newest)
def get_alphavantage_value(av_quarterly_reports, field_name, quarter_offset_from_latest=0):
    if not av_quarterly_reports or len(av_quarterly_reports) <= quarter_offset_from_latest:
        return None
    report_index = len(av_quarterly_reports) - 1 - quarter_offset_from_latest
    if report_index < 0: return None
    report = av_quarterly_reports[report_index]
    return safe_get_float(report, field_name)


class StockAnalyzer:
    def __init__(self, ticker):
        self.ticker = ticker.upper()
        self.finnhub = FinnhubClient()
        self.fmp = FinancialModelingPrepClient()
        self.alphavantage = AlphaVantageClient()
        self.eodhd = EODHDClient()
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()

        self.db_session = next(get_db_session())
        self.stock_db_entry = None
        self._financial_data_cache = {}  # For storing fetched data during the analysis of one stock

        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"CRITICAL: Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            self._close_session_if_active()
            # Re-raise as a more specific error to indicate initialization failure
            raise RuntimeError(
                f"StockAnalyzer for {self.ticker} could not be initialized due to DB/API issues during stock entry setup.") from e

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close();
                logger.debug(f"DB session closed for {self.ticker}.")
            except Exception as e_close:
                logger.warning(f"Error closing session for {self.ticker}: {e_close}")

    def _get_or_create_stock_entry(self):
        if not self.db_session.is_active:  # Ensure session is active
            logger.warning(f"Session for {self.ticker} inactive in _get_or_create. Re-establishing.")
            self._close_session_if_active();  # Close stale session if any
            self.db_session = next(get_db_session())  # Get a fresh one

        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()
        company_name, industry, sector, cik = None, None, None, None

        # Try FMP first for profile
        profile_fmp_list = self.fmp.get_company_profile(self.ticker);
        time.sleep(1)  # API courtesy
        if profile_fmp_list and isinstance(profile_fmp_list, list) and len(profile_fmp_list) > 0 and profile_fmp_list[
            0]:
            data = profile_fmp_list[0];
            self._financial_data_cache['profile_fmp'] = data
            company_name = data.get('companyName')
            industry = data.get('industry')
            sector = data.get('sector')
            cik = data.get('cik')  # FMP often provides CIK
            logger.info(f"Fetched profile from FMP for {self.ticker}.")
        else:
            logger.warning(f"FMP profile failed for {self.ticker}. Trying Finnhub.")
            profile_fh = self.finnhub.get_company_profile2(self.ticker);
            time.sleep(1)
            if profile_fh:
                self._financial_data_cache['profile_finnhub'] = profile_fh
                company_name = profile_fh.get('name')
                industry = profile_fh.get('finnhubIndustry')  # Finnhub has its own industry classification
                # Finnhub profile2 doesn't usually have sector or CIK directly
                logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Finnhub profile failed for {self.ticker}. Trying Alpha Vantage Overview.")
                overview_av = self.alphavantage.get_company_overview(self.ticker);
                time.sleep(2)  # AV is slower
                if overview_av and overview_av.get("Symbol") == self.ticker:  # Check if response is valid
                    self._financial_data_cache['overview_alphavantage'] = overview_av
                    company_name = overview_av.get('Name')
                    industry = overview_av.get('Industry')
                    sector = overview_av.get('Sector')
                    cik = overview_av.get('CIK')  # Alpha Vantage sometimes has CIK
                    logger.info(f"Fetched overview from Alpha Vantage for {self.ticker}.")
                else:
                    logger.warning(f"All primary profile fetches (FMP, Finnhub, AV) failed for {self.ticker}.")

        if not company_name: company_name = self.ticker  # Fallback company name
        if not cik and self.ticker:  # If CIK still not found from any profile source
            logger.info(f"CIK not found from profiles for {self.ticker}. Querying SEC EDGAR.")
            cik_from_edgar = self.sec_edgar.get_cik_by_ticker(self.ticker);
            time.sleep(0.5)
            if cik_from_edgar:
                cik = cik_from_edgar
                logger.info(f"Fetched CIK {cik} from SEC EDGAR for {self.ticker}.")
            else:
                logger.warning(f"Could not fetch CIK from SEC EDGAR for {self.ticker}.")

        if not self.stock_db_entry:
            logger.info(f"Stock {self.ticker} not found in DB, creating new entry.")
            self.stock_db_entry = Stock(ticker=self.ticker, company_name=company_name, industry=industry, sector=sector,
                                        cik=cik)
            self.db_session.add(self.stock_db_entry)
            try:
                self.db_session.commit();
                self.db_session.refresh(self.stock_db_entry)
            except SQLAlchemyError as e:
                self.db_session.rollback();
                logger.error(f"Error creating stock entry for {self.ticker}: {e}");
                raise
        else:  # Stock entry exists, update if necessary
            updated = False
            if company_name and self.stock_db_entry.company_name != company_name: self.stock_db_entry.company_name = company_name; updated = True
            if industry and self.stock_db_entry.industry != industry: self.stock_db_entry.industry = industry; updated = True
            if sector and self.stock_db_entry.sector != sector: self.stock_db_entry.sector = sector; updated = True
            if cik and self.stock_db_entry.cik != cik:  # Update CIK if new one found or was null
                self.stock_db_entry.cik = cik;
                updated = True
            elif not self.stock_db_entry.cik and cik:  # If CIK was null and now we have one
                self.stock_db_entry.cik = cik;
                updated = True

            if updated:
                try:
                    self.db_session.commit();
                    self.db_session.refresh(self.stock_db_entry)
                    logger.info(f"Updated stock entry for {self.ticker}.")
                except SQLAlchemyError as e:
                    self.db_session.rollback();
                    logger.error(f"Error updating stock entry for {self.ticker}: {e}")
        logger.info(
            f"Stock entry for {self.ticker} (ID: {self.stock_db_entry.id if self.stock_db_entry else 'N/A'}, CIK: {self.stock_db_entry.cik if self.stock_db_entry and self.stock_db_entry.cik else 'N/A'}) ready.")

    def _fetch_financial_statements(self):
        logger.info(f"Fetching financial statements for {self.ticker}...")
        statements_cache = {  # Initialize with empty structures
            "fmp_income_annual": [], "fmp_balance_annual": [], "fmp_cashflow_annual": [],
            "finnhub_financials_quarterly_reported": {"data": []},  # Finnhub wraps in 'data'
            "alphavantage_income_quarterly": {"quarterlyReports": []},  # AV wraps in 'quarterlyReports'
            "alphavantage_balance_quarterly": {"quarterlyReports": []},
            "alphavantage_cashflow_quarterly": {"quarterlyReports": []}
        }
        try:
            # FMP Annual Statements (typically 5-10 years for free, more for paid)
            statements_cache["fmp_income_annual"] = self.fmp.get_financial_statements(self.ticker, "income-statement",
                                                                                      "annual",
                                                                                      STOCK_FINANCIAL_YEARS) or []
            time.sleep(1.5)  # API courtesy
            statements_cache["fmp_balance_annual"] = self.fmp.get_financial_statements(self.ticker,
                                                                                       "balance-sheet-statement",
                                                                                       "annual",
                                                                                       STOCK_FINANCIAL_YEARS) or []
            time.sleep(1.5)
            statements_cache["fmp_cashflow_annual"] = self.fmp.get_financial_statements(self.ticker,
                                                                                        "cash-flow-statement", "annual",
                                                                                        STOCK_FINANCIAL_YEARS) or []
            time.sleep(1.5)
            logger.info(
                f"FMP Annuals for {self.ticker}: Income({len(statements_cache['fmp_income_annual'])} records), Balance({len(statements_cache['fmp_balance_annual'])} records), Cashflow({len(statements_cache['fmp_cashflow_annual'])} records).")

            # Finnhub Quarterly Reported Financials (as filed, can be extensive)
            fh_q_data = self.finnhub.get_financials_reported(self.ticker, freq="quarterly")  # Fetches multiple quarters
            time.sleep(1.5)
            if fh_q_data and isinstance(fh_q_data, dict) and fh_q_data.get("data"):
                statements_cache["finnhub_financials_quarterly_reported"] = fh_q_data
                logger.info(f"Fetched {len(fh_q_data['data'])} quarterly reports from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Finnhub quarterly financials reported data missing or malformed for {self.ticker}.")

            # Alpha Vantage Quarterly Statements (API can be slow, hence longer sleep)
            av_income_q = self.alphavantage.get_income_statement_quarterly(self.ticker)
            time.sleep(15)  # AlphaVantage free tier is slow and has tight limits
            if av_income_q and isinstance(av_income_q, dict) and av_income_q.get("quarterlyReports"):
                statements_cache["alphavantage_income_quarterly"] = av_income_q
                logger.info(
                    f"Fetched {len(av_income_q['quarterlyReports'])} quarterly income reports from Alpha Vantage for {self.ticker}.")
            else:
                logger.warning(f"Alpha Vantage quarterly income reports missing or malformed for {self.ticker}.")

            av_balance_q = self.alphavantage.get_balance_sheet_quarterly(self.ticker)
            time.sleep(15)
            if av_balance_q and isinstance(av_balance_q, dict) and av_balance_q.get("quarterlyReports"):
                statements_cache["alphavantage_balance_quarterly"] = av_balance_q
                logger.info(
                    f"Fetched {len(av_balance_q['quarterlyReports'])} quarterly balance reports from Alpha Vantage for {self.ticker}.")
            else:
                logger.warning(f"Alpha Vantage quarterly balance reports missing or malformed for {self.ticker}.")

            av_cashflow_q = self.alphavantage.get_cash_flow_quarterly(self.ticker)
            time.sleep(15)
            if av_cashflow_q and isinstance(av_cashflow_q, dict) and av_cashflow_q.get("quarterlyReports"):
                statements_cache["alphavantage_cashflow_quarterly"] = av_cashflow_q
                logger.info(
                    f"Fetched {len(av_cashflow_q['quarterlyReports'])} quarterly cash flow reports from Alpha Vantage for {self.ticker}.")
            else:
                logger.warning(f"Alpha Vantage quarterly cash flow reports missing or malformed for {self.ticker}.")

        except Exception as e:
            logger.warning(f"Error during financial statements fetch for {self.ticker}: {e}.", exc_info=True)
        self._financial_data_cache['financial_statements'] = statements_cache
        return statements_cache

    def _fetch_key_metrics_and_profile_data(self):
        logger.info(f"Fetching key metrics and profile for {self.ticker}.")
        # FMP Key Metrics (Annual & Quarterly) - Quarterly might fail on free tier
        key_metrics_annual_fmp = self.fmp.get_key_metrics(self.ticker, "annual",
                                                          STOCK_FINANCIAL_YEARS + 2)  # +2 for CAGR calculations
        time.sleep(1.5);
        self._financial_data_cache['key_metrics_annual_fmp'] = key_metrics_annual_fmp or []

        key_metrics_quarterly_fmp = self.fmp.get_key_metrics(self.ticker, "quarterly",
                                                             8)  # Fetch recent 8 quarters for TTM if possible
        time.sleep(1.5)
        if key_metrics_quarterly_fmp is None:  # API call itself failed or returned None (e.g. 403 error logged by API client)
            logger.warning(
                f"FMP quarterly key metrics API call failed or returned None for {self.ticker}. Data will be empty.")
            self._financial_data_cache['key_metrics_quarterly_fmp'] = []
        else:
            self._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp or []

        # Finnhub Basic Financials (often has TTM metrics)
        basic_fin_fh = self.finnhub.get_basic_financials(self.ticker)  # Provides 'metric' dict
        time.sleep(1.5);
        self._financial_data_cache['basic_financials_finnhub'] = basic_fin_fh or {}

        # Ensure FMP profile is loaded if not already (e.g. if AV or Finnhub profile was primary)
        if 'profile_fmp' not in self._financial_data_cache or not self._financial_data_cache.get('profile_fmp'):
            profile_fmp_list = self.fmp.get_company_profile(self.ticker);
            time.sleep(1.5)
            self._financial_data_cache['profile_fmp'] = profile_fmp_list[0] if profile_fmp_list and isinstance(
                profile_fmp_list, list) and profile_fmp_list[0] else {}

        logger.info(
            f"FMP KM Annual for {self.ticker}: {len(self._financial_data_cache['key_metrics_annual_fmp'])} records. "
            f"FMP KM Quarterly for {self.ticker}: {len(self._financial_data_cache['key_metrics_quarterly_fmp'])} records. "
            f"Finnhub Basic Financials for {self.ticker}: {'OK' if self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric') else 'Data missing or Metric key not found'}.")

    def _calculate_derived_metrics(self):
        logger.info(f"Calculating derived metrics for {self.ticker}...")
        metrics = {"key_metrics_snapshot": {}}  # For storing specific data points used in email

        # Retrieve cached data
        statements = self._financial_data_cache.get('financial_statements', {})
        income_annual = sorted(statements.get('fmp_income_annual', []), key=lambda x: x.get("date", ""), reverse=True)
        balance_annual = sorted(statements.get('fmp_balance_annual', []), key=lambda x: x.get("date", ""), reverse=True)
        cashflow_annual = sorted(statements.get('fmp_cashflow_annual', []), key=lambda x: x.get("date", ""),
                                 reverse=True)

        av_income_q_reports = statements.get('alphavantage_income_quarterly', {}).get('quarterlyReports', [])
        fh_q_reports_list = statements.get('finnhub_financials_quarterly_reported', {}).get('data', [])

        key_metrics_annual = self._financial_data_cache.get('key_metrics_annual_fmp', [])
        key_metrics_quarterly = self._financial_data_cache.get('key_metrics_quarterly_fmp',
                                                               [])  # Might be empty if FMP restricted

        basic_fin_fh_metric = self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric',
                                                                                                 {})  # Finnhub's basic fin data
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})

        # Get latest available data points from FMP key metrics (annual and quarterly TTM) and Finnhub
        latest_km_q = key_metrics_quarterly[0] if key_metrics_quarterly and isinstance(key_metrics_quarterly, list) and \
                                                  key_metrics_quarterly[0] else {}
        latest_km_a = key_metrics_annual[0] if key_metrics_annual and isinstance(key_metrics_annual, list) and \
                                               key_metrics_annual[0] else {}

        # Valuation Ratios (Prefer TTM from FMP quarterly, fallback to FMP annual, then Finnhub TTM)
        metrics["pe_ratio"] = safe_get_float(latest_km_q, "peRatioTTM") or safe_get_float(latest_km_a,
                                                                                          "peRatio") or safe_get_float(
            basic_fin_fh_metric, "peTTM")
        metrics["pb_ratio"] = safe_get_float(latest_km_q, "priceToBookRatioTTM") or safe_get_float(latest_km_a,
                                                                                                   "pbRatio") or safe_get_float(
            basic_fin_fh_metric, "pbAnnual")  # Finnhub often calls this pbAnnual
        metrics["ps_ratio"] = safe_get_float(latest_km_q, "priceToSalesRatioTTM") or safe_get_float(latest_km_a,
                                                                                                    "priceSalesRatio") or safe_get_float(
            basic_fin_fh_metric, "psTTM")

        metrics["ev_to_sales"] = safe_get_float(latest_km_q, "enterpriseValueOverRevenueTTM") or safe_get_float(
            latest_km_a, "enterpriseValueOverRevenue")
        if metrics["ev_to_sales"] is None: logger.debug(
            f"{self.ticker}: EV/Sales is None. FMP Q TTM: {latest_km_q.get('enterpriseValueOverRevenueTTM')}, FMP A: {latest_km_a.get('enterpriseValueOverRevenue')}")

        metrics["ev_to_ebitda"] = safe_get_float(latest_km_q, "evToEbitdaTTM") or safe_get_float(latest_km_a,
                                                                                                 "evToEbitda")
        if metrics["ev_to_ebitda"] is None: logger.debug(
            f"{self.ticker}: EV/EBITDA is None. FMP Q TTM: {latest_km_q.get('evToEbitdaTTM')}, FMP A: {latest_km_a.get('evToEbitda')}")

        # Dividend Yield (FMP TTM, FMP Annual, Finnhub Annual - note Finnhub gives % not decimal)
        div_yield_fmp_q = safe_get_float(latest_km_q, "dividendYieldTTM")
        div_yield_fmp_a = safe_get_float(latest_km_a, "dividendYield")
        div_yield_fh = safe_get_float(basic_fin_fh_metric,
                                      "dividendYieldAnnual")  # This is a percentage value e.g. 1.5 for 1.5%
        if div_yield_fh is not None: div_yield_fh /= 100.0  # Convert to decimal
        metrics["dividend_yield"] = div_yield_fmp_q if div_yield_fmp_q is not None else (
            div_yield_fmp_a if div_yield_fmp_a is not None else div_yield_fh)

        # Profitability & EPS from FMP Annual Income Statements (most recent year)
        if income_annual:
            latest_ia = income_annual[0]
            metrics["eps"] = safe_get_float(latest_ia, "eps") or safe_get_float(latest_km_a,
                                                                                "eps")  # Fallback to KM eps
            metrics["net_profit_margin"] = safe_get_float(latest_ia, "netProfitMargin")
            if metrics["net_profit_margin"] is None: logger.debug(
                f"{self.ticker}: Net Profit Margin from FMP annual income ('{latest_ia.get('date')}') is None. Raw value: {latest_ia.get('netProfitMargin')}")
            metrics["gross_profit_margin"] = safe_get_float(latest_ia, "grossProfitMargin")
            if metrics["gross_profit_margin"] is None: logger.debug(
                f"{self.ticker}: Gross Profit Margin from FMP annual income ('{latest_ia.get('date')}') is None. Raw value: {latest_ia.get('grossProfitMargin')}")
            metrics["operating_profit_margin"] = safe_get_float(latest_ia,
                                                                "operatingIncomeRatio")  # FMP calls it operatingIncomeRatio

            ebit = safe_get_float(latest_ia, "operatingIncome")  # Can also use 'ebitda' - 'depreciationAndAmortization'
            interest_expense = safe_get_float(latest_ia, "interestExpense")
            if ebit is not None and interest_expense is not None and abs(
                    interest_expense) > 1e-6:  # Avoid division by zero/small num
                metrics["interest_coverage_ratio"] = ebit / abs(interest_expense)
            else:
                logger.debug(
                    f"{self.ticker}: Cannot calculate Interest Coverage. EBIT: {ebit}, Interest Expense: {interest_expense}")

        # Financial Health from FMP Annual Balance Sheets & Key Metrics
        if balance_annual:
            latest_ba = balance_annual[0]
            total_equity = safe_get_float(latest_ba, "totalStockholdersEquity")
            total_assets = safe_get_float(latest_ba, "totalAssets")
            latest_net_income = get_value_from_statement_list(income_annual, "netIncome", 0, latest_ba.get('date'))

            if total_equity and total_equity != 0 and latest_net_income is not None: metrics[
                "roe"] = latest_net_income / total_equity
            if total_assets and total_assets != 0 and latest_net_income is not None: metrics[
                "roa"] = latest_net_income / total_assets

            metrics["debt_to_equity"] = safe_get_float(latest_km_a, "debtToEquity")  # Prefer KM D/E
            if metrics["debt_to_equity"] is None:  # Calculate if not in KM
                total_debt_ba = safe_get_float(latest_ba, "totalDebt")
                if total_debt_ba is not None and total_equity and total_equity != 0:
                    metrics["debt_to_equity"] = total_debt_ba / total_equity

            current_assets = safe_get_float(latest_ba, "totalCurrentAssets")
            current_liabilities = safe_get_float(latest_ba, "totalCurrentLiabilities")
            if current_assets is not None and current_liabilities is not None and current_liabilities != 0:
                metrics["current_ratio"] = current_assets / current_liabilities

            cash_equivalents = safe_get_float(latest_ba, "cashAndCashEquivalents", 0)
            short_term_investments = safe_get_float(latest_ba, "shortTermInvestments", 0)
            net_receivables = safe_get_float(latest_ba, "netReceivables", 0)
            if current_liabilities is not None and current_liabilities != 0:
                metrics["quick_ratio"] = (
                                                     cash_equivalents + short_term_investments + net_receivables) / current_liabilities

        # Debt to EBITDA (from FMP annual KM or income statement + balance sheet)
        latest_annual_ebitda = safe_get_float(latest_km_a, "ebitda") or get_value_from_statement_list(income_annual,
                                                                                                      "ebitda", 0)
        if latest_annual_ebitda and latest_annual_ebitda != 0 and balance_annual:
            total_debt_val = get_value_from_statement_list(balance_annual, "totalDebt", 0)
            if total_debt_val is not None: metrics["debt_to_ebitda"] = total_debt_val / latest_annual_ebitda

        # Growth Rates (YoY, CAGR from FMP Annual Income Statements)
        metrics["revenue_growth_yoy"] = calculate_growth(get_value_from_statement_list(income_annual, "revenue", 0),
                                                         get_value_from_statement_list(income_annual, "revenue", 1))
        metrics["eps_growth_yoy"] = calculate_growth(get_value_from_statement_list(income_annual, "eps", 0),
                                                     get_value_from_statement_list(income_annual, "eps", 1))

        if len(income_annual) >= 3:  # Need 3 years of data for 2-year span for 3yr CAGR (end/start^(1/2)-1)
            metrics["revenue_growth_cagr_3yr"] = calculate_cagr(
                get_value_from_statement_list(income_annual, "revenue", 0),
                get_value_from_statement_list(income_annual, "revenue", 2), 2)
            metrics["eps_growth_cagr_3yr"] = calculate_cagr(get_value_from_statement_list(income_annual, "eps", 0),
                                                            get_value_from_statement_list(income_annual, "eps", 2), 2)
        if len(income_annual) >= 5:  # Need 5 years of data for 4-year span
            metrics["revenue_growth_cagr_5yr"] = calculate_cagr(
                get_value_from_statement_list(income_annual, "revenue", 0),
                get_value_from_statement_list(income_annual, "revenue", 4), 4)
            metrics["eps_growth_cagr_5yr"] = calculate_cagr(get_value_from_statement_list(income_annual, "eps", 0),
                                                            get_value_from_statement_list(income_annual, "eps", 4), 4)

        # Quarterly Revenue Growth (QoQ) - Prefer Alpha Vantage, fallback to Finnhub
        latest_q_revenue_av = get_alphavantage_value(av_income_q_reports, "totalRevenue", 0)  # Newest quarter
        previous_q_revenue_av = get_alphavantage_value(av_income_q_reports, "totalRevenue", 1)  # Quarter before newest
        if latest_q_revenue_av is not None and previous_q_revenue_av is not None:
            metrics["revenue_growth_qoq"] = calculate_growth(latest_q_revenue_av, previous_q_revenue_av)
            metrics["key_metrics_snapshot"]["q_revenue_source"], metrics["key_metrics_snapshot"][
                "latest_q_revenue"] = "AlphaVantage", latest_q_revenue_av
        else:  # Fallback to Finnhub if AV data is missing
            revenue_concepts_fh = ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "TotalRevenues",
                                   "NetSales"]
            latest_q_revenue_fh = get_finnhub_concept_value(fh_q_reports_list, 'ic', revenue_concepts_fh,
                                                            0)  # Newest quarter
            previous_q_revenue_fh = get_finnhub_concept_value(fh_q_reports_list, 'ic', revenue_concepts_fh,
                                                              1)  # Quarter before
            if latest_q_revenue_fh is not None and previous_q_revenue_fh is not None:
                metrics["revenue_growth_qoq"] = calculate_growth(latest_q_revenue_fh, previous_q_revenue_fh)
                metrics["key_metrics_snapshot"]["q_revenue_source"], metrics["key_metrics_snapshot"][
                    "latest_q_revenue"] = "Finnhub", latest_q_revenue_fh
            else:
                logger.info(f"Could not calculate QoQ revenue for {self.ticker} from AlphaVantage or Finnhub.");
                metrics["revenue_growth_qoq"] = None

        # Free Cash Flow Metrics
        if cashflow_annual:
            fcf_latest_annual = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
            # Shares outstanding from FMP profile
            shares_outstanding = safe_get_float(profile_fmp, "sharesOutstanding") or \
                                 (safe_get_float(profile_fmp, "mktCap") / safe_get_float(profile_fmp, "price")
                                  if safe_get_float(profile_fmp, "price") and safe_get_float(profile_fmp,
                                                                                             "price") != 0 else None)

            if fcf_latest_annual is not None and shares_outstanding and shares_outstanding != 0:
                metrics["free_cash_flow_per_share"] = fcf_latest_annual / shares_outstanding
                market_cap_for_yield = safe_get_float(profile_fmp, "mktCap")
                if market_cap_for_yield and market_cap_for_yield != 0:
                    metrics["free_cash_flow_yield"] = fcf_latest_annual / market_cap_for_yield

            # FCF Trend (simple check on last 3 years of annual FCF)
            if len(cashflow_annual) >= 3:
                fcf0 = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
                fcf1 = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 1)
                fcf2 = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 2)
                # Ensure all are numbers before comparison
                if all(isinstance(x, (int, float)) for x in [fcf0, fcf1, fcf2] if x is not None):
                    if fcf0 is not None and fcf1 is not None and fcf2 is not None:
                        if fcf0 > fcf1 > fcf2:
                            metrics["free_cash_flow_trend"] = "Growing"
                        elif fcf0 < fcf1 < fcf2:
                            metrics["free_cash_flow_trend"] = "Declining"
                        else:
                            metrics["free_cash_flow_trend"] = "Mixed/Stable"
                    else:
                        metrics["free_cash_flow_trend"] = "Data Incomplete"  # Some FCF values are None
                else:
                    metrics["free_cash_flow_trend"] = "Non-Numeric Data"
            else:
                metrics["free_cash_flow_trend"] = "Data N/A (Less than 3 years)"

        # Retained Earnings Trend (simple check on last 3 years)
        if len(balance_annual) >= 3:
            re0 = get_value_from_statement_list(balance_annual, "retainedEarnings", 0)
            re1 = get_value_from_statement_list(balance_annual, "retainedEarnings", 1)
            re2 = get_value_from_statement_list(balance_annual, "retainedEarnings", 2)
            if all(isinstance(x, (int, float)) for x in [re0, re1, re2] if x is not None):
                if re0 is not None and re1 is not None and re2 is not None:
                    if re0 > re1 > re2:
                        metrics["retained_earnings_trend"] = "Growing"
                    elif re0 < re1 < re2:
                        metrics["retained_earnings_trend"] = "Declining"
                    else:
                        metrics["retained_earnings_trend"] = "Mixed/Stable"
                else:
                    metrics["retained_earnings_trend"] = "Data Incomplete"
            else:
                metrics["retained_earnings_trend"] = "Non-Numeric Data"
        else:
            metrics["retained_earnings_trend"] = "Data N/A (Less than 3 years)"

        # ROIC (Return on Invested Capital)
        if income_annual and balance_annual:
            # NOPAT = EBIT * (1 - Tax Rate)
            ebit_roic = get_value_from_statement_list(income_annual, "operatingIncome", 0)  # EBIT
            income_tax_expense_roic = get_value_from_statement_list(income_annual, "incomeTaxExpense", 0)
            income_before_tax_roic = get_value_from_statement_list(income_annual, "incomeBeforeTax", 0)

            effective_tax_rate = 0.21  # Default tax rate if cannot calculate
            if income_tax_expense_roic is not None and income_before_tax_roic is not None and income_before_tax_roic != 0:
                effective_tax_rate = income_tax_expense_roic / income_before_tax_roic

            nopat = None
            if ebit_roic is not None:
                nopat = ebit_roic * (1 - effective_tax_rate)

            # Invested Capital = Total Debt + Total Equity - Cash & Cash Equivalents
            total_debt_roic = get_value_from_statement_list(balance_annual, "totalDebt", 0)
            total_equity_roic = get_value_from_statement_list(balance_annual, "totalStockholdersEquity", 0)
            cash_equivalents_roic = get_value_from_statement_list(balance_annual, "cashAndCashEquivalents",
                                                                  0) or 0  # Default to 0 if None

            if total_debt_roic is not None and total_equity_roic is not None:
                invested_capital = total_debt_roic + total_equity_roic - cash_equivalents_roic
                if nopat is not None and invested_capital is not None and invested_capital != 0:
                    metrics["roic"] = nopat / invested_capital

        # Final cleanup: replace NaN/inf with None for all metrics
        final_metrics = {}
        for k, v in metrics.items():
            if k == "key_metrics_snapshot":  # Keep snapshot dict as is, assuming its values are clean
                final_metrics[k] = {sk: sv for sk, sv in v.items() if sv is not None and not (
                            isinstance(sv, float) and (math.isnan(sv) or math.isinf(sv)))}
            elif isinstance(v, float):
                final_metrics[k] = v if not (math.isnan(v) or math.isinf(v)) else None
            elif v is not None:  # Handles strings, bools, etc.
                final_metrics[k] = v
            else:  # Value is already None
                final_metrics[k] = None

        log_metrics = {k: v for k, v in final_metrics.items() if
                       k != "key_metrics_snapshot"}  # Exclude snapshot from this log line
        logger.info(f"Calculated metrics for {self.ticker}: {log_metrics}")
        self._financial_data_cache['calculated_metrics'] = final_metrics
        return final_metrics

    def _perform_dcf_analysis(self):
        logger.info(f"Performing simplified DCF analysis for {self.ticker}...")
        dcf_results = {
            "dcf_intrinsic_value": None, "dcf_upside_percentage": None,
            "dcf_assumptions": {
                "discount_rate": DEFAULT_DISCOUNT_RATE,
                "perpetual_growth_rate": DEFAULT_PERPETUAL_GROWTH_RATE,
                "projection_years": DEFAULT_FCF_PROJECTION_YEARS,
                "start_fcf": None,
                "fcf_growth_rates_projection": []  # Store the actual growth rates used
            }
        }

        cashflow_annual = sorted(
            self._financial_data_cache.get('financial_statements', {}).get('fmp_cashflow_annual', []),
            key=lambda x: x.get("date", ""), reverse=True)
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        calculated_metrics = self._financial_data_cache.get('calculated_metrics', {})  # For FCF growth rate proxy

        current_price = safe_get_float(profile_fmp, "price")
        shares_outstanding = safe_get_float(profile_fmp, "sharesOutstanding") or \
                             (safe_get_float(profile_fmp, "mktCap") / current_price
                              if current_price and current_price != 0 else None)

        if not cashflow_annual or not profile_fmp or current_price is None or shares_outstanding is None or shares_outstanding == 0:
            logger.warning(
                f"Insufficient data for DCF for {self.ticker} (FCF statements, profile, price, or shares missing/zero).");
            return dcf_results

        current_fcf = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
        if current_fcf is None or current_fcf <= 10000:  # Need positive FCF, arbitrary small threshold
            logger.warning(f"Current FCF for {self.ticker} is {current_fcf}. DCF requires substantial positive FCF.");
            return dcf_results
        dcf_results["dcf_assumptions"]["start_fcf"] = current_fcf

        # Estimate initial FCF growth rate (prefer historical FCF CAGR, then revenue CAGR, then default)
        fcf_growth_rate_3yr_cagr = None
        if len(cashflow_annual) >= 4:  # Need 4 years data for 3-year CAGR over 3 periods
            fcf_start_for_cagr = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 3)  # FCF 3 years ago
            if fcf_start_for_cagr and fcf_start_for_cagr > 0:  # Avoid issues with negative/zero start
                fcf_growth_rate_3yr_cagr = calculate_cagr(current_fcf, fcf_start_for_cagr, 3)

        initial_fcf_growth_rate = fcf_growth_rate_3yr_cagr if fcf_growth_rate_3yr_cagr is not None else \
            calculated_metrics.get("revenue_growth_cagr_3yr") if calculated_metrics.get(
                "revenue_growth_cagr_3yr") is not None else \
                calculated_metrics.get("revenue_growth_yoy") if calculated_metrics.get(
                    "revenue_growth_yoy") is not None else 0.05  # Default 5%

        if not isinstance(initial_fcf_growth_rate, (int, float)): initial_fcf_growth_rate = 0.05  # Ensure numeric
        initial_fcf_growth_rate = min(max(initial_fcf_growth_rate, -0.10), 0.20)  # Cap growth rate bounds for stability

        projected_fcfs = []
        last_projected_fcf = current_fcf
        # Linearly decline growth rate from initial to perpetual over projection_years
        growth_rate_decline_per_year = (initial_fcf_growth_rate - DEFAULT_PERPETUAL_GROWTH_RATE) / float(
            DEFAULT_FCF_PROJECTION_YEARS) \
            if DEFAULT_FCF_PROJECTION_YEARS > 0 else 0

        for i in range(DEFAULT_FCF_PROJECTION_YEARS):
            current_year_growth_rate = max(initial_fcf_growth_rate - (growth_rate_decline_per_year * i),
                                           DEFAULT_PERPETUAL_GROWTH_RATE)
            projected_fcf = last_projected_fcf * (1 + current_year_growth_rate);
            projected_fcfs.append(projected_fcf);
            last_projected_fcf = projected_fcf
            dcf_results["dcf_assumptions"]["fcf_growth_rates_projection"].append(round(current_year_growth_rate, 4))

        if not projected_fcfs: logger.error(f"DCF: No projected FCFs generated for {self.ticker}."); return dcf_results

        # Terminal Value Calculation (Gordon Growth Model)
        terminal_year_fcf = projected_fcfs[-1] * (1 + DEFAULT_PERPETUAL_GROWTH_RATE)
        terminal_value_denominator = DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE
        terminal_value = terminal_year_fcf / terminal_value_denominator if terminal_value_denominator > 1e-6 else 0  # Avoid div by zero
        if terminal_value_denominator <= 1e-6: logger.warning(
            f"DCF for {self.ticker}: Discount rate too close or below perpetual growth rate. Terminal Value may be unreliable.")

        # Discount projected FCFs and Terminal Value
        sum_discounted_fcf = sum(fcf / ((1 + DEFAULT_DISCOUNT_RATE) ** (i + 1)) for i, fcf in enumerate(projected_fcfs))
        discounted_terminal_value = terminal_value / ((1 + DEFAULT_DISCOUNT_RATE) ** DEFAULT_FCF_PROJECTION_YEARS)

        intrinsic_equity_value = sum_discounted_fcf + discounted_terminal_value

        if shares_outstanding != 0:
            intrinsic_value_per_share = intrinsic_equity_value / shares_outstanding;
            dcf_results["dcf_intrinsic_value"] = intrinsic_value_per_share
            if current_price and current_price != 0:
                dcf_results["dcf_upside_percentage"] = (intrinsic_value_per_share - current_price) / current_price

        logger.info(
            f"DCF for {self.ticker}: Intrinsic Value/Share: {dcf_results.get('dcf_intrinsic_value', 'N/A')}, "
            f"Upside: {dcf_results.get('dcf_upside_percentage', 'N/A') * 100 if dcf_results.get('dcf_upside_percentage') is not None else 'N/A'}%")
        self._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    def _fetch_and_summarize_10k(self):
        logger.info(f"Fetching and attempting to summarize latest 10-K for {self.ticker}")
        summary_results = {"qualitative_sources_summary": {}}  # To store metadata about sources
        if not self.stock_db_entry or not self.stock_db_entry.cik:
            logger.warning(f"No CIK for {self.ticker}. Cannot fetch 10-K.");
            return summary_results

        # Try 10-K first, then 10-K/A (amendment)
        filing_url = self.sec_edgar.get_filing_document_url(self.stock_db_entry.cik, "10-K");
        time.sleep(0.5)
        if not filing_url:
            logger.info(f"No 10-K found for {self.ticker}, trying 10-K/A.")
            filing_url = self.sec_edgar.get_filing_document_url(self.stock_db_entry.cik, "10-K/A");
            time.sleep(0.5)

        if not filing_url:
            logger.warning(f"No 10-K or 10-K/A URL found for {self.ticker} (CIK: {self.stock_db_entry.cik})");
            return summary_results
        summary_results["qualitative_sources_summary"]["10k_filing_url_used"] = filing_url

        text_content = self.sec_edgar.get_filing_text(filing_url)  # Fetches from cache or SEC
        if not text_content:
            logger.warning(f"Failed to fetch/load 10-K text from {filing_url}");
            return summary_results
        logger.info(f"Fetched 10-K text (length: {len(text_content)}) for {self.ticker}. Extracting sections.")

        sections = extract_S1_text_sections(text_content, TEN_K_KEY_SECTIONS)  # Re-use S1 extractor logic
        company_name_for_prompt = self.stock_db_entry.company_name or self.ticker

        def summarize_section_with_gemini(section_text, section_name_for_prompt, company_name_ticker_prompt,
                                          max_len_gemini):
            if not section_text: return None, 0

            # Tailor context for Gemini based on section
            context_for_gemini = f"The following is the '{section_name_for_prompt}' section from the 10-K filing for {company_name_ticker_prompt}. "
            if section_name_for_prompt.lower() == "business":
                context_for_gemini += "Please summarize the company's core business operations, products/services, revenue generation model, and primary markets."
            elif section_name_for_prompt.lower() == "risk factors":
                context_for_gemini += "Identify and summarize the 3-5 most significant risk factors disclosed. Be concise."
            elif section_name_for_prompt.lower() == "management's discussion and analysis" or section_name_for_prompt.lower() == "mda":
                context_for_gemini += "Summarize key insights into financial performance drivers, financial condition, liquidity, and management's outlook or focus areas."
            else:  # Generic summary instruction
                context_for_gemini += "Please provide a concise factual summary of this section."

            summary = self.gemini.summarize_text_with_context(section_text, context_for_gemini, max_len_gemini)
            time.sleep(3)  # API courtesy
            return (summary if summary and not summary.startswith(
                "Error:") else f"AI summary error or no content for {section_name_for_prompt}."), len(section_text)

        summary_results["business_summary"], summary_results["qualitative_sources_summary"][
            "business_10k_source_length"] = \
            summarize_section_with_gemini(sections.get("business"), "Business",
                                          f"{company_name_for_prompt} ({self.ticker})",
                                          MAX_10K_SECTION_LENGTH_FOR_GEMINI)

        summary_results["risk_factors_summary"], summary_results["qualitative_sources_summary"][
            "risk_factors_10k_source_length"] = \
            summarize_section_with_gemini(sections.get("risk_factors"), "Risk Factors",
                                          f"{company_name_for_prompt} ({self.ticker})",
                                          MAX_10K_SECTION_LENGTH_FOR_GEMINI)

        summary_results["management_assessment_summary"], summary_results["qualitative_sources_summary"][
            "mda_10k_source_length"] = \
            summarize_section_with_gemini(sections.get("mda"), "Management's Discussion and Analysis",
                                          f"{company_name_for_prompt} ({self.ticker})",
                                          MAX_10K_SECTION_LENGTH_FOR_GEMINI)

        # Derived qualitative summaries using Gemini (e.g., competitive landscape, economic moat)
        # Ensure base summaries are strings before concatenation
        biz_summary_str = summary_results.get("business_summary", "") or ""
        mda_summary_str = summary_results.get("management_assessment_summary", "") or ""
        risk_summary_str = summary_results.get("risk_factors_summary", "") or ""

        # Competitive Landscape
        comp_input_text = (biz_summary_str + "\n" + mda_summary_str)[:MAX_GEMINI_TEXT_LENGTH].strip()
        if comp_input_text:
            comp_prompt = (
                f"Based on the business description and MD&A for {company_name_for_prompt} ({self.ticker}):\n\"\"\"\n{comp_input_text}\n\"\"\"\n"
                f"Describe the company's competitive landscape, identify its key competitors (if mentioned or inferable), and discuss its market positioning relative to them. Focus on factual statements from the provided text or reasonable inferences based on it.")
            comp_summary = self.gemini.generate_text(comp_prompt);
            time.sleep(3)
            if comp_summary and not comp_summary.startswith("Error:"): summary_results[
                "competitive_landscape_summary"] = comp_summary

        # Economic Moat
        comp_summary_str = summary_results.get("competitive_landscape_summary", "") or ""
        moat_input_text = (biz_summary_str + "\n" + comp_summary_str + "\n" + risk_summary_str)[
                          :MAX_GEMINI_TEXT_LENGTH].strip()
        if moat_input_text:  # and not summary_results.get("economic_moat_summary"): # Can generate even if some other source provides it, for 10-K perspective
            moat_prompt = (
                f"Analyze the primary economic moats (e.g., brand, network effects, switching costs, intangible assets, cost advantages) for {company_name_for_prompt} ({self.ticker}), "
                f"based on the following information:\n\"\"\"\n{moat_input_text}\n\"\"\"\nProvide a concise summary of its key moats and their strength.")
            moat_summary = self.gemini.generate_text(moat_prompt);
            time.sleep(3)
            if moat_summary and not moat_summary.startswith("Error:"): summary_results[
                "economic_moat_summary"] = moat_summary

        # Industry Trends
        industry_context_text = (biz_summary_str + "\nRelevant Industry: " + (
                    self.stock_db_entry.industry or "Not Specified") + "\nRelevant Sector: " + (
                                             self.stock_db_entry.sector or "Not Specified"))[
                                :MAX_GEMINI_TEXT_LENGTH].strip()
        if industry_context_text:  # and not summary_results.get("industry_trends_summary"):
            industry_prompt = (
                f"For {company_name_for_prompt} ({self.ticker}), operating in the '{self.stock_db_entry.industry}' industry, "
                f"consider the following context:\n\"\"\"\n{industry_context_text}\n\"\"\"\n"
                f"Analyze key trends, opportunities, and challenges relevant to this industry. How does the company appear to be positioned in relation to these trends?")
            industry_summary = self.gemini.generate_text(industry_prompt);
            time.sleep(3)
            if industry_summary and not industry_summary.startswith("Error:"): summary_results[
                "industry_trends_summary"] = industry_summary

        logger.info(f"10-K qualitative summaries generated for {self.ticker}.")
        self._financial_data_cache['10k_summaries'] = summary_results
        return summary_results

    def _parse_ai_investment_thesis_response(self, ai_response_text):
        # Default values
        parsed_data = {
            "investment_thesis_full": "AI response not fully processed or 'Investment Thesis:' section missing.",
            "investment_decision": "Review AI Output",  # Default if not parsed
            "strategy_type": "Not Specified by AI",
            "confidence_level": "Not Specified by AI",
            "reasoning": "AI response not fully processed or 'Key Reasoning Points:' section missing."
        }

        if not ai_response_text or ai_response_text.startswith("Error:"):
            error_message = ai_response_text if ai_response_text else "Error: Empty response from AI."
            parsed_data["investment_thesis_full"] = error_message
            parsed_data["reasoning"] = error_message
            parsed_data["investment_decision"] = "AI Error"
            parsed_data["strategy_type"] = "AI Error"
            parsed_data["confidence_level"] = "AI Error"
            return parsed_data

        text_content = ai_response_text.replace('\r\n', '\n').strip()

        # Define regex patterns for each section header
        # Using re.IGNORECASE | re.MULTILINE | re.DOTALL
        # The pattern captures everything after the header until the next known header or end of string.
        section_patterns = {
            "investment_thesis_full": re.compile(
                r"^\s*Investment Thesis:\s*\n?(.*?)(?=\n\s*(?:Investment Decision:|Strategy Type:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)",
                re.I | re.M | re.S),
            "investment_decision": re.compile(
                r"^\s*Investment Decision:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Strategy Type:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)",
                re.I | re.M | re.S),
            "strategy_type": re.compile(
                r"^\s*Strategy Type:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)",
                re.I | re.M | re.S),
            "confidence_level": re.compile(
                r"^\s*Confidence Level:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Strategy Type:|Key Reasoning Points:)|^\s*$|\Z)",
                re.I | re.M | re.S),
            "reasoning": re.compile(
                r"^\s*Key Reasoning Points:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Strategy Type:|Confidence Level:)|^\s*$|\Z)",
                re.I | re.M | re.S)
        }

        found_any_section = False
        for key, pattern in section_patterns.items():
            match = pattern.search(text_content)
            if match:
                content = match.group(1).strip()
                if content:
                    # For single-line expected fields, often the first line of the match is enough
                    if key in ["investment_decision", "strategy_type", "confidence_level"]:
                        parsed_data[key] = content.split('\n')[0].strip()  # Take first line
                    else:
                        parsed_data[key] = content
                    found_any_section = True
                else:
                    logger.debug(f"Section '{key}' found but content was empty for {self.ticker}.")
            else:
                logger.debug(f"Section header for '{key}' not found via regex for {self.ticker}.")

        # If no sections were parsed by regex (e.g., AI didn't follow headings strictly)
        # and the response isn't an error, put the whole thing in the thesis for review.
        if not found_any_section and not ai_response_text.startswith("Error:"):
            logger.warning(
                f"Could not parse distinct sections from AI response for {self.ticker}. Full response in thesis.")
            parsed_data["investment_thesis_full"] = text_content
            # Decision, strategy, etc., will remain "Review AI Output" or "Not Specified"

        # A final check: if investment_decision is still "Review AI Output" but thesis contains a clear "Investment Decision:" line, try to grab it.
        # This is a fallback for the regex not catching it perfectly.
        if parsed_data["investment_decision"] == "Review AI Output" and "investment decision:" in text_content.lower():
            try:
                lines = text_content.split('\n')
                for i, line in enumerate(lines):
                    if "investment decision:" in line.lower():
                        decision_val = line.split(":", 1)[1].strip()
                        if decision_val: parsed_data["investment_decision"] = decision_val; break
            except Exception:
                pass  # Stick to "Review AI Output"

        return parsed_data

    def _determine_investment_thesis(self):
        logger.info(f"Synthesizing investment thesis for {self.ticker}...")
        metrics = self._financial_data_cache.get('calculated_metrics', {})
        qual_summaries = self._financial_data_cache.get('10k_summaries', {})
        dcf_results = self._financial_data_cache.get('dcf_results', {})
        profile = self._financial_data_cache.get('profile_fmp', {})  # For current price

        company_name = self.stock_db_entry.company_name or self.ticker
        industry = self.stock_db_entry.industry or "N/A"
        sector = self.stock_db_entry.sector or "N/A"

        prompt = f"Company: {company_name} ({self.ticker})\nIndustry: {industry}, Sector: {sector}\n\n"
        prompt += "Key Financial Metrics & Data:\n"
        # Select key metrics for the prompt
        metrics_for_prompt = {
            "P/E Ratio": metrics.get("pe_ratio"), "P/B Ratio": metrics.get("pb_ratio"),
            "P/S Ratio": metrics.get("ps_ratio"), "Dividend Yield": metrics.get("dividend_yield"),
            "ROE (Return on Equity)": metrics.get("roe"), "ROIC (Return on Invested Capital)": metrics.get("roic"),
            "Debt-to-Equity": metrics.get("debt_to_equity"),
            "Revenue Growth YoY": metrics.get("revenue_growth_yoy"),
            "Revenue Growth QoQ": metrics.get("revenue_growth_qoq"),
            "EPS Growth YoY": metrics.get("eps_growth_yoy"),
            "Net Profit Margin": metrics.get("net_profit_margin"),
            "Free Cash Flow Yield": metrics.get("free_cash_flow_yield"),
            "Free Cash Flow Trend": metrics.get("free_cash_flow_trend"),
            "Retained Earnings Trend": metrics.get("retained_earnings_trend"),
        }
        for name, val in metrics_for_prompt.items():
            if val is not None:
                if isinstance(val, float) and (
                        name.endswith("Yield") or "Growth" in name or "Margin" in name or name in [
                    "ROE (Return on Equity)", "ROIC (Return on Invested Capital)"]):
                    val_str = f"{val:.2%}"
                elif isinstance(val, float):
                    val_str = f"{val:.2f}"
                else:
                    val_str = str(val)
                prompt += f"- {name}: {val_str}\n"

        dcf_intrinsic_value = dcf_results.get("dcf_intrinsic_value")
        dcf_upside = dcf_results.get("dcf_upside_percentage")
        current_stock_price = profile.get("price")
        if current_stock_price is not None: prompt += f"- Current Stock Price: {current_stock_price:.2f}\n"
        if dcf_intrinsic_value is not None: prompt += f"- DCF Intrinsic Value per Share: {dcf_intrinsic_value:.2f}\n"
        if dcf_upside is not None: prompt += f"- DCF Upside/Downside: {dcf_upside:.2%}\n"
        prompt += "\n"

        prompt += "Qualitative Summaries (from 10-K & AI analysis):\n"
        qual_for_prompt = {
            "Business Model & Operations": qual_summaries.get("business_summary"),
            "Economic Moat": qual_summaries.get("economic_moat_summary"),
            "Industry Trends & Outlook": qual_summaries.get("industry_trends_summary"),
            "Competitive Landscape": qual_summaries.get("competitive_landscape_summary"),
            "Management's Discussion (MD&A Highlights)": qual_summaries.get("management_assessment_summary"),
            "Key Risk Factors": qual_summaries.get("risk_factors_summary"),
        }
        for name, text_val in qual_for_prompt.items():
            if text_val and isinstance(text_val, str):
                prompt += f"- {name}: {text_val[:250].replace('...', '').strip()}...\n"  # Truncate for prompt
        prompt += "\n"

        prompt += (
            "Instructions for AI: Based on all the above information, provide a detailed financial analysis. "
            "Structure your response *exactly* as follows, using these specific headings on separate lines:\n\n"
            "Investment Thesis:\n"
            "[Provide a comprehensive investment thesis (2-4 paragraphs) synthesizing all data, discussing positives and negatives, and the overall outlook.]\n\n"
            "Investment Decision:\n"
            "[State one of the following: Strong Buy, Buy, Hold, Monitor, Reduce, Sell, Avoid. Base this on the overall analysis.]\n\n"
            "Strategy Type:\n"
            "[Suggest an appropriate investment strategy, e.g., Value, GARP (Growth at a Reasonable Price), Growth, Income, Speculative, Special Situation, Turnaround.]\n\n"
            "Confidence Level:\n"
            "[State one of the following: High, Medium, Low. This reflects confidence in the *analysis and decision*, considering data quality and forecastability.]\n\n"
            "Key Reasoning Points:\n"
            "[Provide 3-7 bullet points summarizing the key reasons for the investment decision. Cover aspects like: \n"
            "* Valuation (e.g., DCF, comparables, P/E relative to growth/sector).\n"
            "* Financial Health & Performance (e.g., profitability, debt, cash flow trends).\n"
            "* Growth Outlook (e.g., revenue/EPS growth drivers, market expansion, innovation pipeline).\n"
            "* Economic Moat & Competitive Advantages (e.g., brand, patents, network effects, cost structure).\n"
            "* Key Risks (e.g., competition, macro factors, regulatory, company-specific issues).\n"
            "* Management & Strategy (e.g., effectiveness of strategy, capital allocation, execution track record, if known).\n"
            "Each point should be concise and directly support the decision.]"
        )

        final_prompt_for_gemini = prompt[:MAX_GEMINI_TEXT_LENGTH]  # Ensure prompt is within limits
        ai_response_text = self.gemini.generate_text(final_prompt_for_gemini)

        parsed_thesis_data = self._parse_ai_investment_thesis_response(ai_response_text)

        logger.info(
            f"Generated thesis for {self.ticker}. Parsed Decision: {parsed_thesis_data.get('investment_decision')}, Strategy: {parsed_thesis_data.get('strategy_type')}, Confidence: {parsed_thesis_data.get('confidence_level')}")
        return parsed_thesis_data

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_data_for_db = {}  # This will hold all data to be saved to StockAnalysis model
        try:
            if not self.stock_db_entry:  # Should have been initialized
                logger.error(f"Stock DB entry for {self.ticker} is not initialized. Aborting analysis.");
                return None
            self._ensure_stock_db_entry_is_bound()  # Ensure SQLAlchemy session attachment

            # --- Data Fetching ---
            self._fetch_financial_statements()
            self._fetch_key_metrics_and_profile_data()

            # --- Calculations & Quantitative Analysis ---
            calculated_metrics = self._calculate_derived_metrics()
            final_data_for_db.update(calculated_metrics)

            dcf_analysis_results = self._perform_dcf_analysis()
            final_data_for_db.update(dcf_analysis_results)

            # --- Qualitative Analysis (10-K & AI Summaries) ---
            qualitative_analysis_results = self._fetch_and_summarize_10k()
            final_data_for_db.update(qualitative_analysis_results)

            # --- Final Synthesis & Decision ---
            investment_thesis_data = self._determine_investment_thesis()
            final_data_for_db.update(investment_thesis_data)

            # Create and populate the StockAnalysis DB entry
            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id, analysis_date=datetime.now(timezone.utc))

            # Get all column names from the StockAnalysis model
            model_fields = [c.key for c in StockAnalysis.__table__.columns if
                            c.key not in ['id', 'stock_id', 'analysis_date']]

            for field_name in model_fields:
                if field_name in final_data_for_db:
                    value_to_set = final_data_for_db[field_name]

                    # Basic type checking/conversion for safety before setting attribute
                    target_column_type = getattr(StockAnalysis, field_name).type.python_type
                    if target_column_type == float:
                        if isinstance(value_to_set, str):  # Attempt conversion if string
                            try:
                                value_to_set = float(value_to_set)
                            except ValueError:
                                value_to_set = None
                        if isinstance(value_to_set, float) and (math.isnan(value_to_set) or math.isinf(value_to_set)):
                            value_to_set = None  # Store NaN/inf as None in DB
                    elif target_column_type == dict and not isinstance(value_to_set,
                                                                       dict):  # Ensure JSON fields are dicts
                        value_to_set = None  # Or {} if appropriate default
                    elif target_column_type == str and not isinstance(value_to_set, str):
                        value_to_set = str(value_to_set) if value_to_set is not None else None

                    setattr(analysis_entry, field_name, value_to_set)

            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = analysis_entry.analysis_date  # Update stock's last analysis date
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved stock data: {self.ticker} (Analysis ID: {analysis_entry.id})")
            return analysis_entry

        except RuntimeError as rt_err:  # Catch initialization errors or other critical runtime issues
            logger.critical(f"Runtime error during full analysis for {self.ticker}: {rt_err}", exc_info=True);
            return None
        except Exception as e:
            logger.error(f"CRITICAL error in full analysis pipeline for {self.ticker}: {e}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try:
                    self.db_session.rollback(); logger.info(
                        f"Rolled back database transaction for {self.ticker} due to error.")
                except Exception as e_rb:
                    logger.error(f"Rollback error for {self.ticker}: {e_rb}")
            return None
        finally:
            self._close_session_if_active()  # Ensure session is closed

    def _ensure_stock_db_entry_is_bound(self):
        """Ensures the stock_db_entry is properly bound to the current active session."""
        if not self.stock_db_entry:
            logger.critical(
                f"CRITICAL: self.stock_db_entry is None for {self.ticker} at _ensure_stock_db_entry_is_bound.")
            raise RuntimeError(f"Stock entry for {self.ticker} is None during binding check.")

        if not self.db_session.is_active:  # If session became inactive for some reason
            logger.warning(f"DB Session for {self.ticker} was INACTIVE before binding check. Re-establishing.")
            self._close_session_if_active()  # Close potentially stale session
            self.db_session = next(get_db_session())  # Get a fresh session

            # After re-establishing session, self.stock_db_entry is likely detached. Re-fetch or re-attach.
            logger.info(f"Attempting to re-associate stock {self.ticker} with new session.")
            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock:
                logger.error(
                    f"Could not re-fetch stock {self.ticker} after session re-establishment. This might indicate a larger issue or first-time creation flow problem.")
                # Potentially try to re-run parts of _get_or_create_stock_entry if this is a common recovery path
                # For now, raise error as this state should be unusual if _get_or_create_stock_entry succeeded.
                raise RuntimeError(f"Failed to re-fetch stock {self.ticker} for new session. Analysis cannot proceed.")
            else:
                self.stock_db_entry = re_fetched_stock
                logger.info(f"Re-fetched and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new session.")
            return  # Return after re-binding

        # Check if the instance is bound to *this* session object
        instance_state = sa_inspect(self.stock_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            obj_id_log = self.stock_db_entry.id if instance_state.has_identity else 'Transient/No ID'
            session_id_actual = id(instance_state.session) if instance_state.session else 'None'
            logger.warning(
                f"Stock {self.ticker} (ID: {obj_id_log}) is DETACHED or bound to a DIFFERENT session "
                f"(Expected session id: {id(self.db_session)}, Actual: {session_id_actual}). Attempting to merge.")
            try:
                # Merge the detached object into the current session
                self.stock_db_entry = self.db_session.merge(self.stock_db_entry)
                logger.info(
                    f"Successfully merged stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session.")
            except Exception as e_merge:
                logger.error(
                    f"Failed to merge stock {self.ticker} into current session: {e_merge}. Attempting re-fetch as fallback.",
                    exc_info=True)
                # Fallback: try to re-fetch from DB using current session
                re_fetched_from_db = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
                if re_fetched_from_db:
                    self.stock_db_entry = re_fetched_from_db
                    logger.info(
                        f"Successfully re-fetched stock {self.ticker} (ID: {self.stock_db_entry.id}) after merge failure.")
                else:
                    # This is a more critical situation if re-fetch also fails.
                    logger.critical(
                        f"CRITICAL: Failed to bind stock {self.ticker} to current session after merge and re-fetch attempts.")
                    raise RuntimeError(f"Failed to bind stock {self.ticker} to session. Analysis cannot proceed.")


if __name__ == '__main__':
    from database import init_db

    # init_db() # Uncomment if DB schema needs to be created/updated

    logger.info("Starting standalone stock analysis test...")
    tickers_to_test = ["AAPL", "MSFT", "NKE"]  # Added NKE to re-test based on initial logs

    for ticker_symbol in tickers_to_test:
        analysis_result_obj = None
        try:
            logger.info(f"--- Analyzing {ticker_symbol} ---")
            analyzer_instance = StockAnalyzer(ticker=ticker_symbol)  # StockAnalyzer manages its own session
            analysis_result_obj = analyzer_instance.analyze()

            if analysis_result_obj and hasattr(analysis_result_obj, 'stock'):  # Check if analysis object is valid
                logger.info(
                    f"Analysis for {analysis_result_obj.stock.ticker} completed. "
                    f"Decision: {analysis_result_obj.investment_decision}, "
                    f"Strategy: {analysis_result_obj.strategy_type}, "
                    f"Confidence: {analysis_result_obj.confidence_level}"
                )
                if analysis_result_obj.dcf_intrinsic_value is not None:
                    logger.info(
                        f"DCF Value: {analysis_result_obj.dcf_intrinsic_value:.2f}, "
                        f"Upside: {analysis_result_obj.dcf_upside_percentage:.2% if analysis_result_obj.dcf_upside_percentage is not None else 'N/A'}"
                    )
                logger.info(
                    f"QoQ Revenue Growth: {analysis_result_obj.revenue_growth_qoq if analysis_result_obj.revenue_growth_qoq is not None else 'N/A'} "
                    f"(Source: {analysis_result_obj.key_metrics_snapshot.get('q_revenue_source', 'N/A') if analysis_result_obj.key_metrics_snapshot else 'N/A'})"
                )
                # Log a few more key metrics
                logger.info(
                    f"  P/E: {analysis_result_obj.pe_ratio}, P/B: {analysis_result_obj.pb_ratio}, ROE: {analysis_result_obj.roe}")
                # logger.debug(f"Full thesis for {ticker_symbol}: {analysis_result_obj.investment_thesis_full}") # For detailed check if needed
                # logger.debug(f"Reasoning for {ticker_symbol}: {analysis_result_obj.reasoning}")

            else:
                logger.error(f"Stock analysis pipeline FAILED or returned invalid result for {ticker_symbol}.")
        except RuntimeError as rt_err:  # Catch errors from StockAnalyzer.__init__
            logger.error(f"Could not run StockAnalyzer for {ticker_symbol} due to initialization error: {rt_err}")
        except Exception as e_main_loop:  # Catch any other unexpected errors during analysis
            logger.error(f"Unhandled error analyzing {ticker_symbol} in __main__ loop: {e_main_loop}", exc_info=True)
        finally:
            logger.info(f"--- Finished processing {ticker_symbol} ---")
            time.sleep(20)  # Respect API rate limits if running multiple tickers