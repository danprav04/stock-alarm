# stock_analyzer.py
import pandas as pd
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone, timedelta
import math  # For DCF calculations
import time  # For API courtesy delays

from api_clients import (
    FinnhubClient, FinancialModelingPrepClient,
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
    if val is None: return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# Helper function for CAGR calculation
def calculate_cagr(end_value, start_value, years):
    if start_value is None or end_value is None or years <= 0: return None
    if start_value == 0: return None
    if start_value < 0:
        if end_value > 0:
            return None
        elif end_value < 0:
            return -((float(end_value) / float(start_value)) ** (1 / float(years)) - 1) if float(
                end_value) != 0 else None
        else:
            return 1.0
    if end_value < 0 and start_value > 0: return None
    return ((float(end_value) / float(start_value)) ** (1 / float(years))) - 1


# Helper function for simple growth (YoY, QoQ)
def calculate_growth(current_value, previous_value):
    if previous_value is None or current_value is None: return None
    if float(previous_value) == 0: return None
    try:
        return (float(current_value) - float(previous_value)) / abs(float(previous_value))
    except (ValueError, TypeError):
        return None


# Helper function to get value from a list of statement dicts (FMP style)
def get_value_from_statement_list(data_list, field, year_offset=0):
    if data_list and len(data_list) > year_offset and data_list[year_offset]:
        return safe_get_float(data_list[year_offset], field)
    return None


# Helper function to get a specific concept value from Finnhub's reported financials
def get_finnhub_concept_value(finnhub_quarterly_reports_data, report_section_key, concept_names_list, quarter_offset=0):
    """
    Extracts a value from Finnhub's quarterly financials structure.

    :param finnhub_quarterly_reports_data: The list of report objects from Finnhub API (e.g., response['data'])
    :param report_section_key: 'ic' (income), 'bs' (balance sheet), 'cf' (cash flow)
    :param concept_names_list: A list of possible 'concept' names to look for (e.g., ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
    :param quarter_offset: 0 for the latest quarter, 1 for the one before, etc.
    :return: The value of the concept if found, else None.
    """
    if not finnhub_quarterly_reports_data or len(finnhub_quarterly_reports_data) <= quarter_offset:
        return None

    report_data = finnhub_quarterly_reports_data[quarter_offset]  # data is typically sorted latest first by Finnhub
    if 'report' not in report_data or report_section_key not in report_data['report']:
        return None

    section_items = report_data['report'][report_section_key]
    if not section_items:
        return None

    for item in section_items:
        if item.get('concept') in concept_names_list or item.get(
                'label') in concept_names_list:  # Check both concept and label
            return safe_get_float(item, 'value')
    return None


class StockAnalyzer:
    def __init__(self, ticker):
        self.ticker = ticker.upper()
        self.finnhub = FinnhubClient()
        self.fmp = FinancialModelingPrepClient()
        self.eodhd = EODHDClient()
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()

        self.db_session = next(get_db_session())
        self.stock_db_entry = None
        self._financial_data_cache = {}  # Unified cache for all financial data

        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"CRITICAL: Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            self._close_session_if_active()
            raise RuntimeError(f"StockAnalyzer for {self.ticker} could not be initialized due to DB/API issues.")

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close()
                logger.debug(f"DB session closed for {self.ticker} in StockAnalyzer.")
            except Exception as e_close:
                logger.warning(f"Error closing session for {self.ticker} in StockAnalyzer: {e_close}")

    def _get_or_create_stock_entry(self):
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} in _get_or_create_stock_entry was inactive. Re-establishing.")
            self._close_session_if_active()
            self.db_session = next(get_db_session())

        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()

        company_name_from_api = None
        industry_from_api = None
        sector_from_api = None
        cik_from_api = None

        profile_fmp_list = self.fmp.get_company_profile(self.ticker)
        time.sleep(1)
        profile_fmp_data = None
        if profile_fmp_list and isinstance(profile_fmp_list, list) and len(profile_fmp_list) > 0 and profile_fmp_list[
            0]:
            profile_fmp_data = profile_fmp_list[0]
            self._financial_data_cache['profile_fmp'] = profile_fmp_data
            company_name_from_api = profile_fmp_data.get('companyName')
            industry_from_api = profile_fmp_data.get('industry')
            sector_from_api = profile_fmp_data.get('sector')
            cik_from_api = profile_fmp_data.get('cik')  # FMP often has CIK
            logger.info(f"Fetched profile from FMP for {self.ticker}.")
        else:
            logger.warning(f"FMP profile fetch failed or empty for {self.ticker}. Trying Finnhub.")
            profile_finnhub = self.finnhub.get_company_profile2(self.ticker)
            time.sleep(1)
            if profile_finnhub:
                self._financial_data_cache['profile_finnhub'] = profile_finnhub
                company_name_from_api = profile_finnhub.get('name')
                industry_from_api = profile_finnhub.get('finnhubIndustry')
                # Finnhub profile doesn't directly provide CIK; it's usually part of 'financialsAsReported' or other filings data.
                logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Failed to fetch profile from FMP and Finnhub for {self.ticker}.")

        if not company_name_from_api:
            company_name_from_api = self.ticker
            logger.info(f"Using ticker '{self.ticker}' as company name due to lack of API data.")

        if not cik_from_api and self.ticker:
            logger.info(f"CIK not found from FMP/Finnhub profile for {self.ticker}. Querying SEC EDGAR.")
            cik_from_api = self.sec_edgar.get_cik_by_ticker(self.ticker)
            time.sleep(0.5)
            if cik_from_api:
                logger.info(f"Fetched CIK {cik_from_api} from SEC EDGAR for {self.ticker}.")
            else:
                logger.warning(f"Could not fetch CIK from SEC EDGAR for {self.ticker}.")

        if not self.stock_db_entry:
            logger.info(f"Stock {self.ticker} not found in DB, creating new entry.")
            self.stock_db_entry = Stock(
                ticker=self.ticker,
                company_name=company_name_from_api,
                industry=industry_from_api,
                sector=sector_from_api,
                cik=cik_from_api
            )
            self.db_session.add(self.stock_db_entry)
            try:
                self.db_session.commit()
                self.db_session.refresh(self.stock_db_entry)
                logger.info(
                    f"Created and refreshed stock entry for {self.ticker} (ID: {self.stock_db_entry.id}). Name: {self.stock_db_entry.company_name}, CIK: {self.stock_db_entry.cik}")
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating stock entry for {self.ticker}: {e}", exc_info=True)
                raise
        else:
            logger.info(
                f"Found existing stock entry for {self.ticker} (ID: {self.stock_db_entry.id}). Current DB CIK: {self.stock_db_entry.cik}")
            updated = False
            if company_name_from_api and self.stock_db_entry.company_name != company_name_from_api:
                logger.info(
                    f"Updating company name for {self.ticker} from '{self.stock_db_entry.company_name}' to '{company_name_from_api}'.")
                self.stock_db_entry.company_name = company_name_from_api
                updated = True
            if industry_from_api and self.stock_db_entry.industry != industry_from_api:
                self.stock_db_entry.industry = industry_from_api
                updated = True
            if sector_from_api and self.stock_db_entry.sector != sector_from_api:
                self.stock_db_entry.sector = sector_from_api
                updated = True
            if cik_from_api and self.stock_db_entry.cik != cik_from_api:
                logger.info(f"Updating CIK for {self.ticker} from '{self.stock_db_entry.cik}' to '{cik_from_api}'.")
                self.stock_db_entry.cik = cik_from_api
                updated = True
            elif not self.stock_db_entry.cik and cik_from_api:
                logger.info(f"Populating missing CIK for {self.ticker} with '{cik_from_api}'.")
                self.stock_db_entry.cik = cik_from_api
                updated = True

            if updated:
                try:
                    self.db_session.commit()
                    self.db_session.refresh(self.stock_db_entry)
                    logger.info(f"Successfully updated stock entry for {self.ticker} in DB.")
                except SQLAlchemyError as e:
                    self.db_session.rollback()
                    logger.error(f"Error updating stock entry for {self.ticker} in DB: {e}")

    def _fetch_financial_statements(self):
        logger.info(f"Fetching financial statements for {self.ticker} for the last {STOCK_FINANCIAL_YEARS} years.")
        # This will store FMP annual statements and Finnhub quarterly reported financials
        statements_cache = {"fmp_income_annual": [], "fmp_balance_annual": [], "fmp_cashflow_annual": [],
                            "finnhub_financials_quarterly_reported": None}

        try:
            # FMP Annual statements (these seem to work on the free tier based on logs)
            income_annual_fmp = self.fmp.get_financial_statements(self.ticker, "income-statement", period="annual",
                                                                  limit=STOCK_FINANCIAL_YEARS)
            time.sleep(1)
            balance_annual_fmp = self.fmp.get_financial_statements(self.ticker, "balance-sheet-statement",
                                                                   period="annual", limit=STOCK_FINANCIAL_YEARS)
            time.sleep(1)
            cashflow_annual_fmp = self.fmp.get_financial_statements(self.ticker, "cash-flow-statement", period="annual",
                                                                    limit=STOCK_FINANCIAL_YEARS)
            time.sleep(1)

            if income_annual_fmp: statements_cache["fmp_income_annual"].extend(income_annual_fmp)
            if balance_annual_fmp: statements_cache["fmp_balance_annual"].extend(balance_annual_fmp)
            if cashflow_annual_fmp: statements_cache["fmp_cashflow_annual"].extend(cashflow_annual_fmp)

            logger.info(f"Fetched from FMP: {len(statements_cache['fmp_income_annual'])} income, "
                        f"{len(statements_cache['fmp_balance_annual'])} balance, "
                        f"{len(statements_cache['fmp_cashflow_annual'])} cashflow annual statements.")

            # Finnhub Quarterly Financials Reported (as replacement for FMP quarterly statements)
            finnhub_quarterly_data = self.finnhub.get_financials_reported(self.ticker, freq="quarterly")
            time.sleep(1)
            if finnhub_quarterly_data and finnhub_quarterly_data.get("data"):
                statements_cache["finnhub_financials_quarterly_reported"] = finnhub_quarterly_data
                logger.info(f"Fetched {len(finnhub_quarterly_data['data'])} quarterly reports from Finnhub.")
            else:
                logger.warning(
                    f"Failed to fetch or parse quarterly financials from Finnhub for {self.ticker}. QoQ analysis might be limited.")
                statements_cache["finnhub_financials_quarterly_reported"] = {
                    "data": []}  # Ensure structure for later access

        except Exception as e:
            logger.warning(
                f"Generic error during financial statements fetch for {self.ticker}: {e}. This may limit trend analysis.",
                exc_info=True)

        self._financial_data_cache['financial_statements'] = statements_cache  # Store under a unified key
        return statements_cache

    def _fetch_key_metrics_and_profile_data(self):
        logger.info(f"Fetching key metrics and profile for {self.ticker}.")

        key_metrics_annual_fmp = self.fmp.get_key_metrics(self.ticker, period="annual", limit=STOCK_FINANCIAL_YEARS + 2)
        time.sleep(1)

        # Attempt FMP quarterly key metrics but expect failure based on logs
        key_metrics_quarterly_fmp = self.fmp.get_key_metrics(self.ticker, period="quarterly", limit=8)
        time.sleep(1)

        self._financial_data_cache['key_metrics_annual_fmp'] = key_metrics_annual_fmp if key_metrics_annual_fmp else []
        if key_metrics_quarterly_fmp is None:  # API call failed (e.g. 403)
            logger.warning(
                f"Failed to fetch FMP quarterly key metrics for {self.ticker} (likely subscription issue). Latest metrics will rely on annual FMP or Finnhub.")
            self._financial_data_cache['key_metrics_quarterly_fmp'] = []
        else:
            self._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp

        # Finnhub basic financials (provides TTM and some snapshot metrics)
        basic_financials_finnhub = self.finnhub.get_basic_financials(self.ticker)
        time.sleep(1)
        self._financial_data_cache[
            'basic_financials_finnhub'] = basic_financials_finnhub if basic_financials_finnhub else {}

        # FMP profile (if not fetched during init)
        if 'profile_fmp' not in self._financial_data_cache or not self._financial_data_cache.get('profile_fmp'):
            profile_fmp_list = self.fmp.get_company_profile(self.ticker)
            time.sleep(1)
            self._financial_data_cache['profile_fmp'] = profile_fmp_list[0] if profile_fmp_list and isinstance(
                profile_fmp_list, list) and profile_fmp_list[0] else {}

        logger.info(
            f"Fetched Key Metrics from FMP (Annual: {len(self._financial_data_cache.get('key_metrics_annual_fmp', []))}, "
            f"Quarterly: {len(self._financial_data_cache.get('key_metrics_quarterly_fmp', []))}). "
            f"Fetched Finnhub Basic Financials: {'successful' if self._financial_data_cache.get('basic_financials_finnhub') else 'failed'}.")

    def _calculate_derived_metrics(self):
        logger.info(f"Calculating derived metrics for {self.ticker}...")
        metrics = {"key_metrics_snapshot": {}}

        # Retrieve cached data
        statements_data = self._financial_data_cache.get('financial_statements', {})
        fmp_income_annual = statements_data.get('fmp_income_annual', [])
        fmp_balance_annual = statements_data.get('fmp_balance_annual', [])
        fmp_cashflow_annual = statements_data.get('fmp_cashflow_annual', [])
        finnhub_quarterly_reports_list = statements_data.get('finnhub_financials_quarterly_reported', {}).get('data',
                                                                                                              [])

        key_metrics_annual_fmp = self._financial_data_cache.get('key_metrics_annual_fmp', [])
        key_metrics_quarterly_fmp = self._financial_data_cache.get('key_metrics_quarterly_fmp',
                                                                   [])  # Likely empty or fails
        basic_fin_finnhub_metrics = self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {})
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})

        # Sort FMP annual statements by date descending (latest first)
        income_annual = sorted([s for s in fmp_income_annual if s], key=lambda x: x.get("date"), reverse=True)
        balance_annual = sorted([s for s in fmp_balance_annual if s], key=lambda x: x.get("date"), reverse=True)
        cashflow_annual = sorted([s for s in fmp_cashflow_annual if s], key=lambda x: x.get("date"), reverse=True)

        # Finnhub quarterly reports are assumed to be sorted latest first by the API
        # If not, they would need sorting here based on year/quarter or endDate.

        latest_km_q_fmp = key_metrics_quarterly_fmp[
            0] if key_metrics_quarterly_fmp else {}  # Prefer FMP quarterly if available
        latest_km_a_fmp = key_metrics_annual_fmp[0] if key_metrics_annual_fmp else {}

        # Valuation Ratios: Prioritize FMP TTM (from quarterly KM if available), then FMP Annual KM, then Finnhub TTM/Annual
        metrics["pe_ratio"] = safe_get_float(latest_km_q_fmp, "peRatioTTM") or \
                              safe_get_float(latest_km_a_fmp, "peRatio") or \
                              safe_get_float(basic_fin_finnhub_metrics, "peTTM") or \
                              safe_get_float(basic_fin_finnhub_metrics, "peAnnual")

        metrics["pb_ratio"] = safe_get_float(latest_km_q_fmp, "priceToBookRatioTTM") or \
                              safe_get_float(latest_km_a_fmp, "pbRatio") or \
                              safe_get_float(basic_fin_finnhub_metrics, "pbAnnual")

        metrics["ps_ratio"] = safe_get_float(latest_km_q_fmp, "priceToSalesRatioTTM") or \
                              safe_get_float(latest_km_a_fmp, "priceSalesRatio") or  # FMP annual uses priceSalesRatio
        safe_get_float(basic_fin_finnhub_metrics, "psTTM") or \
        safe_get_float(basic_fin_finnhub_metrics, "psAnnual")

    metrics["ev_to_sales"] = safe_get_float(latest_km_q_fmp, "enterpriseValueOverRevenueTTM") or \
                             safe_get_float(latest_km_a_fmp, "enterpriseValueOverRevenue")

    metrics["ev_to_ebitda"] = safe_get_float(latest_km_q_fmp, "evToEbitdaTTM") or \
                              safe_get_float(latest_km_a_fmp, "evToEbitda")

    div_yield_fmp_q = safe_get_float(latest_km_q_fmp, "dividendYieldTTM")
    div_yield_fmp_a = safe_get_float(latest_km_a_fmp, "dividendYield")
    div_yield_finnhub = safe_get_float(basic_fin_finnhub_metrics,
                                       "dividendYieldAnnual")  # Finnhub returns percentage as number e.g. 1.5 for 1.5%
    if div_yield_finnhub is not None: div_yield_finnhub /= 100.0  # Convert to decimal
    metrics["dividend_yield"] = div_yield_fmp_q if div_yield_fmp_q is not None else (
        div_yield_fmp_a if div_yield_fmp_a is not None else div_yield_finnhub)

    metrics["key_metrics_snapshot"]["FMP_peRatio"] = metrics["pe_ratio"]  # Example of snapshotting source
    metrics["key_metrics_snapshot"]["FMP_pbRatio"] = metrics["pb_ratio"]

    # Profitability & Solvency from Annual Statements (FMP)
    if income_annual:
        latest_income_a = income_annual[0]
        metrics["eps"] = safe_get_float(latest_income_a, "eps") or safe_get_float(latest_km_a_fmp, "eps")
        metrics["net_profit_margin"] = safe_get_float(latest_income_a, "netProfitMargin")
        metrics["gross_profit_margin"] = safe_get_float(latest_income_a, "grossProfitMargin")
        metrics["operating_profit_margin"] = safe_get_float(latest_income_a, "operatingIncomeRatio")

        ebit = safe_get_float(latest_income_a, "operatingIncome")
        interest_expense = safe_get_float(latest_income_a, "interestExpense")
        if ebit is not None and interest_expense is not None and abs(interest_expense) > 1e-6:
            metrics["interest_coverage_ratio"] = ebit / abs(interest_expense)

    if balance_annual:
        latest_balance_a = balance_annual[0]
        total_equity = safe_get_float(latest_balance_a, "totalStockholdersEquity")
        total_assets = safe_get_float(latest_balance_a, "totalAssets")
        latest_net_income = get_value_from_statement_list(income_annual, "netIncome", 0)

        if total_equity and total_equity != 0 and latest_net_income is not None:
            metrics["roe"] = latest_net_income / total_equity
        if total_assets and total_assets != 0 and latest_net_income is not None:
            metrics["roa"] = latest_net_income / total_assets

        metrics["debt_to_equity"] = safe_get_float(latest_km_a_fmp, "debtToEquity")
        if metrics["debt_to_equity"] is None:
            total_debt_val = safe_get_float(latest_balance_a, "totalDebt")
            if total_debt_val is not None and total_equity and total_equity != 0:
                metrics["debt_to_equity"] = total_debt_val / total_equity

        current_assets = safe_get_float(latest_balance_a, "totalCurrentAssets")
        current_liabilities = safe_get_float(latest_balance_a, "totalCurrentLiabilities")
        if current_assets is not None and current_liabilities is not None and current_liabilities != 0:
            metrics["current_ratio"] = current_assets / current_liabilities

        cash_equivalents = safe_get_float(latest_balance_a, "cashAndCashEquivalents", 0.0)
        short_term_investments = safe_get_float(latest_balance_a, "shortTermInvestments", 0.0)
        accounts_receivable = safe_get_float(latest_balance_a, "netReceivables", 0.0)
        if current_liabilities is not None and current_liabilities != 0:
            quick_assets = (cash_equivalents or 0) + (short_term_investments or 0) + (accounts_receivable or 0)
            metrics["quick_ratio"] = quick_assets / current_liabilities

    ebitda_for_debt_ratio = safe_get_float(latest_km_a_fmp, "ebitda")
    if not ebitda_for_debt_ratio and income_annual:
        ebitda_for_debt_ratio = get_value_from_statement_list(income_annual, "ebitda", 0)
    if ebitda_for_debt_ratio and ebitda_for_debt_ratio != 0 and balance_annual:
        total_debt_val = get_value_from_statement_list(balance_annual, "totalDebt", 0)
        if total_debt_val is not None:
            metrics["debt_to_ebitda"] = total_debt_val / ebitda_for_debt_ratio

    # Growth Rates (YoY from FMP annual)
    metrics["revenue_growth_yoy"] = calculate_growth(
        get_value_from_statement_list(income_annual, "revenue", 0),
        get_value_from_statement_list(income_annual, "revenue", 1))
    metrics["eps_growth_yoy"] = calculate_growth(
        get_value_from_statement_list(income_annual, "eps", 0),
        get_value_from_statement_list(income_annual, "eps", 1))

    # CAGR from FMP annual
    if len(income_annual) >= 3:
        metrics["revenue_growth_cagr_3yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual, "revenue", 0),
            get_value_from_statement_list(income_annual, "revenue", 2), 2)
        metrics["eps_growth_cagr_3yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual, "eps", 0),
            get_value_from_statement_list(income_annual, "eps", 2), 2)
    if len(income_annual) >= 5:
        metrics["revenue_growth_cagr_5yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual, "revenue", 0),
            get_value_from_statement_list(income_annual, "revenue", 4), 4)
        metrics["eps_growth_cagr_5yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual, "eps", 0),
            get_value_from_statement_list(income_annual, "eps", 4), 4)

    # QoQ Revenue Growth from Finnhub quarterly reported financials
    # Common Finnhub concepts for revenue:
    revenue_concepts = ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "TotalRevenues", "NetSales"]
    latest_q_revenue = get_finnhub_concept_value(finnhub_quarterly_reports_list, 'ic', revenue_concepts, 0)
    prev_q_revenue = get_finnhub_concept_value(finnhub_quarterly_reports_list, 'ic', revenue_concepts, 1)

    if latest_q_revenue is not None and prev_q_revenue is not None:
        metrics["revenue_growth_qoq"] = calculate_growth(latest_q_revenue, prev_q_revenue)
        metrics["key_metrics_snapshot"]["finnhub_latest_q_revenue"] = latest_q_revenue
        metrics["key_metrics_snapshot"]["finnhub_prev_q_revenue"] = prev_q_revenue
    else:
        logger.info(
            f"Could not calculate QoQ revenue for {self.ticker} from Finnhub data. Latest Q: {latest_q_revenue}, Prev Q: {prev_q_revenue}")
        metrics["revenue_growth_qoq"] = None

    # Cash Flow Metrics from FMP annual statements and profile
    if cashflow_annual:
        fcf = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
        shares_outstanding_fcf = safe_get_float(profile_fmp, "sharesOutstanding")
        if not shares_outstanding_fcf:
            mkt_cap = safe_get_float(profile_fmp, "mktCap")
            price = safe_get_float(profile_fmp, "price")
            if mkt_cap and price and price != 0: shares_outstanding_fcf = mkt_cap / price

        if fcf is not None and shares_outstanding_fcf and shares_outstanding_fcf != 0:
            metrics["free_cash_flow_per_share"] = fcf / shares_outstanding_fcf
            mkt_cap_for_yield = safe_get_float(profile_fmp, "mktCap")
            if mkt_cap_for_yield and mkt_cap_for_yield != 0:
                metrics["free_cash_flow_yield"] = fcf / mkt_cap_for_yield

        if len(cashflow_annual) >= 3:
            fcf_curr = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
            fcf_prev1 = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 1)
            fcf_prev2 = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 2)
            if all(isinstance(x, (int, float)) for x in [fcf_curr, fcf_prev1, fcf_prev2] if x is not None):
                if fcf_curr > fcf_prev1 > fcf_prev2:
                    metrics["free_cash_flow_trend"] = "Growing"
                elif fcf_curr < fcf_prev1 < fcf_prev2:
                    metrics["free_cash_flow_trend"] = "Declining"
                else:
                    metrics["free_cash_flow_trend"] = "Mixed/Stable"
        else:
            metrics["free_cash_flow_trend"] = "Data N/A (needs 3+ years)"

    if len(balance_annual) >= 3:
        re_curr = get_value_from_statement_list(balance_annual, "retainedEarnings", 0)
        re_prev1 = get_value_from_statement_list(balance_annual, "retainedEarnings", 1)
        re_prev2 = get_value_from_statement_list(balance_annual, "retainedEarnings", 2)
        if all(isinstance(x, (int, float)) for x in [re_curr, re_prev1, re_prev2] if x is not None):
            if re_curr > re_prev1 > re_prev2:
                metrics["retained_earnings_trend"] = "Growing"
            elif re_curr < re_prev1 < re_prev2:
                metrics["retained_earnings_trend"] = "Declining"
            else:
                metrics["retained_earnings_trend"] = "Mixed/Stable"
    else:
        metrics["retained_earnings_trend"] = "Data N/A (needs 3+ years)"

    # ROIC from FMP annual
    if income_annual and balance_annual:
        ebit_roic = get_value_from_statement_list(income_annual, "operatingIncome", 0)
        tax_provision = get_value_from_statement_list(income_annual, "incomeTaxExpense", 0)
        income_before_tax = get_value_from_statement_list(income_annual, "incomeBeforeTax", 0)
        eff_tax_rate = (
                    tax_provision / income_before_tax) if income_before_tax and tax_provision and income_before_tax != 0 else 0.21
        nopat = ebit_roic * (1 - eff_tax_rate) if ebit_roic is not None else None
        total_debt_roic = get_value_from_statement_list(balance_annual, "totalDebt", 0)
        total_equity_roic = get_value_from_statement_list(balance_annual, "totalStockholdersEquity", 0)
        cash_eq_roic = get_value_from_statement_list(balance_annual, "cashAndCashEquivalents", 0) or 0.0
        if total_debt_roic is not None and total_equity_roic is not None:
            invested_capital = total_debt_roic + total_equity_roic - cash_eq_roic
            if nopat is not None and invested_capital is not None and invested_capital != 0:
                metrics["roic"] = nopat / invested_capital

    final_metrics = {}
    for k, v in metrics.items():
        if k == "key_metrics_snapshot":
            final_metrics[k] = {sk: sv for sk, sv in v.items() if
                                sv is not None and not (isinstance(sv, float) and (math.isnan(sv) or math.isinf(sv)))}
        elif isinstance(v, float):
            final_metrics[k] = v if not (math.isnan(v) or math.isinf(v)) else None
        elif v is not None:
            final_metrics[k] = v
        else:
            final_metrics[k] = None

    logger.info(
        f"Calculated metrics for {self.ticker} (excluding snapshot): {{ {', '.join([f'{k}: {v}' for k, v in final_metrics.items() if k != 'key_metrics_snapshot'])} }}")
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
            "start_fcf": None, "fcf_growth_rates_projection": []
        }
    }

    cashflow_annual = self._financial_data_cache.get('financial_statements', {}).get('fmp_cashflow_annual', [])
    cashflow_annual = sorted([s for s in cashflow_annual if s], key=lambda x: x.get("date"), reverse=True)

    profile_fmp = self._financial_data_cache.get('profile_fmp', {})
    calculated_metrics = self._financial_data_cache.get('calculated_metrics', {})

    current_price = safe_get_float(profile_fmp, "price")
    shares_outstanding = safe_get_float(profile_fmp, "sharesOutstanding")
    if not shares_outstanding:
        mkt_cap = safe_get_float(profile_fmp, "mktCap")
        if mkt_cap and current_price and current_price != 0:
            shares_outstanding = mkt_cap / current_price

    if not cashflow_annual or not profile_fmp or current_price is None or shares_outstanding is None or shares_outstanding == 0:
        logger.warning(
            f"Insufficient data for DCF for {self.ticker}: FCF history ({len(cashflow_annual)} years), current price ({current_price}), or shares outstanding ({shares_outstanding}) missing/invalid.")
        return dcf_results

    current_fcf = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
    if current_fcf is None or current_fcf <= 10000:
        logger.warning(
            f"Current FCF for {self.ticker} is {current_fcf}. Simplified DCF requires positive & significant starting FCF.")
        return dcf_results
    dcf_results["dcf_assumptions"]["start_fcf"] = current_fcf

    fcf_growth_hist_3yr = None
    if len(cashflow_annual) >= 4:
        fcf_start_3yr = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 3)
        if fcf_start_3yr and fcf_start_3yr > 0:
            fcf_growth_hist_3yr = calculate_cagr(current_fcf, fcf_start_3yr, 3)

    fcf_growth_initial = fcf_growth_hist_3yr if fcf_growth_hist_3yr is not None \
        else calculated_metrics.get("revenue_growth_cagr_3yr") if calculated_metrics.get(
        "revenue_growth_cagr_3yr") is not None \
        else calculated_metrics.get("revenue_growth_yoy") if calculated_metrics.get("revenue_growth_yoy") is not None \
        else 0.05

    if not isinstance(fcf_growth_initial, (int, float)): fcf_growth_initial = 0.05
    fcf_growth_initial = min(max(fcf_growth_initial, -0.10), 0.20)  # Bound growth

    projected_fcfs = []
    last_projected_fcf = current_fcf
    growth_decline_rate = (fcf_growth_initial - DEFAULT_PERPETUAL_GROWTH_RATE) / float(
        DEFAULT_FCF_PROJECTION_YEARS) if DEFAULT_FCF_PROJECTION_YEARS > 0 else 0

    for i in range(DEFAULT_FCF_PROJECTION_YEARS):
        current_year_growth_rate = max(fcf_growth_initial - (growth_decline_rate * i), DEFAULT_PERPETUAL_GROWTH_RATE)
        projected_fcf = last_projected_fcf * (1 + current_year_growth_rate)
        projected_fcfs.append(projected_fcf)
        last_projected_fcf = projected_fcf
        dcf_results["dcf_assumptions"]["fcf_growth_rates_projection"].append(round(current_year_growth_rate, 4))

    if not projected_fcfs:
        logger.error(f"DCF: No projected FCFs generated for {self.ticker}.")
        return dcf_results

    terminal_fcf_for_calc = projected_fcfs[-1] * (1 + DEFAULT_PERPETUAL_GROWTH_RATE)
    denominator = DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE
    terminal_value = 0
    if denominator <= 1e-6:
        logger.warning(f"DCF for {self.ticker}: Discount rate too close to perpetual growth. TV calc unreliable.")
    else:
        terminal_value = terminal_fcf_for_calc / denominator

    discounted_values_sum = sum(
        fcf_val / ((1 + DEFAULT_DISCOUNT_RATE) ** (i + 1)) for i, fcf_val in enumerate(projected_fcfs))
    discounted_terminal_value = terminal_value / ((1 + DEFAULT_DISCOUNT_RATE) ** DEFAULT_FCF_PROJECTION_YEARS)
    intrinsic_equity_value = discounted_values_sum + discounted_terminal_value

    if shares_outstanding != 0:
        intrinsic_value_per_share = intrinsic_equity_value / shares_outstanding
        dcf_results["dcf_intrinsic_value"] = intrinsic_value_per_share
        if current_price and current_price != 0:
            dcf_results["dcf_upside_percentage"] = (intrinsic_value_per_share - current_price) / current_price

    logger.info(
        f"DCF for {self.ticker}: Intrinsic Value/Share: {dcf_results.get('dcf_intrinsic_value', 'N/A')}, Upside: {dcf_results.get('dcf_upside_percentage', 'N/A')}")
    self._financial_data_cache['dcf_results'] = dcf_results
    return dcf_results


def _fetch_and_summarize_10k(self):
    logger.info(f"Fetching and attempting to summarize latest 10-K for {self.ticker}")
    summary_results = {"qualitative_sources_summary": {}}

    if not self.stock_db_entry or not self.stock_db_entry.cik:
        logger.warning(f"No CIK found for {self.ticker} in DB. Cannot fetch 10-K from EDGAR directly.")
        return summary_results

    filing_url = self.sec_edgar.get_filing_document_url(cik=self.stock_db_entry.cik, form_type="10-K")
    time.sleep(0.5)
    if not filing_url:
        filing_url = self.sec_edgar.get_filing_document_url(cik=self.stock_db_entry.cik, form_type="10-K/A")
        time.sleep(0.5)

    if not filing_url:
        logger.warning(
            f"Could not retrieve 10-K (or 10-K/A) filing URL for {self.ticker} (CIK: {self.stock_db_entry.cik})")
        return summary_results

    ten_k_text_content = self.sec_edgar.get_filing_text(filing_url)
    if not ten_k_text_content:
        logger.warning(f"Failed to fetch 10-K text content from {filing_url}")
        return summary_results

    logger.info(f"Fetched 10-K text (length: {len(ten_k_text_content)}) for {self.ticker}. Extracting sections.")
    extracted_sections = extract_S1_text_sections(ten_k_text_content, TEN_K_KEY_SECTIONS)

    company_name = self.stock_db_entry.company_name or self.ticker
    summary_results["qualitative_sources_summary"]["10k_filing_url_used"] = filing_url

    business_text = extracted_sections.get("business", "")
    if business_text:
        prompt_ctx = f"This is 'Business' section (Item 1) from 10-K for {company_name} ({self.ticker}). Summarize core operations, products/services, revenue streams, target markets."
        summary = self.gemini.summarize_text_with_context(business_text, prompt_ctx, MAX_10K_SECTION_LENGTH_FOR_GEMINI)
        if not summary.startswith("Error:"): summary_results["business_summary"] = summary
        summary_results["qualitative_sources_summary"]["business_10k_source_length"] = len(business_text)
        time.sleep(3)

    risk_text = extracted_sections.get("risk_factors", "")
    if risk_text:
        prompt_ctx = f"This is 'Risk Factors' (Item 1A) from 10-K for {company_name} ({self.ticker}). Summarize 3-5 most material risks."
        summary = self.gemini.summarize_text_with_context(risk_text, prompt_ctx, MAX_10K_SECTION_LENGTH_FOR_GEMINI)
        if not summary.startswith("Error:"): summary_results["risk_factors_summary"] = summary
        summary_results["qualitative_sources_summary"]["risk_factors_10k_source_length"] = len(risk_text)
        time.sleep(3)

    mda_text = extracted_sections.get("mda", "")
    if mda_text:
        prompt_ctx = f"This is MD&A (Item 7) from 10-K for {company_name} ({self.ticker}). Summarize key performance drivers, financial condition, liquidity, capital resources, management's outlook."
        summary = self.gemini.summarize_text_with_context(mda_text, prompt_ctx, MAX_10K_SECTION_LENGTH_FOR_GEMINI)
        if not summary.startswith("Error:"): summary_results["management_assessment_summary"] = summary
        summary_results["qualitative_sources_summary"]["mda_10k_source_length"] = len(mda_text)
        time.sleep(3)

    comp_landscape_input = (summary_results.get("business_summary", "") + "\n" + summary_results.get(
        "management_assessment_summary", ""))[:MAX_GEMINI_TEXT_LENGTH].strip()
    if comp_landscape_input:
        comp_prompt = (f"Based on business & MD&A for {company_name} ({self.ticker}): \"{comp_landscape_input}\"\n"
                       f"Describe its competitive landscape, key competitors, and {company_name}'s competitive positioning.")
        comp_summary = self.gemini.generate_text(comp_prompt);
        time.sleep(3)
        if not comp_summary.startswith("Error:"): summary_results["competitive_landscape_summary"] = comp_summary
        summary_results["qualitative_sources_summary"][
            "competitive_landscape_context"] = "Derived from 10-K Business/MD&A summaries."

    moat_input = (summary_results.get("business_summary", "") + "\n" + summary_results.get(
        "competitive_landscape_summary", "") + "\n" + summary_results.get("risk_factors_summary", ""))[
                 :MAX_GEMINI_TEXT_LENGTH].strip()
    if moat_input and not summary_results.get("economic_moat_summary"):
        moat_prompt = (f"Based on info for {company_name} ({self.ticker}): \"{moat_input}\"\n"
                       f"Analyze its primary economic moats (e.g., brand, network effects, switching costs, IP, cost advantages). Provide concise summary.")
        moat_summary = self.gemini.generate_text(moat_prompt);
        time.sleep(3)
        if not moat_summary.startswith("Error:"): summary_results["economic_moat_summary"] = moat_summary

    industry_input = (summary_results.get("business_summary", "") + "\n" + (
                self.stock_db_entry.industry or "") + "\n" + (self.stock_db_entry.sector or ""))[
                     :MAX_GEMINI_TEXT_LENGTH].strip()
    if industry_input and not summary_results.get("industry_trends_summary"):
        industry_prompt = (
            f"Company: {company_name} in '{self.stock_db_entry.industry}' industry, '{self.stock_db_entry.sector}' sector.\n"
            f"Context (from 10-K Business Summary): \"{summary_results.get('business_summary', 'N/A')}\"\n"
            f"Analyze current key trends, opportunities, and challenges for this industry/sector. How is {company_name} positioned?")
        industry_summary = self.gemini.generate_text(industry_prompt);
        time.sleep(3)
        if not industry_summary.startswith("Error:"): summary_results["industry_trends_summary"] = industry_summary

    logger.info(f"10-K based qualitative summaries generated for {self.ticker}.")
    self._financial_data_cache['10k_summaries'] = summary_results
    return summary_results


def _determine_investment_thesis(self):
    logger.info(f"Synthesizing investment thesis for {self.ticker}...")
    metrics = self._financial_data_cache.get('calculated_metrics', {})
    qual_summaries = self._financial_data_cache.get('10k_summaries', {})
    dcf_results = self._financial_data_cache.get('dcf_results', {})
    company_profile = self._financial_data_cache.get('profile_fmp', {})

    company_name = self.stock_db_entry.company_name or self.ticker
    industry = self.stock_db_entry.industry or "N/A"
    sector = self.stock_db_entry.sector or "N/A"

    prompt = f"Company: {company_name} ({self.ticker})\nIndustry: {industry}, Sector: {sector}\n\n"
    prompt += "Key Financial Metrics (approximate values):\n"
    metrics_for_prompt = {
        "P/E Ratio": metrics.get("pe_ratio"), "P/B Ratio": metrics.get("pb_ratio"),
        "P/S Ratio": metrics.get("ps_ratio"), "EV/Sales": metrics.get("ev_to_sales"),
        "EV/EBITDA": metrics.get("ev_to_ebitda"), "Dividend Yield": metrics.get("dividend_yield"),
        "ROE": metrics.get("roe"), "ROIC": metrics.get("roic"),
        "Debt/Equity": metrics.get("debt_to_equity"), "Debt/EBITDA": metrics.get("debt_to_ebitda"),
        "Revenue Growth YoY": metrics.get("revenue_growth_yoy"),
        "Revenue Growth QoQ": metrics.get("revenue_growth_qoq"),
        "Revenue Growth CAGR 3Yr": metrics.get("revenue_growth_cagr_3yr"),
        "EPS Growth YoY": metrics.get("eps_growth_yoy"),
        "Net Profit Margin": metrics.get("net_profit_margin"),
        "Gross Profit Margin": metrics.get("gross_profit_margin"),
        "FCF Yield": metrics.get("free_cash_flow_yield"),
        "FCF Trend": metrics.get("free_cash_flow_trend")
    }
    for name, val in metrics_for_prompt.items():
        if val is not None:
            if isinstance(val, float):
                if name.endswith("Yield") or "Growth" in name or "Margin" in name or name in ["ROE", "ROIC"]:
                    val_str = f"{val:.2%}"
                else:
                    val_str = f"{val:.2f}"
            else:
                val_str = str(val)
            prompt += f"- {name}: {val_str}\n"

    dcf_val = dcf_results.get("dcf_intrinsic_value");
    dcf_upside = dcf_results.get("dcf_upside_percentage")
    current_price = company_profile.get("price")
    if dcf_val is not None: prompt += f"\nDCF Intrinsic Value per Share: {dcf_val:.2f}\n"
    if dcf_upside is not None: prompt += f"DCF Upside: {dcf_upside:.2%}\n"
    if current_price is not None: prompt += f"Current Stock Price: {current_price:.2f}\n\n"

    prompt += "Qualitative Summary (derived from 10-K, Profile, AI analysis):\n"
    for key, text_val in qual_summaries.items():
        if key != "qualitative_sources_summary" and text_val and isinstance(text_val, str):
            prompt += f"- {key.replace('_', ' ').title()}: {text_val[:300]}...\n"
    prompt += "\n"

    prompt += ("Instructions for AI:\n"
               "1. Provide a comprehensive investment thesis (2-4 paragraphs).\n"
               "2. State Investment Decision: 'Strong Buy', 'Buy', 'Hold', 'Monitor for Entry', 'Reduce', 'Sell', or 'Avoid'.\n"
               "3. Suggest Investment Strategy Type: 'Deep Value', 'Value', 'GARP', 'Growth', 'Aggressive Growth', 'Dividend Growth', 'Special Situation', 'Speculative'.\n"
               "4. Indicate Confidence Level: 'High', 'Medium', or 'Low'.\n"
               "5. Detail Key Reasoning (bullet points): Valuation, Financial Health, Growth, Moat, Risks, Management.\n"
               "Be objective, balanced. Structure with headings: 'Investment Thesis:', 'Investment Decision:', etc.")

    final_prompt = prompt[:MAX_GEMINI_TEXT_LENGTH]
    if len(prompt) > MAX_GEMINI_TEXT_LENGTH:
        logger.warning(f"Thesis prompt for {self.ticker} truncated to {MAX_GEMINI_TEXT_LENGTH} chars.")

    ai_response = self.gemini.generate_text(final_prompt);
    time.sleep(3)

    if ai_response.startswith("Error:"):
        logger.error(f"Gemini failed thesis for {self.ticker}: {ai_response}")
        return {"investment_decision": "AI Error", "reasoning": ai_response, "investment_thesis_full": ai_response}

    parsed_thesis = {};
    current_section_key = None
    section_map = {"investment thesis:": "investment_thesis_full", "investment decision:": "investment_decision",
                   "strategy type:": "strategy_type", "confidence level:": "confidence_level",
                   "key reasoning:": "reasoning"}
    collected_lines = {key: [] for key in section_map.values()}
    for line_original in ai_response.split('\n'):
        line_stripped_lower = line_original.strip().lower();
        found_new_section = False
        for header_lower, key_name in section_map.items():
            if line_stripped_lower.startswith(header_lower):
                current_section_key = key_name
                content_on_header = line_original.strip()[len(header_lower):].strip()
                if content_on_header: collected_lines[current_section_key].append(content_on_header)
                found_new_section = True;
                break
        if not found_new_section and current_section_key: collected_lines[current_section_key].append(line_original)
    for key, lines_list in collected_lines.items(): parsed_thesis[key] = "\n".join(lines_list).strip() or "Not found."

    if parsed_thesis.get("investment_decision", "").startswith("Not found"): parsed_thesis[
        "investment_decision"] = "Review AI Output"
    if parsed_thesis.get("reasoning", "").startswith("Not found"): parsed_thesis["reasoning"] = ai_response
    if parsed_thesis.get("investment_thesis_full", "").startswith("Not found"): parsed_thesis[
        "investment_thesis_full"] = ai_response

    logger.info(f"Generated investment thesis for {self.ticker}. Decision: {parsed_thesis.get('investment_decision')}")
    return parsed_thesis


def analyze(self):
    logger.info(f"Full analysis pipeline started for {self.ticker}...")
    final_analysis_data = {}
    try:
        if not self.stock_db_entry:
            logger.error(f"Stock entry for {self.ticker} not properly initialized. Aborting analysis.")
            return None
        self._ensure_stock_db_entry_is_bound()

        self._fetch_financial_statements()
        self._fetch_key_metrics_and_profile_data()
        calculated_metrics = self._calculate_derived_metrics();
        final_analysis_data.update(calculated_metrics)
        dcf_results = self._perform_dcf_analysis();
        final_analysis_data.update(dcf_results)
        qual_summaries = self._fetch_and_summarize_10k();
        final_analysis_data.update(qual_summaries)
        investment_thesis_parts = self._determine_investment_thesis();
        final_analysis_data.update(investment_thesis_parts)

        analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id, analysis_date=datetime.now(timezone.utc))
        model_fields = [c.key for c in StockAnalysis.__table__.columns if
                        c.key not in ['id', 'stock_id', 'analysis_date']]
        for field in model_fields:
            if field in final_analysis_data:
                value_to_set = final_analysis_data[field]
                target_type = getattr(StockAnalysis, field).type.python_type
                if target_type == float:
                    if isinstance(value_to_set, str):
                        try:
                            value_to_set = float(value_to_set)
                        except ValueError:
                            value_to_set = None
                    if isinstance(value_to_set, float) and (math.isnan(value_to_set) or math.isinf(value_to_set)):
                        value_to_set = None
                elif target_type == dict and not isinstance(value_to_set, dict):
                    value_to_set = None
                setattr(analysis_entry, field, value_to_set)

        self.db_session.add(analysis_entry)
        self.stock_db_entry.last_analysis_date = analysis_entry.analysis_date
        self.db_session.commit()
        logger.info(f"Successfully analyzed and saved stock: {self.ticker} (Analysis ID: {analysis_entry.id})")
        return analysis_entry
    except RuntimeError as r_err:
        logger.critical(f"Runtime error during analysis for {self.ticker}: {r_err}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"CRITICAL error during full analysis pipeline for {self.ticker}: {e}", exc_info=True)
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.rollback(); logger.info(f"Rolled back transaction for {self.ticker}.")
            except Exception as e_rb:
                logger.error(f"Error during rollback for {self.ticker}: {e_rb}")
        return None
    finally:
        self._close_session_if_active()


def _ensure_stock_db_entry_is_bound(self):
    if not self.db_session.is_active:
        logger.warning(f"Session for {self.ticker} INACTIVE before binding. Re-establishing.")
        self._close_session_if_active()
        self.db_session = next(get_db_session())
        try:
            self._get_or_create_stock_entry()  # This will re-fetch or re-create self.stock_db_entry with new session
        except Exception as e_recreate:
            logger.critical(f"Failed to re-init stock_db_entry for {self.ticker} after session fault: {e_recreate}")
            raise RuntimeError(f"Failed to bind stock {self.ticker} to active session.") from e_recreate
        if not self.stock_db_entry: raise RuntimeError(f"Stock entry for {self.ticker} None after re-init attempt.")
        return

    if self.stock_db_entry is None:
        logger.critical(
            f"CRITICAL: self.stock_db_entry is None for {self.ticker} before binding. Analysis cannot proceed.")
        raise RuntimeError(f"Stock entry for {self.ticker} is None.")

    instance_state = sa_inspect(self.stock_db_entry)
    if not instance_state.session or instance_state.session is not self.db_session:
        obj_id_log = self.stock_db_entry.id if instance_state.has_identity else 'Transient'
        logger.warning(f"Stock {self.ticker} (ID: {obj_id_log}) DETACHED or bound to DIFFERENT session. Merging.")
        try:
            self.stock_db_entry = self.db_session.merge(self.stock_db_entry)
            logger.info(
                f"Successfully merged/re-associated stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session.")
        except Exception as e_merge:
            logger.error(f"Failed to merge stock {self.ticker}: {e_merge}. Re-fetching.", exc_info=True)
            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if re_fetched_stock:
                self.stock_db_entry = re_fetched_stock
            else:
                raise RuntimeError(f"Failed to bind stock {self.ticker} to session after merge fail and re-fetch fail.")


if __name__ == '__main__':
    from database import init_db

    # init_db()

    logger.info("Starting standalone stock analysis test...")
    tickers_to_test = ["AAPL", "MSFT"]  # Reduced for brevity

    for ticker_symbol in tickers_to_test:
        analysis_result_obj = None
        try:
            logger.info(f"--- Analyzing {ticker_symbol} ---")
            analyzer_instance = StockAnalyzer(ticker=ticker_symbol)
            analysis_result_obj = analyzer_instance.analyze()

            if analysis_result_obj:
                logger.info(
                    f"Analysis for {analysis_result_obj.stock.ticker} completed. Decision: {analysis_result_obj.investment_decision}, Confidence: {analysis_result_obj.confidence_level}")
                if analysis_result_obj.dcf_intrinsic_value is not None:
                    logger.info(
                        f"DCF Value: {analysis_result_obj.dcf_intrinsic_value:.2f}, Upside: {analysis_result_obj.dcf_upside_percentage:.2% if analysis_result_obj.dcf_upside_percentage is not None else 'N/A'}")
                else:
                    logger.info("DCF analysis did not yield an intrinsic value.")
                logger.info(f"Reasoning highlights: {str(analysis_result_obj.reasoning)[:200]}...")
                logger.info(
                    f"QoQ Revenue Growth: {analysis_result_obj.revenue_growth_qoq if analysis_result_obj.revenue_growth_qoq is not None else 'N/A'}")
            else:
                logger.error(f"Stock analysis pipeline FAILED for {ticker_symbol} (returned None).")
        except RuntimeError as rt_err:
            logger.error(f"Could not initialize or run StockAnalyzer for {ticker_symbol}: {rt_err}")
        except Exception as e_main:
            logger.error(f"Unhandled error analyzing {ticker_symbol} in __main__: {e_main}", exc_info=True)
        finally:
            logger.info(f"--- Finished processing {ticker_symbol} ---")
            if analysis_result_obj is None: logger.info(f"No analysis result object for {ticker_symbol}")
            time.sleep(10)