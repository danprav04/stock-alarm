# stock_analyzer.py
import pandas as pd
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone, timedelta
import math  # For DCF calculations
import time  # For API courtesy delays

from api_clients import (
    FinnhubClient, FinancialModelingPrepClient,
    EODHDClient, GeminiAPIClient, SECEDGARClient, extract_S1_text_sections  # Added SECEDGARClient and helper
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
    if start_value == 0: return None  # Avoid division by zero
    # Handle cases where start_value might be negative (e.g. for EPS)
    if start_value < 0:
        if end_value > 0:  # From loss to profit, CAGR is not straightforwardly meaningful
            return None
        elif end_value < 0:  # Both negative, growth of negative numbers (lessening loss is positive)
            return -((end_value / start_value) ** (
                        1 / years) - 1) if end_value != 0 else None  # avoid division by zero if end_value is 0
        else:  # end_value is 0 from negative start
            return 1.0  # 100% growth to zero loss
    if end_value < 0 and start_value > 0:  # From profit to loss
        return None  # Meaningful positive CAGR not possible

    # Standard CAGR for positive start/end values
    return ((end_value / start_value) ** (1 / years)) - 1


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
        self._financial_data_cache = {}

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

        profile_fmp_list = self.fmp.get_company_profile(self.ticker)  # API Call
        profile_fmp_data = None
        if profile_fmp_list and isinstance(profile_fmp_list, list) and profile_fmp_list[0]:
            profile_fmp_data = profile_fmp_list[0]
            self._financial_data_cache['profile_fmp'] = profile_fmp_data
            company_name_from_api = profile_fmp_data.get('companyName')
            industry_from_api = profile_fmp_data.get('industry')
            sector_from_api = profile_fmp_data.get('sector')
            cik_from_api = profile_fmp_data.get('cik')
            logger.info(f"Fetched profile from FMP for {self.ticker}.")
        else:
            logger.warning(f"FMP profile fetch failed or empty for {self.ticker}. Trying Finnhub.")
            profile_finnhub = self.finnhub.get_company_profile2(self.ticker)  # API Call
            if profile_finnhub:
                self._financial_data_cache['profile_finnhub'] = profile_finnhub
                company_name_from_api = profile_finnhub.get('name')
                industry_from_api = profile_finnhub.get('finnhubIndustry')
                logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Failed to fetch profile from FMP and Finnhub for {self.ticker}.")

        if not company_name_from_api:
            company_name_from_api = self.ticker
            logger.info(f"Using ticker '{self.ticker}' as company name due to lack of API data.")

        if not cik_from_api and self.ticker:
            logger.info(f"CIK not found from FMP/Finnhub profile for {self.ticker}. Querying SEC EDGAR.")
            cik_from_api = self.sec_edgar.get_cik_by_ticker(self.ticker)  # API Call
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
        statements = {"income": [], "balance": [], "cashflow": []}

        try:
            income_annual_fmp = self.fmp.get_financial_statements(self.ticker, "income-statement", period="annual",
                                                                  limit=STOCK_FINANCIAL_YEARS)
            balance_annual_fmp = self.fmp.get_financial_statements(self.ticker, "balance-sheet-statement",
                                                                   period="annual", limit=STOCK_FINANCIAL_YEARS)
            cashflow_annual_fmp = self.fmp.get_financial_statements(self.ticker, "cash-flow-statement", period="annual",
                                                                    limit=STOCK_FINANCIAL_YEARS)

            if income_annual_fmp: statements["income"].extend(income_annual_fmp)
            if balance_annual_fmp: statements["balance"].extend(balance_annual_fmp)
            if cashflow_annual_fmp: statements["cashflow"].extend(cashflow_annual_fmp)

            income_quarterly_fmp = self.fmp.get_financial_statements(self.ticker, "income-statement",
                                                                     period="quarterly", limit=8)
            if income_quarterly_fmp is None:  # API call failed (e.g. 403)
                logger.warning(
                    f"Failed to fetch FMP quarterly income statements for {self.ticker} (likely subscription issue). QoQ analysis will be limited.")
                self._financial_data_cache['income_quarterly_fmp'] = []
            else:
                self._financial_data_cache['income_quarterly_fmp'] = income_quarterly_fmp

            logger.info(
                f"Fetched from FMP: {len(statements['income'])} income, {len(statements['balance'])} balance, {len(statements['cashflow'])} cashflow annual statements. {len(self._financial_data_cache.get('income_quarterly_fmp', []))} quarterly income statements.")

        except Exception as e:
            logger.warning(
                f"Generic error during FMP financial statements fetch for {self.ticker}: {e}. This may limit trend analysis.",
                exc_info=True)

        self._financial_data_cache['financial_statements'] = statements
        return statements

    def _fetch_key_metrics_and_profile_data(self):
        logger.info(f"Fetching key metrics and profile for {self.ticker}.")

        key_metrics_annual_fmp = self.fmp.get_key_metrics(self.ticker, period="annual", limit=STOCK_FINANCIAL_YEARS + 2)
        key_metrics_quarterly_fmp = self.fmp.get_key_metrics(self.ticker, period="quarterly", limit=8)

        self._financial_data_cache['key_metrics_annual_fmp'] = key_metrics_annual_fmp or []
        if key_metrics_quarterly_fmp is None:  # API call failed (e.g. 403)
            logger.warning(
                f"Failed to fetch FMP quarterly key metrics for {self.ticker} (likely subscription issue). Latest metrics might rely on annual or Finnhub.")
            self._financial_data_cache['key_metrics_quarterly_fmp'] = []
        else:
            self._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp

        basic_financials_finnhub = self.finnhub.get_basic_financials(self.ticker)
        self._financial_data_cache['basic_financials_finnhub'] = basic_financials_finnhub or {}

        if 'profile_fmp' not in self._financial_data_cache or not self._financial_data_cache.get(
                'profile_fmp'):  # If not fetched during init
            profile_fmp_list = self.fmp.get_company_profile(self.ticker)
            self._financial_data_cache['profile_fmp'] = profile_fmp_list[0] if profile_fmp_list and isinstance(
                profile_fmp_list, list) else {}

        logger.info(
            f"Fetched Key Metrics from FMP (Annual: {len(self._financial_data_cache['key_metrics_annual_fmp'])}, Quarterly: {len(self._financial_data_cache['key_metrics_quarterly_fmp'])}). Fetched Finnhub Basic Financials.")

    def _calculate_derived_metrics(self):
        logger.info(f"Calculating derived metrics for {self.ticker}...")
        metrics = {"key_metrics_snapshot": {}}
        statements = self._financial_data_cache.get('financial_statements',
                                                    {"income": [], "balance": [], "cashflow": []})
        key_metrics_annual = self._financial_data_cache.get('key_metrics_annual_fmp', [])
        key_metrics_quarterly = self._financial_data_cache.get('key_metrics_quarterly_fmp', [])
        basic_fin_finnhub_data = self._financial_data_cache.get('basic_financials_finnhub', {})
        basic_fin_finnhub = basic_fin_finnhub_data.get('metric',
                                                       {}) if basic_fin_finnhub_data else {}  # Ensure 'metric' key exists
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})

        latest_km_q = key_metrics_quarterly[0] if key_metrics_quarterly else {}
        latest_km_a = key_metrics_annual[0] if key_metrics_annual else {}

        metrics["pe_ratio"] = safe_get_float(latest_km_q, "peRatioTTM") or safe_get_float(latest_km_a,
                                                                                          "peRatio") or safe_get_float(
            basic_fin_finnhub, "peTTM") or safe_get_float(basic_fin_finnhub, "peAnnual")
        metrics["pb_ratio"] = safe_get_float(latest_km_q, "priceToBookRatioTTM") or safe_get_float(latest_km_a,
                                                                                                   "pbRatio") or safe_get_float(
            basic_fin_finnhub, "pbAnnual")
        metrics["ps_ratio"] = safe_get_float(latest_km_q, "priceToSalesRatioTTM") or safe_get_float(latest_km_a,
                                                                                                    "priceSalesRatio")
        metrics["ev_to_sales"] = safe_get_float(latest_km_q, "enterpriseValueOverRevenueTTM") or safe_get_float(
            latest_km_a, "enterpriseValueOverRevenue")
        metrics["ev_to_ebitda"] = safe_get_float(latest_km_q, "evToEbitdaTTM") or safe_get_float(latest_km_a,
                                                                                                 "evToEbitda")

        div_yield_fmp_q = safe_get_float(latest_km_q, "dividendYieldTTM")
        div_yield_fmp_a = safe_get_float(latest_km_a, "dividendYield")
        div_yield_finnhub_val = safe_get_float(basic_fin_finnhub, "dividendYieldAnnual")
        if div_yield_finnhub_val is not None: div_yield_finnhub_val = div_yield_finnhub_val / 100
        metrics["dividend_yield"] = div_yield_fmp_q or div_yield_fmp_a or div_yield_finnhub_val

        metrics["key_metrics_snapshot"]["FMP_peRatioTTM"] = metrics["pe_ratio"]
        metrics["key_metrics_snapshot"]["FMP_pbRatioTTM"] = metrics["pb_ratio"]

        income_annual = sorted([s for s in statements.get("income", []) if s], key=lambda x: x.get("date"),
                               reverse=True)
        balance_annual = sorted([s for s in statements.get("balance", []) if s], key=lambda x: x.get("date"),
                                reverse=True)
        cashflow_annual = sorted([s for s in statements.get("cashflow", []) if s], key=lambda x: x.get("date"),
                                 reverse=True)

        if income_annual:
            latest_income_a = income_annual[0]
            metrics["eps"] = safe_get_float(latest_income_a, "eps") or safe_get_float(latest_km_a, "eps")
            metrics["net_profit_margin"] = safe_get_float(latest_income_a, "netProfitMargin")
            metrics["gross_profit_margin"] = safe_get_float(latest_income_a, "grossProfitMargin")
            metrics["operating_profit_margin"] = safe_get_float(latest_income_a, "operatingIncomeRatio")

            ebit = safe_get_float(latest_income_a, "operatingIncome")
            interest_expense = safe_get_float(latest_income_a, "interestExpense")
            if ebit is not None and interest_expense is not None and abs(
                    interest_expense) > 1e-6:  # Avoid division by near-zero
                metrics["interest_coverage_ratio"] = ebit / abs(interest_expense)

        if balance_annual:
            latest_balance_a = balance_annual[0]
            total_equity = safe_get_float(latest_balance_a, "totalStockholdersEquity")
            total_assets = safe_get_float(latest_balance_a, "totalAssets")
            latest_net_income = safe_get_float(income_annual[0], "netIncome") if income_annual else None

            if total_equity and total_equity != 0 and latest_net_income is not None:
                metrics["roe"] = latest_net_income / total_equity

            if total_assets and total_assets != 0 and latest_net_income is not None:
                metrics["roa"] = latest_net_income / total_assets

            metrics["debt_to_equity"] = safe_get_float(latest_balance_a, "debtToEquity") or safe_get_float(latest_km_a,
                                                                                                           "debtToEquity")
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
                metrics["quick_ratio"] = (
                                                     cash_equivalents + short_term_investments + accounts_receivable) / current_liabilities

        ebitda_for_debt_ratio = safe_get_float(latest_km_a, "ebitda")
        if not ebitda_for_debt_ratio and income_annual:
            ebitda_for_debt_ratio = safe_get_float(income_annual[0], "ebitda")

        if ebitda_for_debt_ratio and ebitda_for_debt_ratio != 0 and balance_annual:
            total_debt_val = safe_get_float(balance_annual[0], "totalDebt")
            if total_debt_val is not None:
                metrics["debt_to_ebitda"] = total_debt_val / ebitda_for_debt_ratio

        def get_value_from_statement_list(data_list, field, year_offset=0):  # More robust
            if data_list and len(data_list) > year_offset and data_list[year_offset]:
                return safe_get_float(data_list[year_offset], field)
            return None

        metrics["revenue_growth_yoy"] = calculate_growth(get_value_from_statement_list(income_annual, "revenue", 0),
                                                         get_value_from_statement_list(income_annual, "revenue", 1))
        metrics["eps_growth_yoy"] = calculate_growth(get_value_from_statement_list(income_annual, "eps", 0),
                                                     get_value_from_statement_list(income_annual, "eps", 1))

        if len(income_annual) >= 3:
            metrics["revenue_growth_cagr_3yr"] = calculate_cagr(
                get_value_from_statement_list(income_annual, "revenue", 0),
                get_value_from_statement_list(income_annual, "revenue", 2), 2)
            metrics["eps_growth_cagr_3yr"] = calculate_cagr(get_value_from_statement_list(income_annual, "eps", 0),
                                                            get_value_from_statement_list(income_annual, "eps", 2), 2)
        if len(income_annual) >= 5:  # Ensure 5 actual years of data, index 4 means 5th year
            metrics["revenue_growth_cagr_5yr"] = calculate_cagr(
                get_value_from_statement_list(income_annual, "revenue", 0),
                get_value_from_statement_list(income_annual, "revenue", 4), 4)
            metrics["eps_growth_cagr_5yr"] = calculate_cagr(get_value_from_statement_list(income_annual, "eps", 0),
                                                            get_value_from_statement_list(income_annual, "eps", 4), 4)

        income_quarterly = self._financial_data_cache.get('income_quarterly_fmp', [])
        if len(income_quarterly) >= 2:
            metrics["revenue_growth_qoq"] = calculate_growth(
                get_value_from_statement_list(income_quarterly, "revenue", 0),
                get_value_from_statement_list(income_quarterly, "revenue", 1))

        if cashflow_annual:
            fcf = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)

            # Get shares outstanding: FMP profile "sharesOutstanding" or mktCap / price
            shares_outstanding_fcf = safe_get_float(profile_fmp, "sharesOutstanding")
            if not shares_outstanding_fcf:
                mkt_cap = safe_get_float(profile_fmp, "mktCap")
                price = safe_get_float(profile_fmp, "price")
                if mkt_cap and price and price != 0:
                    shares_outstanding_fcf = mkt_cap / price

            if fcf is not None and shares_outstanding_fcf and shares_outstanding_fcf != 0:
                metrics["free_cash_flow_per_share"] = fcf / shares_outstanding_fcf
                mkt_cap_for_yield = safe_get_float(profile_fmp, "mktCap")
                if mkt_cap_for_yield and mkt_cap_for_yield != 0:
                    metrics["free_cash_flow_yield"] = fcf / mkt_cap_for_yield

            if len(cashflow_annual) >= 3:
                fcf_curr = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
                fcf_prev1 = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 1)
                fcf_prev2 = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 2)
                if all(isinstance(x, (int, float)) for x in [fcf_curr, fcf_prev1, fcf_prev2] if
                       x is not None):  # Check for None too
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

        if income_annual and balance_annual:
            ebit = get_value_from_statement_list(income_annual, "operatingIncome", 0)
            tax_provision = get_value_from_statement_list(income_annual, "incomeTaxExpense", 0)
            income_before_tax = get_value_from_statement_list(income_annual, "incomeBeforeTax", 0)

            effective_tax_rate = (
                        tax_provision / income_before_tax) if income_before_tax and tax_provision and income_before_tax != 0 else 0.21
            nopat = ebit * (1 - effective_tax_rate) if ebit is not None else None

            total_debt = get_value_from_statement_list(balance_annual, "totalDebt", 0)
            total_equity = get_value_from_statement_list(balance_annual, "totalStockholdersEquity", 0)
            cash_and_equivalents = get_value_from_statement_list(balance_annual, "cashAndCashEquivalents", 0) or 0.0

            if total_debt is not None and total_equity is not None:  # Ensure they are not None
                invested_capital = total_debt + total_equity - cash_and_equivalents
                if nopat is not None and invested_capital is not None and invested_capital != 0:
                    metrics["roic"] = nopat / invested_capital

        final_metrics = {}
        for k, v in metrics.items():
            if k == "key_metrics_snapshot":
                final_metrics[k] = {sk: sv for sk, sv in v.items() if sv is not None and not (
                            isinstance(sv, float) and (math.isnan(sv) or math.isinf(sv)))}
            elif isinstance(v, (int, float)) and not math.isnan(v) and not math.isinf(v):
                final_metrics[k] = v
            elif isinstance(v, str) and v != "N/A":
                final_metrics[k] = v
            else:
                final_metrics[k] = None

        logger.info(
            f"Calculated metrics for {self.ticker}: { {k: v for k, v in final_metrics.items() if k != 'key_metrics_snapshot'} }")
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

        cashflow_annual = self._financial_data_cache.get('financial_statements', {}).get('cashflow', [])
        # Sort cashflow_annual by date descending to ensure cashflow_annual[0] is the latest.
        cashflow_annual = sorted([s for s in cashflow_annual if s], key=lambda x: x.get("date"), reverse=True)

        profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        calculated_metrics = self._financial_data_cache.get('calculated_metrics', {})

        current_price = safe_get_float(profile_fmp, "price")
        # Determine shares outstanding carefully: FMP profile "sharesOutstanding" or from mktCap / price
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
        if current_fcf is None or current_fcf <= 10000:  # Require FCF > 10k to be meaningful for DCF
            logger.warning(
                f"Current FCF for {self.ticker} is {current_fcf}. Simplified DCF requires positive & significant starting FCF.")
            return dcf_results

        dcf_results["dcf_assumptions"]["start_fcf"] = current_fcf

        fcf_growth_hist_3yr = None
        if len(cashflow_annual) >= 4:  # Need 4 years for 3 growth periods
            fcf_start_3yr = get_value_from_statement_list(cashflow_annual, "freeCashFlow",
                                                          3)  # 4th element (index 3) for 3-year period
            fcf_end_3yr = current_fcf  # current_fcf is from index 0
            if fcf_start_3yr and fcf_start_3yr > 0:
                fcf_growth_hist_3yr = calculate_cagr(fcf_end_3yr, fcf_start_3yr, 3)

        fcf_growth_initial = fcf_growth_hist_3yr \
                             or calculated_metrics.get("revenue_growth_cagr_3yr") \
                             or calculated_metrics.get("revenue_growth_yoy") \
                             or 0.05
        if not isinstance(fcf_growth_initial, (int, float)): fcf_growth_initial = 0.05  # Ensure it's a number

        fcf_growth_initial = min(fcf_growth_initial, 0.20)
        fcf_growth_initial = max(fcf_growth_initial, -0.10)

        projected_fcfs = []
        last_projected_fcf = current_fcf

        growth_decline_rate = (
                                          fcf_growth_initial - DEFAULT_PERPETUAL_GROWTH_RATE) / DEFAULT_FCF_PROJECTION_YEARS if DEFAULT_FCF_PROJECTION_YEARS > 0 else 0

        for i in range(DEFAULT_FCF_PROJECTION_YEARS):
            current_year_growth_rate = fcf_growth_initial - (growth_decline_rate * i)
            current_year_growth_rate = max(current_year_growth_rate, DEFAULT_PERPETUAL_GROWTH_RATE)

            projected_fcf = last_projected_fcf * (1 + current_year_growth_rate)
            projected_fcfs.append(projected_fcf)
            last_projected_fcf = projected_fcf
            dcf_results["dcf_assumptions"]["fcf_growth_rates_projection"].append(round(current_year_growth_rate, 4))

        if not projected_fcfs:  # Should not happen if DEFAULT_FCF_PROJECTION_YEARS > 0
            logger.error(f"DCF: No projected FCFs generated for {self.ticker}.")
            return dcf_results

        terminal_fcf_for_calc = projected_fcfs[-1] * (1 + DEFAULT_PERPETUAL_GROWTH_RATE)

        denominator = DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE
        if denominator <= 1e-6:  # Avoid division by zero or very small numbers
            logger.warning(
                f"DCF for {self.ticker}: Discount rate ({DEFAULT_DISCOUNT_RATE}) is too close to or less than perpetual growth rate ({DEFAULT_PERPETUAL_GROWTH_RATE}). Terminal value calculation is unreliable.")
            terminal_value = 0  # Or handle as error; cannot reliably calculate TV
        else:
            terminal_value = terminal_fcf_for_calc / denominator

        discounted_values_sum = 0
        for i, fcf_val in enumerate(projected_fcfs):
            discounted_values_sum += fcf_val / ((1 + DEFAULT_DISCOUNT_RATE) ** (i + 1))

        discounted_terminal_value = terminal_value / ((1 + DEFAULT_DISCOUNT_RATE) ** DEFAULT_FCF_PROJECTION_YEARS)

        intrinsic_equity_value = discounted_values_sum + discounted_terminal_value

        intrinsic_value_per_share = intrinsic_equity_value / shares_outstanding
        dcf_results["dcf_intrinsic_value"] = intrinsic_value_per_share

        if current_price and current_price != 0 and intrinsic_value_per_share is not None:
            dcf_results["dcf_upside_percentage"] = (intrinsic_value_per_share - current_price) / current_price

        logger.info(
            f"DCF for {self.ticker}: Intrinsic Value/Share: {dcf_results['dcf_intrinsic_value'] if dcf_results['dcf_intrinsic_value'] is not None else 'N/A'}, Upside: {dcf_results['dcf_upside_percentage'] if dcf_results['dcf_upside_percentage'] is not None else 'N/A'}")
        self._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    def _fetch_and_summarize_10k(self):
        # This method structure is largely okay from previous, ensure logging and error handling is fine.
        # Key is that it uses self.stock_db_entry.cik which should now be populated more reliably.
        logger.info(f"Fetching and attempting to summarize latest 10-K for {self.ticker}")
        summary_results = {"qualitative_sources_summary": {}}

        if not self.stock_db_entry or not self.stock_db_entry.cik:
            logger.warning(f"No CIK found for {self.ticker} in DB. Cannot fetch 10-K from EDGAR directly.")
            return summary_results

        filing_url = self.sec_edgar.get_filing_document_url(cik=self.stock_db_entry.cik, form_type="10-K")
        if not filing_url:
            filing_url = self.sec_edgar.get_filing_document_url(cik=self.stock_db_entry.cik, form_type="10-K/A")

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

        # Summarize key sections using Gemini
        # Business Summary
        business_text = extracted_sections.get("business", "")
        if business_text:
            prompt_context = f"This is the 'Business' section (Item 1) from the 10-K for {company_name} ({self.ticker}). Summarize core operations, products/services, revenue streams, and target markets."
            summary = self.gemini.summarize_text_with_context(business_text, prompt_context,
                                                              MAX_10K_SECTION_LENGTH_FOR_GEMINI)
            if not summary.startswith("Error:"): summary_results["business_summary"] = summary
            summary_results["qualitative_sources_summary"]["business_10k_source_length"] = len(business_text)
            time.sleep(2)

            # Risk Factors
        risk_text = extracted_sections.get("risk_factors", "")
        if risk_text:
            prompt_context = f"This is 'Risk Factors' (Item 1A) from 10-K for {company_name} ({self.ticker}). Summarize 3-5 most material risks."
            summary = self.gemini.summarize_text_with_context(risk_text, prompt_context,
                                                              MAX_10K_SECTION_LENGTH_FOR_GEMINI)
            if not summary.startswith("Error:"): summary_results["risk_factors_summary"] = summary
            summary_results["qualitative_sources_summary"]["risk_factors_10k_source_length"] = len(risk_text)
            time.sleep(2)

        # MD&A
        mda_text = extracted_sections.get("mda", "")
        if mda_text:
            prompt_context = f"This is MD&A (Item 7) from 10-K for {company_name} ({self.ticker}). Summarize key performance drivers, financial condition, liquidity, capital resources, and management's outlook."
            summary = self.gemini.summarize_text_with_context(mda_text, prompt_context,
                                                              MAX_10K_SECTION_LENGTH_FOR_GEMINI)
            if not summary.startswith("Error:"): summary_results["management_assessment_summary"] = summary
            summary_results["qualitative_sources_summary"]["mda_10k_source_length"] = len(mda_text)
            time.sleep(2)

        # Competitive Landscape from Business & MD&A
        comp_landscape_input_text = (summary_results.get("business_summary", "") + "\n" + summary_results.get(
            "management_assessment_summary", ""))[:MAX_GEMINI_TEXT_LENGTH].strip()
        if comp_landscape_input_text:
            comp_prompt = (
                f"Based on business & MD&A for {company_name} ({self.ticker}): \"{comp_landscape_input_text}\"\n"
                f"Describe its competitive landscape, key competitors, and {company_name}'s competitive positioning.")
            summary_results["competitive_landscape_summary"] = self.gemini.generate_text(comp_prompt)
            summary_results["qualitative_sources_summary"][
                "competitive_landscape_context"] = "Derived from 10-K Business/MD&A summaries."
            time.sleep(2)

        # Economic Moat (if not directly from 10-K summaries)
        moat_input_text = (summary_results.get("business_summary", "") + "\n" + summary_results.get(
            "competitive_landscape_summary", "") + "\n" + summary_results.get("risk_factors_summary", ""))[
                          :MAX_GEMINI_TEXT_LENGTH].strip()
        if moat_input_text and not summary_results.get("economic_moat_summary"):
            moat_prompt = (f"Based on info for {company_name} ({self.ticker}): \"{moat_input_text}\"\n"
                           f"Analyze its primary economic moats (brand, network effects, switching costs, IP, cost advantages). Concise summary.")
            summary_results["economic_moat_summary"] = self.gemini.generate_text(moat_prompt)
            time.sleep(2)

        # Industry Trends (if not directly from 10-K summaries)
        industry_input_text = (summary_results.get("business_summary", "") + "\n" + (
                    self.stock_db_entry.industry or "") + "\n" + (self.stock_db_entry.sector or ""))[
                              :MAX_GEMINI_TEXT_LENGTH].strip()
        if industry_input_text and not summary_results.get("industry_trends_summary"):
            industry_prompt = (
                f"Company: {company_name} in '{self.stock_db_entry.industry}' industry, '{self.stock_db_entry.sector}' sector.\n"
                f"Context: \"{industry_input_text}\"\n"
                f"Analyze current key trends, opportunities, and challenges for this industry/sector. How is {company_name} positioned?")
            summary_results["industry_trends_summary"] = self.gemini.generate_text(industry_prompt)
            time.sleep(2)

        logger.info(f"10-K based qualitative summaries generated for {self.ticker}.")
        self._financial_data_cache['10k_summaries'] = summary_results
        return summary_results

    def _determine_investment_thesis(self):
        # This method structure is okay, ensure it uses the richer data from cache.
        logger.info(f"Synthesizing investment thesis for {self.ticker}...")
        metrics = self._financial_data_cache.get('calculated_metrics', {})
        qual_summaries = self._financial_data_cache.get('10k_summaries', {})
        dcf_results = self._financial_data_cache.get('dcf_results', {})
        company_profile = self._financial_data_cache.get('profile_fmp', {})

        company_name = self.stock_db_entry.company_name or self.ticker
        industry = self.stock_db_entry.industry or "N/A"
        sector = self.stock_db_entry.sector or "N/A"

        prompt = f"Company: {company_name} ({self.ticker})\n"
        prompt += f"Industry: {industry}, Sector: {sector}\n\n"
        prompt += "Key Financial Metrics (approximate values):\n"

        metrics_for_prompt = {
            "P/E Ratio": metrics.get("pe_ratio"), "P/B Ratio": metrics.get("pb_ratio"),
            "P/S Ratio": metrics.get("ps_ratio"), "EV/Sales": metrics.get("ev_to_sales"),
            "EV/EBITDA": metrics.get("ev_to_ebitda"), "Dividend Yield": metrics.get("dividend_yield"),
            "ROE": metrics.get("roe"), "ROIC": metrics.get("roic"),
            "Debt/Equity": metrics.get("debt_to_equity"), "Debt/EBITDA": metrics.get("debt_to_ebitda"),
            "Revenue Growth YoY": metrics.get("revenue_growth_yoy"),
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
                    if name.endswith(
                            "Yield") or "Growth" in name or "Margin" in name or name == "ROE" or name == "ROIC":
                        val_str = f"{val:.2%}"
                    else:
                        val_str = f"{val:.2f}"
                else:
                    val_str = str(val)
                prompt += f"- {name}: {val_str}\n"

        dcf_val = dcf_results.get("dcf_intrinsic_value")
        dcf_upside = dcf_results.get("dcf_upside_percentage")
        current_price = company_profile.get("price")

        if dcf_val is not None: prompt += f"\nDCF Intrinsic Value per Share: {dcf_val:.2f}\n"
        if dcf_upside is not None: prompt += f"DCF Upside: {dcf_upside:.2%}\n"
        if current_price is not None: prompt += f"Current Stock Price: {current_price:.2f}\n\n"

        prompt += "Qualitative Summary (derived from 10-K, Profile, and AI analysis):\n"
        prompt += f"- Business Overview: {qual_summaries.get('business_summary', 'N/A')[:300]}...\n"
        prompt += f"- Economic Moat: {qual_summaries.get('economic_moat_summary', 'N/A')[:300]}...\n"
        prompt += f"- Competitive Landscape: {qual_summaries.get('competitive_landscape_summary', 'N/A')[:300]}...\n"
        prompt += f"- Industry Trends & Position: {qual_summaries.get('industry_trends_summary', 'N/A')[:300]}...\n"
        prompt += f"- Management Discussion Highlights (MD&A): {qual_summaries.get('management_assessment_summary', 'N/A')[:300]}...\n"
        prompt += f"- Key Risk Factors: {qual_summaries.get('risk_factors_summary', 'N/A')[:300]}...\n\n"

        prompt += (
            "Instructions for AI:\n"
            "1. Provide a comprehensive investment thesis (2-4 paragraphs) incorporating all provided quantitative and qualitative insights.\n"
            "2. State a specific Investment Decision: 'Strong Buy', 'Buy', 'Hold', 'Monitor for Entry', 'Reduce', 'Sell', or 'Avoid'.\n"
            "3. Suggest a suitable Investment Strategy Type: 'Deep Value', 'Value', 'GARP', 'Growth', 'Aggressive Growth', 'Dividend Growth', 'Special Situation', 'Speculative'.\n"
            "4. Indicate a Confidence Level for the assessment: 'High', 'Medium', or 'Low'.\n"
            "5. Detail the Key Reasoning in bullet points, covering:\n"
            "   - Valuation (relative to intrinsic value/peers, considering growth).\n"
            "   - Financial Health & Profitability (margins, debt, cash flow).\n"
            "   - Growth Prospects (historical, future drivers, industry tailwinds).\n"
            "   - Competitive Advantages/Moat.\n"
            "   - Key Risks & Concerns.\n"
            "   - Management & Strategy (if discernible from MD&A).\n"
            "Be objective, balanced, and explicitly state if data is limited or assumptions are strong."
        )

        final_prompt = prompt[:MAX_GEMINI_TEXT_LENGTH]
        if len(prompt) > MAX_GEMINI_TEXT_LENGTH:
            logger.warning(
                f"Investment thesis prompt for {self.ticker} truncated to {MAX_GEMINI_TEXT_LENGTH} chars for Gemini.")

        ai_response = self.gemini.generate_text(final_prompt)

        if ai_response.startswith("Error:"):
            logger.error(f"Gemini failed to generate investment thesis for {self.ticker}: {ai_response}")
            return {
                "investment_decision": "AI Error", "reasoning": ai_response,
                "strategy_type": "N/A", "confidence_level": "N/A",
                "investment_thesis_full": ai_response
            }

        parsed_thesis = {}
        current_section_key = None
        # Simple parser based on keywords; a more robust regex parser might be better
        # For now, ensure these keys match the StockAnalysis model fields where appropriate
        section_map = {
            "investment thesis:": "investment_thesis_full",
            "investment decision:": "investment_decision",
            "strategy type:": "strategy_type",
            "confidence level:": "confidence_level",
            "key reasoning:": "reasoning"
        }

        collected_lines = {}  # To store lines for multi-line sections

        for line in ai_response.split('\n'):
            line_l_strip = line.lower().strip()
            found_new_section = False
            for header, key_name in section_map.items():
                if line_l_strip.startswith(header):
                    current_section_key = key_name
                    # Initialize list for this section's lines
                    collected_lines[current_section_key] = [line[len(header):].strip()]
                    found_new_section = True
                    break  # Found a header, move to next line

            if not found_new_section and current_section_key:
                # If we are in a section, append the line
                collected_lines[current_section_key].append(line)

        # Join collected lines for each section
        for key, lines_list in collected_lines.items():
            parsed_thesis[key] = "\n".join(lines_list).strip()

        # Fallbacks
        if "investment_decision" not in parsed_thesis: parsed_thesis["investment_decision"] = "Review AI Output"
        if "reasoning" not in parsed_thesis: parsed_thesis["reasoning"] = ai_response  # fallback
        if "investment_thesis_full" not in parsed_thesis: parsed_thesis[
            "investment_thesis_full"] = ai_response  # fallback

        logger.info(
            f"Generated investment thesis for {self.ticker}. Decision: {parsed_thesis.get('investment_decision')}")
        return parsed_thesis

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_analysis_data = {}

        try:
            if not self.stock_db_entry:
                logger.error(f"Stock entry for {self.ticker} was not properly initialized. Aborting analysis.")
                return None

            self._ensure_stock_db_entry_is_bound()

            self._fetch_financial_statements()
            self._fetch_key_metrics_and_profile_data()

            calculated_metrics = self._calculate_derived_metrics()
            final_analysis_data.update(calculated_metrics)

            dcf_results = self._perform_dcf_analysis()
            final_analysis_data.update(dcf_results)

            qual_summaries = self._fetch_and_summarize_10k()
            final_analysis_data.update(qual_summaries)

            investment_thesis_parts = self._determine_investment_thesis()
            final_analysis_data.update(investment_thesis_parts)

            # Ensure the 'reasoning' field gets the detailed bullet points if available,
            # and 'investment_thesis_full' gets the narrative thesis.
            # The parser for _determine_investment_thesis now separates these.
            # If 'reasoning' is not specifically parsed, it might default to the full response.
            # Let's ensure that "reasoning" is specifically the key points.

            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id)

            model_fields = [col.key for col in StockAnalysis.__table__.columns if
                            col.key not in ['id', 'stock_id', 'analysis_date']]

            for field in model_fields:
                if field in final_analysis_data:
                    value_to_set = final_analysis_data[field]
                    if getattr(StockAnalysis, field).type.python_type == float:
                        if isinstance(value_to_set, str) and value_to_set.lower() == "n/a":
                            value_to_set = None
                        elif isinstance(value_to_set, str):  # Try to convert string to float if it's a number
                            try:
                                value_to_set = float(value_to_set)
                            except ValueError:
                                value_to_set = None  # If conversion fails, set to None
                        if isinstance(value_to_set, float) and (math.isnan(value_to_set) or math.isinf(value_to_set)):
                            value_to_set = None

                    setattr(analysis_entry, field, value_to_set)

            # Ensure the specific parsed reasoning is used if available
            if "reasoning" in investment_thesis_parts and investment_thesis_parts[
                "reasoning"] != "Reasoning not explicitly parsed.":
                analysis_entry.reasoning = investment_thesis_parts["reasoning"]
            elif "investment_thesis_full" in investment_thesis_parts:  # Fallback if detailed reasoning wasn't clear
                analysis_entry.reasoning = investment_thesis_parts["investment_thesis_full"]

            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = datetime.now(timezone.utc)
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
                    self.db_session.rollback()
                    logger.info(f"Rolled back transaction for {self.ticker} due to error.")
                except Exception as e_rollback:
                    logger.error(f"Error during rollback for {self.ticker}: {e_rollback}")
            return None
        finally:
            self._close_session_if_active()

    def _ensure_stock_db_entry_is_bound(self):
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} is INACTIVE. Re-establishing and re-fetching stock entry.")
            self._close_session_if_active()
            self.db_session = next(get_db_session())

            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock:
                logger.error(
                    f"Could not re-fetch stock {self.ticker} after session re-establishment. This may indicate a problem with initial creation or commit.")
                self._get_or_create_stock_entry()  # Attempt to fix it by re-running the creation/fetch logic
                if not self.stock_db_entry:  # If still no stock_db_entry, then critical failure.
                    raise RuntimeError(f"Failed to create or re-fetch stock {self.ticker} for active session.")
                return
            self.stock_db_entry = re_fetched_stock
            logger.info(f"Re-fetched and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new session.")
            return

        instance_state = sa_inspect(self.stock_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            object_id_for_log = self.stock_db_entry.id if instance_state.has_identity else 'Unknown ID (transient)'
            current_session_id = id(self.db_session)
            instance_session_id = id(instance_state.session) if instance_state.session else 'None'

            logger.warning(
                f"Stock entry {self.ticker} (ID: {object_id_for_log}) is DETACHED or bound to a DIFFERENT session "
                f"(Expected: {current_session_id}, Actual: {instance_session_id}). Attempting to merge."
            )
            try:
                merged_stock = self.db_session.merge(self.stock_db_entry)
                self.stock_db_entry = merged_stock
                logger.info(
                    f"Successfully merged/re-associated stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session {id(self.db_session)}.")
            except Exception as e_merge:
                logger.error(f"Failed to merge stock {self.ticker} into session: {e_merge}. Re-fetching as fallback.",
                             exc_info=True)
                primary_key_to_fetch = self.stock_db_entry.id if instance_state.has_identity and self.stock_db_entry.id else None

                re_fetched_stock_on_merge_fail = None
                if primary_key_to_fetch:
                    re_fetched_stock_on_merge_fail = self.db_session.query(Stock).get(primary_key_to_fetch)

                if not re_fetched_stock_on_merge_fail:
                    re_fetched_stock_on_merge_fail = self.db_session.query(Stock).filter(
                        Stock.ticker == self.ticker).first()

                if re_fetched_stock_on_merge_fail:
                    self.stock_db_entry = re_fetched_stock_on_merge_fail
                    logger.info(
                        f"Successfully re-fetched stock {self.ticker} (ID: {self.stock_db_entry.id}) after merge failure.")
                else:
                    logger.critical(
                        f"CRITICAL: Failed to re-associate stock {self.ticker} with current session after merge failure and could not re-fetch. Analysis cannot proceed reliably.")
                    raise RuntimeError(f"Failed to bind stock {self.ticker} to session for analysis.")


if __name__ == '__main__':
    from database import init_db

    # init_db()

    logger.info("Starting standalone stock analysis test...")
    tickers_to_test = ["AAPL", "MSFT", "GOOG"]  # Test with GOOG which had DCF issue in log

    for ticker_symbol in tickers_to_test:
        analysis_result = None
        try:
            logger.info(f"--- Analyzing {ticker_symbol} ---")
            analyzer = StockAnalyzer(ticker=ticker_symbol)
            analysis_result = analyzer.analyze()

            if analysis_result:
                logger.info(
                    f"Analysis for {analysis_result.stock.ticker} completed. Decision: {analysis_result.investment_decision}, Confidence: {analysis_result.confidence_level}")
                if analysis_result.dcf_intrinsic_value is not None:
                    logger.info(
                        f"DCF Value: {analysis_result.dcf_intrinsic_value:.2f}, Upside: {analysis_result.dcf_upside_percentage:.2% if analysis_result.dcf_upside_percentage is not None else 'N/A'}")
                else:
                    logger.info("DCF analysis did not yield an intrinsic value.")
                logger.info(
                    f"Reasoning highlights: {str(analysis_result.reasoning)[:300]}...")  # Ensure reasoning is string

            else:
                logger.error(f"Stock analysis pipeline FAILED for {ticker_symbol} (returned None).")
        except RuntimeError as rt_err:
            logger.error(f"Could not initialize StockAnalyzer for {ticker_symbol}: {rt_err}")
        except Exception as e:
            logger.error(f"Unhandled error analyzing {ticker_symbol} in __main__: {e}", exc_info=True)
        finally:
            logger.info(f"--- Finished processing {ticker_symbol} ---")
            if analysis_result is None: logger.info(f"No analysis result object for {ticker_symbol}")
            time.sleep(10)