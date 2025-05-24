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


class StockAnalyzer:
    def __init__(self, ticker):
        self.ticker = ticker.upper()
        self.finnhub = FinnhubClient()
        self.fmp = FinancialModelingPrepClient()
        self.eodhd = EODHDClient()  # Retained for potential future use or different data points
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()

        self.db_session = next(get_db_session())
        self.stock_db_entry = None
        self._financial_data_cache = {}  # In-memory cache for fetched data during a single analysis run

        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"CRITICAL: Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            self._close_session_if_active()
            # Re-raise to prevent using a partially initialized or broken analyzer object
            # This ensures the calling code knows this instance is not usable.
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
            self._close_session_if_active()  # Close old one if possible
            self.db_session = next(get_db_session())  # Get new one

        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()

        company_name_from_api = None
        industry_from_api = None
        sector_from_api = None
        cik_from_api = None  # Explicitly initialize

        # Attempt to fetch profile data to populate/update stock entry
        # Prioritize FMP as it often has CIK, industry, sector
        profile_fmp = self.fmp.get_company_profile(self.ticker)  # API Call
        if profile_fmp and isinstance(profile_fmp, list) and profile_fmp[0]:
            self._financial_data_cache['profile_fmp'] = profile_fmp[0]  # Cache for later use
            company_name_from_api = profile_fmp[0].get('companyName')
            industry_from_api = profile_fmp[0].get('industry')
            sector_from_api = profile_fmp[0].get('sector')
            cik_from_api = profile_fmp[0].get('cik')
            logger.info(f"Fetched profile from FMP for {self.ticker}.")
        else:  # Fallback to Finnhub if FMP fails or returns no data
            logger.warning(f"FMP profile fetch failed or empty for {self.ticker}. Trying Finnhub.")
            profile_finnhub = self.finnhub.get_company_profile2(self.ticker)  # API Call
            if profile_finnhub:
                self._financial_data_cache['profile_finnhub'] = profile_finnhub  # Cache for later
                company_name_from_api = profile_finnhub.get('name')
                industry_from_api = profile_finnhub.get('finnhubIndustry')
                # Finnhub doesn't typically provide a broad "sector" or CIK in company_profile2
                logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Failed to fetch profile from FMP and Finnhub for {self.ticker}.")

        if not company_name_from_api:  # If still no name, use ticker as placeholder
            company_name_from_api = self.ticker
            logger.info(f"Using ticker '{self.ticker}' as company name due to lack of API data.")

        # If CIK is still missing, try to get it directly from SEC EDGAR client
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
                cik=cik_from_api  # Store CIK if found
            )
            self.db_session.add(self.stock_db_entry)
            try:
                self.db_session.commit()
                self.db_session.refresh(self.stock_db_entry)  # Load defaults, ID
                logger.info(
                    f"Created and refreshed stock entry for {self.ticker} (ID: {self.stock_db_entry.id}). Name: {self.stock_db_entry.company_name}, CIK: {self.stock_db_entry.cik}")
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating stock entry for {self.ticker}: {e}", exc_info=True)
                raise  # Critical failure
        else:  # Existing entry, check for updates
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
            if cik_from_api and self.stock_db_entry.cik != cik_from_api:  # Update CIK if newly found or changed
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
                    # Non-critical, proceed with old data if update fails

    def _fetch_financial_statements(self):
        logger.info(f"Fetching financial statements for {self.ticker} for the last {STOCK_FINANCIAL_YEARS} years.")
        statements = {"income": [], "balance": [], "cashflow": []}

        try:
            # FMP is primary for historical depth if available
            income_annual_fmp = self.fmp.get_financial_statements(self.ticker, "income-statement", period="annual",
                                                                  limit=STOCK_FINANCIAL_YEARS)
            balance_annual_fmp = self.fmp.get_financial_statements(self.ticker, "balance-sheet-statement",
                                                                   period="annual", limit=STOCK_FINANCIAL_YEARS)
            cashflow_annual_fmp = self.fmp.get_financial_statements(self.ticker, "cash-flow-statement", period="annual",
                                                                    limit=STOCK_FINANCIAL_YEARS)

            if income_annual_fmp: statements["income"].extend(income_annual_fmp)
            if balance_annual_fmp: statements["balance"].extend(balance_annual_fmp)
            if cashflow_annual_fmp: statements["cashflow"].extend(cashflow_annual_fmp)

            # Fetch quarterly data for recent trends (e.g., last 8 quarters for QoQ)
            income_quarterly_fmp = self.fmp.get_financial_statements(self.ticker, "income-statement",
                                                                     period="quarterly", limit=8)
            self._financial_data_cache['income_quarterly_fmp'] = income_quarterly_fmp or []  # Ensure it's a list

            logger.info(
                f"Fetched from FMP: {len(statements['income'])} income, {len(statements['balance'])} balance, {len(statements['cashflow'])} cashflow annual statements. {len(income_quarterly_fmp or [])} quarterly income statements.")

        except Exception as e:
            logger.warning(
                f"Error fetching FMP financial statements for {self.ticker}: {e}. This may limit trend analysis.",
                exc_info=True)

        # Fallback or supplement with Finnhub if FMP data is insufficient.
        # Finnhub's `financials-reported` gives quarterly by default. We'd need to aggregate for annuals.
        # This is complex. For now, if FMP fails, we have less historical data for deep trends.
        # If essential, one could implement logic to pull Finnhub quarterly and sum up to annuals.

        self._financial_data_cache['financial_statements'] = statements
        return statements

    def _fetch_key_metrics_and_profile_data(self):  # Renamed for clarity
        logger.info(f"Fetching key metrics and profile for {self.ticker}.")

        # FMP Key Metrics (annual for trends, quarterly for latest)
        # Ensure limit is reasonable based on typical API responses and desired history.
        key_metrics_annual_fmp = self.fmp.get_key_metrics(self.ticker, period="annual",
                                                          limit=STOCK_FINANCIAL_YEARS + 2)  # Get a bit more for safety
        key_metrics_quarterly_fmp = self.fmp.get_key_metrics(self.ticker, period="quarterly",
                                                             limit=8)  # Last 8 quarters

        self._financial_data_cache['key_metrics_annual_fmp'] = key_metrics_annual_fmp or []
        self._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp or []

        # Finnhub Basic Financials for potentially more real-time metrics like PE
        basic_financials_finnhub = self.finnhub.get_basic_financials(self.ticker)
        self._financial_data_cache['basic_financials_finnhub'] = basic_financials_finnhub or {}

        # FMP Profile (might already be in cache from _get_or_create_stock_entry)
        if not self._financial_data_cache.get('profile_fmp'):
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
        basic_fin_finnhub = self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {})
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})

        latest_km_q = key_metrics_quarterly[0] if key_metrics_quarterly else {}
        latest_km_a = key_metrics_annual[0] if key_metrics_annual else {}

        metrics["pe_ratio"] = latest_km_q.get("peRatioTTM") or latest_km_a.get("peRatio") or basic_fin_finnhub.get(
            "peTTM") or basic_fin_finnhub.get("peAnnual")
        metrics["pb_ratio"] = latest_km_q.get("priceToBookRatioTTM") or latest_km_a.get(
            "pbRatio") or basic_fin_finnhub.get("pbAnnual")
        metrics["ps_ratio"] = latest_km_q.get("priceToSalesRatioTTM") or latest_km_a.get("priceSalesRatio")
        metrics["ev_to_sales"] = latest_km_q.get("enterpriseValueOverRevenueTTM") or latest_km_a.get(
            "enterpriseValueOverRevenue")
        metrics["ev_to_ebitda"] = latest_km_q.get("evToEbitdaTTM") or latest_km_a.get("evToEbitda")

        div_yield_fmp_q = latest_km_q.get("dividendYieldTTM")
        div_yield_fmp_a = latest_km_a.get("dividendYield")
        div_yield_finnhub = basic_fin_finnhub.get("dividendYieldAnnual")
        if div_yield_finnhub is not None: div_yield_finnhub = div_yield_finnhub / 100  # Finnhub yield is often percentage
        metrics["dividend_yield"] = div_yield_fmp_q or div_yield_fmp_a or div_yield_finnhub

        metrics["key_metrics_snapshot"]["FMP_peRatioTTM"] = metrics["pe_ratio"]
        metrics["key_metrics_snapshot"]["FMP_pbRatioTTM"] = metrics["pb_ratio"]
        metrics["key_metrics_snapshot"]["FMP_psRatioTTM"] = metrics["ps_ratio"]
        metrics["key_metrics_snapshot"]["Finnhub_peTTM"] = basic_fin_finnhub.get("peTTM")  # Store for comparison

        income_annual = sorted(statements.get("income", []), key=lambda x: x.get("date"), reverse=True)
        balance_annual = sorted(statements.get("balance", []), key=lambda x: x.get("date"), reverse=True)
        cashflow_annual = sorted(statements.get("cashflow", []), key=lambda x: x.get("date"), reverse=True)

        if income_annual:
            latest_income_a = income_annual[0]
            metrics["eps"] = latest_income_a.get("eps") or latest_km_a.get("eps")
            metrics["net_profit_margin"] = latest_income_a.get("netProfitMargin")
            metrics["gross_profit_margin"] = latest_income_a.get("grossProfitMargin")
            metrics["operating_profit_margin"] = latest_income_a.get(
                "operatingIncomeRatio")  # FMP: operatingIncome / revenue

            ebit = latest_income_a.get("operatingIncome")  # Proxy for EBIT
            interest_expense = latest_income_a.get("interestExpense")
            if ebit is not None and interest_expense is not None and abs(interest_expense) > 0:
                metrics["interest_coverage_ratio"] = ebit / abs(interest_expense)

        if balance_annual:
            latest_balance_a = balance_annual[0]
            total_equity = latest_balance_a.get("totalStockholdersEquity")
            total_assets = latest_balance_a.get("totalAssets")

            if total_equity and total_equity != 0 and income_annual and income_annual[0].get("netIncome") is not None:
                metrics["roe"] = income_annual[0].get("netIncome") / total_equity

            if total_assets and total_assets != 0 and income_annual and income_annual[0].get("netIncome") is not None:
                metrics["roa"] = income_annual[0].get("netIncome") / total_assets

            metrics["debt_to_equity"] = latest_balance_a.get("debtToEquity") or latest_km_a.get("debtToEquity")
            if metrics["debt_to_equity"] is None and latest_balance_a.get(
                    "totalDebt") is not None and total_equity and total_equity != 0:
                metrics["debt_to_equity"] = latest_balance_a.get("totalDebt") / total_equity

            current_assets = latest_balance_a.get("totalCurrentAssets")
            current_liabilities = latest_balance_a.get("totalCurrentLiabilities")
            if current_assets is not None and current_liabilities is not None and current_liabilities != 0:
                metrics["current_ratio"] = current_assets / current_liabilities

            cash_equivalents = latest_balance_a.get("cashAndCashEquivalents", 0)
            short_term_investments = latest_balance_a.get("shortTermInvestments", 0)
            accounts_receivable = latest_balance_a.get("netReceivables", 0)
            if current_liabilities is not None and current_liabilities != 0:
                metrics["quick_ratio"] = (
                                                     cash_equivalents + short_term_investments + accounts_receivable) / current_liabilities

        ebitda_for_debt_ratio = latest_km_a.get("ebitda")  # Prefer from key metrics
        if not ebitda_for_debt_ratio and income_annual and income_annual[0].get("ebitda"):
            ebitda_for_debt_ratio = income_annual[0].get("ebitda")

        if ebitda_for_debt_ratio and ebitda_for_debt_ratio != 0 and balance_annual and balance_annual[0].get(
                "totalDebt") is not None:
            metrics["debt_to_ebitda"] = balance_annual[0].get("totalDebt") / ebitda_for_debt_ratio

        # Growth Rates
        def get_value_from_statement_by_year_offset(statement_list, field, year_offset=0):
            if len(statement_list) > year_offset:
                return statement_list[year_offset].get(field)
            return None

        def calculate_growth(current, previous):
            if current is not None and previous is not None and previous != 0:
                return (current - previous) / abs(previous)
            return None

        def calculate_cagr(end_value, start_value, years):
            if start_value is None or end_value is None or start_value == 0 or years <= 0: return None
            # Handle cases where start_value might be negative if dealing with earnings.
            # For revenue, start_value should be positive.
            if start_value < 0 and end_value > 0:  # From loss to profit, CAGR is complex/misleading
                return None  # Or some other indicator
            if start_value < 0 and end_value < 0:  # Both negative, growth of negative numbers
                return ((abs(start_value) / abs(end_value)) ** (1 / years)) - 1  # growth in reduction of loss
            if end_value < 0 and start_value > 0:  # From profit to loss
                return - ((abs(end_value) / start_value) ** (
                            1 / years))  # This is not perfect, but indicates negative trend

            return ((end_value / start_value) ** (1 / years)) - 1

        metrics["revenue_growth_yoy"] = calculate_growth(
            get_value_from_statement_by_year_offset(income_annual, "revenue", 0),
            get_value_from_statement_by_year_offset(income_annual, "revenue", 1))
        metrics["eps_growth_yoy"] = calculate_growth(get_value_from_statement_by_year_offset(income_annual, "eps", 0),
                                                     get_value_from_statement_by_year_offset(income_annual, "eps", 1))

        if len(income_annual) >= 3:
            metrics["revenue_growth_cagr_3yr"] = calculate_cagr(
                get_value_from_statement_by_year_offset(income_annual, "revenue", 0),
                get_value_from_statement_by_year_offset(income_annual, "revenue", 2), 2)
            metrics["eps_growth_cagr_3yr"] = calculate_cagr(
                get_value_from_statement_by_year_offset(income_annual, "eps", 0),
                get_value_from_statement_by_year_offset(income_annual, "eps", 2), 2)
        if len(income_annual) >= 5:
            metrics["revenue_growth_cagr_5yr"] = calculate_cagr(
                get_value_from_statement_by_year_offset(income_annual, "revenue", 0),
                get_value_from_statement_by_year_offset(income_annual, "revenue", 4), 4)
            metrics["eps_growth_cagr_5yr"] = calculate_cagr(
                get_value_from_statement_by_year_offset(income_annual, "eps", 0),
                get_value_from_statement_by_year_offset(income_annual, "eps", 4), 4)

        income_quarterly = self._financial_data_cache.get('income_quarterly_fmp', [])
        if len(income_quarterly) >= 2:  # QoQ for revenue
            metrics["revenue_growth_qoq"] = calculate_growth(
                get_value_from_statement_by_year_offset(income_quarterly, "revenue", 0),
                get_value_from_statement_by_year_offset(income_quarterly, "revenue", 1))

        # FCF calculations
        if cashflow_annual:
            latest_cashflow_a = cashflow_annual[0]
            fcf = latest_cashflow_a.get("freeCashFlow")
            shares_outstanding_fcf = profile_fmp.get("mktCap") / profile_fmp.get("price") if profile_fmp.get(
                "mktCap") and profile_fmp.get("price") and profile_fmp.get("price") != 0 else profile_fmp.get(
                "sharesOutstanding")

            if fcf is not None and shares_outstanding_fcf and shares_outstanding_fcf != 0:
                metrics["free_cash_flow_per_share"] = fcf / shares_outstanding_fcf
                if profile_fmp.get("mktCap") and profile_fmp.get("mktCap") != 0:
                    metrics["free_cash_flow_yield"] = fcf / profile_fmp.get("mktCap")

            if len(cashflow_annual) >= 3:
                fcf_curr = get_value_from_statement_by_year_offset(cashflow_annual, "freeCashFlow", 0)
                fcf_prev1 = get_value_from_statement_by_year_offset(cashflow_annual, "freeCashFlow", 1)
                fcf_prev2 = get_value_from_statement_by_year_offset(cashflow_annual, "freeCashFlow", 2)
                if all(isinstance(x, (int, float)) for x in [fcf_curr, fcf_prev1, fcf_prev2]):
                    if fcf_curr > fcf_prev1 > fcf_prev2:
                        metrics["free_cash_flow_trend"] = "Growing"
                    elif fcf_curr < fcf_prev1 < fcf_prev2:
                        metrics["free_cash_flow_trend"] = "Declining"
                    else:
                        metrics["free_cash_flow_trend"] = "Mixed/Stable"
            else:
                metrics["free_cash_flow_trend"] = "Data N/A (needs 3+ years)"

        if len(balance_annual) >= 3:  # Retained Earnings Trend
            re_curr = get_value_from_statement_by_year_offset(balance_annual, "retainedEarnings", 0)
            re_prev1 = get_value_from_statement_by_year_offset(balance_annual, "retainedEarnings", 1)
            re_prev2 = get_value_from_statement_by_year_offset(balance_annual, "retainedEarnings", 2)
            if all(isinstance(x, (int, float)) for x in [re_curr, re_prev1, re_prev2]):
                if re_curr > re_prev1 > re_prev2:
                    metrics["retained_earnings_trend"] = "Growing"
                elif re_curr < re_prev1 < re_prev2:
                    metrics["retained_earnings_trend"] = "Declining"
                else:
                    metrics["retained_earnings_trend"] = "Mixed/Stable"
        else:
            metrics["retained_earnings_trend"] = "Data N/A (needs 3+ years)"

        # ROIC
        if income_annual and balance_annual:
            ebit = get_value_from_statement_by_year_offset(income_annual, "operatingIncome", 0)
            tax_provision = get_value_from_statement_by_year_offset(income_annual, "incomeTaxExpense", 0)
            income_before_tax = get_value_from_statement_by_year_offset(income_annual, "incomeBeforeTax", 0)

            effective_tax_rate = (
                        tax_provision / income_before_tax) if income_before_tax and tax_provision and income_before_tax != 0 else 0.21  # Default tax rate
            nopat = ebit * (1 - effective_tax_rate) if ebit is not None else None

            total_debt = get_value_from_statement_by_year_offset(balance_annual, "totalDebt", 0)
            total_equity = get_value_from_statement_by_year_offset(balance_annual, "totalStockholdersEquity", 0)
            cash_and_equivalents = get_value_from_statement_by_year_offset(balance_annual, "cashAndCashEquivalents",
                                                                           0) or 0

            if total_debt is not None and total_equity is not None:
                invested_capital = total_debt + total_equity - cash_and_equivalents
                if nopat is not None and invested_capital is not None and invested_capital != 0:
                    metrics["roic"] = nopat / invested_capital

        # Clean up Nones for JSON snapshot and ensure proper types for DB
        final_metrics = {}
        for k, v in metrics.items():
            if k == "key_metrics_snapshot":  # Handle snapshot separately
                final_metrics[k] = {sk: sv for sk, sv in v.items() if sv is not None and not (
                            isinstance(sv, float) and (math.isnan(sv) or math.isinf(sv)))}
            elif isinstance(v, (int, float)) and not math.isnan(v) and not math.isinf(v):
                final_metrics[k] = v
            elif isinstance(v, str) and v != "N/A":
                final_metrics[k] = v
            else:  # If v is None, "N/A", nan, or inf for numerical fields
                final_metrics[k] = None  # Store as None in the database for numerical fields

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
                "start_fcf": None, "fcf_growth_rates_projection": []  # Renamed for clarity
            }
        }

        cashflow_annual = self._financial_data_cache.get('financial_statements', {}).get('cashflow', [])
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        calculated_metrics = self._financial_data_cache.get('calculated_metrics', {})

        if not cashflow_annual or not profile_fmp or profile_fmp.get("price") is None or profile_fmp.get(
                "sharesOutstanding") is None:
            logger.warning(
                f"Insufficient data for DCF: missing FCF history, price, or shares outstanding for {self.ticker}.")
            return dcf_results

        current_fcf = get_value_from_statement_by_year_offset(cashflow_annual, "freeCashFlow", 0)
        if current_fcf is None or current_fcf <= 0:
            logger.warning(
                f"Current FCF for {self.ticker} is {current_fcf}. Simplified DCF requires positive starting FCF.")
            # Could try to normalize FCF or use an average, but that adds complexity.
            # For this version, we'll stop if FCF is not positive and usable.
            return dcf_results

        dcf_results["dcf_assumptions"]["start_fcf"] = current_fcf

        # Estimate FCF growth rate for projection period
        # Prioritize historical FCF growth, then revenue growth, then default
        fcf_growth_hist_3yr = None
        if len(cashflow_annual) >= 4:  # Need 4 years for 3 periods of FCF growth
            fcf_start_3yr = get_value_from_statement_by_year_offset(cashflow_annual, "freeCashFlow", 3)
            fcf_end_3yr = get_value_from_statement_by_year_offset(cashflow_annual, "freeCashFlow", 0)
            if fcf_start_3yr and fcf_start_3yr > 0:  # Avoid issues with negative or zero start
                fcf_growth_hist_3yr = calculate_cagr(fcf_end_3yr, fcf_start_3yr, 3)

        fcf_growth_initial = fcf_growth_hist_3yr \
                             or calculated_metrics.get("revenue_growth_cagr_3yr") \
                             or calculated_metrics.get("revenue_growth_yoy") \
                             or 0.05  # Default to 5%

        fcf_growth_initial = min(fcf_growth_initial, 0.20)  # Cap initial growth at 20%
        fcf_growth_initial = max(fcf_growth_initial, -0.10)  # Floor initial decline at -10% (FCF can decline)

        projected_fcfs = []
        last_projected_fcf = current_fcf

        # Simple declining growth model: start with fcf_growth_initial, linearly decline to perpetual_growth_rate
        growth_decline_rate = (
                                          fcf_growth_initial - DEFAULT_PERPETUAL_GROWTH_RATE) / DEFAULT_FCF_PROJECTION_YEARS if DEFAULT_FCF_PROJECTION_YEARS > 0 else 0

        for i in range(DEFAULT_FCF_PROJECTION_YEARS):
            current_year_growth_rate = fcf_growth_initial - (growth_decline_rate * i)
            current_year_growth_rate = max(current_year_growth_rate,
                                           DEFAULT_PERPETUAL_GROWTH_RATE)  # Don't go below perpetual

            projected_fcf = last_projected_fcf * (1 + current_year_growth_rate)
            projected_fcfs.append(projected_fcf)
            last_projected_fcf = projected_fcf
            dcf_results["dcf_assumptions"]["fcf_growth_rates_projection"].append(current_year_growth_rate)

        # Calculate Terminal Value (Gordon Growth Model)
        terminal_fcf_for_calc = projected_fcfs[-1] * (1 + DEFAULT_PERPETUAL_GROWTH_RATE)
        if (
                DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE) <= 0:  # Avoid division by zero or negative denominator
            logger.warning(
                f"DCF for {self.ticker}: Discount rate ({DEFAULT_DISCOUNT_RATE}) <= perpetual growth rate ({DEFAULT_PERPETUAL_GROWTH_RATE}). Terminal value is problematic.")
            # In such cases, a multi-stage or exit multiple TV approach is better.
            # For this simplified model, if this occurs, the TV might be unreliable or infinite.
            # Setting TV to 0 or a very large number if DR is slightly lower could be one way, but it's flawed.
            # Let's assume DR > PGR for this simplified model to work.
            if DEFAULT_DISCOUNT_RATE <= DEFAULT_PERPETUAL_GROWTH_RATE:
                logger.error(
                    "DCF cannot be reliably calculated as Discount Rate is not sufficiently above Perpetual Growth Rate.")
                return dcf_results  # Or set intrinsic value to error/None
            terminal_value = terminal_fcf_for_calc / (DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE)
        else:
            terminal_value = terminal_fcf_for_calc / (DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE)

        discounted_values_sum = 0
        for i, fcf_val in enumerate(projected_fcfs):
            discounted_values_sum += fcf_val / ((1 + DEFAULT_DISCOUNT_RATE) ** (i + 1))

        discounted_terminal_value = terminal_value / ((1 + DEFAULT_DISCOUNT_RATE) ** DEFAULT_FCF_PROJECTION_YEARS)

        # This simplified DCF calculates equity value directly assuming FCFE-like input
        intrinsic_equity_value = discounted_values_sum + discounted_terminal_value

        shares_outstanding = profile_fmp.get("mktCap") / profile_fmp.get("price") if profile_fmp.get(
            "mktCap") and profile_fmp.get("price") and profile_fmp.get("price") != 0 else profile_fmp.get(
            "sharesOutstanding")
        if not shares_outstanding or shares_outstanding == 0:
            logger.warning(
                f"Shares outstanding is zero or unavailable for {self.ticker}. Cannot calculate DCF per share.")
            return dcf_results

        intrinsic_value_per_share = intrinsic_equity_value / shares_outstanding
        dcf_results["dcf_intrinsic_value"] = intrinsic_value_per_share

        current_price = profile_fmp.get("price")
        if current_price and current_price != 0 and intrinsic_value_per_share is not None:
            dcf_results["dcf_upside_percentage"] = (intrinsic_value_per_share - current_price) / current_price

        logger.info(
            f"DCF for {self.ticker}: Intrinsic Value/Share: {dcf_results['dcf_intrinsic_value'] if dcf_results['dcf_intrinsic_value'] is not None else 'N/A'}, Upside: {dcf_results['dcf_upside_percentage'] if dcf_results['dcf_upside_percentage'] is not None else 'N/A'}")
        self._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    def _fetch_and_summarize_10k(self):
        logger.info(f"Fetching and attempting to summarize latest 10-K for {self.ticker}")
        summary_results = {"qualitative_sources_summary": {}}  # Initialize this dict

        if not self.stock_db_entry or not self.stock_db_entry.cik:
            logger.warning(f"No CIK found for {self.ticker} in DB. Cannot fetch 10-K from EDGAR directly.")
            # As a fallback, one might try to find 10-K URL via Finnhub filings, but this is less direct.
            # For now, if no CIK, we skip direct 10-K fetching. Qualitative analysis will use API profile data.
            return summary_results  # Return empty or with profile-based summaries later

        filing_url = self.sec_edgar.get_filing_document_url(cik=self.stock_db_entry.cik, form_type="10-K")
        if not filing_url:  # Try 10-K/A if 10-K not found
            filing_url = self.sec_edgar.get_filing_document_url(cik=self.stock_db_entry.cik, form_type="10-K/A")

        if not filing_url:
            logger.warning(
                f"Could not retrieve 10-K (or 10-K/A) filing URL for {self.ticker} (CIK: {self.stock_db_entry.cik})")
            return summary_results

        ten_k_text_content = self.sec_edgar.get_filing_text(filing_url)  # API Call for text
        if not ten_k_text_content:
            logger.warning(f"Failed to fetch 10-K text content from {filing_url}")
            return summary_results

        logger.info(f"Fetched 10-K text (length: {len(ten_k_text_content)}) for {self.ticker}. Extracting sections.")
        extracted_sections = extract_S1_text_sections(ten_k_text_content,
                                                      TEN_K_KEY_SECTIONS)  # Reusing general section helper

        company_name = self.stock_db_entry.company_name or self.ticker
        summary_results["qualitative_sources_summary"]["10k_filing_url_used"] = filing_url

        # Summarize key sections using Gemini
        if extracted_sections.get("business"):
            prompt_context = f"This is the 'Business' section (Item 1) from the 10-K filing for {company_name} ({self.ticker}). Summarize the core business operations, products/services, revenue streams, and target markets."
            summary = self.gemini.summarize_text_with_context(
                extracted_sections["business"], prompt_context, MAX_10K_SECTION_LENGTH_FOR_GEMINI
            )
            if not summary.startswith("Error:"): summary_results["business_summary"] = summary
            summary_results["qualitative_sources_summary"]["business_10k_source_length"] = len(
                extracted_sections["business"])
            time.sleep(2)  # API courtesy

        if extracted_sections.get("risk_factors"):
            prompt_context = f"This is the 'Risk Factors' section (Item 1A) from the 10-K filing for {company_name} ({self.ticker}). Summarize the 3-5 most material and unique risks the company faces."
            summary = self.gemini.summarize_text_with_context(
                extracted_sections["risk_factors"], prompt_context, MAX_10K_SECTION_LENGTH_FOR_GEMINI
            )
            if not summary.startswith("Error:"): summary_results["risk_factors_summary"] = summary
            summary_results["qualitative_sources_summary"]["risk_factors_10k_source_length"] = len(
                extracted_sections["risk_factors"])
            time.sleep(2)

        if extracted_sections.get("mda"):
            prompt_context = f"This is the MD&A (Item 7) from the 10-K for {company_name} ({self.ticker}). Summarize key performance drivers, financial condition changes, liquidity, capital resources, and management's outlook if stated."
            summary = self.gemini.summarize_text_with_context(
                extracted_sections["mda"], prompt_context, MAX_10K_SECTION_LENGTH_FOR_GEMINI
            )
            if not summary.startswith("Error:"): summary_results[
                "management_assessment_summary"] = summary  # Using this field for MD&A summary
            summary_results["qualitative_sources_summary"]["mda_10k_source_length"] = len(extracted_sections["mda"])
            time.sleep(2)

        # For competitive landscape, combine Business and MD&A if available
        comp_landscape_input = (summary_results.get("business_summary", "") + "\n" + summary_results.get(
            "management_assessment_summary", ""))[:MAX_GEMINI_TEXT_LENGTH]
        if comp_landscape_input.strip():
            prompt_context = f"Company: {company_name} ({self.ticker}). Industry: {self.stock_db_entry.industry}."
            comp_prompt = (f"Based on the business overview and MD&A for {company_name}: \"{comp_landscape_input}\"\n"
                           f"Describe its competitive landscape. Identify key competitors if mentioned or inferable, and discuss {company_name}'s competitive positioning (strengths/weaknesses relative to peers).")
            summary_results["competitive_landscape_summary"] = self.gemini.generate_text(comp_prompt)
            summary_results["qualitative_sources_summary"][
                "competitive_landscape_context"] = "Based on 10-K business/MD&A summaries."
            time.sleep(2)

        # Fallback to API profile if 10-K summaries are sparse for moat/industry
        business_desc_for_qual = summary_results.get("business_summary") or self._financial_data_cache.get(
            'profile_fmp', {}).get('description', '')
        if business_desc_for_qual and not summary_results.get(
                "economic_moat_summary"):  # Only if not already populated from more specific 10K section prompts
            qual_prompt_context = f"Company: {company_name} ({self.ticker}). Industry: {self.stock_db_entry.industry}."
            full_text_for_qual = f"{qual_prompt_context}\nBusiness Description: {business_desc_for_qual[:1000]}"
            if summary_results.get("risk_factors_summary"):
                full_text_for_qual += f"\nKey Risks: {summary_results.get('risk_factors_summary')[:500]}"

            moat_prompt = (
                f"Based on the business description and risks for {company_name}: \"{full_text_for_qual[:MAX_GEMINI_TEXT_LENGTH - 200]}\"\n"
                f"Analyze its primary economic moats (e.g., brand, network effects, switching costs, intangible assets, cost advantages). Provide a concise summary.")
            summary_results["economic_moat_summary"] = self.gemini.generate_text(moat_prompt)
            time.sleep(2)

        if self.stock_db_entry.industry and not summary_results.get("industry_trends_summary"):
            industry_prompt = (
                f"Company: {company_name} operates in the '{self.stock_db_entry.industry}' industry and '{self.stock_db_entry.sector}' sector.\n"
                f"Provide a concise analysis of the current key trends, growth drivers, opportunities, and significant challenges/risks for this industry and sector. How might {company_name} be generally positioned within these dynamics?")
            summary_results["industry_trends_summary"] = self.gemini.generate_text(industry_prompt)
            time.sleep(2)

        logger.info(f"10-K based qualitative summaries generated for {self.ticker}.")
        self._financial_data_cache['10k_summaries'] = summary_results
        return summary_results

    def _determine_investment_thesis(self):
        # This method remains largely the same as in the previous thought process,
        # but now it consumes from `self._financial_data_cache` which holds richer data.
        logger.info(f"Synthesizing investment thesis for {self.ticker}...")
        metrics = self._financial_data_cache.get('calculated_metrics', {})
        qual_summaries = self._financial_data_cache.get('10k_summaries', {})  # Now from 10-K
        dcf_results = self._financial_data_cache.get('dcf_results', {})
        company_profile = self._financial_data_cache.get('profile_fmp', {})

        company_name = self.stock_db_entry.company_name or self.ticker
        industry = self.stock_db_entry.industry or "N/A"
        sector = self.stock_db_entry.sector or "N/A"

        prompt = f"Company: {company_name} ({self.ticker})\n"
        prompt += f"Industry: {industry}, Sector: {sector}\n\n"
        prompt += "Key Financial Metrics (approximate values):\n"

        # Prioritize key metrics for the prompt to keep it concise yet informative
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
                        val_str = f"{val:.2%}"  # Percentage
                    else:
                        val_str = f"{val:.2f}"  # Decimal
                else:
                    val_str = str(val)  # String like "Growing"
                prompt += f"- {name}: {val_str}\n"

        if dcf_results.get("dcf_intrinsic_value") is not None:
            prompt += f"\nDCF Intrinsic Value per Share: {dcf_results['dcf_intrinsic_value']:.2f}\n"
            if dcf_results.get("dcf_upside_percentage") is not None:
                prompt += f"DCF Upside: {dcf_results['dcf_upside_percentage']:.2%}\n"
        if company_profile.get("price") is not None:
            prompt += f"Current Stock Price: {company_profile['price']:.2f}\n\n"

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

        final_prompt = prompt[:MAX_GEMINI_TEXT_LENGTH]  # Ensure prompt length
        if len(prompt) > MAX_GEMINI_TEXT_LENGTH:
            logger.warning(
                f"Investment thesis prompt for {self.ticker} truncated to {MAX_GEMINI_TEXT_LENGTH} chars for Gemini.")

        ai_response = self.gemini.generate_text(final_prompt)

        if ai_response.startswith("Error:"):
            logger.error(f"Gemini failed to generate investment thesis for {self.ticker}: {ai_response}")
            return {
                "investment_decision": "AI Error",
                "reasoning": ai_response,  # Store the error message
                "strategy_type": "N/A",
                "confidence_level": "N/A",
                "investment_thesis_full": ai_response  # Store the error message
            }

        # Enhanced parsing for structured output
        parsed_thesis = {}
        current_section = None
        thesis_lines = []
        reasoning_lines = []

        for line in ai_response.split('\n'):
            line_l = line.lower().strip()
            if line_l.startswith("investment thesis:"):
                current_section = "thesis"
                thesis_lines.append(line.split(":", 1)[1].strip())
                continue
            elif line_l.startswith("investment decision:"):
                current_section = "decision"
                parsed_thesis["investment_decision"] = line.split(":", 1)[1].strip()
                continue
            elif line_l.startswith("strategy type:"):
                current_section = "strategy"
                parsed_thesis["strategy_type"] = line.split(":", 1)[1].strip()
                continue
            elif line_l.startswith("confidence level:"):
                current_section = "confidence"
                parsed_thesis["confidence_level"] = line.split(":", 1)[1].strip()
                continue
            elif line_l.startswith("key reasoning:"):
                current_section = "reasoning"
                # Content after "Key Reasoning:" might be on the same line or start on next
                reasoning_content_on_header = line.split(":", 1)[1].strip()
                if reasoning_content_on_header: reasoning_lines.append(reasoning_content_on_header)
                continue

            if current_section == "thesis":
                thesis_lines.append(line)
            elif current_section == "reasoning":
                reasoning_lines.append(line)

        parsed_thesis["investment_thesis_full"] = "\n".join(
            thesis_lines).strip() if thesis_lines else ai_response  # Fallback
        parsed_thesis["reasoning"] = "\n".join(
            reasoning_lines).strip() if reasoning_lines else "Reasoning not explicitly parsed."

        # Fallbacks if specific sections aren't found
        if "investment_decision" not in parsed_thesis: parsed_thesis["investment_decision"] = "Review AI Output"
        if "strategy_type" not in parsed_thesis: parsed_thesis["strategy_type"] = "N/A"
        if "confidence_level" not in parsed_thesis: parsed_thesis["confidence_level"] = "N/A"

        logger.info(
            f"Generated investment thesis for {self.ticker}. Decision: {parsed_thesis.get('investment_decision')}")
        return parsed_thesis

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_analysis_data = {}  # This will hold all data for the StockAnalysis model instance

        try:
            if not self.stock_db_entry:  # Should have been caught by __init__ if critical
                logger.error(f"Stock entry for {self.ticker} was not properly initialized. Aborting analysis.")
                return None  # Analysis cannot proceed

            self._ensure_stock_db_entry_is_bound()  # Crucial for DB operations

            # --- Data Fetching ---
            self._fetch_financial_statements()  # Populates self._financial_data_cache
            self._fetch_key_metrics_and_profile_data()  # Populates self._financial_data_cache

            # --- Calculations ---
            calculated_metrics = self._calculate_derived_metrics()  # Uses data from cache
            final_analysis_data.update(calculated_metrics)

            dcf_results = self._perform_dcf_analysis()  # Uses data from cache
            final_analysis_data.update(dcf_results)

            # --- Qualitative Analysis from 10-K (if CIK available) or Profile ---
            qual_summaries = self._fetch_and_summarize_10k()  # Uses CIK from stock_db_entry
            final_analysis_data.update(qual_summaries)

            # --- Investment Thesis Synthesis ---
            investment_thesis_parts = self._determine_investment_thesis()  # Uses all cached data
            final_analysis_data.update(investment_thesis_parts)
            # Store the full thesis text separately if desired
            final_analysis_data["reasoning"] = investment_thesis_parts.get("investment_thesis_full",
                                                                           investment_thesis_parts.get("reasoning"))

            # --- Persist to DB ---
            # Map final_analysis_data to StockAnalysis model fields
            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id)

            model_fields = [col.key for col in StockAnalysis.__table__.columns if
                            col.key not in ['id', 'stock_id', 'analysis_date']]

            for field in model_fields:
                if field in final_analysis_data:
                    value_to_set = final_analysis_data[field]
                    # Ensure "N/A" or problematic floats don't go into numeric DB fields
                    if getattr(StockAnalysis, field).type.python_type == float:
                        if isinstance(value_to_set, str) and value_to_set == "N/A":
                            value_to_set = None
                        elif isinstance(value_to_set, float) and (math.isnan(value_to_set) or math.isinf(value_to_set)):
                            value_to_set = None

                    setattr(analysis_entry, field, value_to_set)

            # Explicitly set the reasoning from the full thesis if parsed structure was different
            analysis_entry.reasoning = final_analysis_data.get("reasoning", "AI reasoning not fully captured.")

            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = datetime.now(timezone.utc)  # Update timestamp on parent
            self.db_session.commit()

            logger.info(f"Successfully analyzed and saved stock: {self.ticker} (Analysis ID: {analysis_entry.id})")
            return analysis_entry

        except RuntimeError as r_err:  # Catch specific critical errors like session binding
            logger.critical(f"Runtime error during analysis for {self.ticker}: {r_err}", exc_info=True)
            # No rollback here as session might be the issue or already handled.
            return None
        except Exception as e:
            logger.error(f"CRITICAL error during full analysis pipeline for {self.ticker}: {e}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try:
                    self.db_session.rollback()
                    logger.info(f"Rolled back transaction for {self.ticker} due to error.")
                except Exception as e_rollback:
                    logger.error(f"Error during rollback for {self.ticker}: {e_rollback}")
            return None  # Indicate failure for this stock
        finally:
            self._close_session_if_active()  # Ensure session is closed

    def _ensure_stock_db_entry_is_bound(self):
        """Ensures the stock_db_entry is bound to the current active session."""
        # This method was detailed in the previous thought process.
        # It handles re-establishing session and merging/re-fetching the stock_db_entry if detached.
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} is INACTIVE. Re-establishing and re-fetching stock entry.")
            self._close_session_if_active()
            self.db_session = next(get_db_session())

            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock:  # Should not happen if _get_or_create_stock_entry succeeded
                logger.error(
                    f"Could not re-fetch stock {self.ticker} ({self.stock_db_entry.id if self.stock_db_entry and self.stock_db_entry.id else 'Unknown ID'}) after session re-establishment. This may indicate a problem with initial creation or commit.")
                # Attempt to re-run _get_or_create_stock_entry to fix the state
                self._get_or_create_stock_entry()
                if not self.stock_db_entry:  # If still no stock_db_entry, then critical failure.
                    raise RuntimeError(f"Failed to create or re-fetch stock {self.ticker} for active session.")
                return  # Successfully re-created/fetched
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
                # If the object is transient (no ID yet, implies it wasn't committed from a previous session or is new)
                # and it's not in the current session, merging might attach it or raise error if PK conflicts.
                # If it has an ID but is detached, merge should re-associate.
                merged_stock = self.db_session.merge(self.stock_db_entry)
                self.stock_db_entry = merged_stock
                # self.db_session.flush() # Ensure it's in session's identity map and any FKs are valid.
                logger.info(
                    f"Successfully merged/re-associated stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session {id(self.db_session)}.")
            except Exception as e_merge:
                logger.error(f"Failed to merge stock {self.ticker} into session: {e_merge}. Re-fetching as fallback.",
                             exc_info=True)
                # As a robust fallback, try to re-fetch the object from the current session using its ID or ticker
                primary_key_to_fetch = self.stock_db_entry.id if instance_state.has_identity and self.stock_db_entry.id else None

                re_fetched_stock_on_merge_fail = None
                if primary_key_to_fetch:
                    re_fetched_stock_on_merge_fail = self.db_session.query(Stock).get(primary_key_to_fetch)

                if not re_fetched_stock_on_merge_fail:  # If ID fetch failed or no ID
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

    # init_db() # Ensure DB is initialized, or use migrations for changes

    logger.info("Starting standalone stock analysis test...")
    # Test with diverse tickers, including one that might not have a CIK easily found by FMP to test SEC EDGAR client path
    tickers_to_test = ["AAPL", "MSFT", "GOOGL", "JPM", "NONEXISTENTTICKERXYZ"]

    for ticker_symbol in tickers_to_test:
        analysis_result = None  # Ensure it's defined for logging in finally
        try:
            logger.info(f"--- Analyzing {ticker_symbol} ---")
            analyzer = StockAnalyzer(ticker=ticker_symbol)  # Initialization might fail
            analysis_result = analyzer.analyze()  # Analysis might fail

            if analysis_result:
                logger.info(
                    f"Analysis for {analysis_result.stock.ticker} completed. Decision: {analysis_result.investment_decision}, Confidence: {analysis_result.confidence_level}")
                if analysis_result.dcf_intrinsic_value is not None:
                    logger.info(
                        f"DCF Value: {analysis_result.dcf_intrinsic_value:.2f}, Upside: {analysis_result.dcf_upside_percentage:.2% if analysis_result.dcf_upside_percentage is not None else 'N/A'}")
                else:
                    logger.info("DCF analysis did not yield an intrinsic value.")
                logger.info(f"Reasoning highlights: {analysis_result.reasoning[:300]}...")

            else:
                logger.error(f"Stock analysis pipeline FAILED for {ticker_symbol} (returned None).")
        except RuntimeError as rt_err:  # Catch init errors
            logger.error(f"Could not initialize StockAnalyzer for {ticker_symbol}: {rt_err}")
        except Exception as e:
            logger.error(f"Unhandled error analyzing {ticker_symbol} in __main__: {e}", exc_info=True)
        finally:
            logger.info(f"--- Finished processing {ticker_symbol} ---")
            if analysis_result is None: logger.info(f"No analysis result object for {ticker_symbol}")
            time.sleep(10)  # Increased delay due to more API calls (SEC, more Gemini) per stock