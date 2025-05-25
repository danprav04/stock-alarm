# stock_analyzer.py
import pandas as pd
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone, timedelta
import math
import time
import warnings
from bs4 import XMLParsedAsHTMLWarning
import re

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
    TEN_K_KEY_SECTIONS,
    SUMMARIZATION_CHUNK_SIZE_CHARS, SUMMARIZATION_CHUNK_OVERLAP_CHARS,
    SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS
)


def safe_get_float(data_dict, key, default=None):
    val = data_dict.get(key)
    if val is None or val == "None" or val == "": return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def calculate_cagr(end_value, start_value, years):
    if start_value is None or end_value is None or not isinstance(years, (int, float)) or years <= 0: return None
    if start_value == 0: return None
    if start_value < 0 and end_value < 0: return ((float(end_value) / float(start_value)) ** (1 / float(years))) - 1
    if start_value < 0 or end_value < 0: return None
    if end_value == 0 and start_value > 0: return -1.0
    return ((float(end_value) / float(start_value)) ** (1 / float(years))) - 1


def calculate_growth(current_value, previous_value):
    if previous_value is None or current_value is None: return None
    if float(previous_value) == 0: return None
    try:
        return (float(current_value) - float(previous_value)) / abs(float(previous_value))
    except (ValueError, TypeError):
        return None


def get_value_from_statement_list(data_list, field, year_offset=0, report_date_for_log=None):
    if data_list and isinstance(data_list, list) and len(data_list) > year_offset:
        report = data_list[year_offset]
        if report and isinstance(report, dict):
            val = safe_get_float(report, field)
            if val is None:
                date_info = report_date_for_log or report.get('date', 'Unknown Date')
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
        self._financial_data_cache = {}
        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"CRITICAL: Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            self._close_session_if_active()
            raise RuntimeError(
                f"StockAnalyzer for {self.ticker} could not be initialized due to DB/API issues during stock entry setup.") from e

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close(); logger.debug(f"DB session closed for {self.ticker}.")
            except Exception as e_close:
                logger.warning(f"Error closing session for {self.ticker}: {e_close}")

    def _get_or_create_stock_entry(self):
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} inactive in _get_or_create. Re-establishing.")
            self._close_session_if_active();
            self.db_session = next(get_db_session())
        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()
        company_name, industry, sector, cik = None, None, None, None
        profile_fmp_list = self.fmp.get_company_profile(self.ticker);
        time.sleep(1)
        if profile_fmp_list and isinstance(profile_fmp_list, list) and len(profile_fmp_list) > 0 and profile_fmp_list[
            0]:
            data = profile_fmp_list[0];
            self._financial_data_cache['profile_fmp'] = data
            company_name, industry, sector, cik = data.get('companyName'), data.get('industry'), data.get(
                'sector'), data.get('cik')
            logger.info(f"Fetched profile from FMP for {self.ticker}.")
        else:
            logger.warning(f"FMP profile failed for {self.ticker}. Trying Finnhub.")
            profile_fh = self.finnhub.get_company_profile2(self.ticker);
            time.sleep(1)
            if profile_fh:
                self._financial_data_cache['profile_finnhub'] = profile_fh
                company_name, industry = profile_fh.get('name'), profile_fh.get('finnhubIndustry')
                logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Finnhub profile failed for {self.ticker}. Trying Alpha Vantage Overview.")
                overview_av = self.alphavantage.get_company_overview(self.ticker);
                time.sleep(2)
                if overview_av and overview_av.get("Symbol") == self.ticker:
                    self._financial_data_cache['overview_alphavantage'] = overview_av
                    company_name, industry, sector, cik = overview_av.get('Name'), overview_av.get(
                        'Industry'), overview_av.get('Sector'), overview_av.get('CIK')
                    logger.info(f"Fetched overview from Alpha Vantage for {self.ticker}.")
                else:
                    logger.warning(f"All primary profile fetches (FMP, Finnhub, AV) failed for {self.ticker}.")
        if not company_name: company_name = self.ticker
        if not cik and self.ticker:
            logger.info(f"CIK not found from profiles for {self.ticker}. Querying SEC EDGAR.")
            cik_from_edgar = self.sec_edgar.get_cik_by_ticker(self.ticker);
            time.sleep(0.5)
            if cik_from_edgar:
                cik = cik_from_edgar; logger.info(f"Fetched CIK {cik} from SEC EDGAR for {self.ticker}.")
            else:
                logger.warning(f"Could not fetch CIK from SEC EDGAR for {self.ticker}.")
        if not self.stock_db_entry:
            logger.info(f"Stock {self.ticker} not found in DB, creating new entry.")
            self.stock_db_entry = Stock(ticker=self.ticker, company_name=company_name, industry=industry, sector=sector,
                                        cik=cik)
            self.db_session.add(self.stock_db_entry)
            try:
                self.db_session.commit(); self.db_session.refresh(self.stock_db_entry)
            except SQLAlchemyError as e:
                self.db_session.rollback(); logger.error(f"Error creating stock entry for {self.ticker}: {e}"); raise
        else:
            updated = False
            if company_name and self.stock_db_entry.company_name != company_name: self.stock_db_entry.company_name = company_name; updated = True
            if industry and self.stock_db_entry.industry != industry: self.stock_db_entry.industry = industry; updated = True
            if sector and self.stock_db_entry.sector != sector: self.stock_db_entry.sector = sector; updated = True
            if cik and self.stock_db_entry.cik != cik:
                self.stock_db_entry.cik = cik; updated = True
            elif not self.stock_db_entry.cik and cik:
                self.stock_db_entry.cik = cik; updated = True
            if updated:
                try:
                    self.db_session.commit(); self.db_session.refresh(self.stock_db_entry); logger.info(
                        f"Updated stock entry for {self.ticker}.")
                except SQLAlchemyError as e:
                    self.db_session.rollback(); logger.error(f"Error updating stock entry for {self.ticker}: {e}")
        logger.info(
            f"Stock entry for {self.ticker} (ID: {self.stock_db_entry.id if self.stock_db_entry else 'N/A'}, CIK: {self.stock_db_entry.cik if self.stock_db_entry and self.stock_db_entry.cik else 'N/A'}) ready.")

    def _fetch_financial_statements(self):
        logger.info(f"Fetching financial statements for {self.ticker}...")
        statements_cache = {
            "fmp_income_annual": [], "fmp_balance_annual": [], "fmp_cashflow_annual": [],
            "finnhub_financials_quarterly_reported": {"data": []},
            "alphavantage_income_quarterly": {"quarterlyReports": []},
            "alphavantage_balance_quarterly": {"quarterlyReports": []},
            "alphavantage_cashflow_quarterly": {"quarterlyReports": []}
        }
        try:
            statements_cache["fmp_income_annual"] = self.fmp.get_financial_statements(self.ticker, "income-statement",
                                                                                      "annual",
                                                                                      STOCK_FINANCIAL_YEARS) or [];
            time.sleep(1.5)
            statements_cache["fmp_balance_annual"] = self.fmp.get_financial_statements(self.ticker,
                                                                                       "balance-sheet-statement",
                                                                                       "annual",
                                                                                       STOCK_FINANCIAL_YEARS) or [];
            time.sleep(1.5)
            statements_cache["fmp_cashflow_annual"] = self.fmp.get_financial_statements(self.ticker,
                                                                                        "cash-flow-statement", "annual",
                                                                                        STOCK_FINANCIAL_YEARS) or [];
            time.sleep(1.5)
            logger.info(
                f"FMP Annuals for {self.ticker}: Income({len(statements_cache['fmp_income_annual'])}), Balance({len(statements_cache['fmp_balance_annual'])}), Cashflow({len(statements_cache['fmp_cashflow_annual'])}).")
            fh_q_data = self.finnhub.get_financials_reported(self.ticker, freq="quarterly");
            time.sleep(1.5)
            if fh_q_data and isinstance(fh_q_data, dict) and fh_q_data.get("data"):
                statements_cache["finnhub_financials_quarterly_reported"] = fh_q_data;
                logger.info(f"Fetched {len(fh_q_data['data'])} quarterly reports from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Finnhub quarterly financials reported data missing or malformed for {self.ticker}.")
            av_income_q = self.alphavantage.get_income_statement_quarterly(self.ticker);
            time.sleep(15)
            if av_income_q and isinstance(av_income_q, dict) and av_income_q.get("quarterlyReports"):
                statements_cache["alphavantage_income_quarterly"] = av_income_q;
                logger.info(
                    f"Fetched {len(av_income_q['quarterlyReports'])} quarterly income reports from Alpha Vantage for {self.ticker}.")
            else:
                logger.warning(f"Alpha Vantage quarterly income reports missing or malformed for {self.ticker}.")
            av_balance_q = self.alphavantage.get_balance_sheet_quarterly(self.ticker);
            time.sleep(15)
            if av_balance_q and isinstance(av_balance_q, dict) and av_balance_q.get("quarterlyReports"):
                statements_cache["alphavantage_balance_quarterly"] = av_balance_q;
                logger.info(
                    f"Fetched {len(av_balance_q['quarterlyReports'])} quarterly balance reports from Alpha Vantage for {self.ticker}.")
            else:
                logger.warning(f"Alpha Vantage quarterly balance reports missing or malformed for {self.ticker}.")
            av_cashflow_q = self.alphavantage.get_cash_flow_quarterly(self.ticker);
            time.sleep(15)
            if av_cashflow_q and isinstance(av_cashflow_q, dict) and av_cashflow_q.get("quarterlyReports"):
                statements_cache["alphavantage_cashflow_quarterly"] = av_cashflow_q;
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
        self._financial_data_cache['key_metrics_annual_fmp'] = self.fmp.get_key_metrics(self.ticker, "annual",
                                                                                        STOCK_FINANCIAL_YEARS + 2) or [];
        time.sleep(1.5)
        key_metrics_quarterly_fmp = self.fmp.get_key_metrics(self.ticker, "quarterly", 8);
        time.sleep(1.5)
        if key_metrics_quarterly_fmp is None:
            logger.warning(f"FMP quarterly key metrics API call failed for {self.ticker}. Data will be empty.");
            self._financial_data_cache['key_metrics_quarterly_fmp'] = []
        else:
            self._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp or []
        self._financial_data_cache['basic_financials_finnhub'] = self.finnhub.get_basic_financials(self.ticker) or {};
        time.sleep(1.5)
        if 'profile_fmp' not in self._financial_data_cache or not self._financial_data_cache.get('profile_fmp'):
            profile_fmp_list = self.fmp.get_company_profile(self.ticker);
            time.sleep(1.5)
            self._financial_data_cache['profile_fmp'] = profile_fmp_list[0] if profile_fmp_list and isinstance(
                profile_fmp_list, list) and profile_fmp_list[0] else {}
        logger.info(
            f"FMP KM Annual for {self.ticker}: {len(self._financial_data_cache['key_metrics_annual_fmp'])}. "
            f"FMP KM Quarterly for {self.ticker}: {len(self._financial_data_cache['key_metrics_quarterly_fmp'])}. "
            f"Finnhub Basic Financials for {self.ticker}: {'OK' if self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric') else 'Data missing'}.")

    def _calculate_valuation_ratios(self, latest_km_q, latest_km_a, basic_fin_fh_metric):
        ratios = {}
        ratios["pe_ratio"] = safe_get_float(latest_km_q, "peRatioTTM") or safe_get_float(latest_km_a,
                                                                                         "peRatio") or safe_get_float(
            basic_fin_fh_metric, "peTTM")
        ratios["pb_ratio"] = safe_get_float(latest_km_q, "priceToBookRatioTTM") or safe_get_float(latest_km_a,
                                                                                                  "pbRatio") or safe_get_float(
            basic_fin_fh_metric, "pbAnnual")
        ratios["ps_ratio"] = safe_get_float(latest_km_q, "priceToSalesRatioTTM") or safe_get_float(latest_km_a,
                                                                                                   "priceSalesRatio") or safe_get_float(
            basic_fin_fh_metric, "psTTM")
        ratios["ev_to_sales"] = safe_get_float(latest_km_q, "enterpriseValueOverRevenueTTM") or safe_get_float(
            latest_km_a, "enterpriseValueOverRevenue")
        ratios["ev_to_ebitda"] = safe_get_float(latest_km_q, "evToEbitdaTTM") or safe_get_float(latest_km_a,
                                                                                                "evToEbitda")
        div_yield_fmp_q = safe_get_float(latest_km_q, "dividendYieldTTM")
        div_yield_fmp_a = safe_get_float(latest_km_a, "dividendYield")
        div_yield_fh = safe_get_float(basic_fin_fh_metric, "dividendYieldAnnual")
        if div_yield_fh is not None: div_yield_fh /= 100.0
        ratios["dividend_yield"] = div_yield_fmp_q if div_yield_fmp_q is not None else (
            div_yield_fmp_a if div_yield_fmp_a is not None else div_yield_fh)
        return ratios

    def _calculate_profitability_metrics(self, income_annual, balance_annual, latest_km_a):
        metrics = {}
        if income_annual:
            latest_ia = income_annual[0]
            metrics["eps"] = safe_get_float(latest_ia, "eps") or safe_get_float(latest_km_a, "eps")
            metrics["net_profit_margin"] = safe_get_float(latest_ia, "netProfitMargin")
            metrics["gross_profit_margin"] = safe_get_float(latest_ia, "grossProfitMargin")
            metrics["operating_profit_margin"] = safe_get_float(latest_ia, "operatingIncomeRatio")
            ebit = safe_get_float(latest_ia, "operatingIncome")
            interest_expense = safe_get_float(latest_ia, "interestExpense")
            if ebit is not None and interest_expense is not None and abs(interest_expense) > 1e-6:
                metrics["interest_coverage_ratio"] = ebit / abs(interest_expense)
        if balance_annual and income_annual:
            latest_ba = balance_annual[0]
            total_equity = safe_get_float(latest_ba, "totalStockholdersEquity")
            total_assets = safe_get_float(latest_ba, "totalAssets")
            latest_net_income = get_value_from_statement_list(income_annual, "netIncome", 0, latest_ba.get('date'))
            if total_equity and total_equity != 0 and latest_net_income is not None: metrics[
                "roe"] = latest_net_income / total_equity
            if total_assets and total_assets != 0 and latest_net_income is not None: metrics[
                "roa"] = latest_net_income / total_assets
            # ROIC
            ebit_roic = get_value_from_statement_list(income_annual, "operatingIncome", 0)
            income_tax_expense_roic = get_value_from_statement_list(income_annual, "incomeTaxExpense", 0)
            income_before_tax_roic = get_value_from_statement_list(income_annual, "incomeBeforeTax", 0)
            effective_tax_rate = 0.21
            if income_tax_expense_roic is not None and income_before_tax_roic is not None and income_before_tax_roic != 0:
                effective_tax_rate = income_tax_expense_roic / income_before_tax_roic
            nopat = ebit_roic * (1 - effective_tax_rate) if ebit_roic is not None else None
            total_debt_roic = get_value_from_statement_list(balance_annual, "totalDebt", 0)
            cash_equivalents_roic = get_value_from_statement_list(balance_annual, "cashAndCashEquivalents", 0) or 0
            if total_debt_roic is not None and total_equity is not None:  # total_equity already fetched
                invested_capital = total_debt_roic + total_equity - cash_equivalents_roic
                if nopat is not None and invested_capital is not None and invested_capital != 0: metrics[
                    "roic"] = nopat / invested_capital
        return metrics

    def _calculate_financial_health_metrics(self, balance_annual, income_annual, latest_km_a):
        metrics = {}
        if balance_annual:
            latest_ba = balance_annual[0]
            total_equity = safe_get_float(latest_ba, "totalStockholdersEquity")
            metrics["debt_to_equity"] = safe_get_float(latest_km_a, "debtToEquity")
            if metrics["debt_to_equity"] is None:
                total_debt_ba = safe_get_float(latest_ba, "totalDebt")
                if total_debt_ba is not None and total_equity and total_equity != 0: metrics[
                    "debt_to_equity"] = total_debt_ba / total_equity
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
        latest_annual_ebitda = safe_get_float(latest_km_a, "ebitda") or get_value_from_statement_list(income_annual,
                                                                                                      "ebitda", 0)
        if latest_annual_ebitda and latest_annual_ebitda != 0 and balance_annual:
            total_debt_val = get_value_from_statement_list(balance_annual, "totalDebt", 0)
            if total_debt_val is not None: metrics["debt_to_ebitda"] = total_debt_val / latest_annual_ebitda
        return metrics

    def _calculate_growth_metrics(self, income_annual, av_income_q_reports, fh_q_reports_list):
        metrics = {"key_metrics_snapshot": {}}  # Snapshot for specific data points like latest quarterly revenue source
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
        if len(income_annual) >= 5:
            metrics["revenue_growth_cagr_5yr"] = calculate_cagr(
                get_value_from_statement_list(income_annual, "revenue", 0),
                get_value_from_statement_list(income_annual, "revenue", 4), 4)
            metrics["eps_growth_cagr_5yr"] = calculate_cagr(get_value_from_statement_list(income_annual, "eps", 0),
                                                            get_value_from_statement_list(income_annual, "eps", 4), 4)
        latest_q_revenue_av = get_alphavantage_value(av_income_q_reports, "totalRevenue", 0)
        previous_q_revenue_av = get_alphavantage_value(av_income_q_reports, "totalRevenue", 1)
        if latest_q_revenue_av is not None and previous_q_revenue_av is not None:
            metrics["revenue_growth_qoq"] = calculate_growth(latest_q_revenue_av, previous_q_revenue_av)
            metrics["key_metrics_snapshot"]["q_revenue_source"], metrics["key_metrics_snapshot"][
                "latest_q_revenue"] = "AlphaVantage", latest_q_revenue_av
        else:
            revenue_concepts_fh = ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "TotalRevenues",
                                   "NetSales"]
            latest_q_revenue_fh = get_finnhub_concept_value(fh_q_reports_list, 'ic', revenue_concepts_fh, 0)
            previous_q_revenue_fh = get_finnhub_concept_value(fh_q_reports_list, 'ic', revenue_concepts_fh, 1)
            if latest_q_revenue_fh is not None and previous_q_revenue_fh is not None:
                metrics["revenue_growth_qoq"] = calculate_growth(latest_q_revenue_fh, previous_q_revenue_fh)
                metrics["key_metrics_snapshot"]["q_revenue_source"], metrics["key_metrics_snapshot"][
                    "latest_q_revenue"] = "Finnhub", latest_q_revenue_fh
            else:
                logger.info(f"Could not calculate QoQ revenue for {self.ticker} from AlphaVantage or Finnhub.");
                metrics["revenue_growth_qoq"] = None
        return metrics

    def _calculate_cash_flow_and_trend_metrics(self, cashflow_annual, balance_annual, profile_fmp):
        metrics = {}
        if cashflow_annual:
            fcf_latest_annual = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
            shares_outstanding = safe_get_float(profile_fmp, "sharesOutstanding") or \
                                 (safe_get_float(profile_fmp, "mktCap") / safe_get_float(profile_fmp, "price")
                                  if safe_get_float(profile_fmp, "price") and safe_get_float(profile_fmp,
                                                                                             "price") != 0 else None)
            if fcf_latest_annual is not None and shares_outstanding and shares_outstanding != 0:
                metrics["free_cash_flow_per_share"] = fcf_latest_annual / shares_outstanding
                market_cap_for_yield = safe_get_float(profile_fmp, "mktCap")
                if market_cap_for_yield and market_cap_for_yield != 0: metrics[
                    "free_cash_flow_yield"] = fcf_latest_annual / market_cap_for_yield
            if len(cashflow_annual) >= 3:
                fcf0, fcf1, fcf2 = get_value_from_statement_list(cashflow_annual, "freeCashFlow",
                                                                 0), get_value_from_statement_list(cashflow_annual,
                                                                                                   "freeCashFlow",
                                                                                                   1), get_value_from_statement_list(
                    cashflow_annual, "freeCashFlow", 2)
                if all(isinstance(x, (int, float)) for x in [fcf0, fcf1, fcf2] if x is not None) and all(
                        x is not None for x in [fcf0, fcf1, fcf2]):
                    if fcf0 > fcf1 > fcf2:
                        metrics["free_cash_flow_trend"] = "Growing"
                    elif fcf0 < fcf1 < fcf2:
                        metrics["free_cash_flow_trend"] = "Declining"
                    else:
                        metrics["free_cash_flow_trend"] = "Mixed/Stable"
                else:
                    metrics["free_cash_flow_trend"] = "Data Incomplete/Non-Numeric"
            else:
                metrics["free_cash_flow_trend"] = "Data N/A (<3 yrs)"
        if len(balance_annual) >= 3:
            re0, re1, re2 = get_value_from_statement_list(balance_annual, "retainedEarnings",
                                                          0), get_value_from_statement_list(balance_annual,
                                                                                            "retainedEarnings",
                                                                                            1), get_value_from_statement_list(
                balance_annual, "retainedEarnings", 2)
            if all(isinstance(x, (int, float)) for x in [re0, re1, re2] if x is not None) and all(
                    x is not None for x in [re0, re1, re2]):
                if re0 > re1 > re2:
                    metrics["retained_earnings_trend"] = "Growing"
                elif re0 < re1 < re2:
                    metrics["retained_earnings_trend"] = "Declining"
                else:
                    metrics["retained_earnings_trend"] = "Mixed/Stable"
            else:
                metrics["retained_earnings_trend"] = "Data Incomplete/Non-Numeric"
        else:
            metrics["retained_earnings_trend"] = "Data N/A (<3 yrs)"
        return metrics

    def _calculate_derived_metrics(self):
        logger.info(f"Calculating derived metrics for {self.ticker}...")
        all_metrics = {}
        statements = self._financial_data_cache.get('financial_statements', {})
        income_annual = sorted(statements.get('fmp_income_annual', []), key=lambda x: x.get("date", ""), reverse=True)
        balance_annual = sorted(statements.get('fmp_balance_annual', []), key=lambda x: x.get("date", ""), reverse=True)
        cashflow_annual = sorted(statements.get('fmp_cashflow_annual', []), key=lambda x: x.get("date", ""),
                                 reverse=True)
        av_income_q_reports = statements.get('alphavantage_income_quarterly', {}).get('quarterlyReports', [])
        fh_q_reports_list = statements.get('finnhub_financials_quarterly_reported', {}).get('data', [])
        key_metrics_annual = self._financial_data_cache.get('key_metrics_annual_fmp', [])
        key_metrics_quarterly = self._financial_data_cache.get('key_metrics_quarterly_fmp', [])
        basic_fin_fh_metric = self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {})
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        latest_km_q = key_metrics_quarterly[0] if key_metrics_quarterly and isinstance(key_metrics_quarterly, list) and \
                                                  key_metrics_quarterly[0] else {}
        latest_km_a = key_metrics_annual[0] if key_metrics_annual and isinstance(key_metrics_annual, list) and \
                                               key_metrics_annual[0] else {}

        all_metrics.update(self._calculate_valuation_ratios(latest_km_q, latest_km_a, basic_fin_fh_metric))
        all_metrics.update(self._calculate_profitability_metrics(income_annual, balance_annual, latest_km_a))
        all_metrics.update(self._calculate_financial_health_metrics(balance_annual, income_annual, latest_km_a))
        growth_metrics = self._calculate_growth_metrics(income_annual, av_income_q_reports, fh_q_reports_list)
        all_metrics.update(growth_metrics)  # Includes key_metrics_snapshot
        all_metrics.update(self._calculate_cash_flow_and_trend_metrics(cashflow_annual, balance_annual, profile_fmp))

        final_metrics = {}
        for k, v in all_metrics.items():
            if k == "key_metrics_snapshot":
                final_metrics[k] = {sk: sv for sk, sv in v.items() if sv is not None and not (
                            isinstance(sv, float) and (math.isnan(sv) or math.isinf(sv)))}
            elif isinstance(v, float):
                final_metrics[k] = v if not (math.isnan(v) or math.isinf(v)) else None
            elif v is not None:
                final_metrics[k] = v
            else:
                final_metrics[k] = None
        log_metrics = {k: v for k, v in final_metrics.items() if k != "key_metrics_snapshot"}
        logger.info(f"Calculated metrics for {self.ticker}: {log_metrics}")
        self._financial_data_cache['calculated_metrics'] = final_metrics
        return final_metrics

    def _perform_dcf_analysis(self):
        logger.info(f"Performing simplified DCF analysis for {self.ticker}...")
        dcf_results = {"dcf_intrinsic_value": None, "dcf_upside_percentage": None,
                       "dcf_assumptions": {"discount_rate": DEFAULT_DISCOUNT_RATE,
                                           "perpetual_growth_rate": DEFAULT_PERPETUAL_GROWTH_RATE,
                                           "projection_years": DEFAULT_FCF_PROJECTION_YEARS, "start_fcf": None,
                                           "fcf_growth_rates_projection": []}}
        cashflow_annual = sorted(
            self._financial_data_cache.get('financial_statements', {}).get('fmp_cashflow_annual', []),
            key=lambda x: x.get("date", ""), reverse=True)
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        calculated_metrics = self._financial_data_cache.get('calculated_metrics', {})
        current_price = safe_get_float(profile_fmp, "price")
        shares_outstanding = safe_get_float(profile_fmp, "sharesOutstanding") or \
                             (safe_get_float(profile_fmp,
                                             "mktCap") / current_price if current_price and current_price != 0 else None)
        if not cashflow_annual or not profile_fmp or current_price is None or shares_outstanding is None or shares_outstanding == 0:
            logger.warning(f"Insufficient data for DCF for {self.ticker}.");
            return dcf_results
        current_fcf = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
        if current_fcf is None or current_fcf <= 10000:
            logger.warning(f"Current FCF for {self.ticker} is {current_fcf}. DCF requires substantial positive FCF.");
            return dcf_results
        dcf_results["dcf_assumptions"]["start_fcf"] = current_fcf
        fcf_growth_rate_3yr_cagr = None
        if len(cashflow_annual) >= 4:
            fcf_start_for_cagr = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 3)
            if fcf_start_for_cagr and fcf_start_for_cagr > 0: fcf_growth_rate_3yr_cagr = calculate_cagr(current_fcf,
                                                                                                        fcf_start_for_cagr,
                                                                                                        3)
        initial_fcf_growth_rate = fcf_growth_rate_3yr_cagr if fcf_growth_rate_3yr_cagr is not None else \
            calculated_metrics.get("revenue_growth_cagr_3yr") if calculated_metrics.get(
                "revenue_growth_cagr_3yr") is not None else \
                calculated_metrics.get("revenue_growth_yoy") if calculated_metrics.get(
                    "revenue_growth_yoy") is not None else 0.05
        if not isinstance(initial_fcf_growth_rate, (int, float)): initial_fcf_growth_rate = 0.05
        initial_fcf_growth_rate = min(max(initial_fcf_growth_rate, -0.10), 0.20)
        projected_fcfs, last_projected_fcf = [], current_fcf
        growth_rate_decline_per_year = (initial_fcf_growth_rate - DEFAULT_PERPETUAL_GROWTH_RATE) / float(
            DEFAULT_FCF_PROJECTION_YEARS) if DEFAULT_FCF_PROJECTION_YEARS > 0 else 0
        for i in range(DEFAULT_FCF_PROJECTION_YEARS):
            current_year_growth_rate = max(initial_fcf_growth_rate - (growth_rate_decline_per_year * i),
                                           DEFAULT_PERPETUAL_GROWTH_RATE)
            projected_fcf = last_projected_fcf * (1 + current_year_growth_rate);
            projected_fcfs.append(projected_fcf);
            last_projected_fcf = projected_fcf
            dcf_results["dcf_assumptions"]["fcf_growth_rates_projection"].append(round(current_year_growth_rate, 4))
        if not projected_fcfs: logger.error(f"DCF: No projected FCFs for {self.ticker}."); return dcf_results
        terminal_year_fcf = projected_fcfs[-1] * (1 + DEFAULT_PERPETUAL_GROWTH_RATE)
        terminal_value_denominator = DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE
        terminal_value = terminal_year_fcf / terminal_value_denominator if terminal_value_denominator > 1e-6 else 0
        if terminal_value_denominator <= 1e-6: logger.warning(
            f"DCF for {self.ticker}: Discount rate near/below perpetual growth. TV unreliable.")
        sum_discounted_fcf = sum(fcf / ((1 + DEFAULT_DISCOUNT_RATE) ** (i + 1)) for i, fcf in enumerate(projected_fcfs))
        discounted_terminal_value = terminal_value / ((1 + DEFAULT_DISCOUNT_RATE) ** DEFAULT_FCF_PROJECTION_YEARS)
        intrinsic_equity_value = sum_discounted_fcf + discounted_terminal_value
        if shares_outstanding != 0:
            intrinsic_value_per_share = intrinsic_equity_value / shares_outstanding;
            dcf_results["dcf_intrinsic_value"] = intrinsic_value_per_share
            if current_price and current_price != 0: dcf_results["dcf_upside_percentage"] = (
                                                                                                        intrinsic_value_per_share - current_price) / current_price
        logger.info(
            f"DCF for {self.ticker}: IV/Share: {dcf_results.get('dcf_intrinsic_value', 'N/A')}, Upside: {dcf_results.get('dcf_upside_percentage', 'N/A') * 100 if dcf_results.get('dcf_upside_percentage') is not None else 'N/A'}%")
        self._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    def _summarize_text_chunked(self, text_to_summarize, base_context, section_specific_instruction,
                                company_name_ticker_prompt):
        """Summarizes text, using chunking for very long inputs."""
        if not text_to_summarize:
            return "No text provided for summarization.", 0

        text_len = len(text_to_summarize)
        logger.info(f"Summarizing section for {company_name_ticker_prompt}, original length: {text_len} chars.")

        # If text is short enough for a single pass (considering prompt overhead)
        if text_len < SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS:  # Max for a single pass, slightly less than hard limit
            logger.info(f"Section length {text_len} is within single-pass limit. Summarizing directly.")
            full_context = f"{base_context} for {company_name_ticker_prompt}."
            summary = self.gemini.summarize_text_with_context(text_to_summarize, full_context,
                                                              section_specific_instruction)
            time.sleep(2)  # API courtesy
            return (summary if summary and not summary.startswith(
                "Error:") else f"AI summary error or no content."), text_len

        # Text is long, proceed with chunking
        logger.info(f"Section length {text_len} exceeds single-pass limit. Applying chunked summarization.")
        chunks = []
        start = 0
        while start < text_len:
            end = start + SUMMARIZATION_CHUNK_SIZE_CHARS
            chunks.append(text_to_summarize[start:end])
            start = end - SUMMARIZATION_CHUNK_OVERLAP_CHARS if end < text_len else end  # Ensure overlap

        chunk_summaries = []
        for i, chunk in enumerate(chunks):
            logger.info(
                f"Summarizing chunk {i + 1}/{len(chunks)} for {company_name_ticker_prompt} (length: {len(chunk)} chars).")
            chunk_context = f"{base_context} for {company_name_ticker_prompt} (this is chunk {i + 1} of {len(chunks)})."
            chunk_instruction = f"Summarize this chunk of the '{base_context}' section. Focus on key facts and figures relevant to {section_specific_instruction}"
            summary = self.gemini.summarize_text_with_context(chunk, chunk_context, chunk_instruction)
            time.sleep(2)  # API courtesy
            if summary and not summary.startswith("Error:"):
                chunk_summaries.append(summary)
            else:
                chunk_summaries.append(f"[AI error or no content for chunk {i + 1}]")

        concatenated_summaries = "\n\n".join(chunk_summaries)
        logger.info(
            f"Concatenated chunk summaries length: {len(concatenated_summaries)} chars for {company_name_ticker_prompt}.")

        if not concatenated_summaries.strip() or all("[AI error" in s for s in chunk_summaries):
            return "Failed to generate summaries for any chunk.", text_len

        # If concatenated summaries are too long, summarize them
        if len(concatenated_summaries) > SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS:
            logger.info(
                f"Concatenated summaries too long. Performing a final 'summary of summaries' pass for {company_name_ticker_prompt}.")
            final_summary_context = f"The following are collated summaries from different parts of the '{base_context}' section for {company_name_ticker_prompt}."
            final_summary_instruction = f"Synthesize these summaries into a single, cohesive overview of the '{base_context}', maintaining factual accuracy and covering points relevant to {section_specific_instruction}."
            final_summary = self.gemini.summarize_text_with_context(concatenated_summaries, final_summary_context,
                                                                    final_summary_instruction)
            time.sleep(2)  # API courtesy
            return (final_summary if final_summary and not final_summary.startswith(
                "Error:") else "AI error in final summary pass."), text_len
        else:
            return concatenated_summaries, text_len

    def _fetch_and_summarize_10k(self):
        logger.info(f"Fetching and attempting to summarize latest 10-K for {self.ticker}")
        summary_results = {"qualitative_sources_summary": {}}
        if not self.stock_db_entry or not self.stock_db_entry.cik:
            logger.warning(f"No CIK for {self.ticker}. Cannot fetch 10-K.");
            return summary_results

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
        text_content = self.sec_edgar.get_filing_text(filing_url)
        if not text_content:
            logger.warning(f"Failed to fetch/load 10-K text from {filing_url}");
            return summary_results

        logger.info(
            f"Fetched 10-K text (length: {len(text_content)}) for {self.ticker}. Extracting and summarizing sections.")
        sections = extract_S1_text_sections(text_content, TEN_K_KEY_SECTIONS)
        company_name_for_prompt = self.stock_db_entry.company_name or self.ticker

        section_details = {
            "business": ("Business",
                         "Summarize the company's core business operations, products/services, revenue generation model, and primary markets."),
            "risk_factors": (
            "Risk Factors", "Identify and summarize the 3-5 most significant risk factors disclosed. Be concise."),
            "mda": ("Management's Discussion and Analysis",
                    "Summarize key insights into financial performance drivers, financial condition, liquidity, and management's outlook or focus areas.")
        }

        for section_key, (prompt_section_name, specific_instruction) in section_details.items():
            section_text = sections.get(section_key)
            summary, source_len = self._summarize_text_chunked(
                section_text,
                prompt_section_name,
                specific_instruction,
                f"{company_name_for_prompt} ({self.ticker})"
            )
            summary_results[f"{section_key}_summary"] = summary  # e.g. business_summary
            summary_results["qualitative_sources_summary"][f"{section_key}_10k_source_length"] = source_len
            logger.info(f"Summary for '{prompt_section_name}' (length {source_len}): {summary[:100]}...")

        biz_summary_str = summary_results.get("business_summary", "") or ""
        mda_summary_str = summary_results.get("mda_summary", "") or ""  # Name updated from mda_summary
        risk_summary_str = summary_results.get("risk_factors_summary", "") or ""

        # Competitive Landscape
        comp_input_text = (biz_summary_str + "\n" + mda_summary_str).strip()
        if comp_input_text:
            comp_prompt = (
                f"Based on the business description and MD&A for {company_name_for_prompt} ({self.ticker}):\n\"\"\"\n{comp_input_text}\n\"\"\"\n"
                f"Describe the company's competitive landscape. Identify its key competitors if explicitly mentioned or clearly inferable from the text. "
                f"Discuss its market positioning relative to them, focusing on factual statements from the provided text or very direct inferences. "
                f"If competitors are not clearly identifiable from the text, state that.")
            comp_summary = self.gemini.generate_text(comp_prompt);
            time.sleep(3)
            if comp_summary and not comp_summary.startswith("Error:"): summary_results[
                "competitive_landscape_summary"] = comp_summary

        # Economic Moat
        comp_summary_str = summary_results.get("competitive_landscape_summary", "") or ""
        moat_input_text = (biz_summary_str + "\n" + comp_summary_str + "\n" + risk_summary_str).strip()
        if moat_input_text:
            moat_prompt = (
                f"Analyze the primary economic moats (e.g., brand, network effects, switching costs, intangible assets like patents, cost advantages) for {company_name_for_prompt} ({self.ticker}), "
                f"based on the following information:\n\"\"\"\n{moat_input_text}\n\"\"\"\nProvide a concise summary of its key moats and their perceived strength based on the text.")
            moat_summary = self.gemini.generate_text(moat_prompt);
            time.sleep(3)
            if moat_summary and not moat_summary.startswith("Error:"): summary_results[
                "economic_moat_summary"] = moat_summary

        # Industry Trends
        industry_context_text = (biz_summary_str + "\nRelevant Industry: " + (
                    self.stock_db_entry.industry or "Not Specified") + "\nRelevant Sector: " + (
                                             self.stock_db_entry.sector or "Not Specified")).strip()
        if industry_context_text:
            industry_prompt = (
                f"For {company_name_for_prompt} ({self.ticker}), operating in the '{self.stock_db_entry.industry}' industry, "
                f"consider the following context:\n\"\"\"\n{industry_context_text}\n\"\"\"\n"
                f"Analyze key trends, opportunities, and challenges relevant to this industry based on the provided business summary. How does the company appear to be positioned in relation to these trends?")
            industry_summary = self.gemini.generate_text(industry_prompt);
            time.sleep(3)
            if industry_summary and not industry_summary.startswith("Error:"): summary_results[
                "industry_trends_summary"] = industry_summary

        # Rename mda_summary to management_assessment_summary for consistency with model
        if "mda_summary" in summary_results:
            summary_results["management_assessment_summary"] = summary_results.pop("mda_summary")
            summary_results["qualitative_sources_summary"]["mda_10k_source_length"] = summary_results[
                "qualitative_sources_summary"].pop("mda_10k_source_length", 0)

        logger.info(f"10-K qualitative summaries generated for {self.ticker}.")
        self._financial_data_cache['10k_summaries'] = summary_results
        return summary_results

    def _parse_ai_investment_thesis_response(self, ai_response_text):
        parsed_data = {
            "investment_thesis_full": "AI response not fully processed or 'Investment Thesis:' section missing.",
            "investment_decision": "Review AI Output", "strategy_type": "Not Specified by AI",
            "confidence_level": "Not Specified by AI",
            "reasoning": "AI response not fully processed or 'Key Reasoning Points:' section missing."}
        if not ai_response_text or ai_response_text.startswith("Error:"):
            error_message = ai_response_text if ai_response_text else "Error: Empty response from AI."
            parsed_data.update({k: error_message for k in parsed_data if k.endswith("_full") or k == "reasoning"})
            parsed_data.update({k: "AI Error" for k in parsed_data if k not in ["investment_thesis_full", "reasoning"]})
            return parsed_data
        text_content = ai_response_text.replace('\r\n', '\n').strip()
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
                re.I | re.M | re.S)}
        found_any_section = False
        for key, pattern in section_patterns.items():
            match = pattern.search(text_content)
            if match:
                content = match.group(1).strip()
                if content:
                    parsed_data[key] = content.split('\n')[0].strip() if key in ["investment_decision", "strategy_type",
                                                                                 "confidence_level"] else content
                    found_any_section = True
        if not found_any_section and not ai_response_text.startswith("Error:"):
            logger.warning(
                f"Could not parse distinct sections from AI response for {self.ticker}. Full response in thesis.")
            parsed_data["investment_thesis_full"] = text_content
        if parsed_data["investment_decision"] == "Review AI Output" and "investment decision:" in text_content.lower():
            try:
                lines = text_content.split('\n')
                for line in lines:
                    if "investment decision:" in line.lower():
                        decision_val = line.split(":", 1)[1].strip();
                        if decision_val: parsed_data["investment_decision"] = decision_val; break
            except Exception:
                pass
        return parsed_data

    def _determine_investment_thesis(self):
        logger.info(f"Synthesizing investment thesis for {self.ticker}...")
        metrics, qual_summaries, dcf_results, profile = (self._financial_data_cache.get('calculated_metrics', {}),
                                                         self._financial_data_cache.get('10k_summaries', {}),
                                                         self._financial_data_cache.get('dcf_results', {}),
                                                         self._financial_data_cache.get('profile_fmp', {}))
        company_name, industry, sector = self.stock_db_entry.company_name or self.ticker, self.stock_db_entry.industry or "N/A", self.stock_db_entry.sector or "N/A"
        prompt = f"Company: {company_name} ({self.ticker})\nIndustry: {industry}, Sector: {sector}\n\nKey Financial Metrics & Data:\n"
        metrics_for_prompt = {"P/E Ratio": metrics.get("pe_ratio"), "P/B Ratio": metrics.get("pb_ratio"),
                              "P/S Ratio": metrics.get("ps_ratio"),
                              "Dividend Yield": metrics.get("dividend_yield"), "ROE": metrics.get("roe"),
                              "ROIC": metrics.get("roic"),
                              "Debt-to-Equity": metrics.get("debt_to_equity"),
                              "Revenue Growth YoY": metrics.get("revenue_growth_yoy"),
                              "Revenue Growth QoQ": metrics.get("revenue_growth_qoq"),
                              "EPS Growth YoY": metrics.get("eps_growth_yoy"),
                              "Net Profit Margin": metrics.get("net_profit_margin"),
                              "Free Cash Flow Yield": metrics.get("free_cash_flow_yield"),
                              "FCF Trend": metrics.get("free_cash_flow_trend"),
                              "Retained Earnings Trend": metrics.get("retained_earnings_trend")}
        for name, val in metrics_for_prompt.items():
            if val is not None:
                val_str = f"{val:.2%}" if isinstance(val, float) and (
                            name.endswith("Yield") or "Growth" in name or "Margin" in name or name in ["ROE",
                                                                                                       "ROIC"]) else (
                    f"{val:.2f}" if isinstance(val, float) else str(val))
                prompt += f"- {name}: {val_str}\n"
        current_stock_price, dcf_intrinsic_value, dcf_upside = profile.get("price"), dcf_results.get(
            "dcf_intrinsic_value"), dcf_results.get("dcf_upside_percentage")
        if current_stock_price is not None: prompt += f"- Current Stock Price: {current_stock_price:.2f}\n"
        if dcf_intrinsic_value is not None: prompt += f"- DCF Intrinsic Value/Share: {dcf_intrinsic_value:.2f}\n"
        if dcf_upside is not None: prompt += f"- DCF Upside/Downside: {dcf_upside:.2%}\n"
        prompt += "\nQualitative Summaries (from 10-K & AI analysis):\n"
        qual_for_prompt = {"Business Model": qual_summaries.get("business_summary"),
                           "Economic Moat": qual_summaries.get("economic_moat_summary"),
                           "Industry Trends": qual_summaries.get("industry_trends_summary"),
                           "Competitive Landscape": qual_summaries.get("competitive_landscape_summary"),
                           "MD&A Highlights": qual_summaries.get("management_assessment_summary"),
                           "Key Risk Factors": qual_summaries.get("risk_factors_summary")}
        for name, text_val in qual_for_prompt.items():
            if text_val and isinstance(text_val,
                                       str): prompt += f"- {name}: {text_val[:300].replace('...', '').strip()}...\n"  # Truncate for prompt context
        prompt += ("\nInstructions for AI: Based on all the above information, provide a detailed financial analysis. "
                   "Structure your response *exactly* as follows, using these specific headings on separate lines:\n\n"
                   "Investment Thesis:\n[Comprehensive thesis (2-4 paragraphs) synthesizing data, positives/negatives, and outlook.]\n\n"
                   "Investment Decision:\n[Strong Buy, Buy, Hold, Monitor, Reduce, Sell, Avoid.]\n\n"
                   "Strategy Type:\n[Value, GARP, Growth, Income, Speculative, Special Situation, Turnaround.]\n\n"
                   "Confidence Level:\n[High, Medium, Low - in analysis & decision.]\n\n"
                   "Key Reasoning Points:\n[3-7 bullet points for decision: Valuation, Financial Health, Growth, Moat, Risks, Management (if known).]\n")
        ai_response_text = self.gemini.generate_text(prompt)  # Relies on GeminiAPIClient's hard truncation
        parsed_thesis_data = self._parse_ai_investment_thesis_response(ai_response_text)
        logger.info(
            f"Generated thesis for {self.ticker}. Parsed Decision: {parsed_thesis_data.get('investment_decision')}, Strategy: {parsed_thesis_data.get('strategy_type')}, Confidence: {parsed_thesis_data.get('confidence_level')}")
        return parsed_thesis_data

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_data_for_db = {}
        try:
            if not self.stock_db_entry: logger.error(
                f"Stock DB entry for {self.ticker} not initialized. Aborting."); return None
            self._ensure_stock_db_entry_is_bound()
            self._fetch_financial_statements();
            self._fetch_key_metrics_and_profile_data()
            final_data_for_db.update(self._calculate_derived_metrics())
            final_data_for_db.update(self._perform_dcf_analysis())
            final_data_for_db.update(self._fetch_and_summarize_10k())
            final_data_for_db.update(self._determine_investment_thesis())
            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id, analysis_date=datetime.now(timezone.utc))
            model_fields = [c.key for c in StockAnalysis.__table__.columns if
                            c.key not in ['id', 'stock_id', 'analysis_date']]
            for field_name in model_fields:
                if field_name in final_data_for_db:
                    value_to_set = final_data_for_db[field_name]
                    target_column_type = getattr(StockAnalysis, field_name).type.python_type
                    if target_column_type == float:
                        if isinstance(value_to_set, str):
                            try:
                                value_to_set = float(value_to_set)
                            except ValueError:
                                value_to_set = None
                        if isinstance(value_to_set, float) and (
                                math.isnan(value_to_set) or math.isinf(value_to_set)): value_to_set = None
                    elif target_column_type == dict and not isinstance(value_to_set, dict):
                        value_to_set = None
                    elif target_column_type == str and not isinstance(value_to_set, str):
                        value_to_set = str(value_to_set) if value_to_set is not None else None
                    setattr(analysis_entry, field_name, value_to_set)
            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = analysis_entry.analysis_date
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved stock data: {self.ticker} (Analysis ID: {analysis_entry.id})")
            return analysis_entry
        except RuntimeError as rt_err:
            logger.critical(f"Runtime error during full analysis for {self.ticker}: {rt_err}",
                            exc_info=True); return None
        except Exception as e:
            logger.error(f"CRITICAL error in full analysis pipeline for {self.ticker}: {e}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try:
                    self.db_session.rollback(); logger.info(
                        f"Rolled back DB transaction for {self.ticker} due to error.")
                except Exception as e_rb:
                    logger.error(f"Rollback error for {self.ticker}: {e_rb}")
            return None
        finally:
            self._close_session_if_active()

    def _ensure_stock_db_entry_is_bound(self):
        if not self.stock_db_entry: raise RuntimeError(f"Stock entry for {self.ticker} is None during binding check.")
        if not self.db_session.is_active:
            logger.warning(f"DB Session for {self.ticker} was INACTIVE. Re-establishing.")
            self._close_session_if_active();
            self.db_session = next(get_db_session())
            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock: raise RuntimeError(f"Failed to re-fetch stock {self.ticker} for new session.")
            self.stock_db_entry = re_fetched_stock;
            logger.info(f"Re-fetched and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new session.")
            return
        instance_state = sa_inspect(self.stock_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            obj_id_log, session_id_actual = (
                self.stock_db_entry.id if instance_state.has_identity else 'Transient/No ID'), (
                id(instance_state.session) if instance_state.session else 'None')
            logger.warning(
                f"Stock {self.ticker} (ID: {obj_id_log}) DETACHED or bound to DIFFERENT session (Expected: {id(self.db_session)}, Actual: {session_id_actual}). Merging.")
            try:
                self.stock_db_entry = self.db_session.merge(self.stock_db_entry); logger.info(
                    f"Successfully merged stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session.")
            except Exception as e_merge:
                logger.error(f"Failed to merge stock {self.ticker} into session: {e_merge}. Re-fetching.",
                             exc_info=True)
                re_fetched_from_db = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
                if re_fetched_from_db:
                    self.stock_db_entry = re_fetched_from_db; logger.info(
                        f"Successfully re-fetched stock {self.ticker} (ID: {self.stock_db_entry.id}) after merge failure.")
                else:
                    raise RuntimeError(f"Failed to bind stock {self.ticker} to session. Analysis cannot proceed.")


if __name__ == '__main__':
    from database import init_db

    # init_db()
    logger.info("Starting standalone stock analysis test...")
    tickers_to_test = ["AAPL", "MSFT", "NKE"]
    for ticker_symbol in tickers_to_test:
        analysis_result_obj = None
        try:
            logger.info(f"--- Analyzing {ticker_symbol} ---")
            analyzer_instance = StockAnalyzer(ticker=ticker_symbol)
            analysis_result_obj = analyzer_instance.analyze()
            if analysis_result_obj and hasattr(analysis_result_obj, 'stock'):
                logger.info(
                    f"Analysis for {analysis_result_obj.stock.ticker} completed. Decision: {analysis_result_obj.investment_decision}, Strategy: {analysis_result_obj.strategy_type}, Confidence: {analysis_result_obj.confidence_level}")
                if analysis_result_obj.dcf_intrinsic_value is not None: logger.info(
                    f"DCF Value: {analysis_result_obj.dcf_intrinsic_value:.2f}, Upside: {analysis_result_obj.dcf_upside_percentage:.2% if analysis_result_obj.dcf_upside_percentage is not None else 'N/A'}")
                logger.info(
                    f"QoQ Revenue Growth: {analysis_result_obj.revenue_growth_qoq if analysis_result_obj.revenue_growth_qoq is not None else 'N/A'} (Source: {analysis_result_obj.key_metrics_snapshot.get('q_revenue_source', 'N/A') if analysis_result_obj.key_metrics_snapshot else 'N/A'})")
                logger.info(
                    f"  P/E: {analysis_result_obj.pe_ratio}, P/B: {analysis_result_obj.pb_ratio}, ROE: {analysis_result_obj.roe}")
            else:
                logger.error(f"Stock analysis pipeline FAILED or returned invalid result for {ticker_symbol}.")
        except RuntimeError as rt_err:
            logger.error(f"Could not run StockAnalyzer for {ticker_symbol} due to initialization error: {rt_err}")
        except Exception as e_main_loop:
            logger.error(f"Unhandled error analyzing {ticker_symbol} in __main__ loop: {e_main_loop}", exc_info=True)
        finally:
            logger.info(f"--- Finished processing {ticker_symbol} ---"); time.sleep(20)
