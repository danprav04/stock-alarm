# stock_analyzer.py
import pandas as pd
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone, timedelta
import math  # For DCF calculations
import time  # For API courtesy delays
import warnings  # For filtering warnings
from bs4 import XMLParsedAsHTMLWarning  # For filtering specific BS4 warning

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
    if val is None or val == "None": return default
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
        if profile_fmp_list and isinstance(profile_fmp_list, list) and profile_fmp_list[0]:
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
                    company_name = overview_av.get('Name')
                    industry = overview_av.get('Industry')
                    sector = overview_av.get('Sector')
                    cik = overview_av.get('CIK')
                    logger.info(f"Fetched overview from Alpha Vantage for {self.ticker}.")
                else:
                    logger.warning(f"All profile fetches failed for {self.ticker}.")

        if not company_name: company_name = self.ticker
        if not cik and self.ticker:
            logger.info(f"CIK not found from profiles for {self.ticker}. Querying SEC EDGAR.")
            cik = self.sec_edgar.get_cik_by_ticker(self.ticker);
            time.sleep(0.5)
            if cik:
                logger.info(f"Fetched CIK {cik} from SEC EDGAR for {self.ticker}.")
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
                self.db_session.rollback(); logger.error(f"Error creating stock {self.ticker}: {e}"); raise
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
                    self.db_session.commit(); self.db_session.refresh(self.stock_db_entry)
                except SQLAlchemyError as e:
                    self.db_session.rollback(); logger.error(f"Error updating stock {self.ticker}: {e}")
        logger.info(
            f"Stock entry for {self.ticker} (ID: {self.stock_db_entry.id if self.stock_db_entry else 'N/A'}, CIK: {self.stock_db_entry.cik if self.stock_db_entry else 'N/A'}) ready.")

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
                                                                                      STOCK_FINANCIAL_YEARS) or []
            time.sleep(1.5)
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
                f"FMP Annuals: Income({len(statements_cache['fmp_income_annual'])}), Balance({len(statements_cache['fmp_balance_annual'])}), Cashflow({len(statements_cache['fmp_cashflow_annual'])}).")

            fh_q_data = self.finnhub.get_financials_reported(self.ticker, freq="quarterly")
            time.sleep(1.5)
            if fh_q_data and fh_q_data.get("data"):
                statements_cache["finnhub_financials_quarterly_reported"] = fh_q_data
                logger.info(f"Fetched {len(fh_q_data['data'])} quarterly reports from Finnhub.")

            av_income_q = self.alphavantage.get_income_statement_quarterly(self.ticker)
            time.sleep(15)
            if av_income_q and av_income_q.get("quarterlyReports"):
                statements_cache["alphavantage_income_quarterly"] = av_income_q
                logger.info(
                    f"Fetched {len(av_income_q['quarterlyReports'])} quarterly income reports from Alpha Vantage.")

            av_balance_q = self.alphavantage.get_balance_sheet_quarterly(self.ticker)
            time.sleep(15)
            if av_balance_q and av_balance_q.get("quarterlyReports"):
                statements_cache["alphavantage_balance_quarterly"] = av_balance_q
                logger.info(
                    f"Fetched {len(av_balance_q['quarterlyReports'])} quarterly balance reports from Alpha Vantage.")

            av_cashflow_q = self.alphavantage.get_cash_flow_quarterly(self.ticker)
            time.sleep(15)
            if av_cashflow_q and av_cashflow_q.get("quarterlyReports"):
                statements_cache["alphavantage_cashflow_quarterly"] = av_cashflow_q
                logger.info(
                    f"Fetched {len(av_cashflow_q['quarterlyReports'])} quarterly cash flow reports from Alpha Vantage.")
        except Exception as e:
            logger.warning(f"Error during financial statements fetch for {self.ticker}: {e}.", exc_info=True)
        self._financial_data_cache['financial_statements'] = statements_cache
        return statements_cache

    def _fetch_key_metrics_and_profile_data(self):
        logger.info(f"Fetching key metrics and profile for {self.ticker}.")
        key_metrics_annual_fmp = self.fmp.get_key_metrics(self.ticker, "annual", STOCK_FINANCIAL_YEARS + 2)
        time.sleep(1.5);
        self._financial_data_cache['key_metrics_annual_fmp'] = key_metrics_annual_fmp or []
        key_metrics_quarterly_fmp = self.fmp.get_key_metrics(self.ticker, "quarterly", 8)
        time.sleep(1.5)
        if key_metrics_quarterly_fmp is None: logger.warning(f"FMP quarterly key metrics failed for {self.ticker}.")
        self._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp or []
        basic_fin_fh = self.finnhub.get_basic_financials(self.ticker)
        time.sleep(1.5);
        self._financial_data_cache['basic_financials_finnhub'] = basic_fin_fh or {}
        if 'profile_fmp' not in self._financial_data_cache or not self._financial_data_cache.get('profile_fmp'):
            profile_fmp_list = self.fmp.get_company_profile(self.ticker);
            time.sleep(1.5)
            self._financial_data_cache['profile_fmp'] = profile_fmp_list[0] if profile_fmp_list and profile_fmp_list[
                0] else {}
        logger.info(
            f"FMP KM Annual: {len(self._financial_data_cache['key_metrics_annual_fmp'])}, FMP KM Quarterly: {len(self._financial_data_cache['key_metrics_quarterly_fmp'])}. Finnhub Basic Financials: {'OK' if self._financial_data_cache['basic_financials_finnhub'] else 'Fail'}.")

    def _calculate_derived_metrics(self):
        logger.info(f"Calculating derived metrics for {self.ticker}...")
        metrics = {"key_metrics_snapshot": {}}
        statements = self._financial_data_cache.get('financial_statements', {})
        income_annual = sorted(statements.get('fmp_income_annual', []), key=lambda x: x.get("date"), reverse=True)
        balance_annual = sorted(statements.get('fmp_balance_annual', []), key=lambda x: x.get("date"), reverse=True)
        cashflow_annual = sorted(statements.get('fmp_cashflow_annual', []), key=lambda x: x.get("date"), reverse=True)
        av_income_q_reports = statements.get('alphavantage_income_quarterly', {}).get('quarterlyReports', [])
        fh_q_reports_list = statements.get('finnhub_financials_quarterly_reported', {}).get('data', [])
        key_metrics_annual = self._financial_data_cache.get('key_metrics_annual_fmp', [])
        key_metrics_quarterly = self._financial_data_cache.get('key_metrics_quarterly_fmp', [])
        basic_fin_fh = self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {})
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        latest_km_q = key_metrics_quarterly[0] if key_metrics_quarterly else {}
        latest_km_a = key_metrics_annual[0] if key_metrics_annual else {}

        metrics["pe_ratio"] = safe_get_float(latest_km_q, "peRatioTTM") or safe_get_float(latest_km_a,
                                                                                          "peRatio") or safe_get_float(
            basic_fin_fh, "peTTM")
        metrics["pb_ratio"] = safe_get_float(latest_km_q, "priceToBookRatioTTM") or safe_get_float(latest_km_a,
                                                                                                   "pbRatio") or safe_get_float(
            basic_fin_fh, "pbAnnual")
        metrics["ps_ratio"] = safe_get_float(latest_km_q, "priceToSalesRatioTTM") or safe_get_float(latest_km_a,
                                                                                                    "priceSalesRatio") or safe_get_float(
            basic_fin_fh, "psTTM")
        metrics["ev_to_sales"] = safe_get_float(latest_km_q, "enterpriseValueOverRevenueTTM") or safe_get_float(
            latest_km_a, "enterpriseValueOverRevenue")
        metrics["ev_to_ebitda"] = safe_get_float(latest_km_q, "evToEbitdaTTM") or safe_get_float(latest_km_a,
                                                                                                 "evToEbitda")
        div_yield_fmp_q, div_yield_fmp_a, div_yield_fh = safe_get_float(latest_km_q,
                                                                        "dividendYieldTTM"), safe_get_float(latest_km_a,
                                                                                                            "dividendYield"), safe_get_float(
            basic_fin_fh, "dividendYieldAnnual")
        if div_yield_fh is not None: div_yield_fh /= 100.0
        metrics["dividend_yield"] = div_yield_fmp_q if div_yield_fmp_q is not None else (
            div_yield_fmp_a if div_yield_fmp_a is not None else div_yield_fh)

        if income_annual:
            latest_ia = income_annual[0]
            metrics["eps"] = safe_get_float(latest_ia, "eps") or safe_get_float(latest_km_a, "eps")
            metrics["net_profit_margin"] = safe_get_float(latest_ia, "netProfitMargin")
            metrics["gross_profit_margin"] = safe_get_float(latest_ia, "grossProfitMargin")
            metrics["operating_profit_margin"] = safe_get_float(latest_ia, "operatingIncomeRatio")
            ebit, int_exp = safe_get_float(latest_ia, "operatingIncome"), safe_get_float(latest_ia, "interestExpense")
            if ebit is not None and int_exp is not None and abs(int_exp) > 1e-6: metrics[
                "interest_coverage_ratio"] = ebit / abs(int_exp)
        if balance_annual:
            latest_ba = balance_annual[0]
            tot_eq, tot_assets = safe_get_float(latest_ba, "totalStockholdersEquity"), safe_get_float(latest_ba,
                                                                                                      "totalAssets")
            latest_ni = get_value_from_statement_list(income_annual, "netIncome", 0)
            if tot_eq and tot_eq != 0 and latest_ni is not None: metrics["roe"] = latest_ni / tot_eq
            if tot_assets and tot_assets != 0 and latest_ni is not None: metrics["roa"] = latest_ni / tot_assets
            metrics["debt_to_equity"] = safe_get_float(latest_km_a, "debtToEquity")
            if metrics["debt_to_equity"] is None:
                tot_debt = safe_get_float(latest_ba, "totalDebt")
                if tot_debt is not None and tot_eq and tot_eq != 0: metrics["debt_to_equity"] = tot_debt / tot_eq
            cur_assets, cur_liab = safe_get_float(latest_ba, "totalCurrentAssets"), safe_get_float(latest_ba,
                                                                                                   "totalCurrentLiabilities")
            if cur_assets is not None and cur_liab is not None and cur_liab != 0: metrics[
                "current_ratio"] = cur_assets / cur_liab
            cash_eq, st_inv, acc_rec = safe_get_float(latest_ba, "cashAndCashEquivalents", 0), safe_get_float(latest_ba,
                                                                                                              "shortTermInvestments",
                                                                                                              0), safe_get_float(
                latest_ba, "netReceivables", 0)
            if cur_liab is not None and cur_liab != 0: metrics["quick_ratio"] = (cash_eq + st_inv + acc_rec) / cur_liab
        ebitda_debt_ratio = safe_get_float(latest_km_a, "ebitda") or get_value_from_statement_list(income_annual,
                                                                                                   "ebitda", 0)
        if ebitda_debt_ratio and ebitda_debt_ratio != 0 and balance_annual:
            tot_debt_val = get_value_from_statement_list(balance_annual, "totalDebt", 0)
            if tot_debt_val is not None: metrics["debt_to_ebitda"] = tot_debt_val / ebitda_debt_ratio

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

        latest_q_rev_av = get_alphavantage_value(av_income_q_reports, "totalRevenue", 0)
        prev_q_rev_av = get_alphavantage_value(av_income_q_reports, "totalRevenue", 1)
        if latest_q_rev_av is not None and prev_q_rev_av is not None:
            metrics["revenue_growth_qoq"] = calculate_growth(latest_q_rev_av, prev_q_rev_av)
            metrics["key_metrics_snapshot"]["q_revenue_source"], metrics["key_metrics_snapshot"][
                "latest_q_revenue"] = "AlphaVantage", latest_q_rev_av
        else:
            revenue_concepts_fh = ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "TotalRevenues",
                                   "NetSales"]
            latest_q_rev_fh = get_finnhub_concept_value(fh_q_reports_list, 'ic', revenue_concepts_fh, 0)
            prev_q_rev_fh = get_finnhub_concept_value(fh_q_reports_list, 'ic', revenue_concepts_fh, 1)
            if latest_q_rev_fh is not None and prev_q_rev_fh is not None:
                metrics["revenue_growth_qoq"] = calculate_growth(latest_q_rev_fh, prev_q_rev_fh)
                metrics["key_metrics_snapshot"]["q_revenue_source"], metrics["key_metrics_snapshot"][
                    "latest_q_revenue"] = "Finnhub", latest_q_rev_fh
            else:
                logger.info(f"Could not calculate QoQ revenue for {self.ticker} from AV or Finnhub."); metrics[
                    "revenue_growth_qoq"] = None

        if cashflow_annual:
            fcf = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
            shares_out = safe_get_float(profile_fmp, "sharesOutstanding") or (
                safe_get_float(profile_fmp, "mktCap") / safe_get_float(profile_fmp, "price") if safe_get_float(
                    profile_fmp, "price") else None)
            if fcf is not None and shares_out and shares_out != 0:
                metrics["free_cash_flow_per_share"] = fcf / shares_out
                mkt_cap_yield = safe_get_float(profile_fmp, "mktCap")
                if mkt_cap_yield and mkt_cap_yield != 0: metrics["free_cash_flow_yield"] = fcf / mkt_cap_yield
            if len(cashflow_annual) >= 3:
                fcf0, fcf1, fcf2 = get_value_from_statement_list(cashflow_annual, "freeCashFlow",
                                                                 0), get_value_from_statement_list(cashflow_annual,
                                                                                                   "freeCashFlow",
                                                                                                   1), get_value_from_statement_list(
                    cashflow_annual, "freeCashFlow", 2)
                if all(isinstance(x, (int, float)) for x in [fcf0, fcf1, fcf2] if x is not None):
                    if fcf0 > fcf1 > fcf2:
                        metrics["free_cash_flow_trend"] = "Growing"
                    elif fcf0 < fcf1 < fcf2:
                        metrics["free_cash_flow_trend"] = "Declining"
                    else:
                        metrics["free_cash_flow_trend"] = "Mixed/Stable"
            else:
                metrics["free_cash_flow_trend"] = "Data N/A"
        if len(balance_annual) >= 3:
            re0, re1, re2 = get_value_from_statement_list(balance_annual, "retainedEarnings",
                                                          0), get_value_from_statement_list(balance_annual,
                                                                                            "retainedEarnings",
                                                                                            1), get_value_from_statement_list(
                balance_annual, "retainedEarnings", 2)
            if all(isinstance(x, (int, float)) for x in [re0, re1, re2] if x is not None):
                if re0 > re1 > re2:
                    metrics["retained_earnings_trend"] = "Growing"
                elif re0 < re1 < re2:
                    metrics["retained_earnings_trend"] = "Declining"
                else:
                    metrics["retained_earnings_trend"] = "Mixed/Stable"
        else:
            metrics["retained_earnings_trend"] = "Data N/A"

        if income_annual and balance_annual:
            ebit_r, tax_p, inc_bt = get_value_from_statement_list(income_annual, "operatingIncome",
                                                                  0), get_value_from_statement_list(income_annual,
                                                                                                    "incomeTaxExpense",
                                                                                                    0), get_value_from_statement_list(
                income_annual, "incomeBeforeTax", 0)
            eff_tax = (tax_p / inc_bt) if inc_bt and tax_p and inc_bt != 0 else 0.21
            nopat = ebit_r * (1 - eff_tax) if ebit_r is not None else None
            tot_debt_r, tot_eq_r, cash_eq_r = get_value_from_statement_list(balance_annual, "totalDebt",
                                                                            0), get_value_from_statement_list(
                balance_annual, "totalStockholdersEquity", 0), get_value_from_statement_list(balance_annual,
                                                                                             "cashAndCashEquivalents",
                                                                                             0) or 0
            if tot_debt_r is not None and tot_eq_r is not None:
                inv_cap = tot_debt_r + tot_eq_r - cash_eq_r
                if nopat is not None and inv_cap is not None and inv_cap != 0: metrics["roic"] = nopat / inv_cap

        final_metrics = {}
        for k, v in metrics.items():
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
            key=lambda x: x.get("date"), reverse=True)
        profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        calculated_metrics = self._financial_data_cache.get('calculated_metrics', {})
        current_price = safe_get_float(profile_fmp, "price")
        shares_out = safe_get_float(profile_fmp, "sharesOutstanding") or (
            safe_get_float(profile_fmp, "mktCap") / current_price if current_price and current_price != 0 else None)

        if not cashflow_annual or not profile_fmp or current_price is None or shares_out is None or shares_out == 0:
            logger.warning(f"Insufficient data for DCF for {self.ticker}.");
            return dcf_results
        current_fcf = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 0)
        if current_fcf is None or current_fcf <= 10000: logger.warning(
            f"Current FCF for {self.ticker} is {current_fcf}. DCF needs positive FCF."); return dcf_results
        dcf_results["dcf_assumptions"]["start_fcf"] = current_fcf

        fcf_g_3yr = None
        if len(cashflow_annual) >= 4:
            fcf_s_3yr = get_value_from_statement_list(cashflow_annual, "freeCashFlow", 3)
            if fcf_s_3yr and fcf_s_3yr > 0: fcf_g_3yr = calculate_cagr(current_fcf, fcf_s_3yr, 3)

        fcf_g_init = fcf_g_3yr if fcf_g_3yr is not None else calculated_metrics.get(
            "revenue_growth_cagr_3yr") if calculated_metrics.get(
            "revenue_growth_cagr_3yr") is not None else calculated_metrics.get(
            "revenue_growth_yoy") if calculated_metrics.get("revenue_growth_yoy") is not None else 0.05
        if not isinstance(fcf_g_init, (int, float)): fcf_g_init = 0.05
        fcf_g_init = min(max(fcf_g_init, -0.10), 0.20)

        proj_fcfs, last_proj_fcf = [], current_fcf
        g_decline = (fcf_g_init - DEFAULT_PERPETUAL_GROWTH_RATE) / float(
            DEFAULT_FCF_PROJECTION_YEARS) if DEFAULT_FCF_PROJECTION_YEARS > 0 else 0
        for i in range(DEFAULT_FCF_PROJECTION_YEARS):
            curr_g = max(fcf_g_init - (g_decline * i), DEFAULT_PERPETUAL_GROWTH_RATE)
            proj_fcf = last_proj_fcf * (1 + curr_g);
            proj_fcfs.append(proj_fcf);
            last_proj_fcf = proj_fcf
            dcf_results["dcf_assumptions"]["fcf_growth_rates_projection"].append(round(curr_g, 4))
        if not proj_fcfs: logger.error(f"DCF: No projected FCFs for {self.ticker}."); return dcf_results

        term_fcf = proj_fcfs[-1] * (1 + DEFAULT_PERPETUAL_GROWTH_RATE)
        denom = DEFAULT_DISCOUNT_RATE - DEFAULT_PERPETUAL_GROWTH_RATE
        term_val = term_fcf / denom if denom > 1e-6 else 0
        if denom <= 1e-6: logger.warning(
            f"DCF {self.ticker}: Discount rate near/below perpetual growth. TV unreliable.")

        sum_disc_fcf = sum(fcf / ((1 + DEFAULT_DISCOUNT_RATE) ** (i + 1)) for i, fcf in enumerate(proj_fcfs))
        disc_term_val = term_val / ((1 + DEFAULT_DISCOUNT_RATE) ** DEFAULT_FCF_PROJECTION_YEARS)
        int_eq_val = sum_disc_fcf + disc_term_val

        if shares_out != 0:
            int_val_ps = int_eq_val / shares_out;
            dcf_results["dcf_intrinsic_value"] = int_val_ps
            if current_price and current_price != 0: dcf_results["dcf_upside_percentage"] = (
                                                                                                        int_val_ps - current_price) / current_price

        logger.info(
            f"DCF for {self.ticker}: Intrinsic Value/Share: {dcf_results.get('dcf_intrinsic_value', 'N/A')}, Upside: {dcf_results.get('dcf_upside_percentage', 'N/A')}")
        self._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    def _fetch_and_summarize_10k(self):
        logger.info(f"Fetching and attempting to summarize latest 10-K for {self.ticker}")
        summary_results = {"qualitative_sources_summary": {}}
        if not self.stock_db_entry or not self.stock_db_entry.cik: logger.warning(
            f"No CIK for {self.ticker}. Cannot fetch 10-K."); return summary_results

        filing_url = self.sec_edgar.get_filing_document_url(self.stock_db_entry.cik, "10-K");
        time.sleep(0.5)
        if not filing_url: filing_url = self.sec_edgar.get_filing_document_url(self.stock_db_entry.cik,
                                                                               "10-K/A"); time.sleep(0.5)
        if not filing_url: logger.warning(
            f"No 10-K URL for {self.ticker} (CIK: {self.stock_db_entry.cik})"); return summary_results

        text_content = self.sec_edgar.get_filing_text(filing_url)
        if not text_content: logger.warning(f"Failed to fetch 10-K text from {filing_url}"); return summary_results
        logger.info(f"Fetched 10-K text (length: {len(text_content)}) for {self.ticker}. Extracting sections.")
        sections = extract_S1_text_sections(text_content, TEN_K_KEY_SECTIONS)
        company_name = self.stock_db_entry.company_name or self.ticker
        summary_results["qualitative_sources_summary"]["10k_filing_url_used"] = filing_url

        def summarize_section(gemini_client, section_text, section_name_for_prompt, company_name_ticker, max_len):
            if not section_text: return None, 0
            prompt_ctx = f"This is '{section_name_for_prompt}' from 10-K for {company_name_ticker}. Summarize key aspects."
            if section_name_for_prompt == "Business":
                prompt_ctx += " Focus on operations, products, revenue streams, markets."
            elif section_name_for_prompt == "Risk Factors":
                prompt_ctx += " Focus on 3-5 most material risks."
            elif section_name_for_prompt == "MD&A":
                prompt_ctx += " Focus on performance drivers, financial condition, liquidity, outlook."
            summary = gemini_client.summarize_text_with_context(section_text, prompt_ctx, max_len)
            return summary if not summary.startswith("Error:") else None, len(section_text)

        summary_results["business_summary"], summary_results["qualitative_sources_summary"][
            "business_10k_source_length"] = summarize_section(self.gemini, sections.get("business"), "Business",
                                                              f"{company_name} ({self.ticker})",
                                                              MAX_10K_SECTION_LENGTH_FOR_GEMINI);
        time.sleep(3)
        summary_results["risk_factors_summary"], summary_results["qualitative_sources_summary"][
            "risk_factors_10k_source_length"] = summarize_section(self.gemini, sections.get("risk_factors"),
                                                                  "Risk Factors", f"{company_name} ({self.ticker})",
                                                                  MAX_10K_SECTION_LENGTH_FOR_GEMINI);
        time.sleep(3)
        summary_results["management_assessment_summary"], summary_results["qualitative_sources_summary"][
            "mda_10k_source_length"] = summarize_section(self.gemini, sections.get("mda"), "MD&A",
                                                         f"{company_name} ({self.ticker})",
                                                         MAX_10K_SECTION_LENGTH_FOR_GEMINI);
        time.sleep(3)

        # Ensure values are strings before concatenation
        biz_summary_str = summary_results.get("business_summary") or ""
        mda_summary_str = summary_results.get("management_assessment_summary") or ""
        risk_summary_str = summary_results.get("risk_factors_summary") or ""

        comp_input = (biz_summary_str + "\n" + mda_summary_str)[:MAX_GEMINI_TEXT_LENGTH].strip()
        if comp_input:
            comp_prompt = (
                f"Based on business & MD&A for {company_name} ({self.ticker}): \"{comp_input}\"\nDescribe competitive landscape, key competitors, positioning.")
            comp_sum = self.gemini.generate_text(comp_prompt);
            time.sleep(3)
            if not comp_sum.startswith("Error:"): summary_results["competitive_landscape_summary"] = comp_sum

        comp_summary_str = summary_results.get("competitive_landscape_summary") or ""
        moat_input = (biz_summary_str + "\n" + comp_summary_str + "\n" + risk_summary_str)[
                     :MAX_GEMINI_TEXT_LENGTH].strip()
        if moat_input and not summary_results.get(
                "economic_moat_summary"):  # Only if not directly available (e.g. from profile)
            moat_prompt = (
                f"Based on info for {company_name} ({self.ticker}): \"{moat_input}\"\nAnalyze primary economic moats. Concise summary.")
            moat_sum = self.gemini.generate_text(moat_prompt);
            time.sleep(3)
            if not moat_sum.startswith("Error:"): summary_results["economic_moat_summary"] = moat_sum

        industry_input = (biz_summary_str + "\n" + (self.stock_db_entry.industry or "") + "\n" + (
                    self.stock_db_entry.sector or ""))[:MAX_GEMINI_TEXT_LENGTH].strip()
        if industry_input and not summary_results.get("industry_trends_summary"):
            ind_prompt = (
                f"Company: {company_name} in '{self.stock_db_entry.industry}' industry. Context: \"{biz_summary_str}\"\nAnalyze key trends, opportunities, challenges for this industry. How is company positioned?")
            ind_sum = self.gemini.generate_text(ind_prompt);
            time.sleep(3)
            if not ind_sum.startswith("Error:"): summary_results["industry_trends_summary"] = ind_sum

        logger.info(f"10-K qualitative summaries generated for {self.ticker}.")
        self._financial_data_cache['10k_summaries'] = summary_results
        return summary_results

    def _determine_investment_thesis(self):
        logger.info(f"Synthesizing investment thesis for {self.ticker}...")
        metrics = self._financial_data_cache.get('calculated_metrics', {})
        qual_summaries = self._financial_data_cache.get('10k_summaries', {})
        dcf_results = self._financial_data_cache.get('dcf_results', {})
        profile = self._financial_data_cache.get('profile_fmp', {})
        company_name, industry, sector = self.stock_db_entry.company_name or self.ticker, self.stock_db_entry.industry or "N/A", self.stock_db_entry.sector or "N/A"

        prompt = f"Company: {company_name} ({self.ticker})\nIndustry: {industry}, Sector: {sector}\n\nKey Financials:\n"
        m_prompt = {"P/E": metrics.get("pe_ratio"), "P/B": metrics.get("pb_ratio"), "P/S": metrics.get("ps_ratio"),
                    "Div Yield": metrics.get("dividend_yield"),
                    "ROE": metrics.get("roe"), "ROIC": metrics.get("roic"), "D/E": metrics.get("debt_to_equity"),
                    "Rev YoY": metrics.get("revenue_growth_yoy"),
                    "Rev QoQ": metrics.get("revenue_growth_qoq"), "EPS YoY": metrics.get("eps_growth_yoy"),
                    "Net Margin": metrics.get("net_profit_margin"),
                    "FCF Yield": metrics.get("free_cash_flow_yield"), "FCF Trend": metrics.get("free_cash_flow_trend")}
        for name, val in m_prompt.items():
            if val is not None:
                if isinstance(val, float) and (
                        name.endswith("Yield") or "Growth" in name or "Margin" in name or name in ["ROE", "ROIC"]):
                    val_str = f"{val:.2%}"
                elif isinstance(val, float):
                    val_str = f"{val:.2f}"
                else:
                    val_str = str(val)
                prompt += f"- {name}: {val_str}\n"

        dcf_val, dcf_up, cur_price = dcf_results.get("dcf_intrinsic_value"), dcf_results.get(
            "dcf_upside_percentage"), profile.get("price")
        if dcf_val is not None: prompt += f"\nDCF Value: {dcf_val:.2f}\n"
        if dcf_up is not None: prompt += f"DCF Upside: {dcf_up:.2%}\n"
        if cur_price is not None: prompt += f"Current Price: {cur_price:.2f}\n\n"

        prompt += "Qualitative Summary:\n"
        for key, text_val in qual_summaries.items():
            if key != "qualitative_sources_summary" and text_val and isinstance(text_val,
                                                                                str): prompt += f"- {key.replace('_', ' ').title()}: {text_val[:250]}...\n"
        prompt += "\nInstructions for AI:\n"
        prompt += ("1. Comprehensive investment thesis (2-4 paragraphs).\n"
                   "2. Investment Decision: 'Strong Buy', 'Buy', 'Hold', 'Monitor', 'Reduce', 'Sell', 'Avoid'.\n"
                   "3. Strategy Type: 'Value', 'GARP', 'Growth', etc.\n4. Confidence: 'High', 'Medium', 'Low'.\n"
                   "5. Key Reasoning (bullets): Valuation, Financials, Growth, Moat, Risks, Management.\n"
                   "Structure with headings: 'Investment Thesis:', etc.")

        final_prompt = prompt[:MAX_GEMINI_TEXT_LENGTH]
        ai_response = self.gemini.generate_text(final_prompt);
        time.sleep(3)
        if ai_response.startswith("Error:"):
            logger.error(f"Gemini thesis error for {self.ticker}: {ai_response}")
            return {"investment_decision": "AI Error", "reasoning": ai_response, "investment_thesis_full": ai_response}

        parsed = {};
        sect_map = {"investment thesis:": "investment_thesis_full", "investment decision:": "investment_decision",
                    "strategy type:": "strategy_type", "confidence level:": "confidence_level",
                    "key reasoning:": "reasoning"}
        coll_lines = {k: [] for k in sect_map.values()};
        curr_key = None
        for line_orig in ai_response.split('\n'):
            line_low = line_orig.strip().lower();
            new_sect = False
            for head_low, key_name in sect_map.items():
                if line_low.startswith(head_low):
                    curr_key = key_name;
                    content_h = line_orig.strip()[len(head_low):].strip()
                    if content_h: coll_lines[curr_key].append(content_h)
                    new_sect = True;
                    break
            if not new_sect and curr_key: coll_lines[curr_key].append(line_orig)
        for k, lines in coll_lines.items(): parsed[k] = "\n".join(lines).strip() or "Not found."
        if parsed.get("investment_decision", "").startswith("Not found"): parsed[
            "investment_decision"] = "Review AI Output"
        if parsed.get("reasoning", "").startswith("Not found"): parsed["reasoning"] = ai_response
        if parsed.get("investment_thesis_full", "").startswith("Not found"): parsed[
            "investment_thesis_full"] = ai_response
        logger.info(f"Generated thesis for {self.ticker}. Decision: {parsed.get('investment_decision')}")
        return parsed

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_data = {}
        try:
            if not self.stock_db_entry: logger.error(f"Stock {self.ticker} not init. Aborting."); return None
            self._ensure_stock_db_entry_is_bound()
            self._fetch_financial_statements()
            self._fetch_key_metrics_and_profile_data()
            final_data.update(self._calculate_derived_metrics())
            final_data.update(self._perform_dcf_analysis())
            final_data.update(self._fetch_and_summarize_10k())
            final_data.update(self._determine_investment_thesis())

            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id, analysis_date=datetime.now(timezone.utc))
            model_fields = [c.key for c in StockAnalysis.__table__.columns if
                            c.key not in ['id', 'stock_id', 'analysis_date']]
            for field in model_fields:
                if field in final_data:
                    val_set = final_data[field];
                    target_type = getattr(StockAnalysis, field).type.python_type
                    if target_type == float:
                        if isinstance(val_set, str):
                            try:
                                val_set = float(val_set)
                            except ValueError:
                                val_set = None
                        if isinstance(val_set, float) and (math.isnan(val_set) or math.isinf(val_set)): val_set = None
                    elif target_type == dict and not isinstance(val_set, dict):
                        val_set = None
                    setattr(analysis_entry, field, val_set)
            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = analysis_entry.analysis_date
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved stock: {self.ticker} (Analysis ID: {analysis_entry.id})")
            return analysis_entry
        except RuntimeError as r_err:
            logger.critical(f"Runtime error for {self.ticker}: {r_err}", exc_info=True); return None
        except Exception as e:
            logger.error(f"CRITICAL error in full analysis for {self.ticker}: {e}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try:
                    self.db_session.rollback(); logger.info(f"Rolled back TX for {self.ticker}.")
                except Exception as e_rb:
                    logger.error(f"Rollback error for {self.ticker}: {e_rb}")
            return None
        finally:
            self._close_session_if_active()

    def _ensure_stock_db_entry_is_bound(self):
        if not self.stock_db_entry:  # Should have been set in __init__
            logger.critical(
                f"CRITICAL: self.stock_db_entry is None for {self.ticker} at _ensure_stock_db_entry_is_bound. This should not happen.")
            raise RuntimeError(f"Stock entry for {self.ticker} is None during binding check.")

        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} INACTIVE before binding. Re-establishing.")
            self._close_session_if_active();
            self.db_session = next(get_db_session())
            # After re-establishing session, self.stock_db_entry might be detached. Re-fetch.
            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock:
                logger.error(
                    f"Could not re-fetch stock {self.ticker} after session re-establishment. Attempting _get_or_create_stock_entry again.")
                try:
                    self._get_or_create_stock_entry()  # This will use the new self.db_session
                except Exception as e_recreate:
                    raise RuntimeError(
                        f"Failed to re-init stock_db_entry for {self.ticker} after session fault: {e_recreate}") from e_recreate
            else:
                self.stock_db_entry = re_fetched_stock
            if not self.stock_db_entry: raise RuntimeError(f"Stock entry for {self.ticker} None after re-init attempt.")
            logger.info(
                f"Re-fetched/Re-created and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new session.")
            return

        instance_state = sa_inspect(self.stock_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            obj_id_log = self.stock_db_entry.id if instance_state.has_identity else 'Transient'
            logger.warning(
                f"Stock {self.ticker} (ID: {obj_id_log}) DETACHED/DIFFERENT session. Session expected: {id(self.db_session)}, actual: {id(instance_state.session) if instance_state.session else 'None'}. Merging.")
            try:
                self.stock_db_entry = self.db_session.merge(self.stock_db_entry)
            except Exception as e_merge:
                logger.error(f"Failed to merge stock {self.ticker}: {e_merge}. Re-fetching.", exc_info=True)
                re_fetched = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
                if re_fetched:
                    self.stock_db_entry = re_fetched
                else:
                    raise RuntimeError(f"Failed to bind stock {self.ticker} to session after merge/re-fetch fail.")


if __name__ == '__main__':
    from database import init_db

    # init_db()

    logger.info("Starting standalone stock analysis test with Alpha Vantage...")
    tickers_to_test = ["AAPL", "MSFT"]

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
                logger.info(
                    f"QoQ Revenue Growth: {analysis_result_obj.revenue_growth_qoq if analysis_result_obj.revenue_growth_qoq is not None else 'N/A'} (Source: {analysis_result_obj.key_metrics_snapshot.get('q_revenue_source', 'N/A')})")
            else:
                logger.error(f"Stock analysis pipeline FAILED for {ticker_symbol}.")
        except RuntimeError as rt_err:
            logger.error(f"Could not run StockAnalyzer for {ticker_symbol}: {rt_err}")
        except Exception as e_main:
            logger.error(f"Unhandled error analyzing {ticker_symbol} in __main__: {e_main}", exc_info=True)
        finally:
            logger.info(f"--- Finished processing {ticker_symbol} ---")
            time.sleep(20)