import pandas as pd
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone, timedelta
import math
import time
import warnings
from bs4 import XMLParsedAsHTMLWarning
import re
import json

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from api_clients import (
    FinnhubClient, FinancialModelingPrepClient, AlphaVantageClient,
    EODHDClient, GeminiAPIClient, SECEDGARClient, extract_S1_text_sections
)
from database import SessionLocal, get_db_session, Stock, StockAnalysis
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError
from core.config import (
    STOCK_FINANCIAL_YEARS, DEFAULT_DISCOUNT_RATE,
    DEFAULT_PERPETUAL_GROWTH_RATE, DEFAULT_FCF_PROJECTION_YEARS,
    TEN_K_KEY_SECTIONS,
    SUMMARIZATION_CHUNK_SIZE_CHARS, SUMMARIZATION_CHUNK_OVERLAP_CHARS,
    SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS,
    MAX_COMPETITORS_TO_ANALYZE, Q_REVENUE_SANITY_CHECK_DEVIATION_THRESHOLD,
    PRIORITY_REVENUE_SOURCES
)


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
    if start_value < 0 and end_value < 0: return None
    if end_value == 0 and start_value > 0: return -1.0
    return ((float(end_value) / float(start_value)) ** (1 / float(years))) - 1

def calculate_growth(current_value, previous_value):
    if previous_value is None or current_value is None: return None
    if float(previous_value) == 0:
        return None if float(current_value) == 0 else (float('inf') if float(current_value) > 0 else float('-inf'))
    try: return (float(current_value) - float(previous_value)) / abs(float(previous_value))
    except (ValueError, TypeError): return None

def get_value_from_statement_list(data_list, field, year_offset=0, report_date_for_log=None):
    if data_list and isinstance(data_list, list) and len(data_list) > year_offset:
        report = data_list[year_offset]
        if report and isinstance(report, dict):
            val = safe_get_float(report, field)
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
        self.data_quality_warnings = []
        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"CRITICAL: Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            self._close_session_if_active()
            raise RuntimeError(f"StockAnalyzer for {self.ticker} could not be initialized due to DB/API issues during stock entry setup.") from e

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try: self.db_session.close(); logger.debug(f"DB session closed for {self.ticker}.")
            except Exception as e_close: logger.warning(f"Error closing session for {self.ticker}: {e_close}")

    def _get_or_create_stock_entry(self):
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} inactive in _get_or_create. Re-establishing.")
            self._close_session_if_active(); self.db_session = next(get_db_session())
        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()
        company_name, industry, sector, cik = None, None, None, None
        profile_fmp_list = self.fmp.get_company_profile(self.ticker); time.sleep(1.5)
        if profile_fmp_list and isinstance(profile_fmp_list, list) and len(profile_fmp_list) > 0 and profile_fmp_list[0]:
            data = profile_fmp_list[0]; self._financial_data_cache['profile_fmp'] = data
            company_name, industry, sector, cik = data.get('companyName'), data.get('industry'), data.get('sector'), data.get('cik')
            if cik: cik = str(cik).zfill(10)
            logger.info(f"Fetched profile from FMP for {self.ticker}.")
        else:
            logger.warning(f"FMP profile failed or empty for {self.ticker}. Trying Finnhub.")
            profile_fh = self.finnhub.get_company_profile2(self.ticker); time.sleep(1.5)
            if profile_fh:
                self._financial_data_cache['profile_finnhub'] = profile_fh
                company_name, industry = profile_fh.get('name'), profile_fh.get('finnhubIndustry')
                logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Finnhub profile failed for {self.ticker}. Trying Alpha Vantage Overview.")
                overview_av = self.alphavantage.get_company_overview(self.ticker); time.sleep(2)
                if overview_av and overview_av.get("Symbol") == self.ticker:
                    self._financial_data_cache['overview_alphavantage'] = overview_av
                    company_name, industry, sector, cik = overview_av.get('Name'), overview_av.get('Industry'), overview_av.get('Sector'), overview_av.get('CIK')
                    if cik: cik = str(cik).zfill(10)
                    logger.info(f"Fetched overview from Alpha Vantage for {self.ticker}.")
                else: logger.warning(f"All primary profile fetches (FMP, Finnhub, AV) failed or incomplete for {self.ticker}.")
        if not company_name: company_name = self.ticker
        if not cik and self.ticker:
            logger.info(f"CIK not found from profiles for {self.ticker}. Querying SEC EDGAR CIK map.")
            cik_from_edgar = self.sec_edgar.get_cik_by_ticker(self.ticker); time.sleep(0.5)
            if cik_from_edgar: cik = str(cik_from_edgar).zfill(10); logger.info(f"Fetched CIK {cik} from SEC EDGAR CIK map for {self.ticker}.")
            else: logger.warning(f"Could not fetch CIK from SEC EDGAR CIK map for {self.ticker}.")

        if not self.stock_db_entry:
            logger.info(f"Stock {self.ticker} not found in DB, creating new entry.")
            self.stock_db_entry = Stock(ticker=self.ticker, company_name=company_name, industry=industry, sector=sector, cik=cik)
            self.db_session.add(self.stock_db_entry)
            try: self.db_session.commit(); self.db_session.refresh(self.stock_db_entry)
            except SQLAlchemyError as e: self.db_session.rollback(); logger.error(f"Error creating stock entry for {self.ticker}: {e}", exc_info=True); raise
        else:
            updated = False
            if company_name and self.stock_db_entry.company_name != company_name: self.stock_db_entry.company_name = company_name; updated = True
            if industry and self.stock_db_entry.industry != industry: self.stock_db_entry.industry = industry; updated = True
            if sector and self.stock_db_entry.sector != sector: self.stock_db_entry.sector = sector; updated = True
            if cik and self.stock_db_entry.cik != cik: self.stock_db_entry.cik = cik; updated = True
            elif not self.stock_db_entry.cik and cik: self.stock_db_entry.cik = cik; updated = True
            if updated:
                try: self.db_session.commit(); self.db_session.refresh(self.stock_db_entry); logger.info(f"Updated stock entry for {self.ticker} with new profile data.")
                except SQLAlchemyError as e: self.db_session.rollback(); logger.error(f"Error updating stock entry for {self.ticker}: {e}")
        logger.info(f"Stock entry for {self.ticker} (ID: {self.stock_db_entry.id if self.stock_db_entry else 'N/A'}, CIK: {self.stock_db_entry.cik if self.stock_db_entry and self.stock_db_entry.cik else 'N/A'}) ready.")

    def _fetch_financial_statements(self):
        logger.info(f"Fetching financial statements for {self.ticker}...")
        statements_cache = {"fmp_income_annual": [], "fmp_balance_annual": [], "fmp_cashflow_annual": [], "fmp_income_quarterly": [], "finnhub_financials_quarterly_reported": {"data": []}, "alphavantage_income_quarterly": {"quarterlyReports": []}, "alphavantage_balance_quarterly": {"quarterlyReports": []}, "alphavantage_cashflow_quarterly": {"quarterlyReports": []}}
        try:
            statements_cache["fmp_income_annual"] = self.fmp.get_financial_statements(self.ticker, "income-statement", "annual", STOCK_FINANCIAL_YEARS) or []; time.sleep(1.5)
            statements_cache["fmp_balance_annual"] = self.fmp.get_financial_statements(self.ticker, "balance-sheet-statement", "annual", STOCK_FINANCIAL_YEARS) or []; time.sleep(1.5)
            statements_cache["fmp_cashflow_annual"] = self.fmp.get_financial_statements(self.ticker, "cash-flow-statement", "annual", STOCK_FINANCIAL_YEARS) or []; time.sleep(1.5)
            logger.info(f"FMP Annuals for {self.ticker}: Income({len(statements_cache['fmp_income_annual'])}), Balance({len(statements_cache['fmp_balance_annual'])}), Cashflow({len(statements_cache['fmp_cashflow_annual'])}).")
            statements_cache["fmp_income_quarterly"] = self.fmp.get_financial_statements(self.ticker, "income-statement", "quarter", 8) or []; time.sleep(1.5)
            logger.info(f"FMP Quarterly Income for {self.ticker}: {len(statements_cache['fmp_income_quarterly'])} records.")
            fh_q_data = self.finnhub.get_financials_reported(self.ticker, freq="quarterly", count=8); time.sleep(1.5)
            if fh_q_data and isinstance(fh_q_data, dict) and fh_q_data.get("data"): statements_cache["finnhub_financials_quarterly_reported"] = fh_q_data; logger.info(f"Fetched {len(fh_q_data['data'])} quarterly reports from Finnhub for {self.ticker}.")
            else: logger.warning(f"Finnhub quarterly financials reported data missing or malformed for {self.ticker}.")
            av_income_q = self.alphavantage.get_income_statement_quarterly(self.ticker); time.sleep(15)
            if av_income_q and isinstance(av_income_q, dict) and av_income_q.get("quarterlyReports"): statements_cache["alphavantage_income_quarterly"] = av_income_q; logger.info(f"Fetched {len(av_income_q['quarterlyReports'])} quarterly income reports from Alpha Vantage for {self.ticker}.")
            else: logger.warning(f"Alpha Vantage quarterly income reports missing or malformed for {self.ticker}.")
            av_balance_q = self.alphavantage.get_balance_sheet_quarterly(self.ticker); time.sleep(15)
            if av_balance_q and isinstance(av_balance_q, dict) and av_balance_q.get("quarterlyReports"): statements_cache["alphavantage_balance_quarterly"] = av_balance_q; logger.info(f"Fetched {len(av_balance_q['quarterlyReports'])} quarterly balance reports from Alpha Vantage for {self.ticker}.")
            else: logger.warning(f"Alpha Vantage quarterly balance reports missing or malformed for {self.ticker}.")
            av_cashflow_q = self.alphavantage.get_cash_flow_quarterly(self.ticker); time.sleep(15)
            if av_cashflow_q and isinstance(av_cashflow_q, dict) and av_cashflow_q.get("quarterlyReports"): statements_cache["alphavantage_cashflow_quarterly"] = av_cashflow_q; logger.info(f"Fetched {len(av_cashflow_q['quarterlyReports'])} quarterly cash flow reports from Alpha Vantage for {self.ticker}.")
            else: logger.warning(f"Alpha Vantage quarterly cash flow reports missing or malformed for {self.ticker}.")
        except Exception as e: logger.warning(f"Error during financial statements fetch for {self.ticker}: {e}.", exc_info=True)
        self._financial_data_cache['financial_statements'] = statements_cache
        return statements_cache

    def _fetch_key_metrics_and_profile_data(self):
        logger.info(f"Fetching key metrics and profile for {self.ticker}.")
        self._financial_data_cache['key_metrics_annual_fmp'] = self.fmp.get_key_metrics(self.ticker, "annual", STOCK_FINANCIAL_YEARS + 2) or []; time.sleep(1.5)
        key_metrics_quarterly_fmp = self.fmp.get_key_metrics(self.ticker, "quarterly", 8); time.sleep(1.5)
        self._financial_data_cache['key_metrics_quarterly_fmp'] = key_metrics_quarterly_fmp if key_metrics_quarterly_fmp is not None else []
        self._financial_data_cache['basic_financials_finnhub'] = self.finnhub.get_basic_financials(self.ticker) or {}; time.sleep(1.5)
        if 'profile_fmp' not in self._financial_data_cache or not self._financial_data_cache.get('profile_fmp'):
            profile_fmp_list = self.fmp.get_company_profile(self.ticker); time.sleep(1.5)
            self._financial_data_cache['profile_fmp'] = profile_fmp_list[0] if profile_fmp_list and isinstance(profile_fmp_list, list) and profile_fmp_list[0] else {}
        logger.info(f"FMP KM Annual for {self.ticker}: {len(self._financial_data_cache['key_metrics_annual_fmp'])}. FMP KM Quarterly for {self.ticker}: {len(self._financial_data_cache['key_metrics_quarterly_fmp'])}. Finnhub Basic Financials for {self.ticker}: {'OK' if self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric') else 'Data missing'}.")

    def _calculate_valuation_ratios(self, latest_km_q_fmp, latest_km_a_fmp, basic_fin_fh_metric):
        ratios = {}
        ratios["pe_ratio"] = safe_get_float(latest_km_q_fmp, "peRatioTTM") or safe_get_float(latest_km_a_fmp, "peRatio") or safe_get_float(basic_fin_fh_metric, "peTTM")
        ratios["pb_ratio"] = safe_get_float(latest_km_q_fmp, "priceToBookRatioTTM") or safe_get_float(latest_km_a_fmp, "pbRatio") or safe_get_float(basic_fin_fh_metric, "pbAnnual")
        ratios["ps_ratio"] = safe_get_float(latest_km_q_fmp, "priceToSalesRatioTTM") or safe_get_float(latest_km_a_fmp, "priceSalesRatio") or safe_get_float(basic_fin_fh_metric, "psTTM")
        ratios["ev_to_sales"] = safe_get_float(latest_km_q_fmp, "enterpriseValueOverRevenueTTM") or safe_get_float(latest_km_a_fmp, "enterpriseValueOverRevenue")
        ratios["ev_to_ebitda"] = safe_get_float(latest_km_q_fmp, "evToEbitdaTTM") or safe_get_float(latest_km_a_fmp, "evToEbitda")
        div_yield_fmp_q = safe_get_float(latest_km_q_fmp, "dividendYieldTTM"); div_yield_fmp_a = safe_get_float(latest_km_a_fmp, "dividendYield")
        div_yield_fh_raw = safe_get_float(basic_fin_fh_metric, "dividendYieldAnnual"); div_yield_fh = div_yield_fh_raw / 100.0 if div_yield_fh_raw is not None else None
        ratios["dividend_yield"] = div_yield_fmp_q if div_yield_fmp_q is not None else (div_yield_fmp_a if div_yield_fmp_a is not None else div_yield_fh)
        return ratios

    def _calculate_profitability_metrics(self, income_annual_fmp, balance_annual_fmp, latest_km_a_fmp):
        metrics = {}
        if income_annual_fmp:
            latest_ia = income_annual_fmp[0]
            metrics["eps"] = safe_get_float(latest_ia, "eps") or safe_get_float(latest_km_a_fmp, "eps")
            metrics["net_profit_margin"] = safe_get_float(latest_ia, "netProfitMargin")
            metrics["gross_profit_margin"] = safe_get_float(latest_ia, "grossProfitMargin")
            metrics["operating_profit_margin"] = safe_get_float(latest_ia, "operatingIncomeRatio")
            ebit = safe_get_float(latest_ia, "operatingIncome"); interest_expense = safe_get_float(latest_ia, "interestExpense")
            if ebit is not None and interest_expense is not None and abs(interest_expense) > 1e-6: metrics["interest_coverage_ratio"] = ebit / abs(interest_expense)
        if balance_annual_fmp and income_annual_fmp:
            total_equity = get_value_from_statement_list(balance_annual_fmp, "totalStockholdersEquity", 0)
            total_assets = get_value_from_statement_list(balance_annual_fmp, "totalAssets", 0)
            latest_net_income = get_value_from_statement_list(income_annual_fmp, "netIncome", 0)
            if total_equity and total_equity != 0 and latest_net_income is not None: metrics["roe"] = latest_net_income / total_equity
            if total_assets and total_assets != 0 and latest_net_income is not None: metrics["roa"] = latest_net_income / total_assets
            ebit_roic = get_value_from_statement_list(income_annual_fmp, "operatingIncome", 0)
            income_tax_expense_roic = get_value_from_statement_list(income_annual_fmp, "incomeTaxExpense", 0)
            income_before_tax_roic = get_value_from_statement_list(income_annual_fmp, "incomeBeforeTax", 0)
            effective_tax_rate = 0.21
            if income_tax_expense_roic is not None and income_before_tax_roic is not None and income_before_tax_roic != 0:
                calculated_tax_rate = income_tax_expense_roic / income_before_tax_roic
                if 0 <= calculated_tax_rate <= 0.50: effective_tax_rate = calculated_tax_rate
                else: logger.debug(f"Calculated tax rate {calculated_tax_rate:.2%} for {self.ticker} is unusual. Using default {effective_tax_rate:.2%}.")
            nopat = ebit_roic * (1 - effective_tax_rate) if ebit_roic is not None else None
            total_debt_roic = get_value_from_statement_list(balance_annual_fmp, "totalDebt", 0)
            cash_equivalents_roic = get_value_from_statement_list(balance_annual_fmp, "cashAndCashEquivalents", 0) or 0
            if total_debt_roic is not None and total_equity is not None:
                invested_capital = total_debt_roic + total_equity - cash_equivalents_roic
                if nopat is not None and invested_capital is not None and invested_capital != 0: metrics["roic"] = nopat / invested_capital
        return metrics

    def _calculate_financial_health_metrics(self, balance_annual_fmp, income_annual_fmp, latest_km_a_fmp):
        metrics = {}
        if balance_annual_fmp:
            latest_ba = balance_annual_fmp[0]; total_equity = safe_get_float(latest_ba, "totalStockholdersEquity")
            metrics["debt_to_equity"] = safe_get_float(latest_km_a_fmp, "debtToEquity")
            if metrics["debt_to_equity"] is None:
                total_debt_ba = safe_get_float(latest_ba, "totalDebt")
                if total_debt_ba is not None and total_equity and total_equity != 0: metrics["debt_to_equity"] = total_debt_ba / total_equity
            current_assets = safe_get_float(latest_ba, "totalCurrentAssets"); current_liabilities = safe_get_float(latest_ba, "totalCurrentLiabilities")
            if current_assets is not None and current_liabilities is not None and current_liabilities != 0: metrics["current_ratio"] = current_assets / current_liabilities
            cash_equivalents = safe_get_float(latest_ba, "cashAndCashEquivalents", 0); short_term_investments = safe_get_float(latest_ba, "shortTermInvestments", 0); net_receivables = safe_get_float(latest_ba, "netReceivables", 0)
            if current_liabilities is not None and current_liabilities != 0: metrics["quick_ratio"] = (cash_equivalents + short_term_investments + net_receivables) / current_liabilities
        latest_annual_ebitda_km = safe_get_float(latest_km_a_fmp, "ebitda"); latest_annual_ebitda_is = get_value_from_statement_list(income_annual_fmp, "ebitda", 0)
        latest_annual_ebitda = latest_annual_ebitda_km if latest_annual_ebitda_km is not None else latest_annual_ebitda_is
        if latest_annual_ebitda and latest_annual_ebitda != 0 and balance_annual_fmp:
            total_debt_val = get_value_from_statement_list(balance_annual_fmp, "totalDebt", 0)
            if total_debt_val is not None: metrics["debt_to_ebitda"] = total_debt_val / latest_annual_ebitda
        return metrics

    def _get_cross_validated_quarterly_revenue(self, statements_cache):
        latest_q_revenue, previous_q_revenue, source_name, historical_revenues = None, None, None, []
        revenue_fields = {"fmp_quarterly": "revenue", "alphavantage_quarterly": "totalRevenue", "finnhub_quarterly": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "TotalRevenues", "NetSales"]}
        for src_key in PRIORITY_REVENUE_SOURCES:
            try:
                if src_key == "fmp_quarterly" and statements_cache.get('fmp_income_quarterly'):
                    reports = statements_cache['fmp_income_quarterly']
                    if not reports: continue
                    latest_val = get_fmp_value(reports, revenue_fields[src_key], 0); prev_val = get_fmp_value(reports, revenue_fields[src_key], 1) if len(reports) > 1 else None
                    if latest_val is not None: latest_q_revenue, previous_q_revenue, source_name = latest_val, prev_val, "FMP"; [historical_revenues.append(h) for i in range(min(len(reports), 5)) if (h := get_fmp_value(reports, revenue_fields[src_key], i)) is not None]; break
                elif src_key == "alphavantage_quarterly" and statements_cache.get('alphavantage_income_quarterly', {}).get('quarterlyReports'):
                    reports = statements_cache['alphavantage_income_quarterly']['quarterlyReports']
                    if not reports: continue
                    latest_val = get_alphavantage_value(reports, revenue_fields[src_key], 0); prev_val = get_alphavantage_value(reports, revenue_fields[src_key], 1) if len(reports) > 1 else None
                    if latest_val is not None: latest_q_revenue, previous_q_revenue, source_name = latest_val, prev_val, "AlphaVantage"; [historical_revenues.append(h) for i in range(min(len(reports), 5)) if (h := get_alphavantage_value(reports, revenue_fields[src_key], i)) is not None]; break
                elif src_key == "finnhub_quarterly" and statements_cache.get('finnhub_financials_quarterly_reported', {}).get('data'):
                    reports = statements_cache['finnhub_financials_quarterly_reported']['data']
                    if not reports: continue
                    latest_val = get_finnhub_concept_value(reports, 'ic', revenue_fields[src_key], 0); prev_val = get_finnhub_concept_value(reports, 'ic', revenue_fields[src_key], 1) if len(reports) > 1 else None
                    if latest_val is not None: latest_q_revenue, previous_q_revenue, source_name = latest_val, prev_val, "Finnhub"; [historical_revenues.append(h) for i in range(min(len(reports), 5)) if (h := get_finnhub_concept_value(reports, 'ic', revenue_fields[src_key], i)) is not None]; break
            except Exception as e: logger.warning(f"Error processing quarterly revenue from {src_key} for {self.ticker}: {e}"); continue
        avg_historical_q_revenue = None
        if historical_revenues:
            points_for_avg = [r for r in historical_revenues if r is not None and r > 0]
            avg_base_points = points_for_avg[1:] if points_for_avg and points_for_avg[0] == latest_q_revenue and len(points_for_avg) > 1 else points_for_avg
            if len(avg_base_points) > 1:
                avg_historical_q_revenue = sum(avg_base_points) / len(avg_base_points)
                if latest_q_revenue is not None and avg_historical_q_revenue > 0:
                    deviation = abs(latest_q_revenue - avg_historical_q_revenue) / avg_historical_q_revenue
                    if deviation > Q_REVENUE_SANITY_CHECK_DEVIATION_THRESHOLD:
                        warning_msg = f"DATA QUALITY WARNING: Latest quarterly revenue ({latest_q_revenue:,.0f} from {source_name}) deviates by {deviation:.2%} from avg of recent historical quarters ({avg_historical_q_revenue:,.0f}). Review data accuracy."
                        logger.warning(warning_msg); self.data_quality_warnings.append(warning_msg)
        else: logger.info(f"Not enough historical quarterly revenue data to perform sanity check for {self.ticker}.")
        if latest_q_revenue is None: logger.error(f"Could not determine latest quarterly revenue for {self.ticker} from any source."); self.data_quality_warnings.append("CRITICAL: Latest quarterly revenue could not be determined.")
        else: logger.info(f"Using latest quarterly revenue: {latest_q_revenue:,.0f} (Source: {source_name}) for {self.ticker}.")
        return latest_q_revenue, previous_q_revenue, source_name, avg_historical_q_revenue

    def _calculate_growth_metrics(self, income_annual_fmp, statements_cache):
        metrics = {"key_metrics_snapshot": {}}
        metrics["revenue_growth_yoy"] = calculate_growth(get_value_from_statement_list(income_annual_fmp, "revenue", 0), get_value_from_statement_list(income_annual_fmp, "revenue", 1))
        metrics["eps_growth_yoy"] = calculate_growth(get_value_from_statement_list(income_annual_fmp, "eps", 0), get_value_from_statement_list(income_annual_fmp, "eps", 1))
        if len(income_annual_fmp) >= 3:
            metrics["revenue_growth_cagr_3yr"] = calculate_cagr(get_value_from_statement_list(income_annual_fmp, "revenue", 0), get_value_from_statement_list(income_annual_fmp, "revenue", 2), 2)
            metrics["eps_growth_cagr_3yr"] = calculate_cagr(get_value_from_statement_list(income_annual_fmp, "eps", 0), get_value_from_statement_list(income_annual_fmp, "eps", 2), 2)
        if len(income_annual_fmp) >= 5:
            metrics["revenue_growth_cagr_5yr"] = calculate_cagr(get_value_from_statement_list(income_annual_fmp, "revenue", 0), get_value_from_statement_list(income_annual_fmp, "revenue", 4), 4)
            metrics["eps_growth_cagr_5yr"] = calculate_cagr(get_value_from_statement_list(income_annual_fmp, "eps", 0), get_value_from_statement_list(income_annual_fmp, "eps", 4), 4)
        latest_q_rev, prev_q_rev, rev_src_name, _ = self._get_cross_validated_quarterly_revenue(statements_cache)
        if latest_q_rev is not None:
            metrics["key_metrics_snapshot"]["q_revenue_source"] = rev_src_name; metrics["key_metrics_snapshot"]["latest_q_revenue"] = latest_q_rev
            if prev_q_rev is not None: metrics["revenue_growth_qoq"] = calculate_growth(latest_q_rev, prev_q_rev)
            else: logger.info(f"Previous quarter revenue not available from source {rev_src_name} for {self.ticker}. Cannot calculate QoQ revenue growth."); metrics["revenue_growth_qoq"] = None
        else: metrics["revenue_growth_qoq"] = None; metrics["key_metrics_snapshot"]["q_revenue_source"] = "N/A"; metrics["key_metrics_snapshot"]["latest_q_revenue"] = None
        return metrics

    def _calculate_cash_flow_and_trend_metrics(self, cashflow_annual_fmp, balance_annual_fmp, profile_fmp):
        metrics = {}
        if cashflow_annual_fmp:
            fcf_latest_annual = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0)
            shares_outstanding_profile = safe_get_float(profile_fmp, "sharesOutstanding"); mkt_cap_profile = safe_get_float(profile_fmp, "mktCap"); price_profile = safe_get_float(profile_fmp, "price")
            shares_outstanding_calc = (mkt_cap_profile / price_profile) if mkt_cap_profile and price_profile and price_profile != 0 else None
            shares_outstanding = shares_outstanding_profile if shares_outstanding_profile is not None and shares_outstanding_profile > 0 else shares_outstanding_calc
            if fcf_latest_annual is not None and shares_outstanding and shares_outstanding != 0:
                metrics["free_cash_flow_per_share"] = fcf_latest_annual / shares_outstanding
                if mkt_cap_profile and mkt_cap_profile != 0: metrics["free_cash_flow_yield"] = fcf_latest_annual / mkt_cap_profile
            if len(cashflow_annual_fmp) >= 3:
                fcf0, fcf1, fcf2 = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0), get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 1), get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 2)
                if all(isinstance(x, (int, float)) for x in [fcf0, fcf1, fcf2] if x is not None) and all(x is not None for x in [fcf0, fcf1, fcf2]):
                    if fcf0 > fcf1 > fcf2: metrics["free_cash_flow_trend"] = "Growing"
                    elif fcf0 < fcf1 < fcf2: metrics["free_cash_flow_trend"] = "Declining"
                    elif fcf0 > fcf1 and fcf1 < fcf2: metrics["free_cash_flow_trend"] = "Volatile (Dip then Rise)"
                    elif fcf0 < fcf1 and fcf1 > fcf2: metrics["free_cash_flow_trend"] = "Volatile (Rise then Dip)"
                    else: metrics["free_cash_flow_trend"] = "Mixed/Stable"
                else: metrics["free_cash_flow_trend"] = "Data Incomplete/Non-Numeric"
            else: metrics["free_cash_flow_trend"] = "Data N/A (<3 yrs)"
        if len(balance_annual_fmp) >= 3:
            re0, re1, re2 = get_value_from_statement_list(balance_annual_fmp, "retainedEarnings", 0), get_value_from_statement_list(balance_annual_fmp, "retainedEarnings", 1), get_value_from_statement_list(balance_annual_fmp, "retainedEarnings", 2)
            if all(isinstance(x, (int, float)) for x in [re0, re1, re2] if x is not None) and all(x is not None for x in [re0, re1, re2]):
                if re0 > re1 > re2: metrics["retained_earnings_trend"] = "Growing"
                elif re0 < re1 < re2: metrics["retained_earnings_trend"] = "Declining"
                else: metrics["retained_earnings_trend"] = "Mixed/Stable"
            else: metrics["retained_earnings_trend"] = "Data Incomplete/Non-Numeric"
        else: metrics["retained_earnings_trend"] = "Data N/A (<3 yrs)"
        return metrics

    def _calculate_derived_metrics(self):
        logger.info(f"Calculating derived metrics for {self.ticker}...")
        all_metrics = {}
        statements = self._financial_data_cache.get('financial_statements', {})
        income_annual_fmp, balance_annual_fmp, cashflow_annual_fmp = statements.get('fmp_income_annual', []), statements.get('fmp_balance_annual', []), statements.get('fmp_cashflow_annual', [])
        key_metrics_annual_fmp, key_metrics_quarterly_fmp = self._financial_data_cache.get('key_metrics_annual_fmp', []), self._financial_data_cache.get('key_metrics_quarterly_fmp', [])
        basic_fin_fh_metric = self._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {}); profile_fmp = self._financial_data_cache.get('profile_fmp', {})
        latest_km_q_fmp, latest_km_a_fmp = key_metrics_quarterly_fmp[0] if key_metrics_quarterly_fmp else {}, key_metrics_annual_fmp[0] if key_metrics_annual_fmp else {}
        all_metrics.update(self._calculate_valuation_ratios(latest_km_q_fmp, latest_km_a_fmp, basic_fin_fh_metric))
        all_metrics.update(self._calculate_profitability_metrics(income_annual_fmp, balance_annual_fmp, latest_km_a_fmp))
        all_metrics.update(self._calculate_financial_health_metrics(balance_annual_fmp, income_annual_fmp, latest_km_a_fmp))
        growth_metrics_result = self._calculate_growth_metrics(income_annual_fmp, statements)
        all_metrics.update(growth_metrics_result)
        all_metrics.update(self._calculate_cash_flow_and_trend_metrics(cashflow_annual_fmp, balance_annual_fmp, profile_fmp))
        final_metrics = {}
        for k, v in all_metrics.items():
            if k == "key_metrics_snapshot": final_metrics[k] = {sk: sv for sk, sv in v.items() if sv is not None and not (isinstance(sv, float) and (math.isnan(sv) or math.isinf(sv)))}
            elif isinstance(v, float): final_metrics[k] = v if not (math.isnan(v) or math.isinf(v)) else None
            elif v is not None: final_metrics[k] = v
            else: final_metrics[k] = None
        log_metrics = {k: v for k, v in final_metrics.items() if k != "key_metrics_snapshot"}
        logger.info(f"Calculated metrics for {self.ticker}: {json.dumps(log_metrics, indent=2)}")
        self._financial_data_cache['calculated_metrics'] = final_metrics
        return final_metrics

    def _perform_dcf_analysis(self):
        logger.info(f"Performing simplified DCF analysis for {self.ticker}...")
        dcf_results = {"dcf_intrinsic_value": None, "dcf_upside_percentage": None, "dcf_assumptions": {"discount_rate": DEFAULT_DISCOUNT_RATE, "perpetual_growth_rate": DEFAULT_PERPETUAL_GROWTH_RATE, "projection_years": DEFAULT_FCF_PROJECTION_YEARS, "start_fcf": None, "start_fcf_basis": "N/A", "fcf_growth_rates_projection": [], "initial_fcf_growth_rate_used": None, "initial_fcf_growth_rate_basis": "N/A", "sensitivity_analysis": []}}
        assumptions = dcf_results["dcf_assumptions"]
        cashflow_annual_fmp = self._financial_data_cache.get('financial_statements', {}).get('fmp_cashflow_annual', []); profile_fmp = self._financial_data_cache.get('profile_fmp', {}); calculated_metrics = self._financial_data_cache.get('calculated_metrics', {})
        current_price = safe_get_float(profile_fmp, "price"); shares_outstanding_profile = safe_get_float(profile_fmp, "sharesOutstanding"); mkt_cap_profile = safe_get_float(profile_fmp, "mktCap")
        shares_outstanding_calc = (mkt_cap_profile / current_price) if mkt_cap_profile and current_price and current_price != 0 else None
        shares_outstanding = shares_outstanding_profile if shares_outstanding_profile is not None and shares_outstanding_profile > 0 else shares_outstanding_calc
        if not cashflow_annual_fmp or not profile_fmp or current_price is None or shares_outstanding is None or shares_outstanding == 0: logger.warning(f"Insufficient data for DCF for {self.ticker} (FCF statements, profile, price, or shares missing/zero)."); return dcf_results
        current_fcf_annual = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0)
        if current_fcf_annual is None or current_fcf_annual <= 10000: logger.warning(f"Current annual FCF for {self.ticker} is {current_fcf_annual}. DCF requires substantial positive FCF. Skipping DCF."); return dcf_results
        assumptions["start_fcf"] = current_fcf_annual; assumptions["start_fcf_basis"] = f"Latest Annual FCF ({cashflow_annual_fmp[0].get('date') if cashflow_annual_fmp else 'N/A'})"
        fcf_growth_rate_3yr_cagr = None
        if len(cashflow_annual_fmp) >= 4:
            fcf_start_for_cagr = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 3)
            if fcf_start_for_cagr and fcf_start_for_cagr > 0: fcf_growth_rate_3yr_cagr = calculate_cagr(current_fcf_annual, fcf_start_for_cagr, 3)
        initial_fcf_growth_rate = DEFAULT_PERPETUAL_GROWTH_RATE; assumptions["initial_fcf_growth_rate_basis"] = "Default (Perpetual Growth Rate)"
        if fcf_growth_rate_3yr_cagr is not None: initial_fcf_growth_rate = fcf_growth_rate_3yr_cagr; assumptions["initial_fcf_growth_rate_basis"] = "Historical 3yr FCF CAGR"
        elif calculated_metrics.get("revenue_growth_cagr_3yr") is not None: initial_fcf_growth_rate = calculated_metrics["revenue_growth_cagr_3yr"]; assumptions["initial_fcf_growth_rate_basis"] = "Proxy: Revenue Growth CAGR (3yr)"
        elif calculated_metrics.get("revenue_growth_yoy") is not None: initial_fcf_growth_rate = calculated_metrics["revenue_growth_yoy"]; assumptions["initial_fcf_growth_rate_basis"] = "Proxy: Revenue Growth YoY"
        if not isinstance(initial_fcf_growth_rate, (int, float)): initial_fcf_growth_rate = DEFAULT_PERPETUAL_GROWTH_RATE
        initial_fcf_growth_rate = min(max(initial_fcf_growth_rate, -0.05), 0.15); assumptions["initial_fcf_growth_rate_used"] = initial_fcf_growth_rate

        def calculate_dcf_value(start_fcf, initial_growth, discount_rate, perpetual_growth, proj_years, shares):
            projected_fcfs, last_projected_fcf, current_year_growth_rates = [], start_fcf, []
            growth_rate_decline_per_year = (initial_growth - perpetual_growth) / float(proj_years) if proj_years > 0 else 0
            for i in range(proj_years):
                current_year_growth_rate = max(initial_growth - (growth_rate_decline_per_year * i), perpetual_growth)
                projected_fcf = last_projected_fcf * (1 + current_year_growth_rate)
                projected_fcfs.append(projected_fcf); last_projected_fcf = projected_fcf; current_year_growth_rates.append(round(current_year_growth_rate, 4))
            if not projected_fcfs: return None, []
            terminal_year_fcf_for_tv = projected_fcfs[-1] * (1 + perpetual_growth); terminal_value_denominator = discount_rate - perpetual_growth
            terminal_value = 0 if terminal_value_denominator <= 1e-6 else terminal_year_fcf_for_tv / terminal_value_denominator
            if terminal_value_denominator <= 1e-6 : logger.warning(f"DCF for {self.ticker}: Discount rate ({discount_rate}) near/below perpetual growth ({perpetual_growth}). Terminal Value unreliable.")
            sum_discounted_fcf = sum(fcf / ((1 + discount_rate) ** (i + 1)) for i, fcf in enumerate(projected_fcfs))
            discounted_terminal_value = terminal_value / ((1 + discount_rate) ** proj_years)
            intrinsic_equity_value = sum_discounted_fcf + discounted_terminal_value
            return intrinsic_equity_value / shares if shares != 0 else None, current_year_growth_rates

        base_iv_per_share, base_fcf_growth_rates = calculate_dcf_value(assumptions["start_fcf"], assumptions["initial_fcf_growth_rate_used"], assumptions["discount_rate"], assumptions["perpetual_growth_rate"], assumptions["projection_years"], shares_outstanding)
        if base_iv_per_share is not None:
            dcf_results["dcf_intrinsic_value"] = base_iv_per_share; assumptions["fcf_growth_rates_projection"] = base_fcf_growth_rates
            if current_price and current_price != 0: dcf_results["dcf_upside_percentage"] = (base_iv_per_share - current_price) / current_price
        else: logger.error(f"DCF base case calculation failed for {self.ticker}."); return dcf_results
        sensitivity_scenarios = [{"dr_adj": -0.005, "pgr_adj": 0.0, "label": "Discount Rate -0.5%"}, {"dr_adj": +0.005, "pgr_adj": 0.0, "label": "Discount Rate +0.5%"}, {"dr_adj": 0.0, "pgr_adj": -0.0025, "label": "Perp. Growth -0.25%"}, {"dr_adj": 0.0, "pgr_adj": +0.0025, "label": "Perp. Growth +0.25%"}]
        for scenario in sensitivity_scenarios:
            sens_dr, sens_pgr = assumptions["discount_rate"] + scenario["dr_adj"], assumptions["perpetual_growth_rate"] + scenario["pgr_adj"]
            if sens_pgr >= sens_dr - 0.001: logger.debug(f"Skipping DCF sensitivity scenario '{scenario['label']}' for {self.ticker} as PGR ({sens_pgr:.3f}) >= DR ({sens_dr:.3f})."); continue
            iv_sens, _ = calculate_dcf_value(assumptions["start_fcf"], assumptions["initial_fcf_growth_rate_used"], sens_dr, sens_pgr, assumptions["projection_years"], shares_outstanding)
            if iv_sens is not None:
                upside_sens = (iv_sens - current_price) / current_price if current_price and current_price != 0 else None
                assumptions["sensitivity_analysis"].append({"scenario": scenario["label"], "discount_rate": sens_dr, "perpetual_growth_rate": sens_pgr, "intrinsic_value": iv_sens, "upside": upside_sens})
        logger.info(f"DCF for {self.ticker}: Base IV/Share: {dcf_results.get('dcf_intrinsic_value', 'N/A'):.2f}, Upside: {dcf_results.get('dcf_upside_percentage', 'N/A') * 100 if dcf_results.get('dcf_upside_percentage') is not None else 'N/A':.2f}%")
        self._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    def _summarize_text_chunked(self, text_to_summarize, base_context, section_specific_instruction, company_name_ticker_prompt):
        if not text_to_summarize or not isinstance(text_to_summarize, str) or not text_to_summarize.strip(): return "No text provided for summarization.", 0
        text_len = len(text_to_summarize)
        logger.info(f"Summarizing '{base_context}' for {company_name_ticker_prompt}, original length: {text_len} chars.")
        if text_len < SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS:
            logger.info(f"Section length {text_len} is within single-pass limit ({SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS}). Summarizing directly.")
            summary = self.gemini.summarize_text_with_context(text_to_summarize, f"{base_context} for {company_name_ticker_prompt}.", section_specific_instruction); time.sleep(2)
            return (summary if summary and not summary.startswith("Error:") else f"AI summary error or no content for '{base_context}'."), text_len
        logger.info(f"Section length {text_len} exceeds single-pass limit. Applying chunked summarization (Chunk size: {SUMMARIZATION_CHUNK_SIZE_CHARS}, Overlap: {SUMMARIZATION_CHUNK_OVERLAP_CHARS}).")
        chunks, start = [], 0
        while start < text_len: end = start + SUMMARIZATION_CHUNK_SIZE_CHARS; chunks.append(text_to_summarize[start:end]); start = end - SUMMARIZATION_CHUNK_OVERLAP_CHARS if end < text_len and SUMMARIZATION_CHUNK_OVERLAP_CHARS < SUMMARIZATION_CHUNK_SIZE_CHARS else end
        chunk_summaries = []
        for i, chunk in enumerate(chunks):
            logger.info(f"Summarizing chunk {i + 1}/{len(chunks)} for '{base_context}' of {company_name_ticker_prompt} (length: {len(chunk)} chars).")
            summary = self.gemini.summarize_text_with_context(chunk, f"This is chunk {i + 1} of {len(chunks)} from the '{base_context}' section for {company_name_ticker_prompt}.", f"Summarize this chunk. Focus on key facts and figures relevant to: {section_specific_instruction}"); time.sleep(2)
            chunk_summaries.append(summary if summary and not summary.startswith("Error:") else f"[AI error or no content for chunk {i + 1} of '{base_context}']")
        if not chunk_summaries: return f"No summaries generated from chunks for '{base_context}'.", text_len
        concatenated_summaries = "\n\n---\n\n".join(chunk_summaries)
        logger.info(f"Concatenated chunk summaries length for '{base_context}': {len(concatenated_summaries)} chars.")
        if not concatenated_summaries.strip() or all("[AI error" in s for s in chunk_summaries): return f"Failed to generate summaries for any chunk of '{base_context}'.", text_len
        if len(concatenated_summaries) > SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS:
            logger.info(f"Concatenated summaries for '{base_context}' too long. Performing a final 'summary of summaries' pass.")
            final_summary = self.gemini.summarize_text_with_context(concatenated_summaries, f"The following are collated summaries from different parts of the '{base_context}' section for {company_name_ticker_prompt}.", f"Synthesize these individual chunk summaries into a single, cohesive overview of the '{base_context}', maintaining factual accuracy and addressing the original goal: {section_specific_instruction}."); time.sleep(2)
            return (final_summary if final_summary and not final_summary.startswith("Error:") else f"AI error in final summary pass for '{base_context}'."), text_len
        else: return concatenated_summaries, text_len

    def _fetch_and_summarize_10k(self):
        logger.info(f"Fetching and attempting to summarize latest 10-K for {self.ticker}")
        summary_results = {"qualitative_sources_summary": {}}
        if not self.stock_db_entry or not self.stock_db_entry.cik: logger.warning(f"No CIK for {self.ticker}. Cannot fetch 10-K."); return summary_results
        filing_url = self.sec_edgar.get_filing_document_url(self.stock_db_entry.cik, "10-K"); time.sleep(0.5)
        if not filing_url: logger.info(f"No recent 10-K found for {self.ticker}, trying 10-K/A."); filing_url = self.sec_edgar.get_filing_document_url(self.stock_db_entry.cik, "10-K/A"); time.sleep(0.5)
        if not filing_url: logger.warning(f"No 10-K or 10-K/A URL found for {self.ticker} (CIK: {self.stock_db_entry.cik})"); return summary_results
        summary_results["qualitative_sources_summary"]["10k_filing_url_used"] = filing_url
        text_content = self.sec_edgar.get_filing_text(filing_url)
        if not text_content: logger.warning(f"Failed to fetch/load 10-K text from {filing_url}"); return summary_results
        logger.info(f"Fetched 10-K text (length: {len(text_content)}) for {self.ticker}. Extracting and summarizing sections.")
        sections = extract_S1_text_sections(text_content, TEN_K_KEY_SECTIONS)
        company_name_for_prompt = self.stock_db_entry.company_name or self.ticker
        section_details = {"business": ("Business (Item 1)", "Summarize the company's core business operations, primary products/services, revenue generation model, key customer segments, and primary markets. Highlight any recent strategic shifts mentioned."), "risk_factors": ("Risk Factors (Item 1A)", "Identify and summarize the 3-5 most significant and company-specific risk factors disclosed. Focus on operational and strategic risks rather than generic market risks. Briefly explain the potential impact of each."), "mda": ("Management's Discussion and Analysis (Item 7)", "Summarize key insights into financial performance drivers (revenue, costs, profitability), financial condition (liquidity, capital resources), and management's outlook or significant focus areas. Note any discussion on margin pressures or segment performance changes.")}
        for section_key, (prompt_section_name, specific_instruction) in section_details.items():
            section_text = sections.get(section_key)
            if not section_text:
                logger.warning(f"Section '{prompt_section_name}' not found in 10-K for {self.ticker}.")
                summary_results[f"{section_key}_summary"] = "Section not found in 10-K document."; summary_results["qualitative_sources_summary"][f"{section_key}_10k_source_length"] = 0; continue
            summary, source_len = self._summarize_text_chunked(section_text, prompt_section_name, specific_instruction, f"{company_name_for_prompt} ({self.ticker})")
            summary_results[f"{section_key}_summary"] = summary; summary_results["qualitative_sources_summary"][f"{section_key}_10k_source_length"] = source_len
            logger.info(f"Summary for '{prompt_section_name}' (source length {source_len}): {summary[:150].replace(chr(10), ' ')}...")
        biz_summary_str = summary_results.get("business_summary", ""); mda_summary_str = summary_results.get("mda_summary", ""); risk_summary_str = summary_results.get("risk_factors_summary", "")
        if biz_summary_str.startswith(("Section not found", "AI summary error")): biz_summary_str = ""
        if mda_summary_str.startswith(("Section not found", "AI summary error")): mda_summary_str = ""
        if risk_summary_str.startswith(("Section not found", "AI summary error")): risk_summary_str = ""
        moat_input_text = (f"Business Summary:\n{biz_summary_str}\n\nRisk Factors Summary:\n{risk_summary_str}").strip()
        if moat_input_text and (biz_summary_str or risk_summary_str):
            moat_prompt = (f"Analyze the primary economic moats (e.g., brand strength, network effects, switching costs, intangible assets like patents/IP, cost advantages from scale/process) for {company_name_for_prompt} ({self.ticker}), based on the following summaries from its 10-K:\n\n{moat_input_text}\n\nProvide a concise analysis of its key economic moats. For each identified moat, briefly explain the evidence from the text and assess its perceived strength (e.g., Very Strong, Strong, Moderate, Weak). If certain moats are not strongly evident, state that.")
            moat_summary = self.gemini.generate_text(moat_prompt); time.sleep(3)
            summary_results["economic_moat_summary"] = moat_summary if moat_summary and not moat_summary.startswith("Error:") else "AI analysis for economic moat failed or no input."
        else: summary_results["economic_moat_summary"] = "Insufficient input from 10-K summaries for economic moat analysis."
        industry_context_text = (f"Company: {company_name_for_prompt} ({self.ticker})\nIndustry: {self.stock_db_entry.industry or 'Not Specified'}\nSector: {self.stock_db_entry.sector or 'Not Specified'}\n\nBusiness Summary (from 10-K):\n{biz_summary_str}\n\nMD&A Highlights (from 10-K):\n{mda_summary_str}").strip()
        if biz_summary_str:
            industry_prompt = (f"Based on the provided information for {company_name_for_prompt} ({self.ticker}):\n\n{industry_context_text}\n\nAnalyze key industry trends relevant to this company. Discuss significant opportunities and challenges within this industry context. How does the company appear to be positioned to capitalize on opportunities and mitigate challenges, based on its business summary and MD&A highlights? Be specific and use information from the text.")
            industry_summary = self.gemini.generate_text(industry_prompt); time.sleep(3)
            summary_results["industry_trends_summary"] = industry_summary if industry_summary and not industry_summary.startswith("Error:") else "AI analysis for industry trends failed or no input."
        else: summary_results["industry_trends_summary"] = "Insufficient input from 10-K (Business Summary missing) for industry analysis."
        if "mda_summary" in summary_results:
            summary_results["management_assessment_summary"] = summary_results.pop("mda_summary")
            if "mda_10k_source_length" in summary_results["qualitative_sources_summary"]: summary_results["qualitative_sources_summary"]["management_assessment_10k_source_length"] = summary_results["qualitative_sources_summary"].pop("mda_10k_source_length")
        logger.info(f"10-K qualitative summaries and AI interpretations generated for {self.ticker}.")
        self._financial_data_cache['10k_summaries'] = summary_results
        return summary_results

    def _fetch_and_analyze_competitors(self):
        logger.info(f"Fetching and analyzing competitor data for {self.ticker}...")
        competitor_analysis_summary = "Competitor analysis not performed or failed."
        peers_data_finnhub = self.finnhub.get_company_peers(self.ticker); time.sleep(1.5)
        if not peers_data_finnhub or not isinstance(peers_data_finnhub, list) or not peers_data_finnhub[0]:
            logger.warning(f"No direct peer data found from Finnhub for {self.ticker}.")
            self._financial_data_cache['competitor_analysis'] = {"summary": "No peer data found from primary source (Finnhub).", "peers_data": []}; return "No peer data found from primary source (Finnhub)."
        if isinstance(peers_data_finnhub[0], list): peers_data_finnhub = peers_data_finnhub[0]
        peer_tickers = [p for p in peers_data_finnhub if p and p.upper() != self.ticker.upper()][:MAX_COMPETITORS_TO_ANALYZE]
        if not peer_tickers:
            logger.info(f"No distinct competitor tickers found after filtering for {self.ticker}.")
            self._financial_data_cache['competitor_analysis'] = {"summary": "No distinct competitor tickers identified.", "peers_data": []}; return "No distinct competitor tickers identified."
        logger.info(f"Identified peers for {self.ticker}: {peer_tickers}. Fetching basic data for comparison.")
        peer_details_list = []
        for peer_ticker in peer_tickers:
            try:
                logger.debug(f"Fetching basic data for peer: {peer_ticker}")
                peer_profile_fmp_list = self.fmp.get_company_profile(peer_ticker); time.sleep(1.5)
                peer_profile_fmp = peer_profile_fmp_list[0] if peer_profile_fmp_list and isinstance(peer_profile_fmp_list, list) and peer_profile_fmp_list[0] else {}
                peer_metrics_fmp_list = self.fmp.get_key_metrics(peer_ticker, period="annual", limit=1); time.sleep(1.5)
                peer_metrics_fmp = peer_metrics_fmp_list[0] if peer_metrics_fmp_list and isinstance(peer_metrics_fmp_list, list) and peer_metrics_fmp_list[0] else {}
                peer_fh_basics = {}
                if not peer_metrics_fmp.get("peRatio") or not peer_metrics_fmp.get("priceSalesRatio"):
                    peer_fh_basics_data = self.finnhub.get_basic_financials(peer_ticker); time.sleep(1.5)
                    peer_fh_basics = peer_fh_basics_data.get("metric", {}) if peer_fh_basics_data else {}
                peer_name = peer_profile_fmp.get("companyName", peer_ticker); market_cap = safe_get_float(peer_profile_fmp, "mktCap")
                pe_ratio = safe_get_float(peer_metrics_fmp, "peRatio") or safe_get_float(peer_fh_basics, "peTTM")
                ps_ratio = safe_get_float(peer_metrics_fmp, "priceSalesRatio") or safe_get_float(peer_fh_basics, "psTTM")
                peer_info = {"ticker": peer_ticker, "name": peer_name, "market_cap": market_cap, "pe_ratio": pe_ratio, "ps_ratio": ps_ratio}
                if peer_name != peer_ticker or market_cap or pe_ratio or ps_ratio: peer_details_list.append(peer_info)
            except Exception as e: logger.warning(f"Error fetching data for peer {peer_ticker}: {e}", exc_info=True)
            if len(peer_details_list) >= MAX_COMPETITORS_TO_ANALYZE: break
        if not peer_details_list:
            competitor_analysis_summary = "Could not fetch sufficient data for identified competitors."
            self._financial_data_cache['competitor_analysis'] = {"summary": competitor_analysis_summary, "peers_data": []}; return competitor_analysis_summary
        company_name_for_prompt = self.stock_db_entry.company_name or self.ticker
        k_summaries = self._financial_data_cache.get('10k_summaries', {}); biz_summary_10k = k_summaries.get('business_summary', 'N/A')
        if biz_summary_10k.startswith("Section not found") or biz_summary_10k.startswith("AI summary error"): biz_summary_10k = "Business summary from 10-K not available or failed."
        prompt_context = (f"Company being analyzed: {company_name_for_prompt} ({self.ticker}).\nIts 10-K Business Summary: {biz_summary_10k}\n\nIdentified Competitors and their basic data:\n")
        for peer in peer_details_list:
            mc_str = f"{peer['market_cap']:,.0f}" if peer['market_cap'] else "N/A"; pe_str = f"{peer['pe_ratio']:.2f}" if peer['pe_ratio'] is not None else "N/A"; ps_str = f"{peer['ps_ratio']:.2f}" if peer['ps_ratio'] is not None else "N/A"
            prompt_context += f"- {peer['name']} ({peer['ticker']}): Market Cap: {mc_str}, P/E: {pe_str}, P/S: {ps_str}\n"
        comp_prompt = (f"{prompt_context}\n\nInstruction: Based on the business summary of {company_name_for_prompt} and the list of its competitors with their financial metrics, provide a concise analysis of the competitive landscape. Discuss {company_name_for_prompt}'s market positioning relative to these competitors. Highlight any key differences in scale (market cap) or valuation (P/E, P/S) that stand out. Address the intensity of competition. Do not invent information not present. If competitor data is sparse, acknowledge that. This summary should complement, not merely repeat, the 10-K business description.")
        comp_summary_ai = self.gemini.generate_text(comp_prompt); time.sleep(3)
        if comp_summary_ai and not comp_summary_ai.startswith("Error:"): competitor_analysis_summary = comp_summary_ai
        else: competitor_analysis_summary = "AI synthesis of competitor data failed. Basic peer data might be available in snapshot."; self.data_quality_warnings.append("Competitor analysis AI synthesis failed.")
        self._financial_data_cache['competitor_analysis'] = {"summary": competitor_analysis_summary, "peers_data": peer_details_list}
        logger.info(f"Competitor analysis summary generated for {self.ticker}.")
        return competitor_analysis_summary

    def _parse_ai_investment_thesis_response(self, ai_response_text):
        parsed_data = {"investment_thesis_full": "AI response not fully processed or 'Investment Thesis:' section missing.", "investment_decision": "Review AI Output", "strategy_type": "Not Specified by AI", "confidence_level": "Not Specified by AI", "reasoning": "AI response not fully processed or 'Key Reasoning Points:' section missing."}
        if not ai_response_text or ai_response_text.startswith("Error:"):
            error_message = ai_response_text if ai_response_text else "Error: Empty response from AI for thesis."
            parsed_data["investment_thesis_full"] = error_message; parsed_data["reasoning"] = error_message; parsed_data["investment_decision"] = "AI Error"; parsed_data["strategy_type"] = "AI Error"; parsed_data["confidence_level"] = "AI Error"; return parsed_data
        text_content = ai_response_text.replace('\r\n', '\n').strip()
        patterns = {"investment_thesis_full": re.compile(r"^\s*Investment Thesis:\s*\n?(.*?)(?=\n\s*(?:Investment Decision:|Strategy Type:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)", re.I | re.M | re.S), "investment_decision": re.compile(r"^\s*Investment Decision:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Strategy Type:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)", re.I | re.M | re.S), "strategy_type": re.compile(r"^\s*Strategy Type:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)", re.I | re.M | re.S), "confidence_level": re.compile(r"^\s*Confidence Level:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Strategy Type:|Key Reasoning Points:)|^\s*$|\Z)", re.I | re.M | re.S), "reasoning": re.compile(r"^\s*Key Reasoning Points:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Strategy Type:|Confidence Level:)|^\s*$|\Z)", re.I | re.M | re.S)}
        found_any_section = False
        for key, pattern in patterns.items():
            match = pattern.search(text_content)
            if match:
                content = match.group(1).strip()
                if content: parsed_data[key] = content.split('\n')[0].strip() if key in ["investment_decision", "strategy_type", "confidence_level"] else content; found_any_section = True
                else: parsed_data[key] = f"'{key.replace('_', ' ').title()}:' section found but content empty."
        if not found_any_section and not ai_response_text.startswith("Error:"):
            logger.warning(f"Could not parse distinct sections from AI thesis response for {self.ticker}. Full response will be in 'investment_thesis_full'."); parsed_data["investment_thesis_full"] = text_content
        return parsed_data

    def _determine_investment_thesis(self, warn=None):
        logger.info(f"Synthesizing investment thesis for {self.ticker}...")
        metrics, qual_summaries, dcf_results, profile, competitor_analysis = self._financial_data_cache.get('calculated_metrics', {}), self._financial_data_cache.get('10k_summaries', {}), self._financial_data_cache.get('dcf_results', {}), self._financial_data_cache.get('profile_fmp', {}), self._financial_data_cache.get('competitor_analysis', {}).get("summary", "N/A")
        company_name, industry, sector = self.stock_db_entry.company_name or self.ticker, self.stock_db_entry.industry or "N/A", self.stock_db_entry.sector or "N/A"
        prompt = f"Company: {company_name} ({self.ticker})\nIndustry: {industry}, Sector: {sector}\n\nKey Financial Metrics & Data:\n"
        metrics_for_prompt = {"P/E Ratio": metrics.get("pe_ratio"), "P/B Ratio": metrics.get("pb_ratio"), "P/S Ratio": metrics.get("ps_ratio"), "Dividend Yield": metrics.get("dividend_yield"), "ROE": metrics.get("roe"), "ROIC": metrics.get("roic"), "Debt-to-Equity": metrics.get("debt_to_equity"), "Debt-to-EBITDA": metrics.get("debt_to_ebitda"), "Revenue Growth YoY": metrics.get("revenue_growth_yoy"), "Revenue Growth QoQ": metrics.get("revenue_growth_qoq"), f"Latest Quarterly Revenue (Source: {metrics.get('key_metrics_snapshot', {}).get('q_revenue_source', 'N/A')})": metrics.get('key_metrics_snapshot', {}).get('latest_q_revenue'), "EPS Growth YoY": metrics.get("eps_growth_yoy"), "Net Profit Margin": metrics.get("net_profit_margin"), "Operating Profit Margin": metrics.get("operating_profit_margin"), "Free Cash Flow Yield": metrics.get("free_cash_flow_yield"), "FCF Trend (3yr)": metrics.get("free_cash_flow_trend"), "Retained Earnings Trend (3yr)": metrics.get("retained_earnings_trend")}
        for name, val in metrics_for_prompt.items():
            if val is not None: prompt += f"- {name}: {f'{val:.2%}' if isinstance(val, float) and any(kw in name.lower() for kw in ['yield', 'growth', 'margin', 'roe', 'roic']) else (f'{val:,.0f}' if 'revenue' in name.lower() and 'growth' not in name.lower() else (f'{val:.2f}' if isinstance(val, float) else str(val)))}\n"
        current_stock_price = safe_get_float(profile, "price"); dcf_iv = dcf_results.get("dcf_intrinsic_value"); dcf_upside = dcf_results.get("dcf_upside_percentage")
        if current_stock_price is not None: prompt += f"- Current Stock Price: {current_stock_price:.2f}\n"
        if dcf_iv is not None: prompt += f"- DCF Intrinsic Value/Share (Base Case): {dcf_iv:.2f}\n"
        if dcf_upside is not None: prompt += f"- DCF Upside/Downside (Base Case): {dcf_upside:.2%}\n"
        if dcf_results.get("dcf_assumptions", {}).get("sensitivity_analysis"):
            prompt += "- DCF Sensitivity Highlights:\n"; [prompt := prompt + f"  - {s['scenario']}: IV {s['intrinsic_value']:.2f} (Upside: {s['upside']:.2% if s['upside'] is not None else 'N/A'})\n" for s in dcf_results["dcf_assumptions"]["sensitivity_analysis"][:2]]
        prompt += "\nQualitative Summaries (from 10-K & AI analysis):\n"
        qual_for_prompt = {"Business Model": qual_summaries.get("business_summary"), "Economic Moat": qual_summaries.get("economic_moat_summary"), "Industry Trends & Positioning": qual_summaries.get("industry_trends_summary"), "Competitive Landscape": competitor_analysis, "Management Discussion Highlights (MD&A)": qual_summaries.get("management_assessment_summary"), "Key Risk Factors (from 10-K)": qual_summaries.get("risk_factors_summary")}
        for name, text_val in qual_for_prompt.items():
            if text_val and isinstance(text_val, str) and not text_val.startswith(("AI analysis", "Section not found", "Insufficient input")): prompt += f"- {name}:\n{text_val[:500].replace('...', '').strip()}...\n\n"
            elif text_val: prompt += f"- {name}: {text_val}\n\n"
        if self.data_quality_warnings: prompt += "IMPORTANT DATA QUALITY CONSIDERATIONS:\n"; [prompt := prompt + f"- WARNING {i+1}: {w}\n" for i,w in enumerate(self.data_quality_warnings)]; prompt += "Acknowledge these warnings in your risk assessment or confidence level.\n\n"
        prompt += ("Instructions for AI: Based on ALL the above information (quantitative, qualitative, DCF, competitor data, and data quality warnings), provide a detailed financial analysis and investment thesis. Structure your response *EXACTLY* as follows, using these specific headings on separate lines:\n\nInvestment Thesis:\n[Comprehensive thesis (2-4 paragraphs) synthesizing all data. Discuss positives, negatives, outlook. If revenue growth is stagnant/negative but EPS growth is positive, explain the drivers (e.g., buybacks, margin expansion) and sustainability. Address any points on margin pressures (e.g., in DTC if mentioned in MD&A) or changes in segment profitability.]\n\nInvestment Decision:\n[Choose ONE: Strong Buy, Buy, Hold, Monitor, Reduce, Sell, Avoid. Base this on the overall analysis.]\n\nStrategy Type:\n[Choose ONE that best fits: Value, GARP (Growth At a Reasonable Price), Growth, Income, Speculative, Special Situation, Turnaround.]\n\nConfidence Level:\n[Choose ONE: High, Medium, Low. This reflects confidence in YOUR analysis and decision, considering data quality and completeness.]\n\nKey Reasoning Points:\n[3-7 bullet points. Each point should be a concise summary of a key factor supporting your decision. Cover: Valuation (DCF, comparables if any), Financial Health & Profitability, Growth Prospects (Revenue & EPS), Economic Moat, Competitive Position, Key Risks (including data quality issues if significant), Management & Strategy (if inferable).]\n")
        ai_response_text = self.gemini.generate_text(prompt); parsed_thesis_data = self._parse_ai_investment_thesis_response(ai_response_text)
        if any("CRITICAL:" in warn for warn in self.data_quality_warnings) or any("DATA QUALITY WARNING:" in warn for warn in self.data_quality_warnings and "revenue" in warn):
            if parsed_thesis_data.get("confidence_level", "").lower() == "high": logger.warning(f"Downgrading AI confidence from High to Medium for {self.ticker} due to critical data quality warnings."); parsed_thesis_data["confidence_level"] = "Medium"
            elif parsed_thesis_data.get("confidence_level", "").lower() == "medium": logger.warning(f"Downgrading AI confidence from Medium to Low for {self.ticker} due to critical data quality warnings."); parsed_thesis_data["confidence_level"] = "Low"
        logger.info(f"Generated thesis for {self.ticker}. Decision: {parsed_thesis_data.get('investment_decision')}, Strategy: {parsed_thesis_data.get('strategy_type')}, Confidence: {parsed_thesis_data.get('confidence_level')}")
        return parsed_thesis_data

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_data_for_db = {}
        try:
            if not self.stock_db_entry: logger.error(f"Stock DB entry for {self.ticker} not initialized properly. Aborting analysis."); return None
            self._ensure_stock_db_entry_is_bound()
            self._fetch_financial_statements(); self._fetch_key_metrics_and_profile_data()
            final_data_for_db.update(self._calculate_derived_metrics()); final_data_for_db.update(self._perform_dcf_analysis())
            qual_summaries = self._fetch_and_summarize_10k(); final_data_for_db.update(qual_summaries)
            final_data_for_db["competitive_landscape_summary"] = self._fetch_and_analyze_competitors()
            final_data_for_db.update(self._determine_investment_thesis())
            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id, analysis_date=datetime.now(timezone.utc))
            model_fields = [c.key for c in StockAnalysis.__table__.columns if c.key not in ['id', 'stock_id', 'analysis_date']]
            for field_name in model_fields:
                if field_name in final_data_for_db:
                    value_to_set = final_data_for_db[field_name]; target_column_type = getattr(StockAnalysis, field_name).type.python_type
                    if target_column_type == float:
                        if isinstance(value_to_set, str): 
                            try: 
                                value_to_set = float(value_to_set)
                            except ValueError: value_to_set = None
                        if isinstance(value_to_set, float) and (math.isnan(value_to_set) or math.isinf(value_to_set)): value_to_set = None
                    elif target_column_type == dict and not isinstance(value_to_set, dict): value_to_set = None
                    elif target_column_type == str and not isinstance(value_to_set, str): value_to_set = str(value_to_set) if value_to_set is not None else None
                    setattr(analysis_entry, field_name, value_to_set)
            self.db_session.add(analysis_entry); self.stock_db_entry.last_analysis_date = analysis_entry.analysis_date
            self.db_session.commit(); logger.info(f"Successfully analyzed and saved stock data: {self.ticker} (Analysis ID: {analysis_entry.id})")
            return analysis_entry
        except RuntimeError as rt_err: logger.critical(f"Runtime error during full analysis for {self.ticker}: {rt_err}", exc_info=True); return None
        except Exception as e:
            logger.error(f"CRITICAL error in full analysis pipeline for {self.ticker}: {e}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try: self.db_session.rollback(); logger.info(f"Rolled back DB transaction for {self.ticker} due to error.")
                except Exception as e_rb: logger.error(f"Rollback error for {self.ticker}: {e_rb}")
            return None
        finally: self._close_session_if_active()

    def _ensure_stock_db_entry_is_bound(self):
        if not self.stock_db_entry: raise RuntimeError(f"Stock entry for {self.ticker} is None during binding check. Prior initialization failure.")
        if not self.db_session.is_active:
            logger.warning(f"DB Session for {self.ticker} was INACTIVE before operation. Re-establishing.")
            self._close_session_if_active(); self.db_session = next(get_db_session())
            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock: raise RuntimeError(f"Failed to re-fetch stock {self.ticker} for new session after inactivity. Critical state.")
            self.stock_db_entry = re_fetched_stock; logger.info(f"Re-fetched and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new active session.")
            return
        instance_state = sa_inspect(self.stock_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            obj_id_log = self.stock_db_entry.id if instance_state.has_identity else 'Transient/No ID'
            logger.warning(f"Stock {self.ticker} (ID: {obj_id_log}) DETACHED or bound to DIFFERENT session. Attempting to merge.")
            try:
                self.stock_db_entry = self.db_session.merge(self.stock_db_entry); self.db_session.flush()
                logger.info(f"Successfully merged stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session.")
            except Exception as e_merge:
                logger.error(f"Failed to merge stock {self.ticker} into session: {e_merge}. Re-fetching as a fallback.", exc_info=True)
                re_fetched_from_db_after_merge_fail = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
                if re_fetched_from_db_after_merge_fail: self.stock_db_entry = re_fetched_from_db_after_merge_fail; logger.info(f"Successfully re-fetched stock {self.ticker} (ID: {self.stock_db_entry.id}) after merge failure.")
                else: raise RuntimeError(f"Failed to bind stock {self.ticker} to session after merge failure and re-fetch attempt. Analysis cannot proceed.")

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
                logger.info(f"Analysis for {analysis_result_obj.stock.ticker} completed. Decision: {analysis_result_obj.investment_decision}, Strategy: {analysis_result_obj.strategy_type}, Confidence: {analysis_result_obj.confidence_level}")
                if analysis_result_obj.dcf_intrinsic_value is not None: logger.info(f"  DCF Value: {analysis_result_obj.dcf_intrinsic_value:.2f}, Upside: {analysis_result_obj.dcf_upside_percentage:.2% if analysis_result_obj.dcf_upside_percentage is not None else 'N/A'}")
                logger.info(f"  QoQ Revenue Growth: {analysis_result_obj.revenue_growth_qoq if analysis_result_obj.revenue_growth_qoq is not None else 'N/A'} (Source: {analysis_result_obj.key_metrics_snapshot.get('q_revenue_source', 'N/A') if analysis_result_obj.key_metrics_snapshot else 'N/A'}, Value: {analysis_result_obj.key_metrics_snapshot.get('latest_q_revenue', 'N/A') if analysis_result_obj.key_metrics_snapshot else 'N/A'})")
                logger.info(f"  P/E: {analysis_result_obj.pe_ratio}, P/B: {analysis_result_obj.pb_ratio}, ROE: {analysis_result_obj.roe}")
                if analysis_result_obj.qualitative_sources_summary: logger.info(f"  10K URL used: {analysis_result_obj.qualitative_sources_summary.get('10k_filing_url_used', 'N/A')}")
                if analysis_result_obj.competitive_landscape_summary: logger.info(f"  Competitive Landscape Summary: {analysis_result_obj.competitive_landscape_summary[:200]}...")
            else: logger.error(f"Stock analysis pipeline FAILED or returned invalid result for {ticker_symbol}.")
        except RuntimeError as rt_err: logger.error(f"Could not run StockAnalyzer for {ticker_symbol} due to critical init error: {rt_err}")
        except Exception as e_main_loop: logger.error(f"Unhandled error analyzing {ticker_symbol} in __main__ loop: {e_main_loop}", exc_info=True)
        finally: logger.info(f"--- Finished processing {ticker_symbol} ---"); time.sleep(20)