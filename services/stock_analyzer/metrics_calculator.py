# services/stock_analyzer/metrics_calculator.py
import math
import json
from core.logging_setup import logger
from .helpers import (
    safe_get_float, calculate_cagr, calculate_growth,
    get_value_from_statement_list, get_fmp_value,
    get_alphavantage_value, get_finnhub_concept_value
)
from core.config import (
    Q_REVENUE_SANITY_CHECK_DEVIATION_THRESHOLD,
    PRIORITY_REVENUE_SOURCES
)


def _calculate_valuation_ratios(latest_km_q_fmp, latest_km_a_fmp, basic_fin_fh_metric, overview_av):
    ratios = {}
    ratios["pe_ratio"] = safe_get_float(latest_km_q_fmp, "peRatioTTM") or \
                         safe_get_float(latest_km_a_fmp, "peRatio") or \
                         safe_get_float(basic_fin_fh_metric, "peTTM")
    ratios["pb_ratio"] = safe_get_float(latest_km_q_fmp, "priceToBookRatioTTM") or \
                         safe_get_float(latest_km_a_fmp, "pbRatio") or \
                         safe_get_float(basic_fin_fh_metric, "pbAnnual")
    ratios["ps_ratio"] = safe_get_float(latest_km_q_fmp, "priceToSalesRatioTTM") or \
                         safe_get_float(latest_km_a_fmp, "priceSalesRatio") or \
                         safe_get_float(basic_fin_fh_metric, "psTTM")

    ratios["ev_to_sales"] = safe_get_float(latest_km_q_fmp, "enterpriseValueOverRevenueTTM") or \
                            safe_get_float(latest_km_a_fmp, "enterpriseValueOverRevenue") or \
                            safe_get_float(overview_av, "EVToRevenue")

    ratios["ev_to_ebitda"] = safe_get_float(latest_km_q_fmp, "evToEbitdaTTM") or \
                             safe_get_float(latest_km_a_fmp, "evToEbitda") or \
                             safe_get_float(overview_av, "EVToEBITDA")

    div_yield_fmp_q = safe_get_float(latest_km_q_fmp, "dividendYieldTTM")
    div_yield_fmp_a = safe_get_float(latest_km_a_fmp, "dividendYield")
    div_yield_fh_raw = safe_get_float(basic_fin_fh_metric, "dividendYieldAnnual")
    div_yield_fh = div_yield_fh_raw / 100.0 if div_yield_fh_raw is not None else None
    div_yield_av = safe_get_float(overview_av, "DividendYield")

    ratios["dividend_yield"] = div_yield_fmp_q if div_yield_fmp_q is not None else \
        (div_yield_fmp_a if div_yield_fmp_a is not None else \
             (div_yield_fh if div_yield_fh is not None else div_yield_av))
    return ratios


def _calculate_profitability_metrics(analyzer_instance, income_annual_fmp, balance_annual_fmp, latest_km_a_fmp,
                                     overview_av):
    metrics = {}
    ticker = analyzer_instance.ticker

    # From FMP Annual Income Statement
    latest_ia_fmp = income_annual_fmp[0] if income_annual_fmp else {}

    metrics["eps"] = safe_get_float(latest_ia_fmp, "eps") or \
                     safe_get_float(latest_km_a_fmp, "eps") or \
                     safe_get_float(overview_av, "EPS")

    metrics["net_profit_margin"] = safe_get_float(latest_ia_fmp, "netProfitMargin") or \
                                   safe_get_float(overview_av, "ProfitMargin")

    # Gross Profit Margin: FMP or (AV GrossProfitTTM / AV RevenueTTM)
    fmp_gross_margin = safe_get_float(latest_ia_fmp, "grossProfitMargin")
    if fmp_gross_margin is not None:
        metrics["gross_profit_margin"] = fmp_gross_margin
    else:
        av_gross_profit_ttm = safe_get_float(overview_av, "GrossProfitTTM")
        av_revenue_ttm = safe_get_float(overview_av, "RevenueTTM")
        if av_gross_profit_ttm is not None and av_revenue_ttm is not None and av_revenue_ttm != 0:
            metrics["gross_profit_margin"] = av_gross_profit_ttm / av_revenue_ttm
        else:
            metrics["gross_profit_margin"] = None

    metrics["operating_profit_margin"] = safe_get_float(latest_ia_fmp,
                                                        "operatingIncomeRatio")  # FMP specific for op margin
    # AlphaVantage overview_av also has "OperatingMarginTTM"
    if metrics["operating_profit_margin"] is None:
        metrics["operating_profit_margin"] = safe_get_float(overview_av, "OperatingMarginTTM")

    ebit_fmp = safe_get_float(latest_ia_fmp, "operatingIncome")
    interest_expense_fmp = safe_get_float(latest_ia_fmp, "interestExpense")
    if ebit_fmp is not None and interest_expense_fmp is not None and abs(interest_expense_fmp) > 1e-6:
        metrics["interest_coverage_ratio"] = ebit_fmp / abs(interest_expense_fmp)
    else:
        metrics["interest_coverage_ratio"] = None

    # ROE, ROA from various sources
    # Priority: FMP calculations > AlphaVantage direct > Finnhub direct
    # FMP calculation parts:
    total_equity_fmp = get_value_from_statement_list(balance_annual_fmp, "totalStockholdersEquity", 0)
    total_assets_fmp = get_value_from_statement_list(balance_annual_fmp, "totalAssets", 0)
    latest_net_income_fmp = get_value_from_statement_list(income_annual_fmp, "netIncome", 0)

    roe_fmp_calc = None
    if total_equity_fmp and total_equity_fmp != 0 and latest_net_income_fmp is not None:
        roe_fmp_calc = latest_net_income_fmp / total_equity_fmp

    roa_fmp_calc = None
    if total_assets_fmp and total_assets_fmp != 0 and latest_net_income_fmp is not None:
        roa_fmp_calc = latest_net_income_fmp / total_assets_fmp

    metrics["roe"] = roe_fmp_calc if roe_fmp_calc is not None else safe_get_float(overview_av, "ReturnOnEquityTTM")
    metrics["roa"] = roa_fmp_calc if roa_fmp_calc is not None else safe_get_float(overview_av, "ReturnOnAssetsTTM")

    # ROIC Calculation (Primarily FMP based due to detail needed)
    ebit_roic_fmp = get_value_from_statement_list(income_annual_fmp, "operatingIncome", 0)
    income_tax_expense_roic_fmp = get_value_from_statement_list(income_annual_fmp, "incomeTaxExpense", 0)
    income_before_tax_roic_fmp = get_value_from_statement_list(income_annual_fmp, "incomeBeforeTax", 0)

    effective_tax_rate = 0.21  # Default
    if income_tax_expense_roic_fmp is not None and income_before_tax_roic_fmp is not None and income_before_tax_roic_fmp != 0:
        calculated_tax_rate = income_tax_expense_roic_fmp / income_before_tax_roic_fmp
        if 0 <= calculated_tax_rate <= 0.50:
            effective_tax_rate = calculated_tax_rate
        else:
            logger.debug(
                f"Calculated tax rate {calculated_tax_rate:.2%} for {ticker} is unusual. Using default {effective_tax_rate:.2%}.")

    nopat_fmp = ebit_roic_fmp * (1 - effective_tax_rate) if ebit_roic_fmp is not None else None

    total_debt_roic_fmp = get_value_from_statement_list(balance_annual_fmp, "totalDebt", 0)
    cash_equivalents_roic_fmp = get_value_from_statement_list(balance_annual_fmp, "cashAndCashEquivalents", 0) or 0

    if total_debt_roic_fmp is not None and total_equity_fmp is not None:  # total_equity_fmp defined above
        invested_capital_fmp = total_debt_roic_fmp + total_equity_fmp - cash_equivalents_roic_fmp
        if nopat_fmp is not None and invested_capital_fmp is not None and invested_capital_fmp != 0:
            metrics["roic"] = nopat_fmp / invested_capital_fmp
        else:
            metrics["roic"] = None  # FMP ROIC calc failed
    else:
        metrics["roic"] = None  # FMP ROIC calc failed

    # AlphaVantage overview_av does not have ROIC directly.
    # Finnhub basic_financials might have roicAnnual, roicTTM under 'metric'
    if metrics["roic"] is None:
        fh_metrics = analyzer_instance._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {})
        metrics["roic"] = safe_get_float(fh_metrics, "roicTTM") or safe_get_float(fh_metrics, "roicAnnual")

    return metrics


def _calculate_financial_health_metrics(balance_annual_fmp, income_annual_fmp, latest_km_a_fmp, overview_av):
    metrics = {}
    latest_ba_fmp = balance_annual_fmp[0] if balance_annual_fmp else {}
    total_equity_fmp = safe_get_float(latest_ba_fmp, "totalStockholdersEquity")

    # Debt-to-Equity: FMP Key Metric > FMP Balance Sheet Calc > AlphaVantage Overview
    metrics["debt_to_equity"] = safe_get_float(latest_km_a_fmp, "debtToEquity")
    if metrics["debt_to_equity"] is None:
        total_debt_ba_fmp = safe_get_float(latest_ba_fmp, "totalDebt")
        if total_debt_ba_fmp is not None and total_equity_fmp and total_equity_fmp != 0:
            metrics["debt_to_equity"] = total_debt_ba_fmp / total_equity_fmp
    if metrics["debt_to_equity"] is None:
        # AlphaVantage has total debt and total equity in quarterly balance sheets, not directly in overview.
        # And DebtToEquityRatio is usually a TTM or annual metric. For now, stick to FMP.
        pass

    current_assets_fmp = safe_get_float(latest_ba_fmp, "totalCurrentAssets")
    current_liabilities_fmp = safe_get_float(latest_ba_fmp, "totalCurrentLiabilities")
    if current_assets_fmp is not None and current_liabilities_fmp is not None and current_liabilities_fmp != 0:
        metrics["current_ratio"] = current_assets_fmp / current_liabilities_fmp
    else:  # Fallback to AlphaVantage if FMP fails
        metrics["current_ratio"] = safe_get_float(overview_av, "CurrentRatio")

    cash_equivalents_fmp = safe_get_float(latest_ba_fmp, "cashAndCashEquivalents", 0)
    short_term_investments_fmp = safe_get_float(latest_ba_fmp, "shortTermInvestments", 0)
    net_receivables_fmp = safe_get_float(latest_ba_fmp, "netReceivables", 0)
    if current_liabilities_fmp is not None and current_liabilities_fmp != 0:  # Requires FMP current_liabilities
        metrics["quick_ratio"] = (
                                             cash_equivalents_fmp + short_term_investments_fmp + net_receivables_fmp) / current_liabilities_fmp
    else:  # Fallback to AlphaVantage if FMP fails
        # AlphaVantage overview_av does not directly provide quick ratio components in a simple way.
        # It might be in the full balance sheet. For now, if FMP fails, quick_ratio might be None.
        metrics["quick_ratio"] = None

    # Debt-to-EBITDA
    # Priority: FMP Key Metric > FMP Calc (Total Debt / EBITDA from Income Statement)
    latest_annual_ebitda_km_fmp = safe_get_float(latest_km_a_fmp, "ebitda")
    latest_annual_ebitda_is_fmp = get_value_from_statement_list(income_annual_fmp, "ebitda", 0)
    latest_annual_ebitda_fmp = latest_annual_ebitda_km_fmp if latest_annual_ebitda_km_fmp is not None else latest_annual_ebitda_is_fmp

    if latest_annual_ebitda_fmp and latest_annual_ebitda_fmp != 0:
        total_debt_val_fmp = get_value_from_statement_list(balance_annual_fmp, "totalDebt", 0)
        if total_debt_val_fmp is not None:
            metrics["debt_to_ebitda"] = total_debt_val_fmp / latest_annual_ebitda_fmp
        else:
            metrics["debt_to_ebitda"] = None
    else:
        metrics["debt_to_ebitda"] = None

    # AlphaVantage overview_av doesn't have DebtToEBITDA.

    return metrics


def _get_cross_validated_quarterly_revenue(analyzer_instance, statements_cache):
    ticker = analyzer_instance.ticker
    latest_q_revenue, previous_q_revenue, source_name, historical_revenues = None, None, None, []

    # Define revenue field names for each source
    revenue_fields = {
        "fmp_quarterly": "revenue",
        "alphavantage_quarterly": "totalRevenue",
        "finnhub_quarterly": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "TotalRevenues",
                              "NetSales"]  # List of possible concepts
    }

    for src_key in PRIORITY_REVENUE_SOURCES:
        try:
            if src_key == "fmp_quarterly" and statements_cache.get('fmp_income_quarterly'):
                reports = statements_cache['fmp_income_quarterly']
                if not reports: continue
                latest_val = get_fmp_value(reports, revenue_fields[src_key], 0)
                prev_val = get_fmp_value(reports, revenue_fields[src_key], 1) if len(reports) > 1 else None
                if latest_val is not None:
                    latest_q_revenue, previous_q_revenue, source_name = latest_val, prev_val, "FMP"
                    for i in range(min(len(reports), 5)):  # Get up to 5 historical points
                        h_val = get_fmp_value(reports, revenue_fields[src_key], i)
                        if h_val is not None: historical_revenues.append(h_val)
                    break
            elif src_key == "alphavantage_quarterly" and statements_cache.get('alphavantage_income_quarterly', {}).get(
                    'quarterlyReports'):
                reports = statements_cache['alphavantage_income_quarterly']['quarterlyReports']
                if not reports: continue
                latest_val = get_alphavantage_value(reports, revenue_fields[src_key], 0)
                prev_val = get_alphavantage_value(reports, revenue_fields[src_key], 1) if len(reports) > 1 else None
                if latest_val is not None:
                    latest_q_revenue, previous_q_revenue, source_name = latest_val, prev_val, "AlphaVantage"
                    for i in range(min(len(reports), 5)):
                        h_val = get_alphavantage_value(reports, revenue_fields[src_key], i)
                        if h_val is not None: historical_revenues.append(h_val)
                    break
            elif src_key == "finnhub_quarterly" and statements_cache.get('finnhub_financials_quarterly_reported',
                                                                         {}).get('data'):
                reports = statements_cache['finnhub_financials_quarterly_reported']['data']
                if not reports: continue
                latest_val = get_finnhub_concept_value(reports, 'ic', revenue_fields[src_key], 0)
                prev_val = get_finnhub_concept_value(reports, 'ic', revenue_fields[src_key], 1) if len(
                    reports) > 1 else None
                if latest_val is not None:
                    latest_q_revenue, previous_q_revenue, source_name = latest_val, prev_val, "Finnhub"
                    for i in range(min(len(reports), 5)):
                        h_val = get_finnhub_concept_value(reports, 'ic', revenue_fields[src_key], i)
                        if h_val is not None: historical_revenues.append(h_val)
                    break
        except Exception as e:
            logger.warning(f"Error processing quarterly revenue from {src_key} for {ticker}: {e}")
            continue

    avg_historical_q_revenue = None
    if historical_revenues:
        points_for_avg = [r for r in historical_revenues if r is not None and r > 0]  # Use only positive values
        # If latest_q_revenue is the first in historical_revenues, exclude it for avg calculation to compare against prior periods
        avg_base_points = points_for_avg[1:] if points_for_avg and points_for_avg[0] == latest_q_revenue and len(
            points_for_avg) > 1 else points_for_avg

        if len(avg_base_points) > 1:  # Need at least two points for a meaningful average
            avg_historical_q_revenue = sum(avg_base_points) / len(avg_base_points)
            if latest_q_revenue is not None and avg_historical_q_revenue > 0:  # Ensure avg is positive for deviation calc
                deviation = abs(latest_q_revenue - avg_historical_q_revenue) / avg_historical_q_revenue
                if deviation > Q_REVENUE_SANITY_CHECK_DEVIATION_THRESHOLD:
                    warning_msg = (
                        f"DATA QUALITY WARNING: Latest quarterly revenue ({latest_q_revenue:,.0f} from {source_name}) "
                        f"deviates by {deviation:.2%} from avg of recent historical quarters ({avg_historical_q_revenue:,.0f}). "
                        f"Review data accuracy.")
                    logger.warning(warning_msg)
                    analyzer_instance.data_quality_warnings.append(warning_msg)
        else:
            logger.info(
                f"Not enough historical quarterly revenue data (after filtering for positive values and excluding current if present) to perform sanity check for {ticker}.")
    else:
        logger.info(f"No historical quarterly revenue data found for sanity check for {ticker}.")

    if latest_q_revenue is None:
        logger.error(f"Could not determine latest quarterly revenue for {ticker} from any source.")
        analyzer_instance.data_quality_warnings.append("CRITICAL: Latest quarterly revenue could not be determined.")
    else:
        logger.info(f"Using latest quarterly revenue: {latest_q_revenue:,.0f} (Source: {source_name}) for {ticker}.")

    return latest_q_revenue, previous_q_revenue, source_name, avg_historical_q_revenue


def _calculate_growth_metrics(analyzer_instance, income_annual_fmp, statements_cache, overview_av):
    metrics = {"key_metrics_snapshot": {}}  # Initialize snapshot dict
    ticker = analyzer_instance.ticker

    # YoY Growth
    # FMP Annual Revenue
    fmp_revenue_y0 = get_value_from_statement_list(income_annual_fmp, "revenue", 0)
    fmp_revenue_y1 = get_value_from_statement_list(income_annual_fmp, "revenue", 1)

    # FMP Annual EPS
    fmp_eps_y0 = get_value_from_statement_list(income_annual_fmp, "eps", 0)
    fmp_eps_y1 = get_value_from_statement_list(income_annual_fmp, "eps", 1)

    metrics["revenue_growth_yoy"] = calculate_growth(fmp_revenue_y0, fmp_revenue_y1)
    metrics["eps_growth_yoy"] = calculate_growth(fmp_eps_y0, fmp_eps_y1)

    # AlphaVantage has "QuarterlyRevenueGrowthYOY", "QuarterlyEarningsGrowthYOY"
    # These are quarterly YoY. We are calculating annual YoY above.
    # We can add AV's TTM RevenueGrowth and EPSGrowth if available as fallbacks for YoY.
    # Overview_av has "RevenueGrowth" but it's usually for TTM or MRQ.
    # Let's stick to FMP for annual YoY growth for now due to clarity of period.

    # CAGR 3-year
    if len(income_annual_fmp) >= 3:
        metrics["revenue_growth_cagr_3yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "revenue", 0),
            get_value_from_statement_list(income_annual_fmp, "revenue", 2), 2
        )
        metrics["eps_growth_cagr_3yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "eps", 0),
            get_value_from_statement_list(income_annual_fmp, "eps", 2), 2
        )
    else:
        metrics["revenue_growth_cagr_3yr"] = None
        metrics["eps_growth_cagr_3yr"] = None

    # CAGR 5-year
    if len(income_annual_fmp) >= 5:
        metrics["revenue_growth_cagr_5yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "revenue", 0),
            get_value_from_statement_list(income_annual_fmp, "revenue", 4), 4
        )
        metrics["eps_growth_cagr_5yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "eps", 0),
            get_value_from_statement_list(income_annual_fmp, "eps", 4), 4
        )
    else:
        metrics["revenue_growth_cagr_5yr"] = None
        metrics["eps_growth_cagr_5yr"] = None

    # QoQ Revenue Growth
    latest_q_rev, prev_q_rev, rev_src_name, avg_hist_q_rev = _get_cross_validated_quarterly_revenue(analyzer_instance,
                                                                                                    statements_cache)

    if latest_q_rev is not None:
        metrics["key_metrics_snapshot"]["q_revenue_source"] = rev_src_name
        metrics["key_metrics_snapshot"]["latest_q_revenue"] = latest_q_rev
        metrics["key_metrics_snapshot"]["avg_historical_q_revenue_for_check"] = avg_hist_q_rev
        if prev_q_rev is not None:
            metrics["revenue_growth_qoq"] = calculate_growth(latest_q_rev, prev_q_rev)
        else:
            logger.info(
                f"Previous quarter revenue not available from source {rev_src_name} for {ticker}. Cannot calculate QoQ revenue growth.")
            metrics["revenue_growth_qoq"] = None
    else:  # Fallback to AlphaVantage QuarterlyRevenueGrowthYOY as a proxy if direct QoQ fails
        metrics["revenue_growth_qoq"] = safe_get_float(overview_av,
                                                       "QuarterlyRevenueGrowthYOY")  # Note: This is YOY not QOQ.
        metrics["key_metrics_snapshot"]["q_revenue_source"] = "N/A (or AV YOY as proxy)" if metrics[
                                                                                                "revenue_growth_qoq"] is None else "AlphaVantage (QuarterlyYoY as QoQ proxy)"
        metrics["key_metrics_snapshot"]["latest_q_revenue"] = None  # Can't determine specific latest Q revenue
        metrics["key_metrics_snapshot"]["avg_historical_q_revenue_for_check"] = None

    return metrics


def _calculate_cash_flow_and_trend_metrics(cashflow_annual_fmp, balance_annual_fmp, profile_fmp, overview_av):
    metrics = {}

    # FCF per Share & FCF Yield
    fcf_latest_annual_fmp = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0)

    shares_outstanding_profile_fmp = safe_get_float(profile_fmp, "sharesOutstanding")
    mkt_cap_profile_fmp = safe_get_float(profile_fmp, "mktCap")
    price_profile_fmp = safe_get_float(profile_fmp, "price")

    # Use AlphaVantage SharesOutstanding if FMP's is missing
    shares_outstanding_av = safe_get_float(overview_av, "SharesOutstanding")
    shares_outstanding = shares_outstanding_profile_fmp if shares_outstanding_profile_fmp is not None and shares_outstanding_profile_fmp > 0 else shares_outstanding_av

    # Calculate shares outstanding if direct value is missing or zero, using mktCap and price
    if (
            shares_outstanding is None or shares_outstanding == 0) and mkt_cap_profile_fmp and price_profile_fmp and price_profile_fmp != 0:
        shares_outstanding = mkt_cap_profile_fmp / price_profile_fmp

    if fcf_latest_annual_fmp is not None and shares_outstanding and shares_outstanding != 0:
        metrics["free_cash_flow_per_share"] = fcf_latest_annual_fmp / shares_outstanding
        # Use MktCap from FMP profile first, then AV overview for FCF Yield
        mkt_cap_for_yield = mkt_cap_profile_fmp if mkt_cap_profile_fmp else safe_get_float(overview_av,
                                                                                           "MarketCapitalization")
        if mkt_cap_for_yield and mkt_cap_for_yield != 0:
            metrics["free_cash_flow_yield"] = fcf_latest_annual_fmp / mkt_cap_for_yield
        else:
            metrics["free_cash_flow_yield"] = None
    else:
        metrics["free_cash_flow_per_share"] = None
        metrics["free_cash_flow_yield"] = None

    # FCF Trend (3-year simple trend from FMP annual data)
    if len(cashflow_annual_fmp) >= 3:
        fcf0 = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0)
        fcf1 = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 1)
        fcf2 = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 2)

        if all(isinstance(x, (int, float)) for x in [fcf0, fcf1, fcf2] if x is not None) and \
                all(x is not None for x in [fcf0, fcf1, fcf2]):
            if fcf0 > fcf1 > fcf2:
                metrics["free_cash_flow_trend"] = "Growing"
            elif fcf0 < fcf1 < fcf2:
                metrics["free_cash_flow_trend"] = "Declining"
            elif fcf0 > fcf1 and fcf1 < fcf2:
                metrics["free_cash_flow_trend"] = "Volatile (Dip then Rise)"
            elif fcf0 < fcf1 and fcf1 > fcf2:
                metrics["free_cash_flow_trend"] = "Volatile (Rise then Dip)"
            else:
                metrics["free_cash_flow_trend"] = "Mixed/Stable"
        else:
            metrics["free_cash_flow_trend"] = "Data Incomplete/Non-Numeric"
    else:
        metrics["free_cash_flow_trend"] = "Data N/A (<3 yrs)"

    # Retained Earnings Trend (3-year simple trend from FMP annual data)
    if len(balance_annual_fmp) >= 3:
        re0 = get_value_from_statement_list(balance_annual_fmp, "retainedEarnings", 0)
        re1 = get_value_from_statement_list(balance_annual_fmp, "retainedEarnings", 1)
        re2 = get_value_from_statement_list(balance_annual_fmp, "retainedEarnings", 2)

        if all(isinstance(x, (int, float)) for x in [re0, re1, re2] if x is not None) and \
                all(x is not None for x in [re0, re1, re2]):
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


def calculate_all_derived_metrics(analyzer_instance):
    logger.info(f"Calculating derived metrics for {analyzer_instance.ticker}...")
    all_metrics_temp = {}

    # Retrieve cached data
    statements = analyzer_instance._financial_data_cache.get('financial_statements', {})
    income_annual_fmp = statements.get('fmp_income_annual', [])
    balance_annual_fmp = statements.get('fmp_balance_annual', [])
    cashflow_annual_fmp = statements.get('fmp_cashflow_annual', [])

    key_metrics_annual_fmp = analyzer_instance._financial_data_cache.get('key_metrics_annual_fmp', [])
    key_metrics_quarterly_fmp = analyzer_instance._financial_data_cache.get('key_metrics_quarterly_fmp', [])

    basic_fin_fh_metric = analyzer_instance._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {})
    profile_fmp = analyzer_instance._financial_data_cache.get('profile_fmp', {})
    # Ensure AlphaVantage overview is available
    overview_av = analyzer_instance._financial_data_cache.get('overview_alphavantage', {})
    if not overview_av:  # If somehow not fetched by stock_analyzer's init
        logger.warning(f"AlphaVantage overview data not found in cache for {analyzer_instance.ticker}. Fetching now.")
        overview_av = analyzer_instance.alphavantage.get_company_overview(analyzer_instance.ticker)
        analyzer_instance._financial_data_cache['overview_alphavantage'] = overview_av if overview_av else {}

    latest_km_q_fmp = key_metrics_quarterly_fmp[0] if key_metrics_quarterly_fmp else {}
    latest_km_a_fmp = key_metrics_annual_fmp[0] if key_metrics_annual_fmp else {}

    all_metrics_temp.update(
        _calculate_valuation_ratios(latest_km_q_fmp, latest_km_a_fmp, basic_fin_fh_metric, overview_av))
    all_metrics_temp.update(
        _calculate_profitability_metrics(analyzer_instance, income_annual_fmp, balance_annual_fmp, latest_km_a_fmp,
                                         overview_av))
    all_metrics_temp.update(
        _calculate_financial_health_metrics(balance_annual_fmp, income_annual_fmp, latest_km_a_fmp, overview_av))

    growth_metrics_result = _calculate_growth_metrics(analyzer_instance, income_annual_fmp, statements, overview_av)
    all_metrics_temp.update(growth_metrics_result)

    all_metrics_temp.update(
        _calculate_cash_flow_and_trend_metrics(cashflow_annual_fmp, balance_annual_fmp, profile_fmp, overview_av))

    final_metrics_cleaned = {}
    key_metrics_snapshot_data = all_metrics_temp.pop("key_metrics_snapshot", {})

    for k, v in all_metrics_temp.items():
        if isinstance(v, float):
            final_metrics_cleaned[k] = v if not (math.isnan(v) or math.isinf(v)) else None
        elif v is not None:
            final_metrics_cleaned[k] = v
        else:
            final_metrics_cleaned[k] = None

    final_metrics_cleaned["key_metrics_snapshot"] = {
        sk: sv for sk, sv in key_metrics_snapshot_data.items()
        if sv is not None and not (isinstance(sv, float) and (math.isnan(sv) or math.isinf(sv)))
    }

    log_metrics_display = {k: v for k, v in final_metrics_cleaned.items() if k != "key_metrics_snapshot"}
    logger.info(
        f"Calculated metrics for {analyzer_instance.ticker}: {json.dumps(log_metrics_display, indent=2, default=str)}")
    if final_metrics_cleaned["key_metrics_snapshot"]:
        logger.info(
            f"Key metrics snapshot for {analyzer_instance.ticker}: {json.dumps(final_metrics_cleaned['key_metrics_snapshot'], indent=2, default=str)}")

    analyzer_instance._financial_data_cache['calculated_metrics'] = final_metrics_cleaned
    return final_metrics_cleaned