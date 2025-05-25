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


def _calculate_valuation_ratios(latest_km_q_fmp, latest_km_a_fmp, basic_fin_fh_metric):
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
                            safe_get_float(latest_km_a_fmp, "enterpriseValueOverRevenue")
    ratios["ev_to_ebitda"] = safe_get_float(latest_km_q_fmp, "evToEbitdaTTM") or \
                             safe_get_float(latest_km_a_fmp, "evToEbitda")

    div_yield_fmp_q = safe_get_float(latest_km_q_fmp, "dividendYieldTTM")
    div_yield_fmp_a = safe_get_float(latest_km_a_fmp, "dividendYield")
    div_yield_fh_raw = safe_get_float(basic_fin_fh_metric, "dividendYieldAnnual")
    div_yield_fh = div_yield_fh_raw / 100.0 if div_yield_fh_raw is not None else None

    ratios["dividend_yield"] = div_yield_fmp_q if div_yield_fmp_q is not None else \
        (div_yield_fmp_a if div_yield_fmp_a is not None else div_yield_fh)
    return ratios


def _calculate_profitability_metrics(analyzer_instance, income_annual_fmp, balance_annual_fmp, latest_km_a_fmp):
    metrics = {}
    ticker = analyzer_instance.ticker
    if income_annual_fmp:
        latest_ia = income_annual_fmp[0]
        metrics["eps"] = safe_get_float(latest_ia, "eps") or safe_get_float(latest_km_a_fmp, "eps")
        metrics["net_profit_margin"] = safe_get_float(latest_ia, "netProfitMargin")
        metrics["gross_profit_margin"] = safe_get_float(latest_ia, "grossProfitMargin")
        metrics["operating_profit_margin"] = safe_get_float(latest_ia, "operatingIncomeRatio")

        ebit = safe_get_float(latest_ia, "operatingIncome")
        interest_expense = safe_get_float(latest_ia, "interestExpense")
        if ebit is not None and interest_expense is not None and abs(
                interest_expense) > 1e-6:  # Avoid division by zero or tiny number
            metrics["interest_coverage_ratio"] = ebit / abs(interest_expense)

    if balance_annual_fmp and income_annual_fmp:
        total_equity = get_value_from_statement_list(balance_annual_fmp, "totalStockholdersEquity", 0)
        total_assets = get_value_from_statement_list(balance_annual_fmp, "totalAssets", 0)
        latest_net_income = get_value_from_statement_list(income_annual_fmp, "netIncome", 0)

        if total_equity and total_equity != 0 and latest_net_income is not None:
            metrics["roe"] = latest_net_income / total_equity
        if total_assets and total_assets != 0 and latest_net_income is not None:
            metrics["roa"] = latest_net_income / total_assets

        # ROIC Calculation
        ebit_roic = get_value_from_statement_list(income_annual_fmp, "operatingIncome", 0)
        income_tax_expense_roic = get_value_from_statement_list(income_annual_fmp, "incomeTaxExpense", 0)
        income_before_tax_roic = get_value_from_statement_list(income_annual_fmp, "incomeBeforeTax", 0)

        effective_tax_rate = 0.21  # Default
        if income_tax_expense_roic is not None and income_before_tax_roic is not None and income_before_tax_roic != 0:
            calculated_tax_rate = income_tax_expense_roic / income_before_tax_roic
            if 0 <= calculated_tax_rate <= 0.50:  # Reasonable range for effective tax rate
                effective_tax_rate = calculated_tax_rate
            else:
                logger.debug(
                    f"Calculated tax rate {calculated_tax_rate:.2%} for {ticker} is unusual. Using default {effective_tax_rate:.2%}.")

        nopat = ebit_roic * (1 - effective_tax_rate) if ebit_roic is not None else None

        total_debt_roic = get_value_from_statement_list(balance_annual_fmp, "totalDebt", 0)
        # Using cashAndCashEquivalents. Sometimes 'cashAndShortTermInvestments' is more appropriate, but depends on definition.
        cash_equivalents_roic = get_value_from_statement_list(balance_annual_fmp, "cashAndCashEquivalents", 0) or 0

        if total_debt_roic is not None and total_equity is not None:
            invested_capital = total_debt_roic + total_equity - cash_equivalents_roic  # Common definition
            if nopat is not None and invested_capital is not None and invested_capital != 0:
                metrics["roic"] = nopat / invested_capital
    return metrics


def _calculate_financial_health_metrics(balance_annual_fmp, income_annual_fmp, latest_km_a_fmp):
    metrics = {}
    if balance_annual_fmp:
        latest_ba = balance_annual_fmp[0]
        total_equity = safe_get_float(latest_ba, "totalStockholdersEquity")

        metrics["debt_to_equity"] = safe_get_float(latest_km_a_fmp, "debtToEquity")
        if metrics["debt_to_equity"] is None:  # Fallback if TTM/latest metric not available
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
            metrics["quick_ratio"] = (cash_equivalents + short_term_investments + net_receivables) / current_liabilities

    latest_annual_ebitda_km = safe_get_float(latest_km_a_fmp, "ebitda")
    latest_annual_ebitda_is = get_value_from_statement_list(income_annual_fmp, "ebitda", 0)  # From income statement
    latest_annual_ebitda = latest_annual_ebitda_km if latest_annual_ebitda_km is not None else latest_annual_ebitda_is

    if latest_annual_ebitda and latest_annual_ebitda != 0 and balance_annual_fmp:
        total_debt_val = get_value_from_statement_list(balance_annual_fmp, "totalDebt", 0)
        if total_debt_val is not None:
            metrics["debt_to_ebitda"] = total_debt_val / latest_annual_ebitda
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


def _calculate_growth_metrics(analyzer_instance, income_annual_fmp, statements_cache):
    metrics = {"key_metrics_snapshot": {}}  # Initialize snapshot dict
    ticker = analyzer_instance.ticker

    # YoY Growth
    metrics["revenue_growth_yoy"] = calculate_growth(
        get_value_from_statement_list(income_annual_fmp, "revenue", 0),
        get_value_from_statement_list(income_annual_fmp, "revenue", 1)
    )
    metrics["eps_growth_yoy"] = calculate_growth(
        get_value_from_statement_list(income_annual_fmp, "eps", 0),
        get_value_from_statement_list(income_annual_fmp, "eps", 1)
    )

    # CAGR 3-year
    if len(income_annual_fmp) >= 3:  # Need current year (offset 0) and year -2 (offset 2)
        metrics["revenue_growth_cagr_3yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "revenue", 0),
            get_value_from_statement_list(income_annual_fmp, "revenue", 2),
            # 2 years prior for 3 data points over 2 periods
            2
        )
        metrics["eps_growth_cagr_3yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "eps", 0),
            get_value_from_statement_list(income_annual_fmp, "eps", 2),
            2
        )

    # CAGR 5-year
    if len(income_annual_fmp) >= 5:  # Need current year (offset 0) and year -4 (offset 4)
        metrics["revenue_growth_cagr_5yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "revenue", 0),
            get_value_from_statement_list(income_annual_fmp, "revenue", 4),
            # 4 years prior for 5 data points over 4 periods
            4
        )
        metrics["eps_growth_cagr_5yr"] = calculate_cagr(
            get_value_from_statement_list(income_annual_fmp, "eps", 0),
            get_value_from_statement_list(income_annual_fmp, "eps", 4),
            4
        )

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
    else:
        metrics["revenue_growth_qoq"] = None
        metrics["key_metrics_snapshot"]["q_revenue_source"] = "N/A"
        metrics["key_metrics_snapshot"]["latest_q_revenue"] = None
        metrics["key_metrics_snapshot"]["avg_historical_q_revenue_for_check"] = None

    return metrics


def _calculate_cash_flow_and_trend_metrics(cashflow_annual_fmp, balance_annual_fmp, profile_fmp):
    metrics = {}

    # FCF per Share & FCF Yield
    if cashflow_annual_fmp:
        fcf_latest_annual = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0)

        shares_outstanding_profile = safe_get_float(profile_fmp, "sharesOutstanding")
        mkt_cap_profile = safe_get_float(profile_fmp, "mktCap")
        price_profile = safe_get_float(profile_fmp, "price")

        # Calculate shares outstanding if direct value is missing or zero, using mktCap and price
        shares_outstanding_calc = (
                    mkt_cap_profile / price_profile) if mkt_cap_profile and price_profile and price_profile != 0 else None
        shares_outstanding = shares_outstanding_profile if shares_outstanding_profile is not None and shares_outstanding_profile > 0 else shares_outstanding_calc

        if fcf_latest_annual is not None and shares_outstanding and shares_outstanding != 0:
            metrics["free_cash_flow_per_share"] = fcf_latest_annual / shares_outstanding
            if mkt_cap_profile and mkt_cap_profile != 0:
                metrics["free_cash_flow_yield"] = fcf_latest_annual / mkt_cap_profile

        # FCF Trend (3-year simple trend)
        if len(cashflow_annual_fmp) >= 3:
            fcf0 = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0)  # Latest
            fcf1 = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 1)  # 1 year prior
            fcf2 = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 2)  # 2 years prior

            if all(isinstance(x, (int, float)) for x in [fcf0, fcf1, fcf2] if x is not None) and \
                    all(x is not None for x in [fcf0, fcf1, fcf2]):  # Ensure all are numeric
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

    # Retained Earnings Trend (3-year simple trend)
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
    all_metrics_temp = {}  # Temporary dict to hold all calculated metrics

    # Retrieve cached data
    statements = analyzer_instance._financial_data_cache.get('financial_statements', {})
    income_annual_fmp = statements.get('fmp_income_annual', [])
    balance_annual_fmp = statements.get('fmp_balance_annual', [])
    cashflow_annual_fmp = statements.get('fmp_cashflow_annual', [])

    key_metrics_annual_fmp = analyzer_instance._financial_data_cache.get('key_metrics_annual_fmp', [])
    key_metrics_quarterly_fmp = analyzer_instance._financial_data_cache.get('key_metrics_quarterly_fmp', [])

    basic_fin_fh_metric = analyzer_instance._financial_data_cache.get('basic_financials_finnhub', {}).get('metric', {})
    profile_fmp = analyzer_instance._financial_data_cache.get('profile_fmp', {})

    # Get latest key metrics (TTM or annual)
    latest_km_q_fmp = key_metrics_quarterly_fmp[0] if key_metrics_quarterly_fmp else {}
    latest_km_a_fmp = key_metrics_annual_fmp[0] if key_metrics_annual_fmp else {}

    # Calculate different categories of metrics
    all_metrics_temp.update(_calculate_valuation_ratios(latest_km_q_fmp, latest_km_a_fmp, basic_fin_fh_metric))
    all_metrics_temp.update(
        _calculate_profitability_metrics(analyzer_instance, income_annual_fmp, balance_annual_fmp, latest_km_a_fmp))
    all_metrics_temp.update(_calculate_financial_health_metrics(balance_annual_fmp, income_annual_fmp, latest_km_a_fmp))

    growth_metrics_result = _calculate_growth_metrics(analyzer_instance, income_annual_fmp, statements)
    all_metrics_temp.update(growth_metrics_result)  # This already includes its own 'key_metrics_snapshot'

    all_metrics_temp.update(
        _calculate_cash_flow_and_trend_metrics(cashflow_annual_fmp, balance_annual_fmp, profile_fmp))

    # Sanitize final metrics: ensure None for NaN/inf, and structure key_metrics_snapshot
    final_metrics_cleaned = {}
    key_metrics_snapshot_data = all_metrics_temp.pop("key_metrics_snapshot", {})  # Extract snapshot

    for k, v in all_metrics_temp.items():
        if isinstance(v, float):
            final_metrics_cleaned[k] = v if not (math.isnan(v) or math.isinf(v)) else None
        elif v is not None:
            final_metrics_cleaned[k] = v
        else:
            final_metrics_cleaned[k] = None  # Ensure explicit None for missing values

    # Add cleaned snapshot back
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