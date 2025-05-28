# services/stock_analyzer/dcf_analyzer.py
from core.logging_setup import logger
from .helpers import safe_get_float, get_value_from_statement_list, calculate_cagr
from core.config import (
    DEFAULT_DISCOUNT_RATE, DEFAULT_PERPETUAL_GROWTH_RATE,
    DEFAULT_FCF_PROJECTION_YEARS
)


def _calculate_dcf_value_internal(ticker_for_log, start_fcf, initial_growth, discount_rate, perpetual_growth,
                                  proj_years, shares_outstanding_val):
    projected_fcfs = []
    last_projected_fcf = start_fcf
    current_year_growth_rates = []

    # Linear decline in growth rate from initial_growth to perpetual_growth over proj_years
    growth_rate_decline_per_year = (initial_growth - perpetual_growth) / float(proj_years) if proj_years > 0 else 0

    for i in range(proj_years):
        current_year_growth_rate = max(initial_growth - (growth_rate_decline_per_year * i), perpetual_growth)
        projected_fcf = last_projected_fcf * (1 + current_year_growth_rate)
        projected_fcfs.append(projected_fcf)
        last_projected_fcf = projected_fcf
        current_year_growth_rates.append(round(current_year_growth_rate, 4))  # Store for assumptions

    if not projected_fcfs:  # Should not happen if proj_years > 0
        return None, []

    # Terminal Value Calculation
    terminal_year_fcf_for_tv = projected_fcfs[-1] * (1 + perpetual_growth)
    terminal_value_denominator = discount_rate - perpetual_growth

    terminal_value = 0
    if terminal_value_denominator <= 1e-6:  # Avoid division by zero or very small numbers, or negative if pgr > dr
        logger.warning(f"DCF for {ticker_for_log}: Discount rate ({discount_rate:.3f}) is too close to or less than "
                       f"perpetual growth rate ({perpetual_growth:.3f}). Terminal Value may be unreliable or infinite. Setting TV to 0.")
    else:
        terminal_value = terminal_year_fcf_for_tv / terminal_value_denominator

    # Discount FCFs and Terminal Value
    sum_discounted_fcf = sum(fcf / ((1 + discount_rate) ** (i + 1)) for i, fcf in enumerate(projected_fcfs))
    discounted_terminal_value = terminal_value / ((1 + discount_rate) ** proj_years)

    intrinsic_equity_value = sum_discounted_fcf + discounted_terminal_value

    if shares_outstanding_val is None or shares_outstanding_val == 0:
        logger.error(f"DCF for {ticker_for_log}: Shares outstanding is zero or None. Cannot calculate per share value.")
        return None, current_year_growth_rates

    return intrinsic_equity_value / shares_outstanding_val, current_year_growth_rates


def perform_dcf_analysis(analyzer_instance):
    ticker = analyzer_instance.ticker
    logger.info(f"Performing simplified DCF analysis for {ticker}...")

    dcf_results = {
        "dcf_intrinsic_value": None,
        "dcf_upside_percentage": None,
        "dcf_assumptions": {
            "discount_rate": DEFAULT_DISCOUNT_RATE,
            "perpetual_growth_rate": DEFAULT_PERPETUAL_GROWTH_RATE,
            "projection_years": DEFAULT_FCF_PROJECTION_YEARS,
            "start_fcf": None,
            "start_fcf_basis": "N/A",
            "fcf_growth_rates_projection": [],
            "initial_fcf_growth_rate_used": None,
            "initial_fcf_growth_rate_basis": "N/A",
            "sensitivity_analysis": []
        }
    }
    assumptions = dcf_results["dcf_assumptions"]

    # Retrieve necessary data from cache
    cashflow_annual_fmp = analyzer_instance._financial_data_cache.get('financial_statements', {}).get(
        'fmp_cashflow_annual', [])
    profile_fmp = analyzer_instance._financial_data_cache.get('profile_fmp', {})
    calculated_metrics = analyzer_instance._financial_data_cache.get('calculated_metrics', {})

    current_price = safe_get_float(profile_fmp, "price")
    shares_outstanding_profile = safe_get_float(profile_fmp, "sharesOutstanding")
    mkt_cap_profile = safe_get_float(profile_fmp, "mktCap")

    shares_outstanding_calc = (
                mkt_cap_profile / current_price) if mkt_cap_profile and current_price and current_price != 0 else None
    shares_outstanding = shares_outstanding_profile if shares_outstanding_profile is not None and shares_outstanding_profile > 0 else shares_outstanding_calc

    if not cashflow_annual_fmp or not profile_fmp or current_price is None or shares_outstanding is None or shares_outstanding == 0:
        logger.warning(
            f"Insufficient data for DCF for {ticker} (FCF statements, profile, price, or shares missing/zero).")
        analyzer_instance._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    current_fcf_annual = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 0)
    if current_fcf_annual is None or current_fcf_annual <= 10000:  # Arbitrary small positive FCF threshold
        logger.warning(
            f"Current annual FCF for {ticker} is {current_fcf_annual}. DCF requires substantial positive FCF. Skipping DCF.")
        analyzer_instance._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results

    assumptions["start_fcf"] = current_fcf_annual
    assumptions[
        "start_fcf_basis"] = f"Latest Annual FCF ({cashflow_annual_fmp[0].get('date') if cashflow_annual_fmp and cashflow_annual_fmp[0] else 'N/A'})"

    # Determine initial FCF growth rate
    fcf_growth_rate_3yr_cagr = None
    if len(cashflow_annual_fmp) >= 4:  # Need 4 data points for 3-year CAGR (current and 3 years prior)
        fcf_start_for_cagr = get_value_from_statement_list(cashflow_annual_fmp, "freeCashFlow", 3)  # 3 years prior
        if fcf_start_for_cagr and fcf_start_for_cagr > 0 and current_fcf_annual > 0:  # Both positive for meaningful CAGR
            fcf_growth_rate_3yr_cagr = calculate_cagr(current_fcf_annual, fcf_start_for_cagr, 3)

    initial_fcf_growth_rate = DEFAULT_PERPETUAL_GROWTH_RATE  # Default
    assumptions["initial_fcf_growth_rate_basis"] = "Default (Perpetual Growth Rate)"

    if fcf_growth_rate_3yr_cagr is not None:
        initial_fcf_growth_rate = fcf_growth_rate_3yr_cagr
        assumptions["initial_fcf_growth_rate_basis"] = "Historical 3yr FCF CAGR"
    elif calculated_metrics.get("revenue_growth_cagr_3yr") is not None:
        initial_fcf_growth_rate = calculated_metrics["revenue_growth_cagr_3yr"]
        assumptions["initial_fcf_growth_rate_basis"] = "Proxy: Revenue Growth CAGR (3yr)"
    elif calculated_metrics.get("revenue_growth_yoy") is not None:
        initial_fcf_growth_rate = calculated_metrics["revenue_growth_yoy"]
        assumptions["initial_fcf_growth_rate_basis"] = "Proxy: Revenue Growth YoY"

    if not isinstance(initial_fcf_growth_rate, (int, float)):  # Ensure it's a number
        initial_fcf_growth_rate = DEFAULT_PERPETUAL_GROWTH_RATE

    # Cap and floor the initial growth rate to reasonable bounds
    initial_fcf_growth_rate = min(max(initial_fcf_growth_rate, -0.05), 0.15)  # e.g., -5% to 15%
    assumptions["initial_fcf_growth_rate_used"] = initial_fcf_growth_rate

    # Base Case DCF
    base_iv_per_share, base_fcf_growth_rates = _calculate_dcf_value_internal(
        ticker, assumptions["start_fcf"], assumptions["initial_fcf_growth_rate_used"],
        assumptions["discount_rate"], assumptions["perpetual_growth_rate"],
        assumptions["projection_years"], shares_outstanding
    )

    if base_iv_per_share is not None:
        dcf_results["dcf_intrinsic_value"] = base_iv_per_share
        assumptions["fcf_growth_rates_projection"] = base_fcf_growth_rates
        if current_price and current_price != 0:
            dcf_results["dcf_upside_percentage"] = (base_iv_per_share - current_price) / current_price
    else:
        logger.error(f"DCF base case calculation failed for {ticker}.")
        analyzer_instance._financial_data_cache['dcf_results'] = dcf_results
        return dcf_results  # Exit if base case fails

    # Sensitivity Analysis
    sensitivity_scenarios = [
        {"dr_adj": -0.005, "pgr_adj": 0.0, "label": "Discount Rate -0.5%"},
        {"dr_adj": +0.005, "pgr_adj": 0.0, "label": "Discount Rate +0.5%"},
        {"dr_adj": 0.0, "pgr_adj": -0.0025, "label": "Perp. Growth -0.25%"},
        {"dr_adj": 0.0, "pgr_adj": +0.0025, "label": "Perp. Growth +0.25%"}
    ]

    for scenario in sensitivity_scenarios:
        sens_dr = assumptions["discount_rate"] + scenario["dr_adj"]
        sens_pgr = assumptions["perpetual_growth_rate"] + scenario["pgr_adj"]

        # Ensure perpetual growth is less than discount rate for stable model
        if sens_pgr >= sens_dr - 0.001:  # Small margin
            logger.debug(
                f"Skipping DCF sensitivity scenario '{scenario['label']}' for {ticker} as PGR ({sens_pgr:.3f}) >= DR ({sens_dr:.3f}).")
            continue

        iv_sens, _ = _calculate_dcf_value_internal(
            ticker, assumptions["start_fcf"], assumptions["initial_fcf_growth_rate_used"],
            sens_dr, sens_pgr, assumptions["projection_years"], shares_outstanding
        )
        if iv_sens is not None:
            upside_sens = (iv_sens - current_price) / current_price if current_price and current_price != 0 else None
            assumptions["sensitivity_analysis"].append({
                "scenario": scenario["label"],
                "discount_rate": sens_dr,
                "perpetual_growth_rate": sens_pgr,
                "intrinsic_value": iv_sens,
                "upside": upside_sens
            })

    logger.info(f"DCF for {ticker}: Base IV/Share: {dcf_results.get('dcf_intrinsic_value', 'N/A'):.2f}, "
                f"Upside: {dcf_results.get('dcf_upside_percentage', 'N/A') * 100 if dcf_results.get('dcf_upside_percentage') is not None else 'N/A':.2f}%")

    analyzer_instance._financial_data_cache['dcf_results'] = dcf_results
    return dcf_results