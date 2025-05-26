# services/stock_analyzer/ai_synthesis.py
import re
import json  # For parsing potential JSON responses
from core.logging_setup import logger
from .helpers import safe_get_float
from core.config import AI_JSON_OUTPUT_INSTRUCTION


def _parse_ai_investment_thesis_json_response(ticker_for_log, ai_response_data):
    parsed_data = {
        "investment_thesis_full": "AI response not fully processed or expected JSON fields missing.",
        "investment_decision": "Review AI Output",
        "strategy_type": "Not Specified by AI",
        "confidence_level": "Not Specified by AI",
        "reasoning": "AI response not fully processed or expected JSON fields missing."
    }

    if not isinstance(ai_response_data, dict):
        error_message = f"Error: AI response for thesis was not a valid dictionary. Response: {str(ai_response_data)[:500]}"
        parsed_data = {key: error_message for key in parsed_data}
        parsed_data["investment_decision"] = "AI Error"
        parsed_data["strategy_type"] = "AI Error"
        parsed_data["confidence_level"] = "AI Error"
        logger.error(f"AI thesis response for {ticker_for_log} is not a dict: {ai_response_data}")
        return parsed_data

    if ai_response_data.get("error"):
        error_message = f"AI Error: {ai_response_data.get('error_details', str(ai_response_data))}"
        parsed_data = {key: error_message for key in parsed_data}
        parsed_data["investment_decision"] = "AI Error"
        parsed_data["strategy_type"] = "AI Error"
        parsed_data["confidence_level"] = "AI Error"
        logger.error(f"AI thesis generation for {ticker_for_log} returned an error: {ai_response_data.get('error')}")
        return parsed_data

    # Expected JSON structure:
    # {
    #   "investmentThesis": "...",
    #   "investmentDecision": "Buy|Hold|Sell|Monitor|etc.",
    #   "strategyType": "GARP|Value|Growth|etc.",
    #   "confidenceLevel": "High|Medium|Low",
    #   "keyReasoningPoints": ["Point 1...", "Point 2..."],
    #   "dataQualityAcknowledgement": "..." (optional)
    # }

    parsed_data["investment_thesis_full"] = ai_response_data.get("investmentThesis",
                                                                 parsed_data["investment_thesis_full"])
    parsed_data["investment_decision"] = ai_response_data.get("investmentDecision", parsed_data["investment_decision"])
    parsed_data["strategy_type"] = ai_response_data.get("strategyType", parsed_data["strategy_type"])
    parsed_data["confidence_level"] = ai_response_data.get("confidenceLevel", parsed_data["confidence_level"])

    reasoning_points = ai_response_data.get("keyReasoningPoints")
    if isinstance(reasoning_points, list) and reasoning_points:
        parsed_data["reasoning"] = "\n".join([f"- {point}" for point in reasoning_points])
    elif isinstance(reasoning_points, str):  # Handle if AI gives a single string
        parsed_data["reasoning"] = reasoning_points
    else:
        parsed_data["reasoning"] = "Key reasoning points not provided in expected format by AI."

    data_quality_ack = ai_response_data.get("dataQualityAcknowledgement")
    if data_quality_ack:
        parsed_data["reasoning"] += f"\n\nAI Data Quality Acknowledgement: {data_quality_ack}"

    return parsed_data


def synthesize_investment_thesis(analyzer_instance):
    ticker = analyzer_instance.ticker
    logger.info(f"Synthesizing investment thesis for {ticker} using JSON format...")

    metrics = analyzer_instance._financial_data_cache.get('calculated_metrics', {})
    qual_summaries = analyzer_instance._financial_data_cache.get('10k_summaries', {})
    dcf_results = analyzer_instance._financial_data_cache.get('dcf_results', {})
    profile = analyzer_instance._financial_data_cache.get('profile_fmp', {})
    competitor_analysis_summary_data = analyzer_instance._financial_data_cache.get('competitor_analysis', {})

    company_name = analyzer_instance.stock_db_entry.company_name or ticker
    industry = analyzer_instance.stock_db_entry.industry or "N/A"
    sector = analyzer_instance.stock_db_entry.sector or "N/A"

    prompt = f"Company: {company_name} ({ticker})\nIndustry: {industry}, Sector: {sector}\n\n"
    prompt += "Key Financial Metrics & Data:\n"
    # ... (metrics formatting as before, unchanged)
    metrics_for_prompt = {
        "P/E Ratio": metrics.get("pe_ratio"), "P/B Ratio": metrics.get("pb_ratio"),
        "P/S Ratio": metrics.get("ps_ratio"), "Dividend Yield": metrics.get("dividend_yield"),
        "ROE": metrics.get("roe"), "ROIC": metrics.get("roic"),
        "Debt-to-Equity": metrics.get("debt_to_equity"), "Debt-to-EBITDA": metrics.get("debt_to_ebitda"),
        "Revenue Growth YoY": metrics.get("revenue_growth_yoy"),
        "Revenue Growth QoQ": metrics.get("revenue_growth_qoq"),
        f"Latest Quarterly Revenue (Source: {metrics.get('key_metrics_snapshot', {}).get('q_revenue_source', 'N/A')})": metrics.get(
            'key_metrics_snapshot', {}).get('latest_q_revenue'),
        "EPS Growth YoY": metrics.get("eps_growth_yoy"),
        "Net Profit Margin": metrics.get("net_profit_margin"),
        "Operating Profit Margin": metrics.get("operating_profit_margin"),
        "Free Cash Flow Yield": metrics.get("free_cash_flow_yield"),
        "FCF Trend (3yr)": metrics.get("free_cash_flow_trend"),
        "Retained Earnings Trend (3yr)": metrics.get("retained_earnings_trend"),
    }

    for name, val in metrics_for_prompt.items():
        if val is not None:
            formatted_val = val
            if isinstance(val, float):
                if any(kw in name.lower() for kw in ['yield', 'growth', 'margin', 'roe', 'roic']):
                    formatted_val = f'{val:.2%}'
                elif 'revenue' in name.lower() and 'growth' not in name.lower():
                    formatted_val = f'{val:,.0f}'
                else:
                    formatted_val = f'{val:.2f}'
            prompt += f"- {name}: {formatted_val}\n"

    current_stock_price = safe_get_float(profile, "price")
    dcf_iv = dcf_results.get("dcf_intrinsic_value")
    dcf_upside = dcf_results.get("dcf_upside_percentage")

    if current_stock_price is not None:
        prompt += f"- Current Stock Price: {current_stock_price:.2f}\n"
    if dcf_iv is not None:
        prompt += f"- DCF Intrinsic Value/Share (Base Case): {dcf_iv:.2f}\n"
    if dcf_upside is not None:
        prompt += f"- DCF Upside/Downside (Base Case): {dcf_upside:.2%}\n"

    if dcf_results.get("dcf_assumptions", {}).get("sensitivity_analysis"):
        prompt += "- DCF Sensitivity Highlights:\n"
        for s_idx, s_data in enumerate(dcf_results["dcf_assumptions"]["sensitivity_analysis"]):
            if s_idx < 2:
                upside_str = f"{s_data['upside']:.2%}" if s_data['upside'] is not None else "N/A"
                prompt += f"  - {s_data['scenario']}: IV {s_data['intrinsic_value']:.2f} (Upside: {upside_str})\n"
    prompt += "\n"

    prompt += "Qualitative Summaries (from 10-K & AI analysis):\n"

    # Helper to get AI summary text or fallback
    def get_summary_text(summary_data, key, fallback="N/A"):
        if isinstance(summary_data, dict) and summary_data.get(key):
            content = summary_data[key]
            if isinstance(content, dict) and "summary" in content:  # If it's a JSON summary object
                return content["summary"]
            elif isinstance(content, str):  # If it's already a string summary
                return content
        return fallback

    qual_for_prompt = {
        "Business Model": get_summary_text(qual_summaries.get("business_summary_data"), "summary"),
        "Economic Moat": get_summary_text(qual_summaries.get("economic_moat_summary_data"), "overallAssessment"),
        "Industry Trends & Positioning": get_summary_text(qual_summaries.get("industry_trends_summary_data"),
                                                          "companyPositioning"),
        "Competitive Landscape": get_summary_text(competitor_analysis_summary_data,
                                                  "landscapeOverview") or competitor_analysis_summary_data.get(
            "summary", "N/A"),
        "Management Discussion Highlights (MD&A)": get_summary_text(
            qual_summaries.get("management_assessment_summary_data"), "summary"),
        "Key Risk Factors (from 10-K)": get_summary_text(qual_summaries.get("risk_factors_summary_data"), "summary"),
    }

    for name, text_val in qual_for_prompt.items():
        if text_val and text_val != "N/A" and not text_val.startswith(
                ("AI analysis error", "Section not found", "Insufficient input")):
            prompt += f"- {name}:\n{text_val[:500].replace('...', '').strip()}...\n\n"
        elif text_val:
            prompt += f"- {name}: {text_val}\n\n"

    if analyzer_instance.data_quality_warnings:
        prompt += "IMPORTANT DATA QUALITY CONSIDERATIONS:\n"
        for i, warn_msg in enumerate(analyzer_instance.data_quality_warnings):
            prompt += f"- WARNING {i + 1}: {warn_msg}\n"
        prompt += "Acknowledge these warnings in your 'dataQualityAcknowledgement' field if they are significant.\n\n"

    prompt += (
        "Instructions for AI: Based on ALL the above information (quantitative, qualitative, DCF, competitor data, and data quality warnings), "
        "provide a detailed financial analysis and investment thesis. "
        f"Your entire response MUST be a single, valid JSON object. Do not include any text outside of this JSON structure. "
        "Use the following exact structure and field names:\n"
        "{\n"
        "  \"investmentThesis\": \"Comprehensive thesis (2-4 paragraphs) synthesizing all data. Discuss positives, negatives, outlook. If revenue growth is stagnant/negative but EPS growth is positive, explain drivers and sustainability. Address margin pressures or segment profitability changes.\",\n"
        "  \"investmentDecision\": \"Strong Buy|Buy|Hold|Monitor|Reduce|Sell|Avoid\",\n"
        "  \"strategyType\": \"Value|GARP|Growth|Income|Speculative|Special Situation|Turnaround\",\n"
        "  \"confidenceLevel\": \"High|Medium|Low (Reflect confidence in YOUR analysis, considering data quality and completeness)\",\n"
        "  \"keyReasoningPoints\": [\n"
        "    \"Bullet point 1: Valuation (DCF, comparables if any)\",\n"
        "    \"Bullet point 2: Financial Health & Profitability\",\n"
        "    \"Bullet point 3: Growth Prospects (Revenue & EPS)\",\n"
        "    \"Bullet point 4: Economic Moat & Competitive Position\",\n"
        "    \"Bullet point 5: Key Risks (including data quality issues if significant)\",\n"
        "    \"Bullet point 6: Management & Strategy (if inferable)\"\n"
        "  ],\n"
        "  \"dataQualityAcknowledgement\": \"Optional: Briefly state if data quality warnings significantly impacted your analysis or confidence.\"\n"
        "}\n"
    )

    ai_response_data = analyzer_instance.gemini.generate_text(prompt, output_format="json")
    parsed_thesis_data = _parse_ai_investment_thesis_json_response(ticker, ai_response_data)

    # Consolidate data quality warnings and adjust confidence
    # If there are CRITICAL warnings, or multiple warnings, confidence should be lowered.
    critical_warnings = [w for w in analyzer_instance.data_quality_warnings if "CRITICAL:" in w.upper()]
    significant_revenue_warnings = [w for w in analyzer_instance.data_quality_warnings if
                                    "REVENUE" in w.upper() and "DEVIATES" in w.upper()]

    current_confidence = parsed_thesis_data.get("confidence_level", "Not Specified by AI").lower()
    new_confidence = current_confidence
    confidence_adjustment_reason = ""

    if critical_warnings:
        new_confidence = "Low"
        confidence_adjustment_reason = f"Critical data quality warnings ({len(critical_warnings)}) present."
    elif significant_revenue_warnings or len(analyzer_instance.data_quality_warnings) >= 2:
        if current_confidence == "high":
            new_confidence = "Medium"
            confidence_adjustment_reason = "Significant data warnings or multiple issues."
        elif current_confidence == "medium":
            new_confidence = "Low"
            confidence_adjustment_reason = "Significant data warnings or multiple issues, and AI was already Medium."

    if new_confidence != current_confidence and current_confidence not in ["low", "ai error", "not specified by ai"]:
        logger.warning(
            f"Adjusting AI confidence for {ticker} from '{current_confidence.capitalize()}' to '{new_confidence.capitalize()}' due to: {confidence_adjustment_reason}")
        parsed_thesis_data["confidence_level"] = new_confidence.capitalize()
        # Add a note to reasoning if not already covered by AI's dataQualityAcknowledgement
        reasoning_update = f"\nSystem Note: Confidence adjusted to {new_confidence.capitalize()} due to data quality concerns ({confidence_adjustment_reason})."
        if "reasoning" in parsed_thesis_data and isinstance(parsed_thesis_data["reasoning"], str):
            if reasoning_update not in parsed_thesis_data["reasoning"]:
                parsed_thesis_data["reasoning"] += reasoning_update
        else:
            parsed_thesis_data["reasoning"] = reasoning_update

    logger.info(f"Generated thesis for {ticker}. Decision: {parsed_thesis_data.get('investment_decision')}, "
                f"Strategy: {parsed_thesis_data.get('strategy_type')}, Confidence: {parsed_thesis_data.get('confidence_level')}")

    return parsed_thesis_data