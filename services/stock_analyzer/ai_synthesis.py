# services/stock_analyzer/ai_synthesis.py
import re
from core.logging_setup import logger
from .helpers import safe_get_float


def _parse_ai_investment_thesis_response(ticker_for_log, ai_response_text):
    parsed_data = {
        "investment_thesis_full": "AI response not fully processed or 'Investment Thesis:' section missing.",
        "investment_decision": "Review AI Output",
        "strategy_type": "Not Specified by AI",
        "confidence_level": "Not Specified by AI",
        "reasoning": "AI response not fully processed or 'Key Reasoning Points:' section missing."
    }

    if not ai_response_text or ai_response_text.startswith("Error:"):
        error_message = ai_response_text if ai_response_text else "Error: Empty response from AI for thesis."
        parsed_data["investment_thesis_full"] = error_message
        parsed_data["reasoning"] = error_message
        parsed_data["investment_decision"] = "AI Error"
        parsed_data["strategy_type"] = "AI Error"
        parsed_data["confidence_level"] = "AI Error"
        return parsed_data

    text_content = ai_response_text.replace('\r\n', '\n').strip()

    # Define patterns for each section
    # Using re.DOTALL to make '.' match newlines, and re.MULTILINE for '^'
    # Lookahead assertions ensure non-greedy matching up to the next known header or end of string
    patterns = {
        "investment_thesis_full": re.compile(
            r"^\s*Investment Thesis:\s*\n?(.*?)(?=\n\s*(?:Investment Decision:|Strategy Type:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)",
            re.IGNORECASE | re.MULTILINE | re.DOTALL
        ),
        "investment_decision": re.compile(
            r"^\s*Investment Decision:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Strategy Type:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)",
            re.IGNORECASE | re.MULTILINE | re.DOTALL
        ),
        "strategy_type": re.compile(
            r"^\s*Strategy Type:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Confidence Level:|Key Reasoning Points:)|^\s*$|\Z)",
            re.IGNORECASE | re.MULTILINE | re.DOTALL
        ),
        "confidence_level": re.compile(
            r"^\s*Confidence Level:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Strategy Type:|Key Reasoning Points:)|^\s*$|\Z)",
            re.IGNORECASE | re.MULTILINE | re.DOTALL
        ),
        "reasoning": re.compile(
            r"^\s*Key Reasoning Points:\s*\n?(.*?)(?=\n\s*(?:Investment Thesis:|Investment Decision:|Strategy Type:|Confidence Level:)|^\s*$|\Z)",
            re.IGNORECASE | re.MULTILINE | re.DOTALL
        )
    }

    found_any_section = False
    for key, pattern in patterns.items():
        match = pattern.search(text_content)
        if match:
            content = match.group(1).strip()
            if content:
                # For single-line answers, take the first line after stripping
                if key in ["investment_decision", "strategy_type", "confidence_level"]:
                    parsed_data[key] = content.split('\n')[0].strip()
                else:  # For multi-line answers like thesis and reasoning
                    parsed_data[key] = content
                found_any_section = True
            else:  # Header found but content is empty
                parsed_data[key] = f"'{key.replace('_', ' ').title()}:' section found but content empty."

    if not found_any_section and not ai_response_text.startswith("Error:"):
        # If no sections are parsed but there is AI text, put all of it in the thesis.
        logger.warning(f"Could not parse distinct sections from AI thesis response for {ticker_for_log}. "
                       f"Full response will be in 'investment_thesis_full'.")
        parsed_data["investment_thesis_full"] = text_content
        # Other fields remain as "Review AI Output" or "Not Specified by AI"

    return parsed_data


def synthesize_investment_thesis(analyzer_instance):
    ticker = analyzer_instance.ticker
    logger.info(f"Synthesizing investment thesis for {ticker}...")

    # Retrieve all necessary data from the analyzer instance's cache
    metrics = analyzer_instance._financial_data_cache.get('calculated_metrics', {})
    qual_summaries = analyzer_instance._financial_data_cache.get('10k_summaries', {})
    dcf_results = analyzer_instance._financial_data_cache.get('dcf_results', {})
    profile = analyzer_instance._financial_data_cache.get('profile_fmp', {})
    competitor_analysis_summary = analyzer_instance._financial_data_cache.get('competitor_analysis', {}).get("summary",
                                                                                                             "N/A")

    company_name = analyzer_instance.stock_db_entry.company_name or ticker
    industry = analyzer_instance.stock_db_entry.industry or "N/A"
    sector = analyzer_instance.stock_db_entry.sector or "N/A"

    prompt = f"Company: {company_name} ({ticker})\nIndustry: {industry}, Sector: {sector}\n\n"
    prompt += "Key Financial Metrics & Data:\n"

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
            if s_idx < 2:  # Limit to a few examples for brevity
                upside_str = f"{s_data['upside']:.2%}" if s_data['upside'] is not None else "N/A"
                prompt += f"  - {s_data['scenario']}: IV {s_data['intrinsic_value']:.2f} (Upside: {upside_str})\n"

    prompt += "\nQualitative Summaries (from 10-K & AI analysis):\n"
    qual_for_prompt = {
        "Business Model": qual_summaries.get("business_summary"),
        "Economic Moat": qual_summaries.get("economic_moat_summary"),
        "Industry Trends & Positioning": qual_summaries.get("industry_trends_summary"),
        "Competitive Landscape": competitor_analysis_summary,  # Use the summary string
        "Management Discussion Highlights (MD&A)": qual_summaries.get("management_assessment_summary"),
        # Renamed from mda_summary
        "Key Risk Factors (from 10-K)": qual_summaries.get("risk_factors_summary"),
    }
    for name, text_val in qual_for_prompt.items():
        if text_val and isinstance(text_val, str) and not text_val.startswith(
                ("AI analysis", "Section not found", "Insufficient input")):
            prompt += f"- {name}:\n{text_val[:500].replace('...', '').strip()}...\n\n"  # Truncate for prompt
        elif text_val:  # Catch-all for other cases, like "N/A" or short error messages
            prompt += f"- {name}: {text_val}\n\n"

    if analyzer_instance.data_quality_warnings:
        prompt += "IMPORTANT DATA QUALITY CONSIDERATIONS:\n"
        for i, warn_msg in enumerate(analyzer_instance.data_quality_warnings):
            prompt += f"- WARNING {i + 1}: {warn_msg}\n"
        prompt += "Acknowledge these warnings in your risk assessment or confidence level.\n\n"

    prompt += (
        "Instructions for AI: Based on ALL the above information (quantitative, qualitative, DCF, competitor data, and data quality warnings), "
        "provide a detailed financial analysis and investment thesis. "
        "Structure your response *EXACTLY* as follows, using these specific headings on separate lines:\n\n"
        "Investment Thesis:\n"
        "[Comprehensive thesis (2-4 paragraphs) synthesizing all data. Discuss positives, negatives, outlook. "
        "If revenue growth is stagnant/negative but EPS growth is positive, explain the drivers (e.g., buybacks, margin expansion) and sustainability. "
        "Address any points on margin pressures (e.g., in DTC if mentioned in MD&A) or changes in segment profitability.]\n\n"
        "Investment Decision:\n"
        "[Choose ONE: Strong Buy, Buy, Hold, Monitor, Reduce, Sell, Avoid. Base this on the overall analysis.]\n\n"
        "Strategy Type:\n"
        "[Choose ONE that best fits: Value, GARP (Growth At a Reasonable Price), Growth, Income, Speculative, Special Situation, Turnaround.]\n\n"
        "Confidence Level:\n"
        "[Choose ONE: High, Medium, Low. This reflects confidence in YOUR analysis and decision, considering data quality and completeness.]\n\n"
        "Key Reasoning Points:\n"
        "[3-7 bullet points. Each point should be a concise summary of a key factor supporting your decision. "
        "Cover: Valuation (DCF, comparables if any), Financial Health & Profitability, Growth Prospects (Revenue & EPS), "
        "Economic Moat, Competitive Position, Key Risks (including data quality issues if significant), Management & Strategy (if inferable).]\n"
    )

    ai_response_text = analyzer_instance.gemini.generate_text(prompt)
    parsed_thesis_data = _parse_ai_investment_thesis_response(ticker, ai_response_text)

    # Adjust confidence based on data quality warnings
    if any("CRITICAL:" in warn for warn in analyzer_instance.data_quality_warnings) or \
            any("DATA QUALITY WARNING:" in warn for warn in analyzer_instance.data_quality_warnings if
                "revenue" in warn.lower()):
        current_confidence = parsed_thesis_data.get("confidence_level", "").lower()
        if current_confidence == "high":
            logger.warning(
                f"Downgrading AI confidence from High to Medium for {ticker} due to critical data quality warnings.")
            parsed_thesis_data["confidence_level"] = "Medium"
        elif current_confidence == "medium":
            logger.warning(
                f"Downgrading AI confidence from Medium to Low for {ticker} due to critical data quality warnings.")
            parsed_thesis_data["confidence_level"] = "Low"

    logger.info(f"Generated thesis for {ticker}. Decision: {parsed_thesis_data.get('investment_decision')}, "
                f"Strategy: {parsed_thesis_data.get('strategy_type')}, Confidence: {parsed_thesis_data.get('confidence_level')}")

    return parsed_thesis_data