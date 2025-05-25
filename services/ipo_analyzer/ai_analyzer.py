# services/ipo_analyzer/ai_analyzer.py
import time
from api_clients import extract_S1_text_sections
from core.logging_setup import logger
from core.config import S1_KEY_SECTIONS, SUMMARIZATION_CHUNK_SIZE_CHARS


def _parse_ai_section_response(ai_text, section_header_keywords):
    """
    Parses a specific section from a larger AI-generated text block.
    section_header_keywords can be a string or a list of strings.
    """
    if not ai_text or ai_text.startswith("Error:"):
        return "AI Error or No Text"

    # Normalize keywords
    keywords_to_check = [k.lower() for k in (
        [section_header_keywords] if isinstance(section_header_keywords, str) else section_header_keywords)]

    lines = ai_text.split('\n')
    capture_content = False
    section_content_lines = []

    # List of all known headers to stop capturing when a new section starts
    # This needs to be comprehensive for the specific AI output format
    all_known_headers_lower = [
        "business model:", "competitive landscape:", "industry outlook:",
        "key risk factors:", "risk factors summary:", "use of ipo proceeds:",
        "financial health from md&a:", "financial health summary:", "md&a summary:",
        "investment stance:", "reasoning:", "critical verification points:"
        # Add any other headers used by Gemini prompts here
    ]

    for line in lines:
        normalized_line_stripped = line.strip().lower()

        # Check if the line starts with one of the target keywords
        matched_current_keyword = next((kw for kw in keywords_to_check if normalized_line_stripped.startswith(
            kw + ":") or normalized_line_stripped == kw), None)

        if matched_current_keyword:
            capture_content = True
            # Get content on the same line as the header, after the colon
            line_content_after_header = line.strip()[len(matched_current_keyword):].lstrip(':').strip()
            if line_content_after_header:
                section_content_lines.append(line_content_after_header)
            continue  # Move to next line

        if capture_content:
            # Check if this line is a different known header (and not one of the target keywords)
            is_another_known_header = any(
                normalized_line_stripped.startswith(h_prefix) for h_prefix in all_known_headers_lower if
                h_prefix not in keywords_to_check)
            if is_another_known_header:
                break  # Stop capturing, new section started
            section_content_lines.append(line)  # Append the original line to preserve formatting

    return "\n".join(section_content_lines).strip() if section_content_lines else "Section not found or empty."


def _parse_ai_synthesis_response(ai_response):
    """Parses the AI synthesis response for investment decision and reasoning."""
    parsed_data = {}
    if ai_response.startswith("Error:") or not ai_response:
        parsed_data["investment_decision"] = "AI Error"
        parsed_data["reasoning"] = ai_response if ai_response else "AI Error: Empty response."
        return parsed_data

    parsed_data["investment_decision"] = _parse_ai_section_response(ai_response, "Investment Stance")
    # Combine Reasoning and Critical Verification Points into one 'reasoning' field
    reasoning_text = _parse_ai_section_response(ai_response, "Reasoning")
    critical_points_text = _parse_ai_section_response(ai_response, "Critical Verification Points")

    combined_reasoning = []
    if reasoning_text and not reasoning_text.startswith("Section not found"):
        combined_reasoning.append(reasoning_text)
    if critical_points_text and not critical_points_text.startswith("Section not found"):
        combined_reasoning.append(f"Critical Verification Points:\n{critical_points_text}")

    parsed_data["reasoning"] = "\n\n".join(combined_reasoning).strip()

    if parsed_data["reasoning"].startswith("Section not found") or not parsed_data["reasoning"]:
        parsed_data["reasoning"] = ai_response  # Fallback to full response if parsing specific parts fails

    if parsed_data["investment_decision"].startswith("Section not found"):
        parsed_data["investment_decision"] = "Review AI Output"  # Default if parsing fails

    return parsed_data


def perform_ai_analysis_for_ipo(analyzer_instance, ipo_db_entry, s1_text, ipo_api_data_raw):
    """Performs AI-driven analysis using S-1 text and other IPO data."""
    analysis_payload = {
        "key_data_snapshot": ipo_api_data_raw,  # Store the raw API data for this IPO
        "s1_sections_used": {}  # Track which S-1 sections were found and used
    }

    s1_sections = {}
    if s1_text:
        s1_sections = extract_S1_text_sections(s1_text, S1_KEY_SECTIONS)
        analysis_payload["s1_sections_used"] = {k: bool(v) for k, v in s1_sections.items()}
    else:  # No S1 text, all sections will be marked as not used
        analysis_payload["s1_sections_used"] = {k: False for k in S1_KEY_SECTIONS.keys()}

    company_prompt_id = f"{ipo_db_entry.company_name} ({ipo_db_entry.symbol or 'N/A'})"
    max_section_len_for_prompt = SUMMARIZATION_CHUNK_SIZE_CHARS  # Max length for each section text in prompt

    # Prepare context from S-1 sections (truncated)
    biz_text_for_prompt = (s1_sections.get("business", "") or "")[:max_section_len_for_prompt]
    risk_text_for_prompt = (s1_sections.get("risk_factors", "") or "")[:max_section_len_for_prompt]
    mda_text_for_prompt = (s1_sections.get("mda", "") or "")[:max_section_len_for_prompt]

    prompt_context_parts = [f"IPO Analysis for: {company_prompt_id}"]
    if biz_text_for_prompt: prompt_context_parts.append(f"S-1 Business Summary Extract: {biz_text_for_prompt}")
    if risk_text_for_prompt: prompt_context_parts.append(f"S-1 Risk Factors Extract: {risk_text_for_prompt}")
    if mda_text_for_prompt: prompt_context_parts.append(f"S-1 MD&A Extract: {mda_text_for_prompt}")

    # Add raw IPO data to prompt context for better AI understanding
    if ipo_api_data_raw:
        context_ipo_data = {
            "name": ipo_api_data_raw.get("name"), "symbol": ipo_api_data_raw.get("symbol"),
            "date": ipo_api_data_raw.get("date"), "price": ipo_api_data_raw.get("price"),
            "exchange": ipo_api_data_raw.get("exchange"), "status": ipo_api_data_raw.get("status")
        }
        prompt_context_parts.append(f"IPO Calendar Data: {context_ipo_data}")

    full_prompt_context = "\n\n".join(prompt_context_parts)

    # Gemini Prompt 1: Business, Competition, Industry
    prompt1_instruction = (
        f"{full_prompt_context}\n\n"
        "Based on the S-1 information (if provided) and IPO calendar data, summarize the following for this IPO candidate:\n"
        "1. Business Model: (Core operations, products/services, revenue generation)\n"
        "2. Competitive Landscape: (Key competitors, company's market position, differentiation)\n"
        "3. Industry Outlook: (Relevant industry trends, growth prospects, challenges)\n"
        "Structure your response clearly with these headings."
    )
    response1 = analyzer_instance.gemini.generate_text(prompt1_instruction)
    time.sleep(3)  # API call delay
    analysis_payload["s1_business_summary"] = _parse_ai_section_response(response1,
                                                                         "Business Model")  # Will be aliased to business_model_summary too
    analysis_payload["competitive_landscape_summary"] = _parse_ai_section_response(response1, "Competitive Landscape")
    analysis_payload["industry_outlook_summary"] = _parse_ai_section_response(response1, "Industry Outlook")

    # Gemini Prompt 2: Risks, Use of Proceeds, Financials
    prompt2_instruction = (
        f"{full_prompt_context}\n\n"
        "Based on the S-1 information (if provided) and IPO calendar data, summarize the following for this IPO candidate:\n"
        "1. Key Risk Factors: (Top 3-5 specific risks from S-1, not generic market risks)\n"
        "2. Use of IPO Proceeds: (How the company plans to use the funds raised)\n"
        "3. Financial Health Summary (from MD&A or inferred): (Key financial performance trends, profitability, debt, liquidity)\n"
        "Structure your response clearly with these headings."
    )
    response2 = analyzer_instance.gemini.generate_text(prompt2_instruction)
    time.sleep(3)  # API call delay
    analysis_payload["s1_risk_factors_summary"] = _parse_ai_section_response(response2, ["Key Risk Factors",
                                                                                         "Risk Factors Summary"])
    analysis_payload["use_of_proceeds_summary"] = _parse_ai_section_response(response2, "Use of IPO Proceeds")
    analysis_payload["s1_financial_health_summary"] = _parse_ai_section_response(response2, ["Financial Health Summary",
                                                                                             "Financial Health from MD&A"])

    # Alias for consistency with older DB fields if they exist or for email service
    analysis_payload["business_model_summary"] = analysis_payload["s1_business_summary"]
    analysis_payload["risk_factors_summary"] = analysis_payload["s1_risk_factors_summary"]
    analysis_payload["pre_ipo_financials_summary"] = analysis_payload["s1_financial_health_summary"]  # Alias
    analysis_payload["s1_mda_summary"] = analysis_payload[
        "s1_financial_health_summary"]  # MDA summary is often about financial health

    # Synthesis Prompt: Investment Decision and Reasoning
    synthesis_prompt_parts = [
        f"Synthesize an IPO investment perspective for {company_prompt_id} using the following information and previously analyzed S-1 summaries."]
    if analysis_payload.get('s1_business_summary') and "Section not found" not in analysis_payload[
        's1_business_summary'] and "AI Error" not in analysis_payload['s1_business_summary']:
        synthesis_prompt_parts.append(f"Business Model Snippet: {analysis_payload['s1_business_summary'][:200]}...")
    if analysis_payload.get('s1_risk_factors_summary') and "Section not found" not in analysis_payload[
        's1_risk_factors_summary'] and "AI Error" not in analysis_payload['s1_risk_factors_summary']:
        synthesis_prompt_parts.append(f"Key Risks Snippet: {analysis_payload['s1_risk_factors_summary'][:200]}...")
    if analysis_payload.get('s1_financial_health_summary') and "Section not found" not in analysis_payload[
        's1_financial_health_summary'] and "AI Error" not in analysis_payload['s1_financial_health_summary']:
        synthesis_prompt_parts.append(
            f"Financial Health Snippet: {analysis_payload['s1_financial_health_summary'][:200]}...")

    synthesis_prompt_parts.append(
        "Instructions: Provide the following structured response:\n"
        "Investment Stance: [Choose ONE: Monitor Closely, Potentially Attractive (with caveats), High Risk/Speculative, Avoid, Further Diligence Required]\n"
        "Reasoning: [Provide 2-4 bullet points explaining the stance, highlighting key pros and cons. Refer to business model, risks, financials, market conditions if relevant.]\n"
        "Critical Verification Points: [List 2-3 specific items from the S-1 (if available) or aspects of the business model that an investor should verify or scrutinize further.]"
    )
    synthesis_prompt = "\n\n".join(synthesis_prompt_parts)

    gemini_synthesis_response = analyzer_instance.gemini.generate_text(synthesis_prompt)
    time.sleep(3)  # API call delay
    parsed_synthesis = _parse_ai_synthesis_response(gemini_synthesis_response)
    analysis_payload.update(parsed_synthesis)  # Adds 'investment_decision' and 'reasoning'

    # Placeholder for other qualitative assessments if needed in future
    analysis_payload["management_team_assessment"] = "Not explicitly analyzed by AI in this version."
    analysis_payload["underwriter_quality_assessment"] = "Not explicitly analyzed by AI in this version."
    analysis_payload[
        "valuation_comparison_summary"] = "Detailed valuation comparison not performed by AI in this version."

    return analysis_payload
