# services/ipo_analyzer/ai_analyzer.py
import time
import json  # For JSON parsing
from api_clients import extract_S1_text_sections
from core.logging_setup import logger
from core.config import S1_KEY_SECTIONS, SUMMARIZATION_CHUNK_SIZE_CHARS, AI_JSON_OUTPUT_INSTRUCTION


def _parse_generic_ai_json_response(ai_response_data, expected_keys_map,
                                    default_error_msg="AI Error or No Valid JSON."):
    """
    Generic parser for AI JSON responses.
    ai_response_data: The direct output from Gemini (expected to be a dict if successful).
    expected_keys_map: A dict mapping desired output keys to keys expected in AI's JSON.
                       e.g., {"business_model_summary": "businessModel"}
    """
    parsed_output = {key: default_error_msg for key in expected_keys_map.keys()}

    if not isinstance(ai_response_data, dict):
        logger.error(f"AI response is not a dictionary: {str(ai_response_data)[:200]}")
        return parsed_output  # All fields will have default_error_msg

    if ai_response_data.get("error"):
        logger.error(f"AI returned an error in JSON: {ai_response_data.get('error')}")
        # Propagate the error to all expected fields
        error_detail = ai_response_data.get('details', default_error_msg)
        for key in expected_keys_map.keys():
            parsed_output[key] = f"AI Error: {error_detail}"
        return parsed_output

    for target_key, ai_json_key_or_path in expected_keys_map.items():
        if isinstance(ai_json_key_or_path, str):  # Simple key
            value = ai_response_data.get(ai_json_key_or_path)
        elif isinstance(ai_json_key_or_path, list):  # Path for nested key e.g. ["section", "subsection"]
            temp_val = ai_response_data
            try:
                for k_part in ai_json_key_or_path:
                    temp_val = temp_val[k_part]
                value = temp_val
            except (KeyError, TypeError):
                value = None
        else:
            value = None

        if value is not None:
            # If value is a dict/list, convert back to string for text fields, or keep as is for JSON fields
            # This depends on how the final `analysis_payload` is structured and DB model.
            # For now, assume we want the structured data if it's complex, or text if it's simple.
            parsed_output[target_key] = value  # Store the direct value (could be string, list, dict)
        else:
            logger.warning(f"Key '{ai_json_key_or_path}' not found in AI JSON response for target '{target_key}'.")
            # Keep default_error_msg or a more specific "Key not found" message
            parsed_output[target_key] = f"Key '{ai_json_key_or_path}' not found in AI response."

    return parsed_output


def perform_ai_analysis_for_ipo(analyzer_instance, ipo_db_entry, s1_text, s1_url, ipo_api_data_raw):
    analysis_payload = {
        "key_data_snapshot": ipo_api_data_raw.copy() if ipo_api_data_raw else {},  # Make a copy to modify
        "s1_sections_used": {}
    }

    # Add s1_url to key_data_snapshot if available
    if s1_url:
        analysis_payload["key_data_snapshot"]["s1_filing_url_from_analysis"] = s1_url

    # Ensure all S1_KEY_SECTIONS are reported in s1_sections_used
    analysis_payload["s1_sections_used"] = {key_name: False for key_name in S1_KEY_SECTIONS.keys()}
    s1_sections = {}
    if s1_text:
        extracted_s1_data = extract_S1_text_sections(s1_text, S1_KEY_SECTIONS)
        s1_sections = extracted_s1_data # Keep the extracted text for use in prompts
        for key_name in S1_KEY_SECTIONS.keys():
            analysis_payload["s1_sections_used"][key_name] = bool(extracted_s1_data.get(key_name))
    else:
        # Already initialized to False above
        pass


    company_prompt_id = f"{ipo_db_entry.company_name} ({ipo_db_entry.symbol or 'N/A'})"
    max_section_len_for_prompt = SUMMARIZATION_CHUNK_SIZE_CHARS // 3  # Divide among sections

    biz_text_for_prompt = (s1_sections.get("business", "") or "")[:max_section_len_for_prompt]
    risk_text_for_prompt = (s1_sections.get("risk_factors", "") or "")[:max_section_len_for_prompt]
    mda_text_for_prompt = (s1_sections.get("mda", "") or "")[:max_section_len_for_prompt]

    prompt_context_parts = [f"IPO Analysis for: {company_prompt_id}"]
    if biz_text_for_prompt: prompt_context_parts.append(
        f"S-1 Business Summary Extract (truncated):\n {biz_text_for_prompt}...")
    if risk_text_for_prompt: prompt_context_parts.append(
        f"S-1 Risk Factors Extract (truncated):\n {risk_text_for_prompt}...")
    if mda_text_for_prompt: prompt_context_parts.append(f"S-1 MD&A Extract (truncated):\n {mda_text_for_prompt}...")

    # Use the modified key_data_snapshot (which might now include s1_filing_url_from_analysis)
    context_ipo_data = {k: analysis_payload["key_data_snapshot"].get(k) for k in
                        ["name", "symbol", "date", "price", "exchange", "status", "numberOfShares", "totalSharesValue", "s1_filing_url_from_analysis"]
                        if analysis_payload["key_data_snapshot"].get(k)}
    if context_ipo_data:
        prompt_context_parts.append(f"IPO Calendar Data (and S-1 URL if found): {json.dumps(context_ipo_data)}")
    full_prompt_context = "\n\n".join(prompt_context_parts)

    # --- Prompt 1: Business, Competition, Industry ---
    json_structure_prompt1 = """
    {
      "businessModel": {"summary": "Core operations, products/services, revenue generation model."},
      "competitiveLandscape": {"summary": "Key competitors, company's market position, differentiation."},
      "industryOutlook": {"summary": "Relevant industry trends, growth prospects, challenges."}
    }
    """
    prompt1_instruction = (
        f"{full_prompt_context}\n\n"
        f"Based on S-1 info (if provided) and IPO calendar data, analyze the IPO candidate. {AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response with these exact top-level keys: \"businessModel\", \"competitiveLandscape\", \"industryOutlook\". Each should contain a \"summary\" field as a string."
        f"Example structure: {json_structure_prompt1}"
    )
    response1_data = analyzer_instance.gemini.generate_text(prompt1_instruction, output_format="json")
    time.sleep(1)

    parsed_response1 = _parse_generic_ai_json_response(response1_data, {
        "s1_business_summary": ["businessModel", "summary"],
        "competitive_landscape_summary": ["competitiveLandscape", "summary"],
        "industry_outlook_summary": ["industryOutlook", "summary"]
    })
    analysis_payload.update(parsed_response1)
    analysis_payload["business_model_summary"] = parsed_response1.get("s1_business_summary", "AI Error")

    # --- Prompt 2: Risks, Use of Proceeds, Financials ---
    json_structure_prompt2 = """
    {
      "keyRiskFactors": {"summary": "Top 3-5 specific risks from S-1, not generic ones. Explain potential impact."},
      "useOfIPOProceeds": {"summary": "How the company plans to use the funds raised."},
      "financialHealthSummary": {"summary": "Key financial performance trends (revenue, profit, burn rate), profitability, debt, liquidity from MD&A or inferred. If financials are missing from S-1, explicitly state this and the implications."}
    }
    """
    prompt2_instruction = (
        f"{full_prompt_context}\n\n"
        f"Analyze the IPO candidate. {AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response with these exact top-level keys: \"keyRiskFactors\", \"useOfIPOProceeds\", \"financialHealthSummary\". Each should contain a \"summary\" field as a string."
        f"Example structure: {json_structure_prompt2}"
    )
    response2_data = analyzer_instance.gemini.generate_text(prompt2_instruction, output_format="json")
    time.sleep(1)

    parsed_response2 = _parse_generic_ai_json_response(response2_data, {
        "s1_risk_factors_summary": ["keyRiskFactors", "summary"],
        "use_of_proceeds_summary": ["useOfIPOProceeds", "summary"],
        "s1_financial_health_summary": ["financialHealthSummary", "summary"]
    })
    analysis_payload.update(parsed_response2)
    analysis_payload["risk_factors_summary"] = parsed_response2.get("s1_risk_factors_summary", "AI Error")
    analysis_payload["pre_ipo_financials_summary"] = parsed_response2.get("s1_financial_health_summary", "AI Error")
    analysis_payload["s1_mda_summary"] = parsed_response2.get("s1_financial_health_summary", "AI Error") # Often combined

    # --- Synthesis Prompt: Investment Decision and Reasoning ---
    synthesis_context_parts = [
        f"Synthesize an IPO investment perspective for {company_prompt_id} using the following information and previously analyzed S-1 summaries."
    ]
    # Add snippets of previously generated summaries (if successful)
    for key, display_name in [
        ("s1_business_summary", "Business Model Snippet"),
        ("s1_risk_factors_summary", "Key Risks Snippet"),
        ("s1_financial_health_summary", "Financial Health Snippet (MD&A based)")]: # Clarified source
        summary_text = analysis_payload.get(key)
        if summary_text and not isinstance(summary_text,
                                           dict) and "AI Error" not in summary_text and "not found" not in summary_text:
            synthesis_context_parts.append(f"{display_name}: {str(summary_text)[:250]}...")  # Ensure it's a string

    json_structure_synthesis = """
    {
      "investmentStance": "Monitor Closely|Potentially Attractive (with caveats)|High Risk/Speculative|Avoid|Further Diligence Required",
      "reasoning": ["Bullet point 1 explaining the stance...", "Bullet point 2..."],
      "criticalVerificationPoints": ["Specific item 1 to verify (e.g., if financials were missing, point this out as critical)...", "Specific item 2..."]
    }
    """
    synthesis_prompt_instruction = (
        "\n\nBased on the above, provide your investment perspective. "
        f"{AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response with these exact keys: \"investmentStance\" (string), \"reasoning\" (list of strings), \"criticalVerificationPoints\" (list of strings)."
        f"Example structure: {json_structure_synthesis}"
    )
    full_synthesis_prompt = "\n\n".join(synthesis_context_parts) + synthesis_prompt_instruction

    synthesis_response_data = analyzer_instance.gemini.generate_text(full_synthesis_prompt, output_format="json")
    time.sleep(1)

    parsed_synthesis = _parse_generic_ai_json_response(synthesis_response_data, {
        "investment_decision": "investmentStance",
        "reasoning_points_list": "reasoning",  # Keep as list for now
        "critical_verification_points_list": "criticalVerificationPoints"
    })

    analysis_payload["investment_decision"] = parsed_synthesis.get("investment_decision", "Review AI Output")

    # Format reasoning and critical points into a single string for the DB 'reasoning' field
    reasoning_str_parts = []
    if isinstance(parsed_synthesis.get("reasoning_points_list"), list):
        reasoning_str_parts.append(
            "Reasoning:\n" + "\n".join([f"- {p}" for p in parsed_synthesis["reasoning_points_list"]]))
    elif parsed_synthesis.get("reasoning_points_list", "").startswith("AI Error"):  # If it's an error string
        reasoning_str_parts.append(f"Reasoning: {parsed_synthesis['reasoning_points_list']}")
    elif parsed_synthesis.get("reasoning_points_list"): # If it's just a string
        reasoning_str_parts.append(f"Reasoning:\n- {parsed_synthesis['reasoning_points_list']}")


    if isinstance(parsed_synthesis.get("critical_verification_points_list"), list):
        reasoning_str_parts.append("\nCritical Verification Points:\n" + "\n".join(
            [f"- {p}" for p in parsed_synthesis["critical_verification_points_list"]]))
    elif parsed_synthesis.get("critical_verification_points_list", "").startswith("AI Error"):
        reasoning_str_parts.append(
            f"\nCritical Verification Points: {parsed_synthesis['critical_verification_points_list']}")
    elif parsed_synthesis.get("critical_verification_points_list"): # If it's just a string
        reasoning_str_parts.append(f"\nCritical Verification Points:\n- {parsed_synthesis['critical_verification_points_list']}")


    analysis_payload["reasoning"] = "\n".join(reasoning_str_parts).strip()
    if not analysis_payload["reasoning"]:  # Fallback if parsing fails badly
        analysis_payload["reasoning"] = str(synthesis_response_data) if isinstance(synthesis_response_data,
                                                                                   dict) and synthesis_response_data.get(
            "error") else "AI reasoning synthesis failed."

    # Placeholders for other qualitative assessments
    analysis_payload["management_team_assessment"] = "Not explicitly analyzed by AI in this version."
    analysis_payload["underwriter_quality_assessment"] = "Not explicitly analyzed by AI in this version."
    analysis_payload[
        "valuation_comparison_summary"] = "Detailed valuation comparison not performed by AI in this version."

    return analysis_payload