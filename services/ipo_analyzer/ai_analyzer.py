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
        # Ensure all fields are marked with a clear error if the entire response is bad
        for key_to_fill in parsed_output.keys():
            parsed_output[key_to_fill] = f"AI Error: Response was not a valid dictionary. Got: {str(ai_response_data)[:100]}"
        return parsed_output

    if ai_response_data.get("error"):
        logger.error(f"AI returned an error in JSON: {ai_response_data.get('error')}")
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
                    temp_val = temp_val[k_part] # type: ignore
                value = temp_val
            except (KeyError, TypeError, AttributeError): # Added AttributeError
                value = None
        else:
            value = None

        if value is not None:
            parsed_output[target_key] = value
        else:
            # Only log warning if the top-level key for a section was expected but missing
            # e.g. if "businessModel" itself is missing from AI response.
            # If a sub-key like "summary" is missing, the default error msg for that field is fine.
            is_top_level_missing = False
            if isinstance(ai_json_key_or_path, str) and ai_json_key_or_path not in ai_response_data:
                is_top_level_missing = True
            elif isinstance(ai_json_key_or_path, list) and ai_json_key_or_path[0] not in ai_response_data:
                is_top_level_missing = True

            if is_top_level_missing:
                logger.warning(f"Top-level key '{ai_json_key_or_path}' not found in AI JSON response for target '{target_key}'.")
                parsed_output[target_key] = f"Key '{ai_json_key_or_path}' not found in AI response."
            # If value is None but key path was valid (e.g. AI returned null for a field), it's kept as None by default_error_msg logic
            # or will be overwritten if default_error_msg is not None.
            # If `default_error_msg` is "AI Error or No Valid JSON.", and `value` is explicitly `None` from AI,
            # `parsed_output[target_key]` retains the default.
            # This behavior seems fine for now.

    return parsed_output


def perform_ai_analysis_for_ipo(analyzer_instance, ipo_db_entry, s1_text, s1_url, ipo_api_data_raw):
    analysis_payload = {
        "key_data_snapshot": ipo_api_data_raw.copy() if ipo_api_data_raw else {},
        "s1_sections_used": {}
    }

    if s1_url:
        analysis_payload["key_data_snapshot"]["s1_filing_url_from_analysis"] = s1_url

    analysis_payload["s1_sections_used"] = {key_name: False for key_name in S1_KEY_SECTIONS.keys()}
    s1_sections_available = False
    s1_data_issue_note = ""

    if s1_text:
        extracted_s1_data = extract_S1_text_sections(s1_text, S1_KEY_SECTIONS)
        for key_name in S1_KEY_SECTIONS.keys():
            if extracted_s1_data.get(key_name):
                analysis_payload["s1_sections_used"][key_name] = True
                s1_sections_available = True
        if not s1_sections_available:
             s1_data_issue_note = "S-1 filing text was retrieved, but no key sections (Business, Risks, MD&A) could be extracted. Analysis will be general."
             logger.warning(f"For {ipo_db_entry.company_name}: {s1_data_issue_note}")
        else:
             s1_data_issue_note = "S-1 sections (Business, Risks, MD&A) were used for this analysis." # Positive note
    elif s1_url and not s1_text:
        s1_data_issue_note = "S-1 filing URL was found, but the text content could not be retrieved or was empty. Analysis will be based on general knowledge and IPO data only."
        logger.warning(f"For {ipo_db_entry.company_name}: {s1_data_issue_note}")
    else: # No s1_url, and therefore no s1_text
        s1_data_issue_note = "No S-1 filing URL was found for this IPO. Analysis will be based on general knowledge and IPO data only."
        logger.warning(f"For {ipo_db_entry.company_name}: {s1_data_issue_note}")

    company_prompt_id = f"{ipo_db_entry.company_name} ({ipo_db_entry.symbol or 'N/A'})"
    max_section_len_for_prompt = SUMMARIZATION_CHUNK_SIZE_CHARS // 3

    biz_text_for_prompt = (extracted_s1_data.get("business", "") if s1_sections_available and extracted_s1_data else "")[:max_section_len_for_prompt]
    risk_text_for_prompt = (extracted_s1_data.get("risk_factors", "") if s1_sections_available and extracted_s1_data else "")[:max_section_len_for_prompt]
    mda_text_for_prompt = (extracted_s1_data.get("mda", "") if s1_sections_available and extracted_s1_data else "")[:max_section_len_for_prompt]

    prompt_context_parts = [f"IPO Analysis for: {company_prompt_id}"]
    if s1_data_issue_note:
        prompt_context_parts.append(f"IMPORTANT S-1 AVAILABILITY NOTE: {s1_data_issue_note}")

    if biz_text_for_prompt: prompt_context_parts.append(f"S-1 Business Summary Extract (truncated):\n {biz_text_for_prompt}...")
    if risk_text_for_prompt: prompt_context_parts.append(f"S-1 Risk Factors Extract (truncated):\n {risk_text_for_prompt}...")
    if mda_text_for_prompt: prompt_context_parts.append(f"S-1 MD&A Extract (truncated):\n {mda_text_for_prompt}...")

    context_ipo_data = {k: analysis_payload["key_data_snapshot"].get(k) for k in
                        ["name", "symbol", "date", "price", "exchange", "status", "numberOfShares", "totalSharesValue", "s1_filing_url_from_analysis"]
                        if analysis_payload["key_data_snapshot"].get(k)}
    if context_ipo_data:
        prompt_context_parts.append(f"IPO Calendar Data (and S-1 URL if found): {json.dumps(context_ipo_data)}")
    full_prompt_context = "\n\n".join(prompt_context_parts)

    # --- Prompt 1: Business, Competition, Industry ---
    json_structure_prompt1 = """
    {
      "businessModel": {"summary": "Core operations, products/services, revenue model. If S-1 was noted as unavailable or sections unextracted, base this on general knowledge of the company type (e.g., SPAC) and IPO data, explicitly stating this limitation."},
      "competitiveLandscape": {"summary": "Key competitors, company's market position, differentiation. If S-1 unavailable, discuss general competitive factors for such an entity."},
      "industryOutlook": {"summary": "Relevant industry trends, growth prospects, challenges. If S-1 unavailable, discuss general outlook for such an entity type."}
    }
    """
    prompt1_instruction = (
        f"{full_prompt_context}\n\n"
        f"Based on the provided context (especially the S-1 availability note), analyze the IPO candidate. {AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response with these exact top-level keys: \"businessModel\", \"competitiveLandscape\", \"industryOutlook\". Each should contain a \"summary\" field as a string."
        f"Example structure: {json_structure_prompt1}"
    )
    response1_data = analyzer_instance.gemini.generate_text(prompt1_instruction, output_format="json")
    time.sleep(1)

    parsed_response1 = _parse_generic_ai_json_response(response1_data, {
        "s1_business_summary": ["businessModel", "summary"], # This field name might be misleading if S-1 not used.
        "competitive_landscape_summary": ["competitiveLandscape", "summary"],
        "industry_outlook_summary": ["industryOutlook", "summary"]
    })
    analysis_payload.update(parsed_response1)
    # Ensure business_model_summary is populated, even if s1_business_summary is the target from parsing.
    # The email template uses business_model_summary if s1_business_summary is empty.
    analysis_payload["business_model_summary"] = parsed_response1.get("s1_business_summary",
                                                                     "AI analysis error for business model.")


    # --- Prompt 2: Risks, Use of Proceeds, Financials ---
    json_structure_prompt2 = """
    {
      "keyRiskFactors": {"summary": "Top 3-5 specific risks. If S-1 available, use it. If S-1 unavailable, list typical risks for this type of IPO (e.g., SPAC risks), explicitly stating this basis."},
      "useOfIPOProceeds": {"summary": "How the company plans to use funds. If S-1 unavailable, state typical use for this IPO type or 'Not specified due to missing S-1'."},
      "financialHealthSummary": {"summary": "Key financial performance trends (revenue, profit, burn rate), profitability, debt, liquidity. If S-1 is missing or lacks financials (e.g. for a SPAC), explicitly state this and its implications (e.g., 'As a SPAC with no operating history, traditional financial health metrics are not applicable pre-merger. Financial health depends on the post-acquisition target.')."}
    }
    """
    prompt2_instruction = (
        f"{full_prompt_context}\n\n"
        f"Analyze the IPO candidate, considering S-1 availability. {AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response as per example: {json_structure_prompt2}"
    )
    response2_data = analyzer_instance.gemini.generate_text(prompt2_instruction, output_format="json")
    time.sleep(1)

    parsed_response2 = _parse_generic_ai_json_response(response2_data, {
        "s1_risk_factors_summary": ["keyRiskFactors", "summary"],
        "use_of_proceeds_summary": ["useOfIPOProceeds", "summary"],
        "s1_financial_health_summary": ["financialHealthSummary", "summary"]
    })
    analysis_payload.update(parsed_response2)
    analysis_payload["risk_factors_summary"] = parsed_response2.get("s1_risk_factors_summary", "AI Error for risk factors.")
    analysis_payload["pre_ipo_financials_summary"] = parsed_response2.get("s1_financial_health_summary", "AI Error for financial health.")
    analysis_payload["s1_mda_summary"] = parsed_response2.get("s1_financial_health_summary", "AI Error for MD&A.")


    # --- Prompt 3: Management, Underwriter, Valuation (New Sections) ---
    json_structure_prompt3 = """
    {
      "managementTeamAssessment": {"summary": "Assessment of management/sponsors. If S-1 is unavailable or for SPACs, emphasize the importance of sponsor track record and state this is typically found in S-1. If S-1 available, summarize bios/experience."},
      "underwriterQualityAssessment": {"summary": "Comment on underwriter quality if info is available (e.g. from S-1). If not, state 'Underwriter details typically in S-1, which was not available/analyzed' or list them if present in IPO data."},
      "valuationComparisonSummary": {"summary": "Valuation comments. For SPACs, explain the $10 IPO price and that traditional valuation is post-merger. If not a SPAC and S-1 available, comment on valuation against peers if possible, or state if info is insufficient."}
    }
    """
    prompt3_instruction = (
        f"{full_prompt_context}\n\n"
        f"Analyze management, underwriters, and valuation for the IPO candidate, noting S-1 availability. {AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response as per example: {json_structure_prompt3}"
    )
    response3_data = analyzer_instance.gemini.generate_text(prompt3_instruction, output_format="json")
    time.sleep(1)

    parsed_response3 = _parse_generic_ai_json_response(response3_data, {
        "management_team_assessment": ["managementTeamAssessment", "summary"],
        "underwriter_quality_assessment": ["underwriterQualityAssessment", "summary"],
        "valuation_comparison_summary": ["valuationComparisonSummary", "summary"]
    })
    analysis_payload.update(parsed_response3)


    # --- Synthesis Prompt: Investment Decision and Reasoning ---
    synthesis_context_parts = [
        f"Synthesize an IPO investment perspective for {company_prompt_id} using the following information and previously analyzed summaries. IMPORTANT S-1 AVAILABILITY NOTE: {s1_data_issue_note}"
    ]
    for key, display_name in [
        ("s1_business_summary", "Business Model Snippet"),
        ("s1_risk_factors_summary", "Key Risks Snippet"),
        ("s1_financial_health_summary", "Financial Health Snippet"),
        ("management_team_assessment", "Management/Sponsor Assessment Snippet"),
        ("valuation_comparison_summary", "Valuation Comments Snippet")]:
        summary_text = analysis_payload.get(key)
        if summary_text and isinstance(summary_text, str) and "AI Error" not in summary_text and "not found" not in summary_text and "N/A" not in summary_text :
            synthesis_context_parts.append(f"{display_name}: {str(summary_text)[:250]}...")

    json_structure_synthesis = """
    {
      "investmentStance": "Monitor Closely|Potentially Attractive (with caveats)|High Risk/Speculative|Avoid|Further Diligence Required",
      "reasoning": ["Bullet point 1 explaining the stance, referencing the S-1 availability note if relevant...", "Bullet point 2..."],
      "criticalVerificationPoints": ["Specific item 1 to verify (e.g., if S-1 was missing, state 'Detailed review of S-1 filing once available')...", "Specific item 2..."]
    }
    """
    synthesis_prompt_instruction = (
        "\n\nBased on the above, provide your investment perspective. "
        f"{AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response with these exact keys: \"investmentStance\" (string), \"reasoning\" (list of strings), \"criticalVerificationPoints\" (list of strings)."
        f"Ensure reasoning acknowledges the S-1 availability note. Critical points should highlight the need to review S-1 if it was unavailable."
        f"Example structure: {json_structure_synthesis}"
    )
    full_synthesis_prompt = "\n\n".join(synthesis_context_parts) + synthesis_prompt_instruction

    synthesis_response_data = analyzer_instance.gemini.generate_text(full_synthesis_prompt, output_format="json")
    time.sleep(1)

    parsed_synthesis = _parse_generic_ai_json_response(synthesis_response_data, {
        "investment_decision": "investmentStance",
        "reasoning_points_list": "reasoning",
        "critical_verification_points_list": "criticalVerificationPoints"
    })

    analysis_payload["investment_decision"] = parsed_synthesis.get("investment_decision", "Review AI Output")

    reasoning_str_parts = []
    # Prepend S-1 availability note to the reasoning if it's not already clearly handled by AI
    # This ensures it's always part of the main reasoning text.
    # However, the prompt now asks AI to include it, so this might be redundant if AI follows instructions.
    # Let's rely on the AI incorporating it based on the new prompt.

    if isinstance(parsed_synthesis.get("reasoning_points_list"), list):
        reasoning_str_parts.append(
            "Reasoning:\n" + "\n".join([f"- {p}" for p in parsed_synthesis["reasoning_points_list"]]))
    elif isinstance(parsed_synthesis.get("reasoning_points_list"), str) and parsed_synthesis.get("reasoning_points_list", "").startswith("AI Error"):
        reasoning_str_parts.append(f"Reasoning: {parsed_synthesis['reasoning_points_list']}")
    elif parsed_synthesis.get("reasoning_points_list"):
        reasoning_str_parts.append(f"Reasoning:\n- {parsed_synthesis['reasoning_points_list']}")


    if isinstance(parsed_synthesis.get("critical_verification_points_list"), list):
        # Ensure "Review S-1" is a critical point if S-1 was not available/processed
        cvp_list = parsed_synthesis["critical_verification_points_list"]
        if not s1_sections_available and not any("S-1" in point for point in cvp_list):
            cvp_list.insert(0, "Detailed review of the S-1 filing (Prospectus) once it becomes available or can be processed.")

        reasoning_str_parts.append("\nCritical Verification Points:\n" + "\n".join(
            [f"- {p}" for p in cvp_list]))
    elif isinstance(parsed_synthesis.get("critical_verification_points_list"), str) and parsed_synthesis.get("critical_verification_points_list", "").startswith("AI Error"):
        reasoning_str_parts.append(
            f"\nCritical Verification Points: {parsed_synthesis['critical_verification_points_list']}")
    elif parsed_synthesis.get("critical_verification_points_list"):
         reasoning_str_parts.append(f"\nCritical Verification Points:\n- {parsed_synthesis['critical_verification_points_list']}")
    elif not s1_sections_available: # If no points provided by AI but S1 was missing
         reasoning_str_parts.append("\nCritical Verification Points:\n- Detailed review of the S-1 filing (Prospectus) once it becomes available or can be processed.")


    analysis_payload["reasoning"] = "\n".join(reasoning_str_parts).strip()
    if not analysis_payload["reasoning"]:
        error_detail = "AI reasoning synthesis failed."
        if isinstance(synthesis_response_data, dict) and synthesis_response_data.get("error"):
            error_detail = f"AI Error in reasoning: {synthesis_response_data.get('details', synthesis_response_data.get('error'))}"
        analysis_payload["reasoning"] = error_detail
        if not s1_sections_available: # Add S1 note even if reasoning failed
             analysis_payload["reasoning"] += "\nCritical Verification Points:\n- Detailed review of the S-1 filing (Prospectus) once it becomes available or can be processed."


    # Add the S-1 availability note to the main reasoning if not captured adequately
    if s1_data_issue_note and s1_data_issue_note not in analysis_payload["reasoning"]:
        analysis_payload["reasoning"] = f"S-1 Status: {s1_data_issue_note}\n\n{analysis_payload['reasoning']}"


    return analysis_payload