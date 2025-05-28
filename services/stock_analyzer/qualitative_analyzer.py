# services/stock_analyzer/qualitative_analyzer.py
import time
import json
from core.logging_setup import logger
from api_clients import extract_S1_text_sections
from core.config import (
    TEN_K_KEY_SECTIONS, SUMMARIZATION_CHUNK_SIZE_CHARS,
    SUMMARIZATION_CHUNK_OVERLAP_CHARS, SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS,
    MAX_COMPETITORS_TO_ANALYZE, AI_JSON_OUTPUT_INSTRUCTION
)
from .helpers import safe_get_float


def _summarize_text_chunked_for_json(analyzer_instance, text_to_summarize, base_context, section_specific_instruction,
                                     company_name_ticker_prompt, json_structure_example):
    gemini_client = analyzer_instance.gemini
    default_error_response = {"error": f"AI summary error or no content for '{base_context}'.", "summary": "",
                              "keyPoints": []}

    if not text_to_summarize or not isinstance(text_to_summarize, str) or not text_to_summarize.strip():
        return default_error_response, 0

    text_len = len(text_to_summarize)
    logger.info(
        f"Summarizing '{base_context}' for {company_name_ticker_prompt} (JSON output), original length: {text_len} chars.")

    final_prompt_instruction = (
        f"{section_specific_instruction}\n"
        f"Your entire response MUST be a single, valid JSON object structured as follows: {json_structure_example}"
    )

    if text_len < SUMMARIZATION_CHUNK_SIZE_CHARS:  # Adjusted for potentially larger JSON structure in prompt/output
        logger.info(
            f"Section length {text_len} is within single-pass limit ({SUMMARIZATION_CHUNK_SIZE_CHARS}). Summarizing directly for JSON.")
        summary_json = gemini_client.generate_text(
            f"Text to Summarize from '{base_context}' for {company_name_ticker_prompt}:\n\"\"\"\n{text_to_summarize}\n\"\"\"\n\n{final_prompt_instruction}",
            output_format="json"
        )
        time.sleep(2)
        if isinstance(summary_json, dict) and not summary_json.get("error"):
            return summary_json, text_len
        else:
            logger.error(f"Direct JSON summarization failed for '{base_context}'. Response: {summary_json}")
            return default_error_response, text_len

    # Chunked summarization
    logger.info(f"Section length {text_len} exceeds single-pass limit. Applying chunked summarization for JSON output.")
    chunks = []
    start = 0
    while start < text_len:
        end = start + SUMMARIZATION_CHUNK_SIZE_CHARS
        chunks.append(text_to_summarize[start:end])
        start = end - SUMMARIZATION_CHUNK_OVERLAP_CHARS if end < text_len else end

    chunk_summaries_text = []  # Store text summaries of chunks
    for i, chunk in enumerate(chunks):
        logger.info(
            f"Summarizing chunk {i + 1}/{len(chunks)} for '{base_context}' of {company_name_ticker_prompt} (length: {len(chunk)} chars) as text part.")
        # Summarize chunks into text first to avoid overly complex JSON handling for each small piece
        chunk_summary_text = gemini_client.summarize_text_with_context(
            chunk,
            f"This is chunk {i + 1} of {len(chunks)} from the '{base_context}' section for {company_name_ticker_prompt}.",
            f"Concisely summarize the key information in this chunk relevant to: {section_specific_instruction.splitlines()[0]}"
            # Simpler instruction for chunk
        )  # output_format="text" by default
        time.sleep(2)
        chunk_summaries_text.append(chunk_summary_text if chunk_summary_text and not chunk_summary_text.startswith(
            "Error:") else f"[AI error or no content for chunk {i + 1}]")

    if not chunk_summaries_text or all("[AI error" in s for s in chunk_summaries_text):
        return default_error_response, text_len

    concatenated_summaries = "\n\n---\n\n".join(chunk_summaries_text)
    logger.info(f"Concatenated chunk text summaries length for '{base_context}': {len(concatenated_summaries)} chars.")

    if len(concatenated_summaries) > SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS:  # If even concatenated text summaries are too long
        logger.warning(
            f"Concatenated text summaries for '{base_context}' too long ({len(concatenated_summaries)}). Truncating for final JSON generation.")
        concatenated_summaries = concatenated_summaries[:SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS]

    # Final pass: generate JSON from the concatenated text summaries
    logger.info(f"Generating final JSON summary for '{base_context}' from concatenated chunk summaries.")
    final_summary_json = gemini_client.generate_text(
        f"The following are collated summaries from different parts of the '{base_context}' section for {company_name_ticker_prompt}:\n\"\"\"\n{concatenated_summaries}\n\"\"\"\n\n"
        f"Synthesize these into a single, cohesive overview for '{base_context}'.\n{final_prompt_instruction}",
        output_format="json"
    )
    time.sleep(2)

    if isinstance(final_summary_json, dict) and not final_summary_json.get("error"):
        return final_summary_json, text_len
    else:
        logger.error(
            f"Final JSON summarization from chunks failed for '{base_context}'. Response: {final_summary_json}")
        # Fallback: return the concatenated text if JSON fails, wrapped in the error structure
        default_error_response[
            "summary"] = "AI error in final JSON summary pass. Concatenated text summaries provided instead."
        default_error_response["keyPoints"] = [concatenated_summaries[:1000]]  # Truncated
        return default_error_response, text_len


def fetch_and_summarize_10k_data(analyzer_instance):
    ticker = analyzer_instance.ticker
    logger.info(f"Fetching and attempting to summarize latest 10-K for {ticker} into JSON...")
    # Initialize with keys for where the JSON data itself will be stored
    summary_results = {
        "qualitative_sources_summary": {},
        "business_summary_data": None,
        "risk_factors_summary_data": None,
        "management_assessment_summary_data": None,  # MDA
        "economic_moat_summary_data": None,
        "industry_trends_summary_data": None,
    }

    if not analyzer_instance.stock_db_entry or not analyzer_instance.stock_db_entry.cik:
        logger.warning(f"No CIK for {ticker}. Cannot fetch 10-K.")
        # Populate error state for summary data fields
        for key in ["business_summary_data", "risk_factors_summary_data", "management_assessment_summary_data"]:
            summary_results[key] = {"error": "No CIK available for 10-K fetching."}
        return summary_results

    filing_url = analyzer_instance.sec_edgar.get_filing_document_url(analyzer_instance.stock_db_entry.cik, "10-K")
    time.sleep(0.5)
    if not filing_url:
        logger.info(f"No recent 10-K found for {ticker}, trying 10-K/A.")
        filing_url = analyzer_instance.sec_edgar.get_filing_document_url(analyzer_instance.stock_db_entry.cik, "10-K/A")
        time.sleep(0.5)

    if not filing_url:
        logger.warning(f"No 10-K or 10-K/A URL found for {ticker} (CIK: {analyzer_instance.stock_db_entry.cik})")
        for key in ["business_summary_data", "risk_factors_summary_data", "management_assessment_summary_data"]:
            summary_results[key] = {"error": "No 10-K or 10-K/A URL found."}
        return summary_results

    summary_results["qualitative_sources_summary"]["10k_filing_url_used"] = filing_url
    text_content = analyzer_instance.sec_edgar.get_filing_text(filing_url)

    if not text_content:
        logger.warning(f"Failed to fetch/load 10-K text from {filing_url}")
        for key in ["business_summary_data", "risk_factors_summary_data", "management_assessment_summary_data"]:
            summary_results[key] = {"error": f"Failed to fetch 10-K text from {filing_url}."}
        return summary_results

    logger.info(f"Fetched 10-K text (length: {len(text_content)}) for {ticker}. Extracting and summarizing sections.")
    sections = extract_S1_text_sections(text_content, TEN_K_KEY_SECTIONS)
    company_name_for_prompt = analyzer_instance.stock_db_entry.company_name or ticker

    # Define JSON structure for basic summaries
    basic_summary_json_structure = "{ \"summary\": \"Comprehensive summary text...\", \"keyPoints\": [\"Key point 1...\", \"Key point 2...\"] }"

    section_details = {
        "business": ("Business (Item 1)",
                     "Summarize the company's core business operations, primary products/services, revenue generation model, key customer segments, and primary markets. Highlight any recent strategic shifts mentioned.",
                     "business_summary_data"),
        "risk_factors": ("Risk Factors (Item 1A)",
                         "Identify and summarize the 3-5 most significant and company-specific risk factors disclosed. Focus on operational and strategic risks. Briefly explain the potential impact of each.",
                         "risk_factors_summary_data"),
        "mda": ("Management's Discussion and Analysis (Item 7)",
                "Summarize key insights into financial performance drivers (revenue, costs, profitability), financial condition (liquidity, capital resources), and management's outlook or significant focus areas. Note any discussion on margin pressures or segment performance changes.",
                "management_assessment_summary_data")  # Target key for summary_results
    }

    for section_key, (prompt_section_name, specific_instruction, target_summary_key) in section_details.items():
        section_text = sections.get(section_key)
        json_response = None
        source_len = 0
        if not section_text:
            logger.warning(f"Section '{prompt_section_name}' not found in 10-K for {ticker}.")
            json_response = {"error": f"Section '{prompt_section_name}' not found in 10-K document."}
        else:
            source_len = len(section_text)
            json_response, _ = _summarize_text_chunked_for_json(
                analyzer_instance, section_text, prompt_section_name,
                specific_instruction, f"{company_name_for_prompt} ({ticker})",
                basic_summary_json_structure
            )
        summary_results[target_summary_key] = json_response
        summary_results["qualitative_sources_summary"][f"{section_key}_10k_source_length"] = source_len
        logger.info(
            f"JSON Summary for '{prompt_section_name}' (source length {source_len}): {str(json_response)[:150].replace(chr(10), ' ')}...")

    # Economic Moat Analysis (derived from business and risk summaries)
    biz_summary_data = summary_results.get("business_summary_data", {})
    risk_summary_data = summary_results.get("risk_factors_summary_data", {})
    biz_summary_text = biz_summary_data.get("summary", "") if isinstance(biz_summary_data, dict) else ""
    risk_summary_text = risk_summary_data.get("summary", "") if isinstance(risk_summary_data, dict) else ""

    moat_json_structure = "{ \"moats\": [ { \"moatType\": \"Brand Strength|Network Effects|etc.\", \"evidence\": \"...\", \"strength\": \"Very Strong|Strong|Moderate|Weak\" } ], \"overallAssessment\": \"Overall summary of moat strength...\" }"
    if biz_summary_text or risk_summary_text:
        moat_input_text = (
            f"Business Summary:\n{biz_summary_text}\n\nRisk Factors Summary:\n{risk_summary_text}").strip()
        moat_prompt = (
            f"Analyze the primary economic moats for {company_name_for_prompt} ({ticker}), based on the following summaries from its 10-K:\n\n{moat_input_text}\n\n"
            f"Provide your analysis as a JSON object. {AI_JSON_OUTPUT_INSTRUCTION} Structure it as: {moat_json_structure}"
        )
        moat_summary_json = analyzer_instance.gemini.generate_text(moat_prompt, output_format="json")
        time.sleep(3)
        summary_results["economic_moat_summary_data"] = moat_summary_json if isinstance(moat_summary_json, dict) else {
            "error": "AI analysis for economic moat failed or returned non-JSON."}
    else:
        summary_results["economic_moat_summary_data"] = {
            "error": "Insufficient input from 10-K summaries for economic moat analysis."}

    # Industry Trends Analysis
    mda_summary_data = summary_results.get("management_assessment_summary_data", {})
    mda_summary_text = mda_summary_data.get("summary", "") if isinstance(mda_summary_data, dict) else ""

    industry_json_structure = "{ \"keyTrends\": [\"Trend 1...\", \"Trend 2...\"], \"opportunities\": [\"Opportunity 1...\"], \"challenges\": [\"Challenge 1...\"], \"companyPositioning\": \"How the company is positioned...\", \"overallOutlook\": \"Brief outlook statement...\" }"
    if biz_summary_text:
        industry_context_text = (
            f"Company: {company_name_for_prompt} ({ticker})\n"
            f"Industry: {analyzer_instance.stock_db_entry.industry or 'Not Specified'}\n"
            f"Sector: {analyzer_instance.stock_db_entry.sector or 'Not Specified'}\n\n"
            f"Business Summary (from 10-K):\n{biz_summary_text}\n\n"
            f"MD&A Highlights (from 10-K):\n{mda_summary_text}"
        ).strip()
        industry_prompt = (
            f"Based on the provided information for {company_name_for_prompt} ({ticker}):\n\n{industry_context_text}\n\n"
            f"Analyze key industry trends, opportunities, challenges, and the company's positioning. "
            f"{AI_JSON_OUTPUT_INSTRUCTION} Structure it as: {industry_json_structure}"
        )
        industry_summary_json = analyzer_instance.gemini.generate_text(industry_prompt, output_format="json")
        time.sleep(3)
        summary_results["industry_trends_summary_data"] = industry_summary_json if isinstance(industry_summary_json,
                                                                                              dict) else {
            "error": "AI analysis for industry trends failed or returned non-JSON."}
    else:
        summary_results["industry_trends_summary_data"] = {
            "error": "Insufficient input (Business Summary missing) for industry analysis."}

    # Remove the old string-based keys if they exist from a previous version, only keep _data suffixed keys for JSON
    for old_key in ["business_summary", "risk_factors_summary", "management_assessment_summary",
                    "economic_moat_summary", "industry_trends_summary"]:
        if old_key in summary_results:
            del summary_results[old_key]

    logger.info(f"10-K qualitative JSON summaries and AI interpretations generated for {ticker}.")
    analyzer_instance._financial_data_cache[
        '10k_summaries'] = summary_results  # This now stores dicts with JSON objects
    return summary_results


def fetch_and_analyze_competitors(analyzer_instance):
    ticker = analyzer_instance.ticker
    logger.info(f"Fetching and analyzing competitor data for {ticker} (JSON output)...")

    default_error_summary = {
        "summary": "Competitor analysis not performed or failed.",
        "landscapeOverview": "N/A",
        "companyPositioning": "N/A",
        "keyDifferentials": [],
        "competitionIntensity": "N/A",
        "peers_data": []
    }

    peers_data_finnhub = analyzer_instance.finnhub.get_company_peers(ticker)
    time.sleep(1)  # Reduced sleep

    if not peers_data_finnhub or not isinstance(peers_data_finnhub, list) or not peers_data_finnhub[0]:
        logger.warning(f"No direct peer data found from Finnhub for {ticker}.")
        analyzer_instance._financial_data_cache['competitor_analysis'] = {**default_error_summary,
                                                                          "summary": "No peer data found from primary source (Finnhub)."}
        return default_error_summary["summary"]  # For compatibility, return the summary string

    # ... (peer fetching logic as before, unchanged, up to peer_details_list)
    if isinstance(peers_data_finnhub[0], list):
        peers_data_finnhub = peers_data_finnhub[0]
    peer_tickers = [p for p in peers_data_finnhub if p and p.upper() != ticker.upper()][:MAX_COMPETITORS_TO_ANALYZE]

    if not peer_tickers:
        logger.info(f"No distinct competitor tickers found after filtering for {ticker}.")
        analyzer_instance._financial_data_cache['competitor_analysis'] = {**default_error_summary,
                                                                          "summary": "No distinct competitor tickers identified."}
        return default_error_summary["summary"]

    logger.info(f"Identified peers for {ticker}: {peer_tickers}. Fetching basic data for comparison.")
    peer_details_list = []
    for peer_ticker_symbol in peer_tickers:
        try:
            logger.debug(f"Fetching basic data for peer: {peer_ticker_symbol}")
            peer_profile_fmp_list = analyzer_instance.fmp.get_company_profile(peer_ticker_symbol);
            time.sleep(1)
            peer_profile_fmp = peer_profile_fmp_list[0] if peer_profile_fmp_list and isinstance(peer_profile_fmp_list,
                                                                                                list) and \
                                                           peer_profile_fmp_list[0] else {}

            peer_metrics_fmp_list = analyzer_instance.fmp.get_key_metrics(peer_ticker_symbol, period="annual", limit=1);
            time.sleep(1)
            peer_metrics_fmp = peer_metrics_fmp_list[0] if peer_metrics_fmp_list and isinstance(peer_metrics_fmp_list,
                                                                                                list) and \
                                                           peer_metrics_fmp_list[0] else {}

            peer_fh_basics = {}
            if not peer_metrics_fmp.get("peRatio") or not peer_metrics_fmp.get("priceSalesRatio"):
                peer_fh_basics_data = analyzer_instance.finnhub.get_basic_financials(peer_ticker_symbol);
                time.sleep(1)
                peer_fh_basics = peer_fh_basics_data.get("metric", {}) if peer_fh_basics_data else {}

            peer_name = peer_profile_fmp.get("companyName", peer_ticker_symbol)
            market_cap = safe_get_float(peer_profile_fmp, "mktCap")
            pe_ratio = safe_get_float(peer_metrics_fmp, "peRatio") or safe_get_float(peer_fh_basics, "peTTM")
            ps_ratio = safe_get_float(peer_metrics_fmp, "priceSalesRatio") or safe_get_float(peer_fh_basics, "psTTM")

            peer_info = {"ticker": peer_ticker_symbol, "name": peer_name, "market_cap": market_cap,
                         "pe_ratio": pe_ratio, "ps_ratio": ps_ratio}
            if peer_name != peer_ticker_symbol or market_cap or pe_ratio or ps_ratio:
                peer_details_list.append(peer_info)
        except Exception as e:
            logger.warning(f"Error fetching data for peer {peer_ticker_symbol}: {e}",
                           exc_info=False)  # exc_info False for brevity
        if len(peer_details_list) >= MAX_COMPETITORS_TO_ANALYZE:
            break
    # ... end of unchanged peer fetching logic

    if not peer_details_list:
        analyzer_instance._financial_data_cache['competitor_analysis'] = {**default_error_summary,
                                                                          "summary": "Could not fetch sufficient data for identified competitors."}
        return default_error_summary["summary"]

    company_name_for_prompt = analyzer_instance.stock_db_entry.company_name or ticker
    k_summaries = analyzer_instance._financial_data_cache.get('10k_summaries', {})
    biz_summary_10k_data = k_summaries.get('business_summary_data', {})
    biz_summary_10k_text = biz_summary_10k_data.get('summary',
                                                    "Business summary from 10-K not available or failed.") if isinstance(
        biz_summary_10k_data, dict) else "N/A"

    prompt_context = (
        f"Company being analyzed: {company_name_for_prompt} ({ticker}).\n"
        f"Its 10-K Business Summary extract: {biz_summary_10k_text[:1000]}...\n\n"  # Truncate for prompt
        f"Identified Competitors and their basic data:\n"
    )
    for peer in peer_details_list:
        mc_str = f"{peer['market_cap']:,.0f}" if peer['market_cap'] else "N/A"
        pe_str = f"{peer['pe_ratio']:.2f}" if peer['pe_ratio'] is not None else "N/A"
        ps_str = f"{peer['ps_ratio']:.2f}" if peer['ps_ratio'] is not None else "N/A"
        prompt_context += f"- {peer['name']} ({peer['ticker']}): Market Cap: {mc_str}, P/E: {pe_str}, P/S: {ps_str}\n"

    competitor_json_structure = """
    {
      "landscapeOverview": "General overview of the competitive landscape...",
      "companyPositioning": "Positioning of the analyzed company relative to competitors...",
      "keyDifferentials": [
        "Key difference 1 (e.g., scale, valuation, focus)...",
        "Key difference 2..."
      ],
      "competitionIntensity": "High|Medium|Low",
      "dataLimitations": "Optional: Note if competitor data was sparse or limited."
    }
    """
    comp_prompt = (
        f"{prompt_context}\n\n"
        f"Instruction: Based on the business summary of {company_name_for_prompt} and the list of its competitors with their financial metrics, "
        f"provide a concise analysis of the competitive landscape. Discuss {company_name_for_prompt}'s market positioning. "
        f"Highlight key differences in scale or valuation. Address competition intensity. Do not invent information. "
        f"{AI_JSON_OUTPUT_INSTRUCTION} Structure it as: {competitor_json_structure}"
    )

    comp_summary_json = analyzer_instance.gemini.generate_text(comp_prompt, output_format="json")
    time.sleep(3)

    final_competitor_analysis_data = {**default_error_summary, "peers_data": peer_details_list}  # Start with default

    if isinstance(comp_summary_json, dict) and not comp_summary_json.get("error"):
        # Update with AI generated fields if they exist
        for key in ["landscapeOverview", "companyPositioning", "keyDifferentials", "competitionIntensity",
                    "dataLimitations"]:
            if key in comp_summary_json:
                final_competitor_analysis_data[key] = comp_summary_json[key]
        final_competitor_analysis_data["summary"] = comp_summary_json.get("landscapeOverview", default_error_summary[
            "summary"])  # Main summary for email
    else:
        logger.error(
            f"AI synthesis of competitor data failed or returned non-JSON for {ticker}. Response: {comp_summary_json}")
        final_competitor_analysis_data["summary"] = "AI synthesis of competitor data failed."
        analyzer_instance.data_quality_warnings.append("Competitor analysis AI synthesis failed.")

    analyzer_instance._financial_data_cache['competitor_analysis'] = final_competitor_analysis_data
    logger.info(f"Competitor analysis JSON summary generated for {ticker}.")
    return final_competitor_analysis_data["summary"]  # Return string for compatibility