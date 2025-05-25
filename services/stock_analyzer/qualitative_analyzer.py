# services/stock_analyzer/qualitative_analyzer.py
import time
from core.logging_setup import logger
from api_clients import extract_S1_text_sections  # Re-check if S1 or TEN_K version
from core.config import (
    TEN_K_KEY_SECTIONS, SUMMARIZATION_CHUNK_SIZE_CHARS,
    SUMMARIZATION_CHUNK_OVERLAP_CHARS, SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS,
    MAX_COMPETITORS_TO_ANALYZE
)
from .helpers import safe_get_float


def _summarize_text_chunked(analyzer_instance, text_to_summarize, base_context, section_specific_instruction,
                            company_name_ticker_prompt):
    gemini_client = analyzer_instance.gemini
    if not text_to_summarize or not isinstance(text_to_summarize, str) or not text_to_summarize.strip():
        return "No text provided for summarization.", 0

    text_len = len(text_to_summarize)
    logger.info(f"Summarizing '{base_context}' for {company_name_ticker_prompt}, original length: {text_len} chars.")

    if text_len < SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS:  # If short enough, summarize directly
        logger.info(
            f"Section length {text_len} is within single-pass limit ({SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS}). Summarizing directly.")
        summary = gemini_client.summarize_text_with_context(
            text_to_summarize,
            f"{base_context} for {company_name_ticker_prompt}.",
            section_specific_instruction
        )
        time.sleep(2)  # API call delay
        return (summary if summary and not summary.startswith(
            "Error:") else f"AI summary error or no content for '{base_context}'."), text_len

    # Chunked summarization
    logger.info(f"Section length {text_len} exceeds single-pass limit. Applying chunked summarization "
                f"(Chunk size: {SUMMARIZATION_CHUNK_SIZE_CHARS}, Overlap: {SUMMARIZATION_CHUNK_OVERLAP_CHARS}).")
    chunks = []
    start = 0
    while start < text_len:
        end = start + SUMMARIZATION_CHUNK_SIZE_CHARS
        chunks.append(text_to_summarize[start:end])
        if end >= text_len or SUMMARIZATION_CHUNK_OVERLAP_CHARS >= SUMMARIZATION_CHUNK_SIZE_CHARS:  # Prevent negative start
            start = end
        else:
            start = end - SUMMARIZATION_CHUNK_OVERLAP_CHARS

    chunk_summaries = []
    for i, chunk in enumerate(chunks):
        logger.info(
            f"Summarizing chunk {i + 1}/{len(chunks)} for '{base_context}' of {company_name_ticker_prompt} (length: {len(chunk)} chars).")
        summary = gemini_client.summarize_text_with_context(
            chunk,
            f"This is chunk {i + 1} of {len(chunks)} from the '{base_context}' section for {company_name_ticker_prompt}.",
            f"Summarize this chunk. Focus on key facts and figures relevant to: {section_specific_instruction}"
        )
        time.sleep(2)  # API call delay
        chunk_summaries.append(summary if summary and not summary.startswith(
            "Error:") else f"[AI error or no content for chunk {i + 1} of '{base_context}']")

    if not chunk_summaries:
        return f"No summaries generated from chunks for '{base_context}'.", text_len

    concatenated_summaries = "\n\n---\n\n".join(chunk_summaries)
    logger.info(f"Concatenated chunk summaries length for '{base_context}': {len(concatenated_summaries)} chars.")

    if not concatenated_summaries.strip() or all("[AI error" in s for s in chunk_summaries):
        return f"Failed to generate summaries for any chunk of '{base_context}'.", text_len

    # If concatenated summaries are too long, summarize the summaries
    if len(concatenated_summaries) > SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS:
        logger.info(
            f"Concatenated summaries for '{base_context}' too long. Performing a final 'summary of summaries' pass.")
        final_summary = gemini_client.summarize_text_with_context(
            concatenated_summaries,
            f"The following are collated summaries from different parts of the '{base_context}' section for {company_name_ticker_prompt}.",
            f"Synthesize these individual chunk summaries into a single, cohesive overview of the '{base_context}', "
            f"maintaining factual accuracy and addressing the original goal: {section_specific_instruction}."
        )
        time.sleep(2)  # API call delay
        return (final_summary if final_summary and not final_summary.startswith(
            "Error:") else f"AI error in final summary pass for '{base_context}'."), text_len
    else:
        return concatenated_summaries, text_len


def fetch_and_summarize_10k_data(analyzer_instance):
    ticker = analyzer_instance.ticker
    logger.info(f"Fetching and attempting to summarize latest 10-K for {ticker}")
    summary_results = {"qualitative_sources_summary": {}}  # Initialize the sub-dictionary

    if not analyzer_instance.stock_db_entry or not analyzer_instance.stock_db_entry.cik:
        logger.warning(f"No CIK for {ticker}. Cannot fetch 10-K.")
        return summary_results

    # Try 10-K first, then 10-K/A
    filing_url = analyzer_instance.sec_edgar.get_filing_document_url(analyzer_instance.stock_db_entry.cik, "10-K")
    time.sleep(0.5)
    if not filing_url:
        logger.info(f"No recent 10-K found for {ticker}, trying 10-K/A.")
        filing_url = analyzer_instance.sec_edgar.get_filing_document_url(analyzer_instance.stock_db_entry.cik, "10-K/A")
        time.sleep(0.5)

    if not filing_url:
        logger.warning(f"No 10-K or 10-K/A URL found for {ticker} (CIK: {analyzer_instance.stock_db_entry.cik})")
        return summary_results

    summary_results["qualitative_sources_summary"]["10k_filing_url_used"] = filing_url
    text_content = analyzer_instance.sec_edgar.get_filing_text(filing_url)

    if not text_content:
        logger.warning(f"Failed to fetch/load 10-K text from {filing_url}")
        return summary_results

    logger.info(f"Fetched 10-K text (length: {len(text_content)}) for {ticker}. Extracting and summarizing sections.")
    sections = extract_S1_text_sections(text_content,
                                        TEN_K_KEY_SECTIONS)  # TEN_K_KEY_SECTIONS is alias for S1_KEY_SECTIONS

    company_name_for_prompt = analyzer_instance.stock_db_entry.company_name or ticker

    section_details = {
        "business": ("Business (Item 1)",
                     "Summarize the company's core business operations, primary products/services, revenue generation model, key customer segments, and primary markets. Highlight any recent strategic shifts mentioned."),
        "risk_factors": ("Risk Factors (Item 1A)",
                         "Identify and summarize the 3-5 most significant and company-specific risk factors disclosed. Focus on operational and strategic risks rather than generic market risks. Briefly explain the potential impact of each."),
        "mda": ("Management's Discussion and Analysis (Item 7)",
                "Summarize key insights into financial performance drivers (revenue, costs, profitability), financial condition (liquidity, capital resources), and management's outlook or significant focus areas. Note any discussion on margin pressures or segment performance changes.")
    }

    for section_key, (prompt_section_name, specific_instruction) in section_details.items():
        section_text = sections.get(section_key)
        if not section_text:
            logger.warning(f"Section '{prompt_section_name}' not found in 10-K for {ticker}.")
            summary_results[f"{section_key}_summary"] = "Section not found in 10-K document."
            summary_results["qualitative_sources_summary"][f"{section_key}_10k_source_length"] = 0
            continue

        summary, source_len = _summarize_text_chunked(analyzer_instance, section_text, prompt_section_name,
                                                      specific_instruction, f"{company_name_for_prompt} ({ticker})")
        summary_results[f"{section_key}_summary"] = summary
        summary_results["qualitative_sources_summary"][f"{section_key}_10k_source_length"] = source_len
        logger.info(
            f"Summary for '{prompt_section_name}' (source length {source_len}): {summary[:150].replace(chr(10), ' ')}...")

    # Economic Moat Analysis (derived from business and risk summaries)
    biz_summary_str = summary_results.get("business_summary", "")
    mda_summary_str = summary_results.get("mda_summary", "")  # For industry trends
    risk_summary_str = summary_results.get("risk_factors_summary", "")

    # Clean up potentially errored summaries for concatenation
    if biz_summary_str.startswith(("Section not found", "AI summary error")): biz_summary_str = ""
    if mda_summary_str.startswith(("Section not found", "AI summary error")): mda_summary_str = ""
    if risk_summary_str.startswith(("Section not found", "AI summary error")): risk_summary_str = ""

    moat_input_text = (f"Business Summary:\n{biz_summary_str}\n\nRisk Factors Summary:\n{risk_summary_str}").strip()
    if moat_input_text and (biz_summary_str or risk_summary_str):  # Check if there's meaningful input
        moat_prompt = (
            f"Analyze the primary economic moats (e.g., brand strength, network effects, switching costs, intangible assets like patents/IP, cost advantages from scale/process) "
            f"for {company_name_for_prompt} ({ticker}), based on the following summaries from its 10-K:\n\n{moat_input_text}\n\n"
            f"Provide a concise analysis of its key economic moats. For each identified moat, briefly explain the evidence from the text and assess its perceived strength (e.g., Very Strong, Strong, Moderate, Weak). If certain moats are not strongly evident, state that."
        )
        moat_summary = analyzer_instance.gemini.generate_text(moat_prompt)
        time.sleep(3)  # API call delay
        summary_results["economic_moat_summary"] = moat_summary if moat_summary and not moat_summary.startswith(
            "Error:") else "AI analysis for economic moat failed or no input."
    else:
        summary_results["economic_moat_summary"] = "Insufficient input from 10-K summaries for economic moat analysis."

    # Industry Trends Analysis
    industry_context_text = (
        f"Company: {company_name_for_prompt} ({ticker})\n"
        f"Industry: {analyzer_instance.stock_db_entry.industry or 'Not Specified'}\n"
        f"Sector: {analyzer_instance.stock_db_entry.sector or 'Not Specified'}\n\n"
        f"Business Summary (from 10-K):\n{biz_summary_str}\n\n"
        f"MD&A Highlights (from 10-K):\n{mda_summary_str}"
    ).strip()

    if biz_summary_str:  # Requires business summary at least
        industry_prompt = (
            f"Based on the provided information for {company_name_for_prompt} ({ticker}):\n\n{industry_context_text}\n\n"
            f"Analyze key industry trends relevant to this company. Discuss significant opportunities and challenges within this industry context. "
            f"How does the company appear to be positioned to capitalize on opportunities and mitigate challenges, based on its business summary and MD&A highlights? Be specific and use information from the text."
        )
        industry_summary = analyzer_instance.gemini.generate_text(industry_prompt)
        time.sleep(3)  # API call delay
        summary_results[
            "industry_trends_summary"] = industry_summary if industry_summary and not industry_summary.startswith(
            "Error:") else "AI analysis for industry trends failed or no input."
    else:
        summary_results[
            "industry_trends_summary"] = "Insufficient input from 10-K (Business Summary missing) for industry analysis."

    # Rename mda_summary to management_assessment_summary for consistency with DB model
    if "mda_summary" in summary_results:
        summary_results["management_assessment_summary"] = summary_results.pop("mda_summary")
        if "mda_10k_source_length" in summary_results["qualitative_sources_summary"]:
            summary_results["qualitative_sources_summary"]["management_assessment_10k_source_length"] = summary_results[
                "qualitative_sources_summary"].pop("mda_10k_source_length")

    logger.info(f"10-K qualitative summaries and AI interpretations generated for {ticker}.")
    analyzer_instance._financial_data_cache['10k_summaries'] = summary_results
    return summary_results


def fetch_and_analyze_competitors(analyzer_instance):
    ticker = analyzer_instance.ticker
    logger.info(f"Fetching and analyzing competitor data for {ticker}...")
    competitor_analysis_summary_text = "Competitor analysis not performed or failed."

    peers_data_finnhub = analyzer_instance.finnhub.get_company_peers(ticker)
    time.sleep(1.5)

    if not peers_data_finnhub or not isinstance(peers_data_finnhub, list) or not peers_data_finnhub[0]:
        logger.warning(f"No direct peer data found from Finnhub for {ticker}.")
        analyzer_instance._financial_data_cache['competitor_analysis'] = {
            "summary": "No peer data found from primary source (Finnhub).", "peers_data": []}
        return "No peer data found from primary source (Finnhub)."

    # Finnhub might return a list containing a list of peers, or just a list of peers
    if isinstance(peers_data_finnhub[0], list):
        peers_data_finnhub = peers_data_finnhub[0]

    # Filter out the current ticker and limit number of peers
    peer_tickers = [p for p in peers_data_finnhub if p and p.upper() != ticker.upper()][:MAX_COMPETITORS_TO_ANALYZE]

    if not peer_tickers:
        logger.info(f"No distinct competitor tickers found after filtering for {ticker}.")
        analyzer_instance._financial_data_cache['competitor_analysis'] = {
            "summary": "No distinct competitor tickers identified.", "peers_data": []}
        return "No distinct competitor tickers identified."

    logger.info(f"Identified peers for {ticker}: {peer_tickers}. Fetching basic data for comparison.")
    peer_details_list = []
    for peer_ticker_symbol in peer_tickers:
        try:
            logger.debug(f"Fetching basic data for peer: {peer_ticker_symbol}")
            # FMP Profile for name and market cap
            peer_profile_fmp_list = analyzer_instance.fmp.get_company_profile(peer_ticker_symbol)
            time.sleep(1.5)
            peer_profile_fmp = peer_profile_fmp_list[0] if peer_profile_fmp_list and isinstance(peer_profile_fmp_list,
                                                                                                list) and \
                                                           peer_profile_fmp_list[0] else {}

            # FMP Key Metrics for P/E, P/S
            peer_metrics_fmp_list = analyzer_instance.fmp.get_key_metrics(peer_ticker_symbol, period="annual",
                                                                          limit=1)  # TTM might be better if available
            time.sleep(1.5)
            peer_metrics_fmp = peer_metrics_fmp_list[0] if peer_metrics_fmp_list and isinstance(peer_metrics_fmp_list,
                                                                                                list) and \
                                                           peer_metrics_fmp_list[0] else {}

            peer_fh_basics = {}
            # Fallback to Finnhub if FMP metrics are missing
            if not peer_metrics_fmp.get("peRatio") or not peer_metrics_fmp.get("priceSalesRatio"):
                peer_fh_basics_data = analyzer_instance.finnhub.get_basic_financials(peer_ticker_symbol)
                time.sleep(1.5)
                peer_fh_basics = peer_fh_basics_data.get("metric", {}) if peer_fh_basics_data else {}

            peer_name = peer_profile_fmp.get("companyName", peer_ticker_symbol)
            market_cap = safe_get_float(peer_profile_fmp, "mktCap")
            pe_ratio = safe_get_float(peer_metrics_fmp, "peRatio") or safe_get_float(peer_fh_basics, "peTTM")
            ps_ratio = safe_get_float(peer_metrics_fmp, "priceSalesRatio") or safe_get_float(peer_fh_basics, "psTTM")

            peer_info = {"ticker": peer_ticker_symbol, "name": peer_name, "market_cap": market_cap,
                         "pe_ratio": pe_ratio, "ps_ratio": ps_ratio}
            if peer_name != peer_ticker_symbol or market_cap or pe_ratio or ps_ratio:  # Add if any meaningful data was found
                peer_details_list.append(peer_info)
        except Exception as e:
            logger.warning(f"Error fetching data for peer {peer_ticker_symbol}: {e}", exc_info=True)
        if len(peer_details_list) >= MAX_COMPETITORS_TO_ANALYZE:
            break

    if not peer_details_list:
        competitor_analysis_summary_text = "Could not fetch sufficient data for identified competitors."
        analyzer_instance._financial_data_cache['competitor_analysis'] = {"summary": competitor_analysis_summary_text,
                                                                          "peers_data": []}
        return competitor_analysis_summary_text

    # AI Synthesis of Competitor Data
    company_name_for_prompt = analyzer_instance.stock_db_entry.company_name or ticker
    k_summaries = analyzer_instance._financial_data_cache.get('10k_summaries', {})
    biz_summary_10k = k_summaries.get('business_summary', 'N/A')
    if biz_summary_10k.startswith("Section not found") or biz_summary_10k.startswith("AI summary error"):
        biz_summary_10k = "Business summary from 10-K not available or failed."

    prompt_context = (
        f"Company being analyzed: {company_name_for_prompt} ({ticker}).\n"
        f"Its 10-K Business Summary: {biz_summary_10k}\n\n"
        f"Identified Competitors and their basic data:\n"
    )
    for peer in peer_details_list:
        mc_str = f"{peer['market_cap']:,.0f}" if peer['market_cap'] else "N/A"
        pe_str = f"{peer['pe_ratio']:.2f}" if peer['pe_ratio'] is not None else "N/A"
        ps_str = f"{peer['ps_ratio']:.2f}" if peer['ps_ratio'] is not None else "N/A"
        prompt_context += f"- {peer['name']} ({peer['ticker']}): Market Cap: {mc_str}, P/E: {pe_str}, P/S: {ps_str}\n"

    comp_prompt = (
        f"{prompt_context}\n\n"
        f"Instruction: Based on the business summary of {company_name_for_prompt} and the list of its competitors with their financial metrics, "
        f"provide a concise analysis of the competitive landscape. Discuss {company_name_for_prompt}'s market positioning relative to these competitors. "
        f"Highlight any key differences in scale (market cap) or valuation (P/E, P/S) that stand out. Address the intensity of competition. "
        f"Do not invent information not present. If competitor data is sparse, acknowledge that. This summary should complement, not merely repeat, the 10-K business description."
    )

    comp_summary_ai = analyzer_instance.gemini.generate_text(comp_prompt)
    time.sleep(3)  # API call delay

    if comp_summary_ai and not comp_summary_ai.startswith("Error:"):
        competitor_analysis_summary_text = comp_summary_ai
    else:
        competitor_analysis_summary_text = "AI synthesis of competitor data failed. Basic peer data might be available in snapshot."
        analyzer_instance.data_quality_warnings.append("Competitor analysis AI synthesis failed.")

    analyzer_instance._financial_data_cache['competitor_analysis'] = {
        "summary": competitor_analysis_summary_text,
        "peers_data": peer_details_list
    }
    logger.info(f"Competitor analysis summary generated for {ticker}.")
    return competitor_analysis_summary_text