# services/news_analyzer/ai_analyzer.py
import time
from core.logging_setup import logger
from core.config import NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION


def _parse_ai_section_from_news_analysis(ai_text, section_header_keywords):
    """
    Parses a specific section from a larger AI-generated text block for news analysis.
    """
    if not ai_text or ai_text.startswith("Error:"):
        return "AI Error or No Text"

    keywords_to_check = [k.lower().strip() for k in (
        [section_header_keywords] if isinstance(section_header_keywords, str) else section_header_keywords)]
    lines = ai_text.split('\n')
    capture_content = False
    section_content_lines = []

    all_known_headers_lower_prefixes = [
        "news summary:", "affected entities:", "affected companies:", "affected stocks/sectors:",
        "mechanism of impact:", "estimated timing & duration:", "estimated timing:",
        "estimated magnitude & direction:", "estimated magnitude/direction:",
        "confidence level:", "investor summary:", "final summary for investor:"
    ]  # Ensure this list is comprehensive for your Gemini prompts

    for line_original in lines:
        line_stripped_lower = line_original.strip().lower()

        matched_current_keyword = next((kw_lower for kw_lower in keywords_to_check if
                                        line_stripped_lower.startswith(kw_lower) or line_stripped_lower == kw_lower),
                                       None)

        if matched_current_keyword:
            capture_content = True
            # Extract content on the same line after the keyword and colon
            content_on_header_line = line_original.strip()
            # Remove the keyword part
            for kw in keywords_to_check:  # Check all variations
                if content_on_header_line.lower().startswith(kw):
                    content_on_header_line = content_on_header_line[len(kw):].strip()
                    break
            if content_on_header_line.startswith(":"):
                content_on_header_line = content_on_header_line[1:].strip()

            if content_on_header_line:
                section_content_lines.append(content_on_header_line)
            continue

        if capture_content:
            # Stop if another known header (not one of the target ones) is encountered
            is_another_known_header = any(line_stripped_lower.startswith(known_header_prefix) for known_header_prefix in
                                          all_known_headers_lower_prefixes if
                                          known_header_prefix not in keywords_to_check)
            if is_another_known_header:
                break
            section_content_lines.append(line_original)  # Append the original line to preserve formatting

    return "\n".join(section_content_lines).strip() if section_content_lines else "Section not found or empty."


def perform_ai_analysis_for_news_item(analyzer_instance, news_event_db_obj):
    """
    Performs AI-driven analysis on the content of a news event.
    """
    headline = news_event_db_obj.event_title
    content_for_analysis = news_event_db_obj.full_article_text
    analysis_source_type = "full article"

    if not content_for_analysis:
        content_for_analysis = headline  # Fallback to headline if no full text
        analysis_source_type = "headline only"
        logger.warning(f"No full article text for '{headline}'. Analyzing based on headline only.")

    # Truncate if content is too long for Gemini
    if len(content_for_analysis) > NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION:
        content_for_analysis = content_for_analysis[
                               :NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION] + "\n... [CONTENT TRUNCATED FOR AI ANALYSIS] ..."
        logger.info(
            f"Truncated news content for '{headline}' to {NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION} chars for Gemini.")
        analysis_source_type += " (truncated)"

    logger.info(f"Analyzing news: '{headline[:70]}...' (using {analysis_source_type})")

    analysis_payload = {
        "key_news_snippets": {"headline": headline, "source_type_used": analysis_source_type}
    }

    # 1. Sentiment Analysis
    sentiment_response = analyzer_instance.gemini.analyze_sentiment_with_reasoning(
        content_for_analysis,
        context=f"News headline for context: {headline}"
    )
    time.sleep(2)  # API delay

    if sentiment_response and not sentiment_response.startswith("Error:"):
        try:
            # Example parsing logic (adjust based on actual Gemini output format)
            # Sentiment: [Positive/Negative/Neutral]\nReasoning: [Explanation]
            parts = sentiment_response.split("Reasoning:", 1)
            sentiment_part = parts[0].split(":", 1)[1].strip() if ":" in parts[0] else parts[0].strip()
            # Take the first word of the sentiment part as the sentiment
            analysis_payload["sentiment"] = sentiment_part.split(' ')[0].split('.')[0].split(',')[0].strip()
            analysis_payload["sentiment_reasoning"] = parts[1].strip() if len(parts) > 1 else sentiment_response
        except Exception as e_parse_sent:
            logger.warning(
                f"Could not parse sentiment response for '{headline}': {sentiment_response}. Error: {e_parse_sent}. Storing raw.")
            analysis_payload["sentiment"] = "Error Parsing"
            analysis_payload["sentiment_reasoning"] = sentiment_response
    else:
        analysis_payload["sentiment"] = "AI Error"
        analysis_payload["sentiment_reasoning"] = sentiment_response or "AI Error: Empty sentiment response."

    # 2. Detailed Impact Analysis
    prompt_detailed_analysis = (
        f"News Headline: \"{headline}\"\n"
        f"News Content (may be truncated or headline only): \"\"\"\n{content_for_analysis}\n\"\"\"\n\n"
        f"Instructions for Analysis:\n"
        f"1. News Summary: Provide a comprehensive yet concise summary of this news article (3-5 key sentences).\n"
        f"2. Affected Entities: Identify specific companies (with ticker symbols if known and highly relevant) and/or specific industry sectors directly or significantly indirectly affected by this news. Explain why briefly for each.\n"
        f"3. Mechanism of Impact: For the primary affected entities, describe how this news will likely affect their fundamentals (e.g., revenue, costs, market share, customer sentiment) or market perception.\n"
        f"4. Estimated Timing & Duration: Estimate the likely timing (e.g., Immediate, Short-term <3mo, Medium-term 3-12mo, Long-term >1yr) and duration of the impact.\n"
        f"5. Estimated Magnitude & Direction: Estimate the potential magnitude (e.g., Low, Medium, High) and direction (e.g., Positive, Negative, Neutral/Mixed) of the impact on the primary affected entities.\n"
        f"6. Confidence Level: State your confidence (High, Medium, Low) in this overall impact assessment, briefly justifying it (e.g., based on clarity of news, directness of impact).\n"
        f"7. Investor Summary: Provide a final 2-sentence summary specifically for an investor, highlighting the most critical implication or takeaway.\n\n"
        f"Structure your response clearly with headings for each point (e.g., 'News Summary:', 'Affected Entities:', etc.)."
    )
    impact_analysis_response = analyzer_instance.gemini.generate_text(prompt_detailed_analysis)
    time.sleep(2)  # API delay

    if impact_analysis_response and not impact_analysis_response.startswith("Error:"):
        analysis_payload["news_summary_detailed"] = _parse_ai_section_from_news_analysis(impact_analysis_response,
                                                                                         "News Summary:")
        # Affected Entities might contain both companies and sectors.
        affected_entities_text = _parse_ai_section_from_news_analysis(impact_analysis_response,
                                                                      ["Affected Entities:", "Affected Companies:",
                                                                       "Affected Stocks/Sectors:"])
        analysis_payload["potential_impact_on_companies"] = affected_entities_text  # Store the full text here

        # Try to parse sectors specifically if "Affected Sectors:" header is used by AI
        sectors_text = _parse_ai_section_from_news_analysis(impact_analysis_response, "Affected Sectors:")
        if sectors_text and not sectors_text.startswith("Section not found") and not sectors_text.startswith(
                "AI Error"):
            analysis_payload["potential_impact_on_sectors"] = sectors_text
        else:  # Fallback if specific sector parsing fails or not present
            analysis_payload[
                "potential_impact_on_sectors"] = affected_entities_text  # It might be mixed in "Affected Entities"

        analysis_payload["mechanism_of_impact"] = _parse_ai_section_from_news_analysis(impact_analysis_response,
                                                                                       "Mechanism of Impact:")
        analysis_payload["estimated_timing_duration"] = _parse_ai_section_from_news_analysis(impact_analysis_response, [
            "Estimated Timing & Duration:", "Estimated Timing:"])
        analysis_payload["estimated_magnitude_direction"] = _parse_ai_section_from_news_analysis(
            impact_analysis_response, ["Estimated Magnitude & Direction:", "Estimated Magnitude/Direction:"])
        analysis_payload["confidence_of_assessment"] = _parse_ai_section_from_news_analysis(impact_analysis_response,
                                                                                            "Confidence Level:")
        analysis_payload["summary_for_email"] = _parse_ai_section_from_news_analysis(impact_analysis_response,
                                                                                     ["Investor Summary:",
                                                                                      "Final Summary for Investor:"])
    else:
        logger.error(f"Gemini failed to provide detailed impact analysis for '{headline}': {impact_analysis_response}")
        analysis_payload[
            "news_summary_detailed"] = impact_analysis_response or "AI Error: Empty impact analysis response."
        # Set other fields to AI Error or similar indication
        error_indicator = analysis_payload["news_summary_detailed"]
        analysis_payload["potential_impact_on_companies"] = error_indicator
        analysis_payload["potential_impact_on_sectors"] = error_indicator
        analysis_payload["mechanism_of_impact"] = error_indicator
        analysis_payload["estimated_timing_duration"] = error_indicator
        analysis_payload["estimated_magnitude_direction"] = error_indicator
        analysis_payload["confidence_of_assessment"] = error_indicator
        analysis_payload["summary_for_email"] = error_indicator

    return analysis_payload