# services/news_analyzer/ai_analyzer.py
import time
import json
from core.logging_setup import logger
from core.config import NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION, AI_JSON_OUTPUT_INSTRUCTION


def perform_ai_analysis_for_news_item(analyzer_instance, news_event_db_obj):
    headline = news_event_db_obj.event_title
    content_for_analysis = news_event_db_obj.full_article_text
    analysis_source_type = "full article"

    if not content_for_analysis:
        content_for_analysis = headline
        analysis_source_type = "headline only"
        logger.warning(f"No full article text for '{headline}'. Analyzing based on headline only.")

    if len(content_for_analysis) > NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION:
        content_for_analysis = content_for_analysis[
                               :NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION] + "\n... [CONTENT TRUNCATED] ..."
        logger.info(f"Truncated news content for '{headline}' for Gemini.")
        analysis_source_type += " (truncated)"

    logger.info(f"Analyzing news (JSON): '{headline[:70]}...' (using {analysis_source_type})")
    analysis_payload = {"key_news_snippets": {"headline": headline, "source_type_used": analysis_source_type}}

    # 1. Sentiment Analysis
    sentiment_json_structure = """
    {
      "sentiment": "Positive|Negative|Neutral",
      "reasoning": "A brief 1-2 sentence explanation, citing specific phrases from the text if possible."
    }
    """
    sentiment_prompt = (
        f"Analyze the sentiment of the following text. Context: News headline '{headline}'.\n\n"
        f"Text to Analyze:\n\"\"\"\n{content_for_analysis}\n\"\"\"\n\n"
        f"{AI_JSON_OUTPUT_INSTRUCTION} Structure it as: {sentiment_json_structure}"
    )
    sentiment_response_data = analyzer_instance.gemini.generate_text(sentiment_prompt, output_format="json")
    time.sleep(1)

    if isinstance(sentiment_response_data, dict) and not sentiment_response_data.get("error"):
        analysis_payload["sentiment"] = sentiment_response_data.get("sentiment", "Error Parsing")
        analysis_payload["sentiment_reasoning"] = sentiment_response_data.get("reasoning",
                                                                              "AI Error: Reasoning not provided.")
    else:
        logger.warning(
            f"Sentiment analysis failed or returned non-JSON for '{headline}'. Response: {sentiment_response_data}")
        analysis_payload["sentiment"] = "AI Error"
        analysis_payload["sentiment_reasoning"] = str(sentiment_response_data)

    # 2. Detailed Impact Analysis
    impact_json_structure = """
    {
      "newsSummary": "Comprehensive yet concise summary (3-5 key sentences).",
      "affectedEntities": [
        {"entityName": "Company Name", "tickerSymbol": "TICKER (if known and highly relevant)", "explanation": "Brief explanation of impact."},
        {"sectorName": "Industry Sector", "explanation": "Brief explanation of impact."}
      ],
      "mechanismOfImpact": "How news likely affects fundamentals (revenue, costs, market share) or market perception.",
      "estimatedTimingAndDuration": {"timing": "Immediate|Short-term (<3mo)|Medium-term (3-12mo)|Long-term (>1yr)", "duration": "Brief|Extended|Ongoing|etc."},
      "estimatedMagnitudeAndDirection": {"magnitude": "Low|Medium|High", "direction": "Positive|Negative|Neutral/Mixed"},
      "confidenceLevel": "High|Medium|Low",
      "confidenceJustification": "Brief justification for confidence level (e.g., clarity of news, directness of impact).",
      "investorSummary": "Final 2-sentence summary for an investor: most critical implication/takeaway."
    }
    """
    prompt_detailed_analysis = (
        f"News Headline: \"{headline}\"\n"
        f"News Content (may be truncated or headline only): \"\"\"\n{content_for_analysis}\n\"\"\"\n\n"
        f"Perform a detailed impact analysis. {AI_JSON_OUTPUT_INSTRUCTION}\n"
        f"Structure your JSON response as follows: {impact_json_structure}"
    )
    impact_analysis_response_data = analyzer_instance.gemini.generate_text(prompt_detailed_analysis,
                                                                           output_format="json")
    time.sleep(1)

    if isinstance(impact_analysis_response_data, dict) and not impact_analysis_response_data.get("error"):
        analysis_payload["news_summary_detailed"] = impact_analysis_response_data.get("newsSummary", "AI Error")

        # Handle affected entities carefully - it's a list of dicts
        affected_entities = impact_analysis_response_data.get("affectedEntities", [])
        if isinstance(affected_entities, list):
            analysis_payload["potential_impact_on_companies"] = json.dumps(
                [e for e in affected_entities if "entityName" in e])  # Store as JSON string
            analysis_payload["potential_impact_on_sectors"] = json.dumps(
                [e for e in affected_entities if "sectorName" in e])  # Store as JSON string
        else:
            analysis_payload["potential_impact_on_companies"] = "[]"  # Empty JSON array string
            analysis_payload["potential_impact_on_sectors"] = "[]"

        analysis_payload["mechanism_of_impact"] = impact_analysis_response_data.get("mechanismOfImpact", "AI Error")

        timing_duration = impact_analysis_response_data.get("estimatedTimingAndDuration", {})
        analysis_payload[
            "estimated_timing_duration"] = f"Timing: {timing_duration.get('timing', 'N/A')}, Duration: {timing_duration.get('duration', 'N/A')}" if isinstance(
            timing_duration, dict) else "AI Error"

        mag_direction = impact_analysis_response_data.get("estimatedMagnitudeAndDirection", {})
        analysis_payload[
            "estimated_magnitude_direction"] = f"Magnitude: {mag_direction.get('magnitude', 'N/A')}, Direction: {mag_direction.get('direction', 'N/A')}" if isinstance(
            mag_direction, dict) else "AI Error"

        analysis_payload["confidence_of_assessment"] = impact_analysis_response_data.get("confidenceLevel", "AI Error")
        if impact_analysis_response_data.get("confidenceJustification"):
            analysis_payload[
                "confidence_of_assessment"] += f" (Justification: {impact_analysis_response_data.get('confidenceJustification')})"

        analysis_payload["summary_for_email"] = impact_analysis_response_data.get("investorSummary", "AI Error")
    else:
        logger.error(
            f"Gemini failed to provide detailed impact analysis JSON for '{headline}': {impact_analysis_response_data}")
        error_indicator = str(impact_analysis_response_data)
        analysis_payload["news_summary_detailed"] = error_indicator
        analysis_payload["potential_impact_on_companies"] = "[]"
        analysis_payload["potential_impact_on_sectors"] = "[]"
        analysis_payload["mechanism_of_impact"] = error_indicator
        analysis_payload["estimated_timing_duration"] = error_indicator
        analysis_payload["estimated_magnitude_direction"] = error_indicator
        analysis_payload["confidence_of_assessment"] = error_indicator
        analysis_payload["summary_for_email"] = error_indicator

    return analysis_payload