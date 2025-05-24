# ipo_analyzer.py
import time
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser  # For flexible date parsing

from api_clients import FinnhubClient, GeminiAPIClient, SECEDGARClient, extract_S1_text_sections
from database import SessionLocal, get_db_session
from models import IPO, IPOAnalysis
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError
from config import (
    S1_KEY_SECTIONS, MAX_S1_SECTION_LENGTH_FOR_GEMINI,
    IPO_ANALYSIS_REANALYZE_DAYS, MAX_GEMINI_TEXT_LENGTH
)


class IPOAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()
        self.db_session = next(get_db_session())

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close()
                logger.debug("DB session closed in IPOAnalyzer.")
            except Exception as e_close:
                logger.warning(f"Error closing session in IPOAnalyzer: {e_close}")

    def _parse_ipo_date(self, date_str):
        if not date_str:
            return None
        try:
            # Handle various common date formats, make it timezone naive for DB storage as Date
            return date_parser.parse(date_str).date()
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse IPO date string '{date_str}': {e}")
            return None

    def fetch_upcoming_ipos(self):
        logger.info("Fetching upcoming IPOs using Finnhub...")
        ipos_data_to_process = []
        today = datetime.now(timezone.utc)
        from_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')  # Look back for recently filed/priced
        to_date = (today + timedelta(days=180)).strftime('%Y-%m-%d')  # Look further ahead

        finnhub_response = self.finnhub.get_ipo_calendar(from_date=from_date, to_date=to_date)
        actual_ipo_list = []

        if finnhub_response and isinstance(finnhub_response, dict) and "ipoCalendar" in finnhub_response:
            actual_ipo_list = finnhub_response["ipoCalendar"]
            if not isinstance(actual_ipo_list, list):
                logger.warning(f"Finnhub response 'ipoCalendar' field is not a list. Found: {type(actual_ipo_list)}")
                actual_ipo_list = []
            elif not actual_ipo_list:
                logger.info("Finnhub 'ipoCalendar' list is empty for the current period.")
        elif finnhub_response is None:  # API call itself failed
            logger.error("Failed to fetch IPOs from Finnhub (API call failed or returned None).")
        else:  # Unexpected format or just no IPOs
            logger.info(f"No IPOs found or unexpected format from Finnhub. Response: {str(finnhub_response)[:200]}")

        if actual_ipo_list:
            for ipo_api_data in actual_ipo_list:
                if not isinstance(ipo_api_data, dict):  # Skip non-dict items
                    logger.warning(f"Skipping non-dictionary item in Finnhub IPO calendar: {ipo_api_data}")
                    continue

                price_range_raw = ipo_api_data.get("price")
                price_low, price_high = None, None

                if isinstance(price_range_raw, str) and price_range_raw.strip():
                    if '-' in price_range_raw:
                        parts = price_range_raw.split('-', 1)
                        try:
                            price_low = float(parts[0].strip())
                        except (ValueError, TypeError):
                            price_low = None
                        try:
                            price_high = float(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else price_low
                        except (ValueError, TypeError):
                            price_high = price_low if price_low is not None else None
                    else:  # Single price
                        try:
                            price_low = float(price_range_raw.strip())
                            price_high = price_low
                        except (ValueError, TypeError):
                            price_low = price_high = None
                elif isinstance(price_range_raw, (float, int)):
                    price_low = float(price_range_raw)
                    price_high = float(price_range_raw)

                parsed_date = self._parse_ipo_date(ipo_api_data.get("date"))

                ipos_data_to_process.append({
                    "company_name": ipo_api_data.get("name"),
                    "symbol": ipo_api_data.get("symbol"),
                    "ipo_date_str": ipo_api_data.get("date"),
                    "ipo_date": parsed_date,
                    "expected_price_range_low": price_low,
                    "expected_price_range_high": price_high,
                    "exchange": ipo_api_data.get("exchange"),
                    "status": ipo_api_data.get("status"),
                    "offered_shares": ipo_api_data.get("numberOfShares"),
                    "total_shares_value": ipo_api_data.get("totalSharesValue"),
                    "source_api": "Finnhub",
                    "raw_data": ipo_api_data
                })
            logger.info(f"Successfully parsed {len(ipos_data_to_process)} IPOs from Finnhub API response.")

        unique_ipos = []
        seen_keys = set()
        for ipo_info in ipos_data_to_process:
            key_name = ipo_info.get("company_name", "").strip().lower() if ipo_info.get(
                "company_name") else "unknown_company"
            key_symbol = ipo_info.get("symbol", "").strip().upper() if ipo_info.get("symbol") else "NO_SYMBOL"
            key_date = ipo_info.get("ipo_date_str", "")  # Use original string date for precise API matching

            unique_tuple = (key_name, key_symbol, key_date)

            if unique_tuple not in seen_keys:
                unique_ipos.append(ipo_info)
                seen_keys.add(unique_tuple)
            else:
                logger.debug(
                    f"Duplicate IPO based on key '{unique_tuple}' found, skipping: {ipo_info.get('company_name')}")

        logger.info(f"Total unique IPOs fetched after deduplication: {len(unique_ipos)}")
        return unique_ipos

    def _get_or_create_ipo_db_entry(self, ipo_data_from_fetch):
        self._ensure_ipo_db_entry_session_active(ipo_data_from_fetch.get('company_name', 'Unknown IPO'))

        ipo_db_entry = None
        # Try finding by symbol first, as it's more unique if present for listed/priced IPOs
        if ipo_data_from_fetch.get("symbol"):
            ipo_db_entry = self.db_session.query(IPO).filter(IPO.symbol == ipo_data_from_fetch["symbol"]).first()

        # If not found by symbol, or no symbol, try by name and original date string (calendar entries)
        if not ipo_db_entry and ipo_data_from_fetch.get("company_name") and ipo_data_from_fetch.get("ipo_date_str"):
            ipo_db_entry = self.db_session.query(IPO).filter(
                IPO.company_name == ipo_data_from_fetch["company_name"],
                IPO.ipo_date_str == ipo_data_from_fetch["ipo_date_str"]
            ).first()

        cik_to_store = ipo_data_from_fetch.get("cik")  # CIK might be passed if pre-fetched
        if not cik_to_store and ipo_data_from_fetch.get("symbol"):
            cik_to_store = self.sec_edgar.get_cik_by_ticker(ipo_data_from_fetch["symbol"])
        elif not cik_to_store and ipo_db_entry and ipo_db_entry.symbol and not ipo_db_entry.cik:  # If updating and CIK was missing
            cik_to_store = self.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol)

        if not ipo_db_entry:
            logger.info(f"IPO '{ipo_data_from_fetch.get('company_name')}' not found in DB, creating new entry.")
            ipo_db_entry = IPO(
                company_name=ipo_data_from_fetch.get("company_name"),
                symbol=ipo_data_from_fetch.get("symbol"),
                ipo_date_str=ipo_data_from_fetch.get("ipo_date_str"),
                ipo_date=ipo_data_from_fetch.get("ipo_date"),
                expected_price_range_low=ipo_data_from_fetch.get("expected_price_range_low"),
                expected_price_range_high=ipo_data_from_fetch.get("expected_price_range_high"),
                offered_shares=ipo_data_from_fetch.get("offered_shares"),
                total_shares_value=ipo_data_from_fetch.get("total_shares_value"),
                exchange=ipo_data_from_fetch.get("exchange"),
                status=ipo_data_from_fetch.get("status"),
                cik=cik_to_store
            )
            self.db_session.add(ipo_db_entry)
            try:
                self.db_session.commit()
                self.db_session.refresh(ipo_db_entry)
                logger.info(
                    f"Created IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}, CIK: {ipo_db_entry.cik})")
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating IPO entry for '{ipo_data_from_fetch.get('company_name')}': {e}",
                             exc_info=True)
                return None  # Critical failure
        else:  # Update existing
            logger.info(
                f"Found existing IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}). Checking for updates.")
            updated = False
            fields_to_update = [
                "company_name", "symbol", "ipo_date_str", "ipo_date", "expected_price_range_low",
                "expected_price_range_high", "offered_shares", "total_shares_value", "exchange", "status"
            ]
            for field in fields_to_update:
                new_value = ipo_data_from_fetch.get(field)
                current_value = getattr(ipo_db_entry, field)
                if new_value is not None and current_value != new_value:  # Only update if new value exists and is different
                    setattr(ipo_db_entry, field, new_value)
                    updated = True

            if cik_to_store and ipo_db_entry.cik != cik_to_store:
                ipo_db_entry.cik = cik_to_store
                updated = True

            if updated:
                try:
                    self.db_session.commit()
                    self.db_session.refresh(ipo_db_entry)
                    logger.info(
                        f"Updated existing IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}).")
                except SQLAlchemyError as e:
                    self.db_session.rollback()
                    logger.error(f"Error updating IPO entry for '{ipo_db_entry.company_name}': {e}", exc_info=True)
        return ipo_db_entry

    def _fetch_s1_data(self, ipo_db_entry):
        if not ipo_db_entry: return None, None
        target_cik = ipo_db_entry.cik  # Use CIK from DB entry

        if not target_cik:  # If CIK not in DB, try to get it via symbol
            if ipo_db_entry.symbol:
                target_cik = self.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol)
                if target_cik:
                    ipo_db_entry.cik = target_cik  # Update DB
                    try:
                        self.db_session.commit()
                        logger.info(f"Found and updated CIK for {ipo_db_entry.company_name} to {target_cik}.")
                    except SQLAlchemyError as e:
                        self.db_session.rollback()
                        logger.error(f"Failed to update CIK for {ipo_db_entry.company_name}: {e}")
                else:
                    logger.warning(
                        f"No CIK found via symbol {ipo_db_entry.symbol} for IPO '{ipo_db_entry.company_name}'.")
                    return None, None
            else:  # No CIK and no symbol
                logger.warning(f"No CIK or symbol for IPO '{ipo_db_entry.company_name}'. Cannot fetch S-1/F-1.")
                return None, None

        logger.info(f"Attempting to fetch S-1/F-1 filing for {ipo_db_entry.company_name} (CIK: {target_cik})")
        # Try S-1, S-1/A, F-1, F-1/A in order of preference for original/latest comprehensive filing
        filing_types_to_try = ["S-1", "S-1/A", "F-1", "F-1/A"]
        s1_url = None
        for form_type in filing_types_to_try:
            s1_url = self.sec_edgar.get_filing_document_url(cik=target_cik, form_type=form_type)
            if s1_url:
                logger.info(f"Found {form_type} filing URL for {ipo_db_entry.company_name}: {s1_url}")
                break

        if s1_url:
            if ipo_db_entry.s1_filing_url != s1_url:  # Store/update the found URL
                ipo_db_entry.s1_filing_url = s1_url
                try:
                    self.db_session.commit()
                except SQLAlchemyError as e:
                    self.db_session.rollback()

            filing_text = self.sec_edgar.get_filing_text(s1_url)
            if filing_text:
                logger.info(
                    f"Fetched S-1/F-1 text content (length: {len(filing_text)}) for {ipo_db_entry.company_name}")
                return filing_text, s1_url
            else:
                logger.warning(f"Failed to fetch S-1/F-1 text content from {s1_url}")
        else:
            logger.warning(f"No S-1 or F-1 type filing URL found for {ipo_db_entry.company_name} (CIK: {target_cik}).")
        return None, None

    def analyze_single_ipo(self, ipo_data_from_fetch):
        ipo_identifier = ipo_data_from_fetch.get("company_name") or ipo_data_from_fetch.get("symbol")
        if not ipo_identifier:
            logger.error(f"Cannot analyze IPO, missing company_name and symbol: {ipo_data_from_fetch}")
            return None  # Cannot proceed
        logger.info(f"Starting analysis for IPO: {ipo_identifier} from source {ipo_data_from_fetch.get('source_api')}")

        self._ensure_ipo_db_entry_session_active(ipo_identifier)
        ipo_db_entry = self._get_or_create_ipo_db_entry(ipo_data_from_fetch)

        if not ipo_db_entry:
            logger.error(f"Could not get or create DB entry for IPO {ipo_identifier}. Aborting analysis.")
            return None

        # Ensure the db entry is properly bound to the current session
        ipo_db_entry = self._ensure_ipo_db_entry_is_bound(ipo_db_entry, ipo_identifier)
        if not ipo_db_entry:  # If binding failed critically
            return None

        # Check for existing recent analysis
        # Datetime comparison fix: Ensure all comparisons are between offset-aware or offset-naive.
        # DB stores aware (UTC), so use aware for threshold.
        reanalyze_threshold_date = datetime.now(timezone.utc) - timedelta(days=IPO_ANALYSIS_REANALYZE_DAYS)

        existing_analysis = self.db_session.query(IPOAnalysis) \
            .filter(IPOAnalysis.ipo_id == ipo_db_entry.id) \
            .order_by(IPOAnalysis.analysis_date.desc()) \
            .first()

        significant_change = False
        if existing_analysis:
            # Compare key calendar data points if they exist in snapshot
            snap = existing_analysis.key_data_snapshot or {}
            parsed_snap_date = self._parse_ipo_date(snap.get("date"))

            # Check for significant changes in calendar data that warrant re-analysis
            if ipo_db_entry.ipo_date != parsed_snap_date or \
                    ipo_db_entry.status != snap.get("status") or \
                    ipo_db_entry.expected_price_range_low != snap.get("price_range_low") or \
                    ipo_db_entry.expected_price_range_high != snap.get("price_range_high"):
                significant_change = True
                logger.info(f"Significant calendar data change detected for IPO {ipo_identifier}.")

            if not significant_change and existing_analysis.analysis_date >= reanalyze_threshold_date:
                logger.info(
                    f"Recent analysis for IPO {ipo_identifier} (ID: {existing_analysis.id}, Date: {existing_analysis.analysis_date}) exists, no significant calendar changes. Skipping full re-analysis.")
                return existing_analysis  # Return existing if recent and no major changes
            else:
                logger.info(
                    f"Re-analyzing IPO {ipo_identifier}. Change: {significant_change}, Analysis Date: {existing_analysis.analysis_date} vs Threshold: {reanalyze_threshold_date}")

        s1_text_content, s1_filing_url_found = self._fetch_s1_data(ipo_db_entry)
        s1_extracted_sections = {}
        if s1_text_content:
            s1_extracted_sections = extract_S1_text_sections(s1_text_content, S1_KEY_SECTIONS)

        analysis_payload = {
            "key_data_snapshot": ipo_data_from_fetch.get("raw_data", {}),  # Store the latest calendar data
            "s1_sections_used": {k: bool(v) for k, v in s1_extracted_sections.items()}
            # Track what S1 sections were found
        }

        company_name_for_prompt = ipo_db_entry.company_name
        symbol_for_prompt = f" (Proposed Ticker: {ipo_db_entry.symbol})" if ipo_db_entry.symbol else ""

        business_text = s1_extracted_sections.get("business", "Not Available from S-1.")[
                        :MAX_S1_SECTION_LENGTH_FOR_GEMINI]
        risk_text = s1_extracted_sections.get("risk_factors", "Not Available from S-1.")[
                    :MAX_S1_SECTION_LENGTH_FOR_GEMINI]
        mda_text = s1_extracted_sections.get("mda", "Not Available from S-1.")[:MAX_S1_SECTION_LENGTH_FOR_GEMINI]

        # Prompts for Gemini based on S-1 if available
        prompt_context = (
            f"Company: {company_name_for_prompt}{symbol_for_prompt}. IPO Status: {ipo_db_entry.status}.\n"
            f"S-1 'Business' Snippet: \"{business_text}\"\n"
            f"S-1 'Risk Factors' Snippet: \"{risk_text}\"\n"
            f"S-1 'MD&A' Snippet: \"{mda_text}\"\n\n"
            f"Instructions: Based *primarily* on the provided S-1 snippets (if available and informative, otherwise infer cautiously based on company name/sector for SPACs or general knowledge):\n"
        )

        # 1. Business Model, Competitive Landscape, Industry Outlook
        prompt1 = prompt_context + (
            f"1. Business Model: Describe core products/services and target market.\n"
            f"2. Competitive Landscape: Identify key competitors and unique selling propositions.\n"
            f"3. Industry Outlook: Summarize key trends, growth drivers, and challenges for its industry.\n"
            f"Provide distinct, concise summaries for each (Business Model, Competitive Landscape, Industry Outlook)."
        )
        response1 = self.gemini.generate_text(prompt1[:MAX_GEMINI_TEXT_LENGTH])
        time.sleep(2)
        analysis_payload["s1_business_summary"] = self._parse_ai_section(response1, "Business Model")
        analysis_payload["competitive_landscape_summary"] = self._parse_ai_section(response1, "Competitive Landscape")
        analysis_payload["industry_outlook_summary"] = self._parse_ai_section(response1, "Industry Outlook")

        # 2. Risk Factors, Use of Proceeds, Financial Health
        prompt2 = prompt_context + (
            f"1. Significant Risk Factors: Summarize the 3-5 most material risks mentioned.\n"
            f"2. Use of IPO Proceeds: Describe the intended primary uses.\n"
            f"3. Financial Health Summary (from MD&A): Summarize recent financial performance, key financial health indicators (revenue growth, profitability/loss trends, cash flow), and outlook.\n"
            f"Provide distinct, concise summaries for each (Risk Factors, Use of Proceeds, Financial Health Summary)."
        )
        response2 = self.gemini.generate_text(prompt2[:MAX_GEMINI_TEXT_LENGTH])
        time.sleep(2)
        analysis_payload["s1_risk_factors_summary"] = self._parse_ai_section(response2, "Significant Risk Factors")
        analysis_payload["use_of_proceeds_summary"] = self._parse_ai_section(response2, "Use of IPO Proceeds")
        analysis_payload["s1_financial_health_summary"] = self._parse_ai_section(response2, "Financial Health Summary")

        # Fallbacks for older model fields if new s1_ fields are empty
        if not analysis_payload.get("s1_business_summary") or analysis_payload.get("s1_business_summary",
                                                                                   "").startswith("Section not found"):
            analysis_payload["business_model_summary"] = analysis_payload[
                "s1_business_summary"]  # Copy if parsed under old name
        if not analysis_payload.get("s1_risk_factors_summary") or analysis_payload.get("s1_risk_factors_summary",
                                                                                       "").startswith(
                "Section not found"):
            analysis_payload["risk_factors_summary"] = analysis_payload["s1_risk_factors_summary"]

        # Management & Underwriter assessments (placeholder - complex from S-1 text)
        analysis_payload[
            "management_team_assessment"] = "Review 'Directors and Executive Officers' section in S-1. AI summary not implemented for this."
        analysis_payload[
            "underwriter_quality_assessment"] = "Review 'Underwriting' section in S-1 for lead underwriters. AI summary not implemented."

        # Final Synthesis Prompt
        synthesis_prompt = (
            f"Synthesize a cautious, preliminary investment perspective for the IPO of {company_name_for_prompt}{symbol_for_prompt}.\n"
            f"IPO Price Range: {ipo_db_entry.expected_price_range_low} - {ipo_db_entry.expected_price_range_high} {ipo_db_entry.expected_price_currency or 'USD'}\n"
            f"S-1 Based Summaries (if available):\n"
            f"  Business: {analysis_payload.get('s1_business_summary', 'N/A')[:250]}...\n"
            f"  Industry: {analysis_payload.get('industry_outlook_summary', 'N/A')[:250]}...\n"
            f"  Competition: {analysis_payload.get('competitive_landscape_summary', 'N/A')[:250]}...\n"
            f"  Risks: {analysis_payload.get('s1_risk_factors_summary', 'N/A')[:250]}...\n"
            f"  Financials (MD&A view): {analysis_payload.get('s1_financial_health_summary', 'N/A')[:250]}...\n"
            f"  Use of Proceeds: {analysis_payload.get('use_of_proceeds_summary', 'N/A')[:150]}...\n\n"
            f"Based on the available information (prioritizing S-1 summaries if present): \n"
            f"1. Investment Stance: Provide a preliminary stance (e.g., 'Potentially Interesting, S-1 Review Critical', 'High Caution Advised', 'Avoid - High Risk / Low Info', 'SPAC - Monitor Target').\n"
            f"2. Reasoning: Briefly explain this stance (3-4 sentences), highlighting key positive/negative factors from the summaries.\n"
            f"3. Critical Verification Points: List 2-3 *critical items* an investor *must* further verify or scrutinize deeply in the full S-1 filing (or await in future filings for SPACs) before any investment decision.\n"
            f"This is a preliminary assessment for research guidance, not financial advice."
        )

        gemini_synthesis = self.gemini.generate_text(synthesis_prompt[:MAX_GEMINI_TEXT_LENGTH])
        time.sleep(2)

        parsed_synthesis = self._parse_ai_synthesis(gemini_synthesis)
        analysis_payload["investment_decision"] = parsed_synthesis.get("decision", "Research Further / Cautious")
        analysis_payload["reasoning"] = parsed_synthesis.get("reasoning_detail", gemini_synthesis)

        current_time_utc = datetime.now(timezone.utc)
        if existing_analysis:
            logger.info(f"Updating existing IPO analysis for {ipo_identifier} (ID: {existing_analysis.id})")
            for key, value in analysis_payload.items():
                setattr(existing_analysis, key, value)
            existing_analysis.analysis_date = current_time_utc
            ipo_analysis_entry_to_save = existing_analysis
        else:
            logger.info(f"Creating new IPO analysis entry for {ipo_identifier}")
            ipo_analysis_entry_to_save = IPOAnalysis(
                ipo_id=ipo_db_entry.id,
                analysis_date=current_time_utc,
                **analysis_payload
            )
            self.db_session.add(ipo_analysis_entry_to_save)

        ipo_db_entry.last_analysis_date = current_time_utc

        try:
            self.db_session.commit()
            logger.info(
                f"Successfully analyzed and saved IPO: {ipo_identifier} (Analysis ID: {ipo_analysis_entry_to_save.id})")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving IPO analysis for {ipo_identifier}: {e}", exc_info=True)
            return None

        return ipo_analysis_entry_to_save

    def _parse_ai_section(self, ai_text, section_header_keywords):
        # Same helper as in stock_analyzer (can be moved to a common utility)
        if not ai_text or ai_text.startswith("Error:"): return "AI Error or No Text"

        if isinstance(section_header_keywords, str):
            keywords_to_check = [section_header_keywords.lower()]
        else:
            keywords_to_check = [k.lower() for k in section_header_keywords]

        lines = ai_text.split('\n')
        capture = False
        section_content = []
        all_known_headers_lower = [
            "business model:", "competitive landscape:", "industry outlook:",
            "significant risk factors:", "use of ipo proceeds:", "financial health summary:",
            "investment stance:", "reasoning:", "critical verification points:"  # From synthesis prompt
        ]

        for i, line in enumerate(lines):
            normalized_line_start_raw = line.strip().lower()

            # Try to match if line STARTS with one of the keywords followed by a colon
            matched_keyword = None
            for kw in keywords_to_check:
                if normalized_line_start_raw.startswith(
                        kw + ":") or normalized_line_start_raw == kw:  # Exact match or with colon
                    matched_keyword = kw
                    break

            if matched_keyword:
                capture = True
                content_on_header_line = line.strip()[len(matched_keyword):].strip()
                if content_on_header_line.startswith(":"): content_on_header_line = content_on_header_line[1:].strip()
                if content_on_header_line: section_content.append(content_on_header_line)
                continue

            if capture:
                # Check if the current line is another known major header, indicating end of current section
                is_another_header = False
                for kh_lower in all_known_headers_lower:
                    if normalized_line_start_raw.startswith(kh_lower) and kh_lower not in keywords_to_check:
                        is_another_header = True
                        break
                if is_another_header:
                    break
                section_content.append(line)  # Append the original line, not normalized

        return "\n".join(section_content).strip() if section_content else "Section not found or empty."

    def _parse_ai_synthesis(self, ai_response):
        # Same helper as in stock_analyzer (can be moved to a common utility)
        parsed = {}
        if ai_response.startswith("Error:") or not ai_response:
            parsed["decision"] = "AI Error"
            parsed["reasoning_detail"] = ai_response
            return parsed

        # Use the more general _parse_ai_section logic
        parsed["decision"] = self._parse_ai_section(ai_response, "Investment Stance")
        parsed["reasoning_detail"] = self._parse_ai_section(ai_response, ["Reasoning",
                                                                          "Critical Verification Points"])  # Combine these for overall reasoning

        if parsed["decision"].startswith("Section not found") or not parsed["decision"]:
            parsed["decision"] = "Review AI Output"  # Fallback
        if parsed["reasoning_detail"].startswith("Section not found") or not parsed["reasoning_detail"]:
            parsed["reasoning_detail"] = ai_response  # Fallback to full response

        return parsed

    def run_ipo_analysis_pipeline(self):
        all_upcoming_ipos = self.fetch_upcoming_ipos()
        analyzed_ipos_results = []
        if not all_upcoming_ipos:
            logger.info("No upcoming IPOs found to analyze.")
            self._close_session_if_active()
            return []

        for ipo_data in all_upcoming_ipos:
            try:
                status = ipo_data.get("status", "").lower()
                # Focus on 'filed', 'expected', 'priced' as these are most actionable for S-1 review
                # 'upcoming' is also fine. 'withdrawn' should be skipped.
                relevant_statuses = ["expected", "filed", "priced", "upcoming", "active"]  # 'active' can be ambiguous
                if status not in relevant_statuses:
                    logger.debug(f"Skipping IPO '{ipo_data.get('company_name')}' with status '{status}'.")
                    continue

                if not ipo_data.get("company_name"):
                    logger.warning(f"Skipping IPO due to missing company name: {ipo_data}")
                    continue

                result = self.analyze_single_ipo(ipo_data)  # This now handles DB session internally for the item
                if result:
                    analyzed_ipos_results.append(result)
            except Exception as e:
                logger.error(f"CRITICAL error in IPO analysis pipeline for item '{ipo_data.get('company_name')}': {e}",
                             exc_info=True)
                # Ensure session is robust for the next IPO if an error occurred
                if self.db_session and not self.db_session.is_active:
                    self.db_session = next(get_db_session())
                elif self.db_session:  # If active but transaction might be bad due to unhandled exception in analyze_single_ipo
                    self.db_session.rollback()
            finally:
                time.sleep(8)  # Increased delay for more intensive S-1 processing and multiple Gemini calls

        logger.info(f"IPO analysis pipeline completed. Analyzed/Updated {len(analyzed_ipos_results)} IPOs.")
        self._close_session_if_active()
        return analyzed_ipos_results

    def _ensure_ipo_db_entry_session_active(self, ipo_identifier_for_log):
        if not self.db_session.is_active:
            logger.warning(f"Session for IPO {ipo_identifier_for_log} was inactive. Re-establishing.")
            self._close_session_if_active()
            self.db_session = next(get_db_session())

    def _ensure_ipo_db_entry_is_bound(self, ipo_db_entry_obj, ipo_identifier_for_log):
        if not ipo_db_entry_obj:  # Should not happen if called after _get_or_create
            logger.error(f"IPO DB entry object is None for {ipo_identifier_for_log} before session binding check.")
            return None

        self._ensure_ipo_db_entry_session_active(ipo_identifier_for_log)  # Ensure session itself is active

        instance_state = sa_inspect(ipo_db_entry_obj)
        if not instance_state.session or instance_state.session is not self.db_session:
            logger.warning(
                f"IPO DB entry {ipo_identifier_for_log} (ID: {ipo_db_entry_obj.id if instance_state.has_identity else 'Transient'}) is not bound to current session {id(self.db_session)} or bound to {id(instance_state.session) if instance_state.session else 'None'}. Attempting merge.")
            try:
                # If object is transient (no ID yet) and session is different, it might be from a failed previous transaction.
                # Re-querying is safer if identity is uncertain or if it's a new object for this session.
                if not instance_state.has_identity and ipo_db_entry_obj.id is None:
                    # Try to find it in the current session by presumed unique keys before merging a new transient one.
                    existing_in_session = self.db_session.query(IPO).filter_by(
                        company_name=ipo_db_entry_obj.company_name,
                        ipo_date_str=ipo_db_entry_obj.ipo_date_str,
                        symbol=ipo_db_entry_obj.symbol
                    ).first()
                    if existing_in_session:
                        ipo_db_entry_obj = existing_in_session  # Use the one from the current session
                        logger.info(
                            f"Replaced transient IPO entry for {ipo_identifier_for_log} with instance from current session (ID: {ipo_db_entry_obj.id}).")
                        return ipo_db_entry_obj  # Return the session-bound object
                    # If not found, it's genuinely new to this session's context, merge will add it.

                merged_ipo = self.db_session.merge(ipo_db_entry_obj)
                # self.db_session.flush() # Optional: ensure it's in identity map. Commit will do this.
                logger.info(
                    f"Successfully merged/re-associated IPO {ipo_identifier_for_log} (ID: {merged_ipo.id}) into current session.")
                return merged_ipo
            except Exception as e_merge:
                logger.error(
                    f"Failed to merge IPO {ipo_identifier_for_log} into session: {e_merge}. Re-fetching as fallback.",
                    exc_info=True)
                # Fallback: try to get by ID if it exists, or by unique constraint if transient and merge failed
                pk_id = ipo_db_entry_obj.id if instance_state.has_identity and ipo_db_entry_obj.id else None
                fallback_ipo = None
                if pk_id:
                    fallback_ipo = self.db_session.query(IPO).get(pk_id)

                if not fallback_ipo:  # If no ID or get by ID failed
                    fallback_ipo = self.db_session.query(IPO).filter_by(
                        company_name=ipo_db_entry_obj.company_name,
                        ipo_date_str=ipo_db_entry_obj.ipo_date_str,
                        symbol=ipo_db_entry_obj.symbol
                    ).first()

                if not fallback_ipo:
                    logger.critical(
                        f"CRITICAL: Failed to re-associate IPO {ipo_identifier_for_log} with current session after merge failure and could not re-fetch.")
                    # Depending on strictness, could raise RuntimeError here.
                    # For now, allow to proceed, but it might fail later if ipo_db_entry_obj is needed for FK.
                    return None  # Indicate critical failure to bind
                logger.info(f"Successfully re-fetched IPO {ipo_identifier_for_log} after merge failure.")
                return fallback_ipo
        return ipo_db_entry_obj  # Already bound or successfully merged/re-fetched


if __name__ == '__main__':
    from database import init_db

    # init_db() # Ensure DB is initialized with new IPO model fields if changed

    logger.info("Starting standalone IPO analysis pipeline test...")
    analyzer = IPOAnalyzer()
    results = analyzer.run_ipo_analysis_pipeline()
    if results:
        logger.info(f"Processed {len(results)} IPOs.")
        for res in results:
            if hasattr(res, 'ipo') and res.ipo:  # Ensure result has 'ipo' attribute
                ipo_info = res.ipo
                logger.info(
                    f"IPO: {ipo_info.company_name} ({ipo_info.symbol}), Decision: {res.investment_decision}, IPO Date: {ipo_info.ipo_date}, Status: {ipo_info.status}, S-1 URL: {ipo_info.s1_filing_url if ipo_info.s1_filing_url else 'Not Found'}")
            else:
                logger.warning(f"Processed IPO result item missing 'ipo' attribute or ipo is None. Result: {res}")

    else:
        logger.info("No IPOs were processed or found by the pipeline.")# ipo_analyzer.py
import time
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser # For flexible date parsing

from api_clients import FinnhubClient, GeminiAPIClient, SECEDGARClient, extract_S1_text_sections
from database import SessionLocal, get_db_session
from models import IPO, IPOAnalysis
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError
from config import (
    S1_KEY_SECTIONS, MAX_S1_SECTION_LENGTH_FOR_GEMINI,
    IPO_ANALYSIS_REANALYZE_DAYS, MAX_GEMINI_TEXT_LENGTH
)

class IPOAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()
        self.db_session = next(get_db_session())

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close()
                logger.debug("DB session closed in IPOAnalyzer.")
            except Exception as e_close:
                logger.warning(f"Error closing session in IPOAnalyzer: {e_close}")

    def _parse_ipo_date(self, date_str):
        if not date_str:
            return None
        try:
            # Handle various common date formats, make it timezone naive for DB storage as Date
            return date_parser.parse(date_str).date()
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse IPO date string '{date_str}': {e}")
            return None

    def fetch_upcoming_ipos(self):
        logger.info("Fetching upcoming IPOs using Finnhub...")
        ipos_data_to_process = []
        today = datetime.now(timezone.utc)
        from_date = (today - timedelta(days=60)).strftime('%Y-%m-%d') # Look back for recently filed/priced
        to_date = (today + timedelta(days=180)).strftime('%Y-%m-%d') # Look further ahead

        finnhub_response = self.finnhub.get_ipo_calendar(from_date=from_date, to_date=to_date)
        actual_ipo_list = []

        if finnhub_response and isinstance(finnhub_response, dict) and "ipoCalendar" in finnhub_response:
            actual_ipo_list = finnhub_response["ipoCalendar"]
            if not isinstance(actual_ipo_list, list):
                logger.warning(f"Finnhub response 'ipoCalendar' field is not a list. Found: {type(actual_ipo_list)}")
                actual_ipo_list = []
            elif not actual_ipo_list:
                logger.info("Finnhub 'ipoCalendar' list is empty for the current period.")
        elif finnhub_response is None: # API call itself failed
            logger.error("Failed to fetch IPOs from Finnhub (API call failed or returned None).")
        else: # Unexpected format or just no IPOs
            logger.info(f"No IPOs found or unexpected format from Finnhub. Response: {str(finnhub_response)[:200]}")

        if actual_ipo_list:
            for ipo_api_data in actual_ipo_list:
                if not isinstance(ipo_api_data, dict): # Skip non-dict items
                    logger.warning(f"Skipping non-dictionary item in Finnhub IPO calendar: {ipo_api_data}")
                    continue

                price_range_raw = ipo_api_data.get("price")
                price_low, price_high = None, None

                if isinstance(price_range_raw, str) and price_range_raw.strip():
                    if '-' in price_range_raw:
                        parts = price_range_raw.split('-', 1)
                        try: price_low = float(parts[0].strip())
                        except (ValueError, TypeError): price_low = None
                        try: price_high = float(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else price_low
                        except (ValueError, TypeError): price_high = price_low if price_low is not None else None
                    else: # Single price
                        try:
                            price_low = float(price_range_raw.strip())
                            price_high = price_low
                        except (ValueError, TypeError): price_low = price_high = None
                elif isinstance(price_range_raw, (float, int)):
                    price_low = float(price_range_raw)
                    price_high = float(price_range_raw)

                parsed_date = self._parse_ipo_date(ipo_api_data.get("date"))

                ipos_data_to_process.append({
                    "company_name": ipo_api_data.get("name"),
                    "symbol": ipo_api_data.get("symbol"),
                    "ipo_date_str": ipo_api_data.get("date"),
                    "ipo_date": parsed_date,
                    "expected_price_range_low": price_low,
                    "expected_price_range_high": price_high,
                    "exchange": ipo_api_data.get("exchange"),
                    "status": ipo_api_data.get("status"),
                    "offered_shares": ipo_api_data.get("numberOfShares"),
                    "total_shares_value": ipo_api_data.get("totalSharesValue"),
                    "source_api": "Finnhub",
                    "raw_data": ipo_api_data
                })
            logger.info(f"Successfully parsed {len(ipos_data_to_process)} IPOs from Finnhub API response.")

        unique_ipos = []
        seen_keys = set()
        for ipo_info in ipos_data_to_process:
            key_name = ipo_info.get("company_name", "").strip().lower() if ipo_info.get("company_name") else "unknown_company"
            key_symbol = ipo_info.get("symbol", "").strip().upper() if ipo_info.get("symbol") else "NO_SYMBOL"
            key_date = ipo_info.get("ipo_date_str", "") # Use original string date for precise API matching

            unique_tuple = (key_name, key_symbol, key_date)

            if unique_tuple not in seen_keys:
                unique_ipos.append(ipo_info)
                seen_keys.add(unique_tuple)
            else:
                logger.debug(f"Duplicate IPO based on key '{unique_tuple}' found, skipping: {ipo_info.get('company_name')}")

        logger.info(f"Total unique IPOs fetched after deduplication: {len(unique_ipos)}")
        return unique_ipos

    def _get_or_create_ipo_db_entry(self, ipo_data_from_fetch):
        self._ensure_ipo_db_entry_session_active(ipo_data_from_fetch.get('company_name', 'Unknown IPO'))

        ipo_db_entry = None
        # Try finding by symbol first, as it's more unique if present for listed/priced IPOs
        if ipo_data_from_fetch.get("symbol"):
            ipo_db_entry = self.db_session.query(IPO).filter(IPO.symbol == ipo_data_from_fetch["symbol"]).first()

        # If not found by symbol, or no symbol, try by name and original date string (calendar entries)
        if not ipo_db_entry and ipo_data_from_fetch.get("company_name") and ipo_data_from_fetch.get("ipo_date_str"):
             ipo_db_entry = self.db_session.query(IPO).filter(
                IPO.company_name == ipo_data_from_fetch["company_name"],
                IPO.ipo_date_str == ipo_data_from_fetch["ipo_date_str"]
            ).first()

        cik_to_store = ipo_data_from_fetch.get("cik") # CIK might be passed if pre-fetched
        if not cik_to_store and ipo_data_from_fetch.get("symbol"):
            cik_to_store = self.sec_edgar.get_cik_by_ticker(ipo_data_from_fetch["symbol"])
        elif not cik_to_store and ipo_db_entry and ipo_db_entry.symbol and not ipo_db_entry.cik: # If updating and CIK was missing
            cik_to_store = self.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol)

        if not ipo_db_entry:
            logger.info(f"IPO '{ipo_data_from_fetch.get('company_name')}' not found in DB, creating new entry.")
            ipo_db_entry = IPO(
                company_name=ipo_data_from_fetch.get("company_name"),
                symbol=ipo_data_from_fetch.get("symbol"),
                ipo_date_str=ipo_data_from_fetch.get("ipo_date_str"),
                ipo_date=ipo_data_from_fetch.get("ipo_date"),
                expected_price_range_low=ipo_data_from_fetch.get("expected_price_range_low"),
                expected_price_range_high=ipo_data_from_fetch.get("expected_price_range_high"),
                offered_shares=ipo_data_from_fetch.get("offered_shares"),
                total_shares_value=ipo_data_from_fetch.get("total_shares_value"),
                exchange=ipo_data_from_fetch.get("exchange"),
                status=ipo_data_from_fetch.get("status"),
                cik=cik_to_store
            )
            self.db_session.add(ipo_db_entry)
            try:
                self.db_session.commit()
                self.db_session.refresh(ipo_db_entry)
                logger.info(f"Created IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}, CIK: {ipo_db_entry.cik})")
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating IPO entry for '{ipo_data_from_fetch.get('company_name')}': {e}", exc_info=True)
                return None # Critical failure
        else: # Update existing
            logger.info(f"Found existing IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}). Checking for updates.")
            updated = False
            fields_to_update = [
                "company_name", "symbol", "ipo_date_str", "ipo_date", "expected_price_range_low",
                "expected_price_range_high", "offered_shares", "total_shares_value", "exchange", "status"
            ]
            for field in fields_to_update:
                new_value = ipo_data_from_fetch.get(field)
                current_value = getattr(ipo_db_entry, field)
                if new_value is not None and current_value != new_value: # Only update if new value exists and is different
                    setattr(ipo_db_entry, field, new_value)
                    updated = True

            if cik_to_store and ipo_db_entry.cik != cik_to_store:
                ipo_db_entry.cik = cik_to_store
                updated = True

            if updated:
                try:
                    self.db_session.commit()
                    self.db_session.refresh(ipo_db_entry)
                    logger.info(f"Updated existing IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}).")
                except SQLAlchemyError as e:
                    self.db_session.rollback()
                    logger.error(f"Error updating IPO entry for '{ipo_db_entry.company_name}': {e}", exc_info=True)
        return ipo_db_entry

    def _fetch_s1_data(self, ipo_db_entry):
        if not ipo_db_entry: return None, None
        target_cik = ipo_db_entry.cik # Use CIK from DB entry

        if not target_cik: # If CIK not in DB, try to get it via symbol
            if ipo_db_entry.symbol:
                target_cik = self.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol)
                if target_cik:
                    ipo_db_entry.cik = target_cik # Update DB
                    try:
                        self.db_session.commit()
                        logger.info(f"Found and updated CIK for {ipo_db_entry.company_name} to {target_cik}.")
                    except SQLAlchemyError as e:
                        self.db_session.rollback()
                        logger.error(f"Failed to update CIK for {ipo_db_entry.company_name}: {e}")
                else:
                    logger.warning(f"No CIK found via symbol {ipo_db_entry.symbol} for IPO '{ipo_db_entry.company_name}'.")
                    return None, None
            else: # No CIK and no symbol
                logger.warning(f"No CIK or symbol for IPO '{ipo_db_entry.company_name}'. Cannot fetch S-1/F-1.")
                return None, None

        logger.info(f"Attempting to fetch S-1/F-1 filing for {ipo_db_entry.company_name} (CIK: {target_cik})")
        # Try S-1, S-1/A, F-1, F-1/A in order of preference for original/latest comprehensive filing
        filing_types_to_try = ["S-1", "S-1/A", "F-1", "F-1/A"]
        s1_url = None
        for form_type in filing_types_to_try:
            s1_url = self.sec_edgar.get_filing_document_url(cik=target_cik, form_type=form_type)
            if s1_url:
                logger.info(f"Found {form_type} filing URL for {ipo_db_entry.company_name}: {s1_url}")
                break

        if s1_url:
            if ipo_db_entry.s1_filing_url != s1_url: # Store/update the found URL
                ipo_db_entry.s1_filing_url = s1_url
                try: self.db_session.commit()
                except SQLAlchemyError as e: self.db_session.rollback()

            filing_text = self.sec_edgar.get_filing_text(s1_url)
            if filing_text:
                logger.info(f"Fetched S-1/F-1 text content (length: {len(filing_text)}) for {ipo_db_entry.company_name}")
                return filing_text, s1_url
            else:
                logger.warning(f"Failed to fetch S-1/F-1 text content from {s1_url}")
        else:
            logger.warning(f"No S-1 or F-1 type filing URL found for {ipo_db_entry.company_name} (CIK: {target_cik}).")
        return None, None


    def analyze_single_ipo(self, ipo_data_from_fetch):
        ipo_identifier = ipo_data_from_fetch.get("company_name") or ipo_data_from_fetch.get("symbol")
        if not ipo_identifier:
            logger.error(f"Cannot analyze IPO, missing company_name and symbol: {ipo_data_from_fetch}")
            return None # Cannot proceed
        logger.info(f"Starting analysis for IPO: {ipo_identifier} from source {ipo_data_from_fetch.get('source_api')}")

        self._ensure_ipo_db_entry_session_active(ipo_identifier)
        ipo_db_entry = self._get_or_create_ipo_db_entry(ipo_data_from_fetch)

        if not ipo_db_entry:
            logger.error(f"Could not get or create DB entry for IPO {ipo_identifier}. Aborting analysis.")
            return None

        # Ensure the db entry is properly bound to the current session
        ipo_db_entry = self._ensure_ipo_db_entry_is_bound(ipo_db_entry, ipo_identifier)
        if not ipo_db_entry: # If binding failed critically
             return None


        # Check for existing recent analysis
        # Datetime comparison fix: Ensure all comparisons are between offset-aware or offset-naive.
        # DB stores aware (UTC), so use aware for threshold.
        reanalyze_threshold_date = datetime.now(timezone.utc) - timedelta(days=IPO_ANALYSIS_REANALYZE_DAYS)

        existing_analysis = self.db_session.query(IPOAnalysis) \
            .filter(IPOAnalysis.ipo_id == ipo_db_entry.id) \
            .order_by(IPOAnalysis.analysis_date.desc()) \
            .first()

        significant_change = False
        if existing_analysis:
            # Compare key calendar data points if they exist in snapshot
            snap = existing_analysis.key_data_snapshot or {}
            parsed_snap_date = self._parse_ipo_date(snap.get("date"))

            # Check for significant changes in calendar data that warrant re-analysis
            if ipo_db_entry.ipo_date != parsed_snap_date or \
               ipo_db_entry.status != snap.get("status") or \
               ipo_db_entry.expected_price_range_low != snap.get("price_range_low") or \
               ipo_db_entry.expected_price_range_high != snap.get("price_range_high"):
                significant_change = True
                logger.info(f"Significant calendar data change detected for IPO {ipo_identifier}.")

            if not significant_change and existing_analysis.analysis_date >= reanalyze_threshold_date:
                logger.info(f"Recent analysis for IPO {ipo_identifier} (ID: {existing_analysis.id}, Date: {existing_analysis.analysis_date}) exists, no significant calendar changes. Skipping full re-analysis.")
                return existing_analysis # Return existing if recent and no major changes
            else:
                 logger.info(f"Re-analyzing IPO {ipo_identifier}. Change: {significant_change}, Analysis Date: {existing_analysis.analysis_date} vs Threshold: {reanalyze_threshold_date}")


        s1_text_content, s1_filing_url_found = self._fetch_s1_data(ipo_db_entry)
        s1_extracted_sections = {}
        if s1_text_content:
            s1_extracted_sections = extract_S1_text_sections(s1_text_content, S1_KEY_SECTIONS)

        analysis_payload = {
            "key_data_snapshot": ipo_data_from_fetch.get("raw_data", {}), # Store the latest calendar data
            "s1_sections_used": {k: bool(v) for k,v in s1_extracted_sections.items()} # Track what S1 sections were found
        }

        company_name_for_prompt = ipo_db_entry.company_name
        symbol_for_prompt = f" (Proposed Ticker: {ipo_db_entry.symbol})" if ipo_db_entry.symbol else ""

        business_text = s1_extracted_sections.get("business", "Not Available from S-1.")[:MAX_S1_SECTION_LENGTH_FOR_GEMINI]
        risk_text = s1_extracted_sections.get("risk_factors", "Not Available from S-1.")[:MAX_S1_SECTION_LENGTH_FOR_GEMINI]
        mda_text = s1_extracted_sections.get("mda", "Not Available from S-1.")[:MAX_S1_SECTION_LENGTH_FOR_GEMINI]

        # Prompts for Gemini based on S-1 if available
        prompt_context = (
            f"Company: {company_name_for_prompt}{symbol_for_prompt}. IPO Status: {ipo_db_entry.status}.\n"
            f"S-1 'Business' Snippet: \"{business_text}\"\n"
            f"S-1 'Risk Factors' Snippet: \"{risk_text}\"\n"
            f"S-1 'MD&A' Snippet: \"{mda_text}\"\n\n"
            f"Instructions: Based *primarily* on the provided S-1 snippets (if available and informative, otherwise infer cautiously based on company name/sector for SPACs or general knowledge):\n"
        )

        # 1. Business Model, Competitive Landscape, Industry Outlook
        prompt1 = prompt_context + (
            f"1. Business Model: Describe core products/services and target market.\n"
            f"2. Competitive Landscape: Identify key competitors and unique selling propositions.\n"
            f"3. Industry Outlook: Summarize key trends, growth drivers, and challenges for its industry.\n"
            f"Provide distinct, concise summaries for each (Business Model, Competitive Landscape, Industry Outlook)."
        )
        response1 = self.gemini.generate_text(prompt1[:MAX_GEMINI_TEXT_LENGTH])
        time.sleep(2)
        analysis_payload["s1_business_summary"] = self._parse_ai_section(response1, "Business Model")
        analysis_payload["competitive_landscape_summary"] = self._parse_ai_section(response1, "Competitive Landscape")
        analysis_payload["industry_outlook_summary"] = self._parse_ai_section(response1, "Industry Outlook")

        # 2. Risk Factors, Use of Proceeds, Financial Health
        prompt2 = prompt_context + (
            f"1. Significant Risk Factors: Summarize the 3-5 most material risks mentioned.\n"
            f"2. Use of IPO Proceeds: Describe the intended primary uses.\n"
            f"3. Financial Health Summary (from MD&A): Summarize recent financial performance, key financial health indicators (revenue growth, profitability/loss trends, cash flow), and outlook.\n"
            f"Provide distinct, concise summaries for each (Risk Factors, Use of Proceeds, Financial Health Summary)."
        )
        response2 = self.gemini.generate_text(prompt2[:MAX_GEMINI_TEXT_LENGTH])
        time.sleep(2)
        analysis_payload["s1_risk_factors_summary"] = self._parse_ai_section(response2, "Significant Risk Factors")
        analysis_payload["use_of_proceeds_summary"] = self._parse_ai_section(response2, "Use of IPO Proceeds")
        analysis_payload["s1_financial_health_summary"] = self._parse_ai_section(response2, "Financial Health Summary")

        # Fallbacks for older model fields if new s1_ fields are empty
        if not analysis_payload.get("s1_business_summary") or analysis_payload.get("s1_business_summary", "").startswith("Section not found"):
            analysis_payload["business_model_summary"] = analysis_payload["s1_business_summary"] # Copy if parsed under old name
        if not analysis_payload.get("s1_risk_factors_summary") or analysis_payload.get("s1_risk_factors_summary", "").startswith("Section not found"):
            analysis_payload["risk_factors_summary"] = analysis_payload["s1_risk_factors_summary"]


        # Management & Underwriter assessments (placeholder - complex from S-1 text)
        analysis_payload["management_team_assessment"] = "Review 'Directors and Executive Officers' section in S-1. AI summary not implemented for this."
        analysis_payload["underwriter_quality_assessment"] = "Review 'Underwriting' section in S-1 for lead underwriters. AI summary not implemented."

        # Final Synthesis Prompt
        synthesis_prompt = (
            f"Synthesize a cautious, preliminary investment perspective for the IPO of {company_name_for_prompt}{symbol_for_prompt}.\n"
            f"IPO Price Range: {ipo_db_entry.expected_price_range_low} - {ipo_db_entry.expected_price_range_high} {ipo_db_entry.expected_price_currency or 'USD'}\n"
            f"S-1 Based Summaries (if available):\n"
            f"  Business: {analysis_payload.get('s1_business_summary', 'N/A')[:250]}...\n"
            f"  Industry: {analysis_payload.get('industry_outlook_summary', 'N/A')[:250]}...\n"
            f"  Competition: {analysis_payload.get('competitive_landscape_summary', 'N/A')[:250]}...\n"
            f"  Risks: {analysis_payload.get('s1_risk_factors_summary', 'N/A')[:250]}...\n"
            f"  Financials (MD&A view): {analysis_payload.get('s1_financial_health_summary', 'N/A')[:250]}...\n"
            f"  Use of Proceeds: {analysis_payload.get('use_of_proceeds_summary', 'N/A')[:150]}...\n\n"
            f"Based on the available information (prioritizing S-1 summaries if present): \n"
            f"1. Investment Stance: Provide a preliminary stance (e.g., 'Potentially Interesting, S-1 Review Critical', 'High Caution Advised', 'Avoid - High Risk / Low Info', 'SPAC - Monitor Target').\n"
            f"2. Reasoning: Briefly explain this stance (3-4 sentences), highlighting key positive/negative factors from the summaries.\n"
            f"3. Critical Verification Points: List 2-3 *critical items* an investor *must* further verify or scrutinize deeply in the full S-1 filing (or await in future filings for SPACs) before any investment decision.\n"
            f"This is a preliminary assessment for research guidance, not financial advice."
        )

        gemini_synthesis = self.gemini.generate_text(synthesis_prompt[:MAX_GEMINI_TEXT_LENGTH])
        time.sleep(2)

        parsed_synthesis = self._parse_ai_synthesis(gemini_synthesis)
        analysis_payload["investment_decision"] = parsed_synthesis.get("decision", "Research Further / Cautious")
        analysis_payload["reasoning"] = parsed_synthesis.get("reasoning_detail", gemini_synthesis)

        current_time_utc = datetime.now(timezone.utc)
        if existing_analysis:
            logger.info(f"Updating existing IPO analysis for {ipo_identifier} (ID: {existing_analysis.id})")
            for key, value in analysis_payload.items():
                setattr(existing_analysis, key, value)
            existing_analysis.analysis_date = current_time_utc
            ipo_analysis_entry_to_save = existing_analysis
        else:
            logger.info(f"Creating new IPO analysis entry for {ipo_identifier}")
            ipo_analysis_entry_to_save = IPOAnalysis(
                ipo_id=ipo_db_entry.id,
                analysis_date=current_time_utc,
                **analysis_payload
            )
            self.db_session.add(ipo_analysis_entry_to_save)

        ipo_db_entry.last_analysis_date = current_time_utc

        try:
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved IPO: {ipo_identifier} (Analysis ID: {ipo_analysis_entry_to_save.id})")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving IPO analysis for {ipo_identifier}: {e}", exc_info=True)
            return None

        return ipo_analysis_entry_to_save

    def _parse_ai_section(self, ai_text, section_header_keywords):
        # Same helper as in stock_analyzer (can be moved to a common utility)
        if not ai_text or ai_text.startswith("Error:"): return "AI Error or No Text"

        if isinstance(section_header_keywords, str):
            keywords_to_check = [section_header_keywords.lower()]
        else:
            keywords_to_check = [k.lower() for k in section_header_keywords]

        lines = ai_text.split('\n')
        capture = False
        section_content = []
        all_known_headers_lower = [
            "business model:", "competitive landscape:", "industry outlook:",
            "significant risk factors:", "use of ipo proceeds:", "financial health summary:",
            "investment stance:", "reasoning:", "critical verification points:" # From synthesis prompt
        ]


        for i, line in enumerate(lines):
            normalized_line_start_raw = line.strip().lower()

            # Try to match if line STARTS with one of the keywords followed by a colon
            matched_keyword = None
            for kw in keywords_to_check:
                if normalized_line_start_raw.startswith(kw + ":") or normalized_line_start_raw == kw : # Exact match or with colon
                    matched_keyword = kw
                    break

            if matched_keyword:
                capture = True
                content_on_header_line = line.strip()[len(matched_keyword):].strip()
                if content_on_header_line.startswith(":"): content_on_header_line = content_on_header_line[1:].strip()
                if content_on_header_line: section_content.append(content_on_header_line)
                continue

            if capture:
                # Check if the current line is another known major header, indicating end of current section
                is_another_header = False
                for kh_lower in all_known_headers_lower:
                    if normalized_line_start_raw.startswith(kh_lower) and kh_lower not in keywords_to_check:
                        is_another_header = True
                        break
                if is_another_header:
                    break
                section_content.append(line) # Append the original line, not normalized

        return "\n".join(section_content).strip() if section_content else "Section not found or empty."

    def _parse_ai_synthesis(self, ai_response):
        # Same helper as in stock_analyzer (can be moved to a common utility)
        parsed = {}
        if ai_response.startswith("Error:") or not ai_response:
            parsed["decision"] = "AI Error"
            parsed["reasoning_detail"] = ai_response
            return parsed

        # Use the more general _parse_ai_section logic
        parsed["decision"] = self._parse_ai_section(ai_response, "Investment Stance")
        parsed["reasoning_detail"] = self._parse_ai_section(ai_response, ["Reasoning", "Critical Verification Points"]) # Combine these for overall reasoning

        if parsed["decision"].startswith("Section not found") or not parsed["decision"]:
             parsed["decision"] = "Review AI Output" # Fallback
        if parsed["reasoning_detail"].startswith("Section not found") or not parsed["reasoning_detail"]:
            parsed["reasoning_detail"] = ai_response # Fallback to full response

        return parsed

    def run_ipo_analysis_pipeline(self):
        all_upcoming_ipos = self.fetch_upcoming_ipos()
        analyzed_ipos_results = []
        if not all_upcoming_ipos:
            logger.info("No upcoming IPOs found to analyze.")
            self._close_session_if_active()
            return []

        for ipo_data in all_upcoming_ipos:
            try:
                status = ipo_data.get("status", "").lower()
                # Focus on 'filed', 'expected', 'priced' as these are most actionable for S-1 review
                # 'upcoming' is also fine. 'withdrawn' should be skipped.
                relevant_statuses = ["expected", "filed", "priced", "upcoming", "active"] # 'active' can be ambiguous
                if status not in relevant_statuses:
                    logger.debug(f"Skipping IPO '{ipo_data.get('company_name')}' with status '{status}'.")
                    continue

                if not ipo_data.get("company_name"):
                    logger.warning(f"Skipping IPO due to missing company name: {ipo_data}")
                    continue

                result = self.analyze_single_ipo(ipo_data) # This now handles DB session internally for the item
                if result:
                    analyzed_ipos_results.append(result)
            except Exception as e:
                logger.error(f"CRITICAL error in IPO analysis pipeline for item '{ipo_data.get('company_name')}': {e}", exc_info=True)
                # Ensure session is robust for the next IPO if an error occurred
                if self.db_session and not self.db_session.is_active:
                    self.db_session = next(get_db_session())
                elif self.db_session: # If active but transaction might be bad due to unhandled exception in analyze_single_ipo
                    self.db_session.rollback()
            finally:
                 time.sleep(8) # Increased delay for more intensive S-1 processing and multiple Gemini calls

        logger.info(f"IPO analysis pipeline completed. Analyzed/Updated {len(analyzed_ipos_results)} IPOs.")
        self._close_session_if_active()
        return analyzed_ipos_results

    def _ensure_ipo_db_entry_session_active(self, ipo_identifier_for_log):
        if not self.db_session.is_active:
            logger.warning(f"Session for IPO {ipo_identifier_for_log} was inactive. Re-establishing.")
            self._close_session_if_active()
            self.db_session = next(get_db_session())

    def _ensure_ipo_db_entry_is_bound(self, ipo_db_entry_obj, ipo_identifier_for_log):
        if not ipo_db_entry_obj: # Should not happen if called after _get_or_create
            logger.error(f"IPO DB entry object is None for {ipo_identifier_for_log} before session binding check.")
            return None

        self._ensure_ipo_db_entry_session_active(ipo_identifier_for_log) # Ensure session itself is active

        instance_state = sa_inspect(ipo_db_entry_obj)
        if not instance_state.session or instance_state.session is not self.db_session:
            logger.warning(f"IPO DB entry {ipo_identifier_for_log} (ID: {ipo_db_entry_obj.id if instance_state.has_identity else 'Transient'}) is not bound to current session {id(self.db_session)} or bound to {id(instance_state.session) if instance_state.session else 'None'}. Attempting merge.")
            try:
                # If object is transient (no ID yet) and session is different, it might be from a failed previous transaction.
                # Re-querying is safer if identity is uncertain or if it's a new object for this session.
                if not instance_state.has_identity and ipo_db_entry_obj.id is None:
                     # Try to find it in the current session by presumed unique keys before merging a new transient one.
                     existing_in_session = self.db_session.query(IPO).filter_by(
                         company_name=ipo_db_entry_obj.company_name,
                         ipo_date_str=ipo_db_entry_obj.ipo_date_str,
                         symbol=ipo_db_entry_obj.symbol
                     ).first()
                     if existing_in_session:
                         ipo_db_entry_obj = existing_in_session # Use the one from the current session
                         logger.info(f"Replaced transient IPO entry for {ipo_identifier_for_log} with instance from current session (ID: {ipo_db_entry_obj.id}).")
                         return ipo_db_entry_obj # Return the session-bound object
                     # If not found, it's genuinely new to this session's context, merge will add it.

                merged_ipo = self.db_session.merge(ipo_db_entry_obj)
                # self.db_session.flush() # Optional: ensure it's in identity map. Commit will do this.
                logger.info(f"Successfully merged/re-associated IPO {ipo_identifier_for_log} (ID: {merged_ipo.id}) into current session.")
                return merged_ipo
            except Exception as e_merge:
                logger.error(f"Failed to merge IPO {ipo_identifier_for_log} into session: {e_merge}. Re-fetching as fallback.", exc_info=True)
                # Fallback: try to get by ID if it exists, or by unique constraint if transient and merge failed
                pk_id = ipo_db_entry_obj.id if instance_state.has_identity and ipo_db_entry_obj.id else None
                fallback_ipo = None
                if pk_id:
                    fallback_ipo = self.db_session.query(IPO).get(pk_id)

                if not fallback_ipo: # If no ID or get by ID failed
                     fallback_ipo = self.db_session.query(IPO).filter_by(
                         company_name=ipo_db_entry_obj.company_name,
                         ipo_date_str=ipo_db_entry_obj.ipo_date_str,
                         symbol=ipo_db_entry_obj.symbol
                     ).first()

                if not fallback_ipo:
                    logger.critical(f"CRITICAL: Failed to re-associate IPO {ipo_identifier_for_log} with current session after merge failure and could not re-fetch.")
                    # Depending on strictness, could raise RuntimeError here.
                    # For now, allow to proceed, but it might fail later if ipo_db_entry_obj is needed for FK.
                    return None # Indicate critical failure to bind
                logger.info(f"Successfully re-fetched IPO {ipo_identifier_for_log} after merge failure.")
                return fallback_ipo
        return ipo_db_entry_obj # Already bound or successfully merged/re-fetched


if __name__ == '__main__':
    from database import init_db
    # init_db() # Ensure DB is initialized with new IPO model fields if changed

    logger.info("Starting standalone IPO analysis pipeline test...")
    analyzer = IPOAnalyzer()
    results = analyzer.run_ipo_analysis_pipeline()
    if results:
        logger.info(f"Processed {len(results)} IPOs.")
        for res in results:
            if hasattr(res, 'ipo') and res.ipo: # Ensure result has 'ipo' attribute
                ipo_info = res.ipo
                logger.info(
                    f"IPO: {ipo_info.company_name} ({ipo_info.symbol}), Decision: {res.investment_decision}, IPO Date: {ipo_info.ipo_date}, Status: {ipo_info.status}, S-1 URL: {ipo_info.s1_filing_url if ipo_info.s1_filing_url else 'Not Found'}")
            else:
                logger.warning(f"Processed IPO result item missing 'ipo' attribute or ipo is None. Result: {res}")

    else:
        logger.info("No IPOs were processed or found by the pipeline.")