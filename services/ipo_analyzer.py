import time
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timedelta, timezone
from dateutil import parser as date_parser
import concurrent.futures

from api_clients import FinnhubClient, GeminiAPIClient, SECEDGARClient, extract_S1_text_sections
from database import SessionLocal, get_db_session, IPO, IPOAnalysis
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError
from core.config import (
    S1_KEY_SECTIONS, IPO_ANALYSIS_REANALYZE_DAYS,
    SUMMARIZATION_CHUNK_SIZE_CHARS
)

MAX_IPO_ANALYSIS_WORKERS = 1 # Module-level constant


class IPOAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()

    def _parse_ipo_date(self, date_str):
        if not date_str:
            return None
        try:
            return date_parser.parse(date_str).date()
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse IPO date string '{date_str}': {e}")
            return None

    def fetch_upcoming_ipos(self):
        logger.info("Fetching upcoming IPOs using Finnhub...")
        ipos_data_to_process = []
        today = datetime.now(timezone.utc)
        from_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
        to_date = (today + timedelta(days=180)).strftime('%Y-%m-%d')

        finnhub_response = self.finnhub.get_ipo_calendar(from_date=from_date, to_date=to_date)
        actual_ipo_list = []

        if finnhub_response and isinstance(finnhub_response, dict) and "ipoCalendar" in finnhub_response:
            actual_ipo_list = finnhub_response["ipoCalendar"]
            if not isinstance(actual_ipo_list, list):
                logger.warning(f"Finnhub response 'ipoCalendar' field is not a list. Found: {type(actual_ipo_list)}")
                actual_ipo_list = []
            elif not actual_ipo_list:
                logger.info("Finnhub 'ipoCalendar' list is empty for the current period.")
        elif finnhub_response is None:
            logger.error("Failed to fetch IPOs from Finnhub (API call failed or returned None).")
        else:
            logger.info(f"No IPOs found or unexpected format from Finnhub. Response: {str(finnhub_response)[:200]}")

        if actual_ipo_list:
            for ipo_api_data in actual_ipo_list:
                if not isinstance(ipo_api_data, dict):
                    logger.warning(f"Skipping non-dictionary item in Finnhub IPO calendar: {ipo_api_data}")
                    continue
                price_range_raw = ipo_api_data.get("price")
                price_low, price_high = None, None
                if isinstance(price_range_raw, str) and price_range_raw.strip():
                    parts = price_range_raw.split('-', 1)
                    try: price_low = float(parts[0].strip())
                    except: pass # pylint: disable=bare-except
                    try: price_high = float(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else price_low
                    except: price_high = price_low if price_low is not None else None # pylint: disable=bare-except
                elif isinstance(price_range_raw, (float, int)):
                    price_low = float(price_range_raw)
                    price_high = float(price_range_raw)

                parsed_date = self._parse_ipo_date(ipo_api_data.get("date"))
                ipos_data_to_process.append({
                    "company_name": ipo_api_data.get("name"), "symbol": ipo_api_data.get("symbol"),
                    "ipo_date_str": ipo_api_data.get("date"), "ipo_date": parsed_date,
                    "expected_price_range_low": price_low, "expected_price_range_high": price_high,
                    "exchange": ipo_api_data.get("exchange"), "status": ipo_api_data.get("status"),
                    "offered_shares": ipo_api_data.get("numberOfShares"),
                    "total_shares_value": ipo_api_data.get("totalSharesValue"),
                    "source_api": "Finnhub", "raw_data": ipo_api_data
                })
            logger.info(f"Successfully parsed {len(ipos_data_to_process)} IPOs from Finnhub API response.")

        unique_ipos = []
        seen_keys = set()
        for ipo_info in ipos_data_to_process:
            key_name = ipo_info.get("company_name", "").strip().lower() if ipo_info.get("company_name") else "unknown_company"
            key_symbol = ipo_info.get("symbol", "").strip().upper() if ipo_info.get("symbol") else "NO_SYMBOL"
            key_date = ipo_info.get("ipo_date_str", "")
            unique_tuple = (key_name, key_symbol, key_date)
            if unique_tuple not in seen_keys:
                unique_ipos.append(ipo_info)
                seen_keys.add(unique_tuple)
        logger.info(f"Total unique IPOs fetched after deduplication: {len(unique_ipos)}")
        return unique_ipos

    def _get_or_create_ipo_db_entry(self, db_session, ipo_data_from_fetch):
        ipo_db_entry = None
        if ipo_data_from_fetch.get("symbol"):
            ipo_db_entry = db_session.query(IPO).filter(IPO.symbol == ipo_data_from_fetch["symbol"]).first()

        if not ipo_db_entry and ipo_data_from_fetch.get("company_name") and ipo_data_from_fetch.get("ipo_date_str"):
            ipo_db_entry = db_session.query(IPO).filter(
                IPO.company_name == ipo_data_from_fetch["company_name"],
                IPO.ipo_date_str == ipo_data_from_fetch["ipo_date_str"]
            ).first()

        cik_to_store = ipo_data_from_fetch.get("cik")
        if not cik_to_store and ipo_data_from_fetch.get("symbol"):
            cik_to_store = self.sec_edgar.get_cik_by_ticker(ipo_data_from_fetch["symbol"])
            time.sleep(0.5)
        elif not cik_to_store and ipo_db_entry and ipo_db_entry.symbol and not ipo_db_entry.cik:
            cik_to_store = self.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol)
            time.sleep(0.5)

        if not ipo_db_entry:
            logger.info(f"IPO '{ipo_data_from_fetch.get('company_name')}' not found in DB, creating new entry.")
            ipo_db_entry = IPO(
                company_name=ipo_data_from_fetch.get("company_name"), symbol=ipo_data_from_fetch.get("symbol"),
                ipo_date_str=ipo_data_from_fetch.get("ipo_date_str"), ipo_date=ipo_data_from_fetch.get("ipo_date"),
                expected_price_range_low=ipo_data_from_fetch.get("expected_price_range_low"),
                expected_price_range_high=ipo_data_from_fetch.get("expected_price_range_high"),
                offered_shares=ipo_data_from_fetch.get("offered_shares"),
                total_shares_value=ipo_data_from_fetch.get("total_shares_value"),
                exchange=ipo_data_from_fetch.get("exchange"), status=ipo_data_from_fetch.get("status"),
                cik=cik_to_store
            )
            db_session.add(ipo_db_entry)
            try:
                db_session.commit(); db_session.refresh(ipo_db_entry)
                logger.info(f"Created IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}, CIK: {ipo_db_entry.cik})")
            except SQLAlchemyError as e:
                db_session.rollback(); logger.error(f"Error creating IPO: {e}", exc_info=True); return None
        else:
            updated = False
            fields_to_update = ["company_name", "symbol", "ipo_date_str", "ipo_date", "expected_price_range_low",
                                "expected_price_range_high", "offered_shares", "total_shares_value", "exchange", "status"]
            for field in fields_to_update:
                new_val = ipo_data_from_fetch.get(field)
                if new_val is not None and getattr(ipo_db_entry, field) != new_val:
                    setattr(ipo_db_entry, field, new_val); updated = True
            if cik_to_store and ipo_db_entry.cik != cik_to_store:
                ipo_db_entry.cik = cik_to_store; updated = True
            if updated:
                try:
                    db_session.commit(); db_session.refresh(ipo_db_entry)
                    logger.info(f"Updated IPO entry for '{ipo_db_entry.company_name}' (ID: {ipo_db_entry.id}).")
                except SQLAlchemyError as e:
                    db_session.rollback(); logger.error(f"Error updating IPO: {e}", exc_info=True)
        return ipo_db_entry

    def _fetch_s1_data(self, db_session, ipo_db_entry):
        if not ipo_db_entry: return None, None
        target_cik = ipo_db_entry.cik
        if not target_cik:
            if ipo_db_entry.symbol:
                target_cik = self.sec_edgar.get_cik_by_ticker(ipo_db_entry.symbol); time.sleep(0.5)
                if target_cik:
                    ipo_db_entry.cik = target_cik
                    try: db_session.commit()
                    except SQLAlchemyError as e: db_session.rollback(); logger.error(f"Failed to update CIK for {ipo_db_entry.company_name}: {e}")
                else: logger.warning(f"No CIK via symbol {ipo_db_entry.symbol} for '{ipo_db_entry.company_name}'."); return None, None
            else: logger.warning(f"No CIK/symbol for '{ipo_db_entry.company_name}'. Cannot fetch S-1."); return None, None

        logger.info(f"Attempting to fetch S-1/F-1 for {ipo_db_entry.company_name} (CIK: {target_cik})")
        s1_url = None
        for form_type in ["S-1", "S-1/A", "F-1", "F-1/A"]:
            s1_url = self.sec_edgar.get_filing_document_url(cik=target_cik, form_type=form_type); time.sleep(0.5)
            if s1_url: logger.info(f"Found {form_type} URL for {ipo_db_entry.company_name}: {s1_url}"); break
        if s1_url:
            if ipo_db_entry.s1_filing_url != s1_url:
                ipo_db_entry.s1_filing_url = s1_url
                try: db_session.commit()
                except SQLAlchemyError as e: db_session.rollback(); logger.warning(f"Failed to update S1 filing URL for {ipo_db_entry.company_name} due to: {e}")
            filing_text = self.sec_edgar.get_filing_text(s1_url)
            if filing_text: logger.info(f"Fetched S-1/F-1 text (len: {len(filing_text)}) for {ipo_db_entry.company_name}"); return filing_text, s1_url
            else: logger.warning(f"Failed to fetch S-1/F-1 text from {s1_url}")
        else: logger.warning(f"No S-1 or F-1 URL found for {ipo_db_entry.company_name} (CIK: {target_cik}).")
        return None, None

    def _analyze_single_ipo_task(self, db_session, ipo_data_from_fetch):
        ipo_identifier = ipo_data_from_fetch.get("company_name") or ipo_data_from_fetch.get("symbol")
        logger.info(f"Task: Starting analysis for IPO: {ipo_identifier} from source {ipo_data_from_fetch.get('source_api')}")
        ipo_db_entry = self._get_or_create_ipo_db_entry(db_session, ipo_data_from_fetch)
        if not ipo_db_entry: logger.error(f"Task: Could not get/create DB entry for IPO {ipo_identifier}. Aborting."); return None

        reanalyze_threshold = datetime.now(timezone.utc) - timedelta(days=IPO_ANALYSIS_REANALYZE_DAYS)
        existing_analysis = db_session.query(IPOAnalysis).filter(IPOAnalysis.ipo_id == ipo_db_entry.id).order_by(IPOAnalysis.analysis_date.desc()).first()
        significant_change = False
        if existing_analysis:
            snap = existing_analysis.key_data_snapshot or {}
            parsed_snap_date = self._parse_ipo_date(snap.get("date"))
            if (ipo_db_entry.ipo_date != parsed_snap_date or ipo_db_entry.status != snap.get("status") or
                ipo_db_entry.expected_price_range_low != snap.get("price_range_low") or # type: ignore
                ipo_db_entry.expected_price_range_high != snap.get("price_range_high")): # type: ignore
                significant_change = True
            if not significant_change and existing_analysis.analysis_date >= reanalyze_threshold:
                logger.info(f"Task: Recent analysis for {ipo_identifier} exists. Skipping."); return existing_analysis

        s1_text, _ = self._fetch_s1_data(db_session, ipo_db_entry)
        s1_sections = extract_S1_text_sections(s1_text, S1_KEY_SECTIONS) if s1_text else {}
        analysis_payload = {"key_data_snapshot": ipo_data_from_fetch.get("raw_data", {}), "s1_sections_used": {k: bool(v) for k,v in s1_sections.items()}}
        company_prompt_id = f"{ipo_db_entry.company_name} ({ipo_db_entry.symbol or 'N/A'})"
        max_section_len_for_prompt = SUMMARIZATION_CHUNK_SIZE_CHARS
        biz_text = (s1_sections.get("business", "") or "")[:max_section_len_for_prompt]
        risk_text = (s1_sections.get("risk_factors", "") or "")[:max_section_len_for_prompt]
        mda_text = (s1_sections.get("mda", "") or "")[:max_section_len_for_prompt]
        prompt_parts = [f"IPO: {company_prompt_id}"]
        if biz_text: prompt_parts.append(f"Business Summary from S-1: {biz_text}")
        if risk_text: prompt_parts.append(f"Risk Factors Summary from S-1: {risk_text}")
        if mda_text: prompt_parts.append(f"MD&A Summary from S-1: {mda_text}")
        prompt_ctx = ". ".join(prompt_parts)

        prompt1 = prompt_ctx + " Based on the S-1 information (if provided), summarize: Business Model, Competitive Landscape, Industry Outlook."
        resp1 = self.gemini.generate_text(prompt1); time.sleep(3)
        analysis_payload["s1_business_summary"] = self._parse_ai_section(resp1, "Business Model")
        analysis_payload["competitive_landscape_summary"] = self._parse_ai_section(resp1, "Competitive Landscape")
        analysis_payload["industry_outlook_summary"] = self._parse_ai_section(resp1, "Industry Outlook")

        prompt2 = prompt_ctx + " Based on the S-1 information (if provided), summarize: Key Risk Factors, Use of IPO Proceeds, Financial Health from MD&A."
        resp2 = self.gemini.generate_text(prompt2); time.sleep(3)
        analysis_payload["s1_risk_factors_summary"] = self._parse_ai_section(resp2, ["Key Risk Factors", "Risk Factors Summary"])
        analysis_payload["use_of_proceeds_summary"] = self._parse_ai_section(resp2, "Use of IPO Proceeds")
        analysis_payload["s1_financial_health_summary"] = self._parse_ai_section(resp2, "Financial Health from MD&A")
        analysis_payload["s1_mda_summary"] = analysis_payload["s1_financial_health_summary"] # Alias for consistency

        if not analysis_payload.get("s1_business_summary") or analysis_payload.get("s1_business_summary","").startswith("Section not found"):
            analysis_payload["business_model_summary"] = analysis_payload.get("s1_business_summary")
        if not analysis_payload.get("s1_risk_factors_summary") or analysis_payload.get("s1_risk_factors_summary","").startswith("Section not found"):
            analysis_payload["risk_factors_summary"] = analysis_payload.get("s1_risk_factors_summary")

        synth_prompt_parts = [f"Synthesize an IPO investment perspective for {company_prompt_id}."]
        if analysis_payload.get('s1_business_summary') and "Section not found" not in analysis_payload['s1_business_summary']: synth_prompt_parts.append(f"Business={analysis_payload['s1_business_summary'][:150]}")
        if analysis_payload.get('s1_risk_factors_summary') and "Section not found" not in analysis_payload['s1_risk_factors_summary']: synth_prompt_parts.append(f"Risks={analysis_payload['s1_risk_factors_summary'][:150]}")
        if analysis_payload.get('s1_financial_health_summary') and "Section not found" not in analysis_payload['s1_financial_health_summary']: synth_prompt_parts.append(f"Financials={analysis_payload['s1_financial_health_summary'][:150]}")
        synth_prompt_parts.append("Provide: Investment Stance (e.g., Monitor, Attractive Risk/Reward, Avoid), Reasoning, Critical Verification Points from S-1.")
        synth_prompt = " ".join(synth_prompt_parts)
        gemini_synth = self.gemini.generate_text(synth_prompt); time.sleep(3)
        parsed_synth = self._parse_ai_synthesis(gemini_synth)
        analysis_payload.update(parsed_synth)

        current_time = datetime.now(timezone.utc)
        if existing_analysis:
            for key, value in analysis_payload.items(): setattr(existing_analysis, key, value)
            existing_analysis.analysis_date = current_time; entry_to_save = existing_analysis
        else:
            entry_to_save = IPOAnalysis(ipo_id=ipo_db_entry.id, analysis_date=current_time, **analysis_payload)
            db_session.add(entry_to_save)
        ipo_db_entry.last_analysis_date = current_time
        try:
            db_session.commit(); logger.info(f"Task: Saved IPO analysis for {ipo_identifier} (ID: {entry_to_save.id})")
        except SQLAlchemyError as e:
            db_session.rollback(); logger.error(f"Task: DB error saving IPO analysis for {ipo_identifier}: {e}", exc_info=True); return None
        return entry_to_save

    def _parse_ai_section(self, ai_text, section_header_keywords):
        if not ai_text or ai_text.startswith("Error:"): return "AI Error or No Text"
        keywords = [k.lower() for k in ([section_header_keywords] if isinstance(section_header_keywords, str) else section_header_keywords)]
        lines, capture, content = ai_text.split('\n'), False, []
        all_headers = ["business model:", "competitive landscape:", "industry outlook:", "significant risk factors:", "key risk factors:", "risk factors summary:", "use of ipo proceeds:", "financial health from md&a:", "financial health summary:", "investment stance:", "reasoning:", "critical verification points:"]
        for line in lines:
            norm_line = line.strip().lower()
            matched_kw = next((kw for kw in keywords if norm_line.startswith(kw + ":") or norm_line == kw), None)
            if matched_kw:
                capture = True; line_content = line.strip()[len(matched_kw):].lstrip(':').strip()
                if line_content: content.append(line_content)
                continue
            if capture:
                if any(norm_line.startswith(h) for h in all_headers if h not in keywords): break
                content.append(line)
        return "\n".join(content).strip() or "Section not found or empty."

    def _parse_ai_synthesis(self, ai_response):
        parsed = {}
        if ai_response.startswith("Error:") or not ai_response:
            parsed["investment_decision"] = "AI Error"; parsed["reasoning"] = ai_response if ai_response else "AI Error: Empty response."; return parsed
        parsed["investment_decision"] = self._parse_ai_section(ai_response, "Investment Stance")
        parsed["reasoning"] = self._parse_ai_section(ai_response, ["Reasoning", "Critical Verification Points"])
        if parsed["investment_decision"].startswith("Section not found"): parsed["investment_decision"] = "Review AI Output"
        if parsed["reasoning"].startswith("Section not found"): parsed["reasoning"] = ai_response
        return parsed

    def run_ipo_analysis_pipeline(self):
        all_upcoming_ipos = self.fetch_upcoming_ipos()
        analyzed_results = []
        if not all_upcoming_ipos: logger.info("No upcoming IPOs found to analyze."); return []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_IPO_ANALYSIS_WORKERS) as executor:
            future_to_ipo_data = {}
            for ipo_data in all_upcoming_ipos:
                status = ipo_data.get("status", "").lower()
                relevant_statuses = ["expected", "filed", "priced", "upcoming", "active"]
                if status not in relevant_statuses or not ipo_data.get("company_name"):
                    logger.debug(f"Skipping IPO '{ipo_data.get('company_name')}' due to status '{status}' or missing name."); continue
                future = executor.submit(self._thread_worker_analyze_ipo, ipo_data)
                future_to_ipo_data[future] = ipo_data.get("company_name")
            for future in concurrent.futures.as_completed(future_to_ipo_data):
                ipo_name = future_to_ipo_data[future]
                try:
                    result = future.result()
                    if result: analyzed_results.append(result)
                except Exception as exc: logger.error(f"IPO analysis for '{ipo_name}' generated an exception: {exc}", exc_info=True)
        logger.info(f"IPO analysis pipeline completed. Processed {len(analyzed_results)} IPOs.")
        return analyzed_results

    def _thread_worker_analyze_ipo(self, ipo_data_from_fetch):
        db_session = SessionLocal() # New session for each thread
        try:
            return self._analyze_single_ipo_task(db_session, ipo_data_from_fetch)
        finally:
            SessionLocal.remove() # Remove session associated with this thread

if __name__ == '__main__':
    from database import init_db
    # init_db()
    logger.info("Starting standalone IPO analysis pipeline test...")
    analyzer = IPOAnalyzer()
    results = analyzer.run_ipo_analysis_pipeline()
    if results:
        logger.info(f"Processed {len(results)} IPOs.")
        for res in results:
            if hasattr(res, 'ipo') and res.ipo:
                logger.info(f"IPO: {res.ipo.company_name} ({res.ipo.symbol}), Decision: {res.investment_decision}, Date: {res.ipo.ipo_date}, Status: {res.ipo.status}")
            else: logger.warning(f"Result item missing 'ipo' or ipo is None: {res}")
    else: logger.info("No IPOs were processed or found by the pipeline.")