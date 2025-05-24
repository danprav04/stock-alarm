# ipo_analyzer.py
import time
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timedelta  # Ensure timedelta is imported
from api_clients import FinnhubClient, GeminiAPIClient
from database import SessionLocal, get_db_session
from models import IPO, IPOAnalysis
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError


class IPOAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()  # Primary IPO data source
        self.gemini = GeminiAPIClient()
        self.db_session = next(get_db_session())

    def fetch_upcoming_ipos(self):
        logger.info("Fetching upcoming IPOs using Finnhub...")
        ipos_data_to_process = []  # Renamed to avoid confusion with the final list
        today = datetime.now()
        from_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        to_date = (today + timedelta(days=90)).strftime('%Y-%m-%d')

        finnhub_response = self.finnhub.get_ipo_calendar(from_date=from_date, to_date=to_date)
        actual_ipo_list = []

        if finnhub_response and isinstance(finnhub_response, dict) and "ipoCalendar" in finnhub_response:
            actual_ipo_list = finnhub_response["ipoCalendar"]
            if not isinstance(actual_ipo_list, list):
                logger.warning(f"Finnhub response 'ipoCalendar' field is not a list. Found: {type(actual_ipo_list)}")
                actual_ipo_list = []  # Ensure it's an empty list if not a list
            elif not actual_ipo_list:  # It's an empty list
                logger.info("Finnhub 'ipoCalendar' list is empty for the current period.")
        elif finnhub_response is None:
            logger.warning("Failed to fetch IPOs from Finnhub (API call failed or returned None).")
            # actual_ipo_list remains []
        else:
            logger.info(f"No IPOs found or unexpected format from Finnhub. Response type: {type(finnhub_response)}")
            # actual_ipo_list remains []

        if actual_ipo_list:
            for ipo in actual_ipo_list:
                price_range_raw = ipo.get(
                    "price")  # Finnhub gives price as string like "18.00-20.00" or just "10.00" or null
                price_low, price_high = "N/A", "N/A"

                if isinstance(price_range_raw, str):
                    if '-' in price_range_raw:
                        parts = price_range_raw.split('-', 1)
                        price_low = parts[0].strip()
                        price_high = parts[1].strip() if len(parts) > 1 else price_low  # Handle cases like "10.00-"
                    elif price_range_raw:  # Non-empty string, assume fixed price
                        price_low = price_range_raw.strip()
                        price_high = price_range_raw.strip()
                elif isinstance(price_range_raw, (float, int)):  # If price is a number
                    price_low = str(price_range_raw)
                    price_high = str(price_range_raw)

                ipos_data_to_process.append({
                    "name": ipo.get("name"),
                    "symbol": ipo.get("symbol"),
                    "date": ipo.get("date"),
                    "price_range_low": price_low,
                    "price_range_high": price_high,
                    "exchange": ipo.get("exchange"),
                    "status": ipo.get("status"),
                    "number_of_shares": ipo.get("numberOfShares"),
                    "total_shares_value": ipo.get("totalSharesValue"),
                    "source": "Finnhub",
                    "raw_data": ipo
                })
            logger.info(f"Successfully parsed {len(ipos_data_to_process)} IPOs from Finnhub response.")
        # Deduplication
        unique_ipos = []
        seen_names_or_symbols = set()
        for ipo_info in ipos_data_to_process:
            key = ipo_info.get("name") or ipo_info.get("symbol")  # Use name as primary key, symbol as fallback
            if key and key not in seen_names_or_symbols:
                unique_ipos.append(ipo_info)
                seen_names_or_symbols.add(key)
            elif not key:
                logger.warning(f"IPO data missing 'name' or 'symbol', cannot reliably deduplicate: {ipo_info}")
            else:  # Log if duplicate is found (though Finnhub data itself is usually clean)
                logger.debug(
                    f"Duplicate IPO based on name/symbol '{key}' found during deduplication pass, skipping: {ipo_info.get('name')}")

        logger.info(f"Total unique IPOs fetched after deduplication: {len(unique_ipos)}")
        return unique_ipos

    def _get_or_create_ipo_entry(self, ipo_data):
        if not self.db_session.is_active:
            logger.warning(
                f"Session for IPO {ipo_data.get('name')} in _get_or_create_ipo_entry was inactive. Re-establishing.")
            try:
                self.db_session.close()
            except:
                pass
            self.db_session = next(get_db_session())

        ipo_identifier_name = ipo_data.get("name")
        ipo_identifier_symbol = ipo_data.get("symbol")

        if not ipo_identifier_name and not ipo_identifier_symbol:
            logger.error(f"Cannot get or create IPO entry, 'name' and 'symbol' are missing in ipo_data: {ipo_data}")
            return None

        # Prefer matching by symbol if available and name is also present for update scenarios
        # If symbol exists, it's a stronger unique identifier usually.
        ipo_entry = None
        if ipo_identifier_symbol:
            ipo_entry = self.db_session.query(IPO).filter(IPO.symbol == ipo_identifier_symbol).first()

        if not ipo_entry and ipo_identifier_name:  # If not found by symbol, or symbol was None, try by name
            ipo_entry = self.db_session.query(IPO).filter(IPO.company_name == ipo_identifier_name).first()

        if not ipo_entry:
            log_id_for_create = ipo_identifier_name or ipo_identifier_symbol
            logger.info(f"IPO '{log_id_for_create}' not found in DB, creating new entry.")
            price_low = ipo_data.get('price_range_low', "N/A")
            price_high = ipo_data.get('price_range_high', "N/A")
            price_range_str = f"{price_low} - {price_high}" if price_low != "N/A" or price_high != "N/A" else "N/A"
            if price_low == price_high and price_low != "N/A":
                price_range_str = price_low

            ipo_entry = IPO(
                company_name=ipo_identifier_name,
                symbol=ipo_identifier_symbol,
                ipo_date=str(ipo_data.get("date", "N/A")),
                expected_price_range=price_range_str,
                exchange=ipo_data.get("exchange"),
                status=ipo_data.get("status")
            )
            self.db_session.add(ipo_entry)
            try:
                self.db_session.commit()
                self.db_session.refresh(ipo_entry)
                logger.info(f"Created and refreshed IPO entry for '{log_id_for_create}' (ID: {ipo_entry.id})")
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating IPO entry for '{log_id_for_create}': {e}", exc_info=True)
                return None
        else:
            log_id_for_update = ipo_entry.company_name or ipo_entry.symbol
            logger.info(
                f"Found existing IPO entry for '{log_id_for_update}' (ID: {ipo_entry.id}). Checking for updates.")
            updated = False
            if ipo_identifier_name and ipo_entry.company_name != ipo_identifier_name:  # Update name if provided and different
                ipo_entry.company_name = ipo_identifier_name
                updated = True
            if ipo_identifier_symbol and ipo_entry.symbol != ipo_identifier_symbol:  # Update symbol if provided and different
                ipo_entry.symbol = ipo_identifier_symbol
                updated = True
            if ipo_data.get("date") and ipo_entry.ipo_date != str(ipo_data.get("date")):
                ipo_entry.ipo_date = str(ipo_data.get("date"))
                updated = True

            price_low = ipo_data.get('price_range_low', "N/A")
            price_high = ipo_data.get('price_range_high', "N/A")
            new_price_range_str = f"{price_low} - {price_high}" if price_low != "N/A" or price_high != "N/A" else "N/A"
            if price_low == price_high and price_low != "N/A": new_price_range_str = price_low

            if ipo_entry.expected_price_range != new_price_range_str:
                ipo_entry.expected_price_range = new_price_range_str
                updated = True
            if ipo_data.get("exchange") and ipo_entry.exchange != ipo_data.get("exchange"):
                ipo_entry.exchange = ipo_data.get("exchange")
                updated = True
            if ipo_data.get("status") and ipo_entry.status != ipo_data.get("status"):
                ipo_entry.status = ipo_data.get("status")
                updated = True

            if updated:
                try:
                    self.db_session.commit()
                    self.db_session.refresh(ipo_entry)
                    logger.info(f"Updated existing IPO entry for '{log_id_for_update}' (ID: {ipo_entry.id})")
                except SQLAlchemyError as e:
                    self.db_session.rollback()
                    logger.error(f"Error updating IPO entry for '{log_id_for_update}': {e}", exc_info=True)
        return ipo_entry

    def analyze_single_ipo(self, ipo_data_from_fetch):
        ipo_name = ipo_data_from_fetch.get("name")
        ipo_symbol = ipo_data_from_fetch.get("symbol")
        ipo_identifier = ipo_name or ipo_symbol

        if not ipo_identifier:
            logger.error(f"Cannot analyze IPO, 'name' or 'symbol' is missing: {ipo_data_from_fetch}")
            return None
        logger.info(f"Starting analysis for IPO: {ipo_identifier} from source {ipo_data_from_fetch.get('source')}")

        if not self.db_session.is_active:
            logger.warning(f"Session for IPO {ipo_identifier} in analyze_single_ipo was inactive. Re-establishing.")
            try:
                self.db_session.close()
            except:
                pass
            self.db_session = next(get_db_session())

        ipo_db_entry = self._get_or_create_ipo_entry(ipo_data_from_fetch)
        if not ipo_db_entry:
            logger.error(f"Could not get or create DB entry for IPO {ipo_identifier}. Aborting analysis.")
            return None

        instance_state = sa_inspect(ipo_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            logger.warning(f"IPO DB entry {ipo_identifier} is not bound to the current session. Merging.")
            try:
                ipo_db_entry = self.db_session.merge(ipo_db_entry)
                self.db_session.flush()
            except Exception as e_merge:
                logger.error(f"Failed to merge IPO {ipo_identifier} into session: {e_merge}. Aborting.", exc_info=True)
                return None

        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        existing_analysis = self.db_session.query(IPOAnalysis) \
            .filter(IPOAnalysis.ipo_id == ipo_db_entry.id) \
            .filter(IPOAnalysis.analysis_date >= seven_days_ago) \
            .order_by(IPOAnalysis.analysis_date.desc()) \
            .first()

        significant_change = False
        if existing_analysis and ipo_db_entry:  # Check ipo_db_entry for current calendar data
            # Compare current calendar data with stored snapshot in analysis
            calendar_status = ipo_data_from_fetch.get("status")
            calendar_price = ipo_data_from_fetch.get("price")  # This is the raw price string/float from API

            analyzed_snapshot_status = existing_analysis.key_data_snapshot.get("status")
            analyzed_snapshot_price = existing_analysis.key_data_snapshot.get("price")

            if calendar_status != analyzed_snapshot_status or calendar_price != analyzed_snapshot_price:
                significant_change = True

        one_day_ago = datetime.utcnow() - timedelta(days=1)
        if existing_analysis and not significant_change and existing_analysis.analysis_date > one_day_ago:
            logger.info(
                f"Recent analysis for IPO {ipo_identifier} exists (ID: {existing_analysis.id}, Date: {existing_analysis.analysis_date}) and no significant calendar changes detected. Skipping Gemini re-analysis.")
            return existing_analysis
        elif existing_analysis:
            logger.info(
                f"Re-analyzing IPO {ipo_identifier} due to significant calendar change or analysis older than one day.")

        analysis_payload = {"key_data_snapshot": ipo_data_from_fetch.get("raw_data", {})}
        company_name_for_prompt = ipo_db_entry.company_name or "This company"
        symbol_for_prompt = f" with proposed ticker {ipo_db_entry.symbol}" if ipo_db_entry.symbol else ""

        company_description_for_prompt = (
            f"The company is named '{company_name_for_prompt}'{symbol_for_prompt}."
            f" It is expected to IPO around {ipo_db_entry.ipo_date} on the {ipo_db_entry.exchange} exchange."
            f" Current status: {ipo_db_entry.status}."
            f" Expected price or price range: {ipo_db_entry.expected_price_range}."
        )

        prompt1_text = (
            f"For the upcoming IPO of '{ipo_identifier}', given: {company_description_for_prompt}\n"
            f"Describe its likely business model based on its name and general industry knowledge. "
            f"Also, briefly outline the competitive landscape it likely operates in and the general health of that industry. "
            f"Focus on what can be inferred generally, as prospectus details are not provided here."
        )
        response1 = self.gemini.generate_text(prompt1_text)
        parts = response1.split("Competitive Landscape:") if response1 and not response1.startswith("Error:") else [
            "N/A"]
        analysis_payload["business_model_summary"] = parts[0].replace("Business Model:", "").strip()
        if len(parts) > 1:
            sub_parts = parts[1].split("Industry Health:")
            analysis_payload["competitive_landscape_summary"] = sub_parts[0].strip()
            if len(sub_parts) > 1:
                analysis_payload["industry_health_summary"] = sub_parts[1].strip()
            else:
                analysis_payload["industry_health_summary"] = "N/A (or see competitive landscape)"
        else:
            analysis_payload["competitive_landscape_summary"] = "N/A (or see business model)"
            analysis_payload["industry_health_summary"] = "N/A (or see business model)"

        prompt2_text = (
            f"For a company like '{ipo_identifier}' in its likely industry (as inferred above), "
            f"what are typical uses of IPO proceeds? "
            f"And what are common major risk factors generally disclosed in prospectuses for such companies? Keep it general."
        )
        response2 = self.gemini.generate_text(prompt2_text)
        parts2 = response2.split("Risk Factors:") if response2 and not response2.startswith("Error:") else ["N/A"]
        analysis_payload["use_of_proceeds_summary"] = "General (Not from Prospectus): " + parts2[0].replace(
            "Typical Uses of IPO Proceeds:", "").strip()
        if len(parts2) > 1:
            analysis_payload["risk_factors_summary"] = "General (Not from Prospectus): " + parts2[1].strip()
        else:
            analysis_payload["risk_factors_summary"] = "N/A (or see use of proceeds)"

        prompt3_text = (
            f"What key financial health indicators (e.g., revenue growth trends, path to profitability, cash burn rate) should an investor typically look for in the S-1 prospectus of a company like '{ipo_identifier}'? "
            f"How would one generally approach valuing such an IPO against its public peers (e.g., relevant ratios like P/S if it's a growth company, or P/E if profitable)? Be general."
        )
        analysis_payload["pre_ipo_financials_summary"] = "Guidance (General Approach): " + self.gemini.generate_text(
            prompt3_text)
        analysis_payload[
            "valuation_comparison_summary"] = "Guidance (General Approach): " + "Valuation typically involves comparing to publicly traded peers using metrics like Price/Sales for growth companies, or Price/Earnings if profitable. Discounted Cash Flow (DCF) models may also be used if sufficient financial history and projections are available. Specifics depend on the S-1 filing."

        analysis_payload[
            "underwriter_quality"] = "Underwriters: N/A from Finnhub calendar. Check S-1 filing. (Note: Quality assessment requires research on their track record)."
        analysis_payload[
            "fresh_issue_vs_ofs"] = "N/A from Finnhub calendar. Check S-1 filing for details on primary vs. secondary share offerings."
        analysis_payload[
            "lock_up_periods_info"] = "N/A from Finnhub calendar. Check S-1 filing (typically 90-180 days for insiders)."
        analysis_payload[
            "investor_demand_summary"] = "To be assessed closer to IPO date from news reports on anchor investors, oversubscription levels, and grey market premiums, if available. Not in Finnhub calendar."

        synthesis_prompt = (
            f"Synthesize a brief, cautious investment perspective (max 3-4 sentences) for the IPO of '{ipo_identifier}'. "
            f"Context: {company_description_for_prompt}\n"
            f"General Business Model Idea: {analysis_payload.get('business_model_summary', 'N/A')[:150]}...\n"
            f"General Industry Outlook: {analysis_payload.get('industry_health_summary', 'N/A')[:150]}...\n"
            f"General Risks for such IPOs: {analysis_payload.get('risk_factors_summary', 'N/A')[:150]}...\n"
            f"What are 2-3 *critical* things an investor MUST verify in the actual S-1 prospectus before considering this IPO? "
            f"Suggest a preliminary stance (e.g., 'Potentially interesting, S-1 review critical', 'Approach with significant caution until S-1 verified', 'High risk/reward, depends heavily on S-1 specifics'). "
            f"Do not give financial advice. This is for general informational purposes to guide further research."
        )
        gemini_synthesis = self.gemini.generate_text(synthesis_prompt)

        final_decision = "Research Further / Cautious"
        if gemini_synthesis and not gemini_synthesis.startswith("Error:"):
            if "interesting" in gemini_synthesis.lower() or "potential" in gemini_synthesis.lower() and "critical" not in gemini_synthesis.lower():
                final_decision = "Potentially Interesting / S-1 Review Critical"
            elif "caution" in gemini_synthesis.lower() or "high risk" in gemini_synthesis.lower() or "skeptical" in gemini_synthesis.lower():
                final_decision = "High Caution / Skeptical Pending S-1"

        analysis_payload["investment_decision"] = final_decision
        analysis_payload["reasoning"] = gemini_synthesis

        if existing_analysis and (significant_change or existing_analysis.analysis_date <= one_day_ago):
            logger.info(f"Updating existing IPO analysis for {ipo_identifier} (ID: {existing_analysis.id})")
            ipo_analysis_entry = existing_analysis
            ipo_analysis_entry.analysis_date = datetime.utcnow()
            for key, value in analysis_payload.items():
                setattr(ipo_analysis_entry, key, value)
        else:
            logger.info(f"Creating new IPO analysis entry for {ipo_identifier}")
            ipo_analysis_entry = IPOAnalysis(
                ipo_id=ipo_db_entry.id,
                **analysis_payload
            )
            self.db_session.add(ipo_analysis_entry)

        ipo_db_entry.last_analysis_date = datetime.utcnow()

        try:
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved IPO: {ipo_identifier} (Analysis ID: {ipo_analysis_entry.id})")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving IPO analysis for {ipo_identifier}: {e}", exc_info=True)
            return None
        time.sleep(1)
        return ipo_analysis_entry

    def run_ipo_analysis_pipeline(self):
        all_upcoming_ipos = self.fetch_upcoming_ipos()
        analyzed_ipos_results = []
        if not all_upcoming_ipos:
            logger.info("No upcoming IPOs found from Finnhub to analyze.")
            if self.db_session.is_active: self.db_session.close()
            return []

        for ipo_data in all_upcoming_ipos:
            if not self.db_session.is_active:
                self.db_session = next(get_db_session())

            status = ipo_data.get("status", "").lower()
            # Broaden accepted statuses to include those that might still be relevant for analysis
            # 'filed' is important, 'expected' is good, 'priced' means it just happened or is about to.
            # 'upcoming' can be a generic term.
            relevant_statuses = ["expected", "filed", "priced", "upcoming", "active"]
            if status not in relevant_statuses:
                logger.info(f"Skipping IPO '{ipo_data.get('name')}' with status '{status}'.")
                continue

            if not ipo_data.get("name") and not ipo_data.get("symbol"):  # Must have at least one identifier
                logger.warning(f"Skipping IPO due to missing name and symbol: {ipo_data}")
                continue

            result = self.analyze_single_ipo(ipo_data)
            if result:
                analyzed_ipos_results.append(result)
            time.sleep(2)

        logger.info(f"IPO analysis pipeline completed. Analyzed/Updated {len(analyzed_ipos_results)} IPOs.")
        if self.db_session.is_active:
            self.db_session.close()
        return analyzed_ipos_results


if __name__ == '__main__':
    from database import init_db

    # from datetime import timedelta # Already imported at top

    try:
        # init_db()
        logger.info("Starting standalone IPO analysis pipeline test...")
        analyzer = IPOAnalyzer()
        results = analyzer.run_ipo_analysis_pipeline()
        if results:
            logger.info(f"Processed {len(results)} IPOs.")
            for res in results:
                ipo_info = res.ipo
                logger.info(
                    f"IPO: {ipo_info.company_name} ({ipo_info.symbol}), Decision: {res.investment_decision}, Date: {ipo_info.ipo_date}, Status: {ipo_info.status}")
        else:
            logger.info("No IPOs were processed or found by Finnhub.")
    except Exception as e:
        logger.error(f"Error during IPO analysis test in __main__: {e}", exc_info=True)