# ipo_analyzer.py
import time  # Ensure time is imported if you add delays
from api_clients import FinancialModelingPrepClient, EODHDClient, RapidAPIUpcomingIPOCalendarClient, GeminiAPIClient
from database import SessionLocal, get_db_session
from models import IPO, IPOAnalysis
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime


class IPOAnalyzer:
    def __init__(self):
        self.fmp = FinancialModelingPrepClient()
        self.eodhd = EODHDClient()
        self.rapid_ipo = RapidAPIUpcomingIPOCalendarClient()  # Instantiate it
        self.gemini = GeminiAPIClient()
        self.db_session = next(get_db_session())

    def fetch_upcoming_ipos(self):
        logger.info("Fetching upcoming IPOs...")
        ipos_data = []

        # Try FMP (will likely fail on free tier, but good to keep the attempt for logs/other tiers)
        logger.info("Attempting to fetch IPOs from FinancialModelingPrep...")
        fmp_ipos = self.fmp.get_ipo_calendar()
        if fmp_ipos and isinstance(fmp_ipos, list):
            for ipo in fmp_ipos:
                ipos_data.append({
                    "name": ipo.get("name"),  # FMP often uses 'name'
                    "symbol": ipo.get("symbol"),
                    "date": ipo.get("date"),
                    "price_range_low": ipo.get("priceFrom"),
                    "price_range_high": ipo.get("priceTo"),
                    "exchange": ipo.get("exchange"),
                    "source": "FMP",
                    "raw_data": ipo
                })
            logger.info(f"Fetched {len(fmp_ipos)} IPOs from FMP.")
        elif fmp_ipos is None:  # Indicates API call failed due to non-200 status, error already logged by APIClient
            logger.warning("Failed to fetch IPOs from FMP (API call failed, check previous logs for 403).")
        else:  # Empty list, means no IPOs or API returned empty valid response
            logger.info("No IPOs found or returned by FMP for the current period/subscription.")

        # Try EODHD (will likely fail on free tier)
        logger.info("Attempting to fetch IPOs from EODHistoricalData...")
        eodhd_ipos = self.eodhd.get_ipo_calendar()
        if eodhd_ipos and isinstance(eodhd_ipos, list) and eodhd_ipos:  # Check if list is not empty
            # EODHD response structure might differ; check their docs for IPO calendar fields.
            # Example assumes keys like 'Code', 'Name', 'Date', 'Exchange', 'PriceFrom', 'PriceTo'.
            for ipo in eodhd_ipos:
                if ipo.get("Name"):  # Basic check for a valid IPO entry
                    ipos_data.append({
                        "name": ipo.get("Name"),
                        "symbol": ipo.get("Code"),  # Or 'Ticker'
                        "date": ipo.get("Date"),
                        "price_range_low": ipo.get("PriceFrom"),
                        "price_range_high": ipo.get("PriceTo"),
                        "exchange": ipo.get("Exchange"),
                        "source": "EODHD",
                        "raw_data": ipo
                    })
            logger.info(f"Fetched {len(eodhd_ipos)} IPOs from EODHD.")
        elif eodhd_ipos is None:
            logger.warning("Failed to fetch IPOs from EODHD (API call failed, check previous logs for 403).")
        else:
            logger.info("No IPOs found or returned by EODHD for the current period/subscription.")

        # --- Try RapidAPI (This is likely your best bet for free/included IPO data) ---
        logger.info("Attempting to fetch IPOs from RapidAPI (upcoming-ipo-calendar)...")
        rapid_data = self.rapid_ipo.get_ipo_calendar()  # APIClient handles request & errors

        if rapid_data and isinstance(rapid_data, list):  # Assuming it returns a list of IPOs
            logger.info(f"Successfully fetched data from RapidAPI. Processing {len(rapid_data)} items.")
            for ipo_item in rapid_data:
                # Adapt keys based on the ACTUAL response structure from this RapidAPI endpoint
                # Common keys might be 'companyName', 'symbol', 'ipoDate', 'priceLow', 'priceHigh', 'exchange'
                # This is a GUESS - YOU MUST INSPECT THE ACTUAL RAPIDAPI RESPONSE
                if ipo_item.get("companyName") or ipo_item.get("name"):  # Check for a name field
                    ipos_data.append({
                        "name": ipo_item.get("companyName") or ipo_item.get("name"),
                        "symbol": ipo_item.get("symbol"),
                        "date": ipo_item.get("ipoDate") or ipo_item.get("date"),
                        "price_range_low": ipo_item.get("priceLow") or ipo_item.get("expectedPrice") or ipo_item.get(
                            "lowPriceRange"),  # Guessing various keys
                        "price_range_high": ipo_item.get("priceHigh") or ipo_item.get("highPriceRange"),
                        "exchange": ipo_item.get("exchange"),
                        "source": "RapidAPI_UpcomingIPOCalendar",
                        "raw_data": ipo_item  # Store the original item
                    })
            logger.info(f"Processed {len(rapid_data)} IPOs from RapidAPI.")
        elif rapid_data is None:
            logger.warning("Failed to fetch IPOs from RapidAPI (API call failed or returned None).")
        else:  # Not a list or unexpected format
            logger.warning(
                f"No IPO data or unexpected format from RapidAPI. Type: {type(rapid_data)}, Data: {str(rapid_data)[:200]}")
        # --- End of RapidAPI section ---

        # Deduplicate IPOs by name (simple deduplication)
        unique_ipos = []
        seen_names = set()
        for ipo_info in ipos_data:
            name = ipo_info.get("name")
            if name and name not in seen_names:
                unique_ipos.append(ipo_info)
                seen_names.add(name)
            elif not name:
                logger.warning(f"IPO data missing 'name' field, cannot deduplicate: {ipo_info}")

        logger.info(f"Total unique IPOs fetched after deduplication: {len(unique_ipos)}")
        return unique_ipos

    def _get_or_create_ipo_entry(self, ipo_data):
        # Ensure session is active
        if not self.db_session.is_active:
            logger.warning(
                f"Session for IPO {ipo_data.get('name')} in _get_or_create_ipo_entry was inactive. Re-establishing.")
            try:
                self.db_session.close()
            except:
                pass
            self.db_session = next(get_db_session())

        ipo_name = ipo_data.get("name")
        if not ipo_name:
            logger.error(f"Cannot get or create IPO entry, 'name' is missing in ipo_data: {ipo_data}")
            return None

        ipo_entry = self.db_session.query(IPO).filter_by(company_name=ipo_name).first()
        if not ipo_entry:
            logger.info(f"IPO {ipo_name} not found in DB, creating new entry.")
            price_low = ipo_data.get('price_range_low', "N/A")
            price_high = ipo_data.get('price_range_high', "N/A")
            price_range_str = f"{price_low} - {price_high}" if price_low != "N/A" or price_high != "N/A" else "N/A"

            ipo_entry = IPO(
                company_name=ipo_name,
                ipo_date=str(ipo_data.get("date", "N/A")),
                expected_price_range=price_range_str
            )
            self.db_session.add(ipo_entry)
            try:
                self.db_session.commit()
                self.db_session.refresh(ipo_entry)  # Refresh after creation
                logger.info(f"Created and refreshed IPO entry for {ipo_name} (ID: {ipo_entry.id})")
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating IPO entry for {ipo_name}: {e}", exc_info=True)
                return None
        else:
            logger.info(f"Found existing IPO entry for {ipo_name} (ID: {ipo_entry.id})")
            # Optionally refresh existing entry if there's a chance data like ipo_date changed
            # self.db_session.refresh(ipo_entry)
        return ipo_entry

    def analyze_single_ipo(self, ipo_data_from_fetch):
        ipo_name = ipo_data_from_fetch.get("name")
        if not ipo_name:
            logger.error(f"Cannot analyze IPO, 'name' is missing: {ipo_data_from_fetch}")
            return None
        logger.info(f"Starting analysis for IPO: {ipo_name} from source {ipo_data_from_fetch.get('source')}")

        # Ensure session is active
        if not self.db_session.is_active:
            logger.warning(f"Session for IPO {ipo_name} in analyze_single_ipo was inactive. Re-establishing.")
            try:
                self.db_session.close()
            except:
                pass
            self.db_session = next(get_db_session())

        ipo_db_entry = self._get_or_create_ipo_entry(ipo_data_from_fetch)
        if not ipo_db_entry:
            logger.error(f"Could not get or create DB entry for IPO {ipo_name}. Aborting analysis.")
            return None

        # Ensure ipo_db_entry is associated with the current session before proceeding
        # This handles cases where _get_or_create_ipo_entry might have used a different session
        # or if the object became detached.
        if not sa_inspect(ipo_db_entry).session or sa_inspect(ipo_db_entry).session is not self.db_session:
            logger.warning(f"IPO DB entry {ipo_name} is not bound to the current session. Merging.")
            ipo_db_entry = self.db_session.merge(ipo_db_entry)

        analysis_payload = {"key_data_snapshot": ipo_data_from_fetch.get("raw_data", {})}
        company_description = ipo_data_from_fetch.get("raw_data", {}).get("companyDescription",
                                                                          ipo_data_from_fetch.get("raw_data", {}).get(
                                                                              "description",
                                                                              "Not available from IPO calendar API."))

        prompt1_text = (
            f"For the upcoming IPO of '{ipo_name}', describe its likely business model. "
            f"Also, briefly outline the competitive landscape it operates in and the general health of its industry. "
            f"Use general knowledge if specific details are not provided. Company description if available: '{company_description}'"
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
        else:  # Default if split fails
            analysis_payload["competitive_landscape_summary"] = "N/A (or see business model)"
            analysis_payload["industry_health_summary"] = "N/A (or see business model)"

        prompt2_text = (
            f"For a company like '{ipo_name}' in its likely industry, what are typical uses of IPO proceeds? "
            f"And what are common major risk factors disclosed in prospectuses for such companies?"
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
            f"What key financial health indicators (e.g., revenue growth, profitability path) should an investor look for in the prospectus of '{ipo_name}'? "
            f"How would one typically approach valuing such an IPO against its public peers (e.g., relevant ratios like P/S if it's a growth company)?"
        )
        analysis_payload["pre_ipo_financials_summary"] = "Guidance: " + self.gemini.generate_text(prompt3_text)

        underwriters = ipo_data_from_fetch.get("raw_data", {}).get("underwriters", "N/A from calendar API")
        if isinstance(underwriters, list): underwriters = ", ".join(underwriters)  # Make it a string if it's a list
        analysis_payload[
            "underwriter_quality"] = f"Underwriters: {underwriters}. (Note: Quality assessment requires research on their track record)."
        analysis_payload["fresh_issue_vs_ofs"] = "N/A from calendar API. Check prospectus."
        analysis_payload["lock_up_periods_info"] = "N/A from calendar API. Check prospectus (typically 90-180 days)."
        analysis_payload[
            "investor_demand_summary"] = "To be assessed closer to IPO date (Anchor investors, subscription levels)."

        synthesis_prompt = (
            f"Based on the general understanding of the IPO for '{ipo_name}' (business model: {analysis_payload.get('business_model_summary', 'N/A')[:200]}..., "  # Snippet
            f"industry: {analysis_payload.get('industry_health_summary', 'N/A')[:200]}...), and considering typical IPO factors "
            f"(use of proceeds (general): {analysis_payload.get('use_of_proceeds_summary', 'N/A')[:200]}..., risks (general): {analysis_payload.get('risk_factors_summary', 'N/A')[:200]}...), "
            f"provide a brief, cautious investment perspective (max 3-4 sentences). What are the key things an investor should verify in the actual prospectus? "
            f"Suggest a preliminary stance (e.g., 'Interesting, research further', 'Approach with caution', 'Potentially attractive if fundamentals are strong in S-1'). "
            f"Do not give financial advice."
        )
        gemini_synthesis = self.gemini.generate_text(synthesis_prompt)

        final_decision = "Research Further / Cautious"
        if gemini_synthesis and not gemini_synthesis.startswith("Error:"):
            if "attractive" in gemini_synthesis.lower() or "interesting" in gemini_synthesis.lower():
                final_decision = "Interesting / Research Further"
            elif "caution" in gemini_synthesis.lower() or "high risk" in gemini_synthesis.lower():
                final_decision = "High Caution / Skeptical"

        analysis_payload["investment_decision"] = final_decision
        analysis_payload["reasoning"] = gemini_synthesis
        analysis_payload[
            "valuation_comparison_summary"] = "Requires peer data and IPO pricing from prospectus. AI was asked for general approach."

        ipo_analysis_entry = IPOAnalysis(
            ipo_id=ipo_db_entry.id,
            business_model_summary=analysis_payload.get("business_model_summary"),
            competitive_landscape_summary=analysis_payload.get("competitive_landscape_summary"),
            industry_health_summary=analysis_payload.get("industry_health_summary"),
            use_of_proceeds_summary=analysis_payload.get("use_of_proceeds_summary"),
            risk_factors_summary=analysis_payload.get("risk_factors_summary"),
            pre_ipo_financials_summary=analysis_payload.get("pre_ipo_financials_summary"),
            valuation_comparison_summary=analysis_payload.get("valuation_comparison_summary"),
            underwriter_quality=analysis_payload.get("underwriter_quality"),
            fresh_issue_vs_ofs=analysis_payload.get("fresh_issue_vs_ofs"),
            lock_up_periods_info=analysis_payload.get("lock_up_periods_info"),
            investor_demand_summary=analysis_payload.get("investor_demand_summary"),
            investment_decision=analysis_payload.get("investment_decision"),
            reasoning=analysis_payload.get("reasoning"),
            key_data_snapshot=analysis_payload.get("key_data_snapshot")
        )
        self.db_session.add(ipo_analysis_entry)
        ipo_db_entry.last_analysis_date = datetime.utcnow()

        try:
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved IPO: {ipo_name} (Analysis ID: {ipo_analysis_entry.id})")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving IPO analysis for {ipo_name}: {e}", exc_info=True)
            return None

        return ipo_analysis_entry

    def run_ipo_analysis_pipeline(self):
        all_upcoming_ipos = self.fetch_upcoming_ipos()
        analyzed_ipos_results = []
        if not all_upcoming_ipos:
            logger.info("No upcoming IPOs found from any source to analyze.")
            if self.db_session.is_active: self.db_session.close()
            return []

        for ipo_data in all_upcoming_ipos:
            if not self.db_session.is_active:  # Ensure session is active for each IPO
                self.db_session = next(get_db_session())

            result = self.analyze_single_ipo(ipo_data)
            if result:
                analyzed_ipos_results.append(result)
            # Consider a small delay if making many Gemini calls in a loop
            # time.sleep(1) # Optional delay

        logger.info(f"IPO analysis pipeline completed. Analyzed {len(analyzed_ipos_results)} IPOs.")
        if self.db_session.is_active:
            self.db_session.close()
        return analyzed_ipos_results


# Example usage:
if __name__ == '__main__':
    from sqlalchemy import inspect as sa_inspect  # For example usage if needed
    from database import init_db

    try:
        # init_db() # Assuming DB already init
        logger.info("Starting standalone IPO analysis pipeline test...")
        analyzer = IPOAnalyzer()
        results = analyzer.run_ipo_analysis_pipeline()
        if results:
            logger.info(f"Processed {len(results)} IPOs.")
            for res in results:
                logger.info(f"IPO: {res.ipo.company_name}, Decision: {res.investment_decision}")
        else:
            logger.info("No IPOs were processed or found by any active API.")
    except Exception as e:
        logger.error(f"Error during IPO analysis test in __main__: {e}", exc_info=True)