# ipo_analyzer.py
import time

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
        self.rapid_ipo = RapidAPIUpcomingIPOCalendarClient()  # This might be the primary source for upcoming
        self.gemini = GeminiAPIClient()
        self.db_session = next(get_db_session())

    def fetch_upcoming_ipos(self):
        logger.info("Fetching upcoming IPOs...")
        ipos_data = []

        # Try FMP
        fmp_ipos = self.fmp.get_ipo_calendar()
        if fmp_ipos and isinstance(fmp_ipos, list):
            for ipo in fmp_ipos:
                ipos_data.append({
                    "name": ipo.get("name"),
                    "symbol": ipo.get("symbol"),  # Often assigned post-IPO or just before
                    "date": ipo.get("date"),
                    "price_range_low": ipo.get("priceFrom"),  # FMP uses priceFrom/priceTo
                    "price_range_high": ipo.get("priceTo"),
                    "exchange": ipo.get("exchange"),
                    "source": "FMP",
                    "raw_data": ipo
                })
            logger.info(f"Fetched {len(fmp_ipos)} IPOs from FMP.")

        # Try EODHD (structure might differ)
        eodhd_ipos = self.eodhd.get_ipo_calendar()  # Check EODHD documentation for date formats if needed
        if eodhd_ipos and isinstance(eodhd_ipos, list):  # Assuming it's a list of IPOs
            for ipo in eodhd_ipos:  # Adjust keys based on actual EODHD response
                if ipo.get("Code") and ipo.get("Name"):  # EODHD might use 'Code' for symbol
                    ipos_data.append({
                        "name": ipo.get("Name"),
                        "symbol": ipo.get("Code"),
                        "date": ipo.get("Date"),  # e.g. '2023-10-27'
                        "price_range_low": ipo.get("price_from"),  # Or similar key
                        "price_range_high": ipo.get("price_to"),
                        "exchange": ipo.get("Exchange"),
                        "source": "EODHD",
                        "raw_data": ipo
                    })
            logger.info(f"Fetched {len(eodhd_ipos)} IPOs from EODHD.")

        # Try RapidAPI (structure will be specific to that API)
        # rapid_data = self.rapid_ipo.get_ipo_calendar()
        # if rapid_data and isinstance(rapid_data, list): # Or dict, depends on API
        #    # Parse and append to ipos_data
        #    logger.info(f"Fetched IPOs from RapidAPI.")
        # else:
        #    logger.warning("No IPO data or unexpected format from RapidAPI.")

        # Deduplicate IPOs by name (simple deduplication)
        unique_ipos = []
        seen_names = set()
        for ipo_info in ipos_data:
            if ipo_info.get("name") and ipo_info["name"] not in seen_names:
                unique_ipos.append(ipo_info)
                seen_names.add(ipo_info["name"])

        logger.info(f"Total unique IPOs fetched: {len(unique_ipos)}")
        return unique_ipos

    def _get_or_create_ipo_entry(self, ipo_data):
        ipo_entry = self.db_session.query(IPO).filter_by(company_name=ipo_data["name"]).first()
        if not ipo_entry:
            logger.info(f"IPO {ipo_data['name']} not found in DB, creating new entry.")
            price_range = f"{ipo_data.get('price_range_low')} - {ipo_data.get('price_range_high')}" if ipo_data.get(
                'price_range_low') else "N/A"
            ipo_entry = IPO(
                company_name=ipo_data["name"],
                ipo_date=str(ipo_data.get("date")),  # Ensure it's string
                expected_price_range=price_range
            )
            self.db_session.add(ipo_entry)
            try:
                self.db_session.commit()
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating IPO entry for {ipo_data['name']}: {e}")
                return None  # Critical failure
        return ipo_entry

    def analyze_single_ipo(self, ipo_data_from_fetch):
        """
        Analyzes a single IPO. `ipo_data_from_fetch` is one item from `fetch_upcoming_ipos`.
        Actual IPO analysis often requires parsing S-1 filings, which is beyond simple API calls.
        This will be a high-level analysis based on what APIs provide and Gemini's general knowledge.
        """
        ipo_name = ipo_data_from_fetch.get("name")
        logger.info(f"Starting analysis for IPO: {ipo_name}")

        ipo_db_entry = self._get_or_create_ipo_entry(ipo_data_from_fetch)
        if not ipo_db_entry:
            logger.error(f"Could not get or create DB entry for IPO {ipo_name}. Aborting analysis.")
            return None

        analysis_payload = {"key_data_snapshot": ipo_data_from_fetch.get("raw_data", {})}  # Store raw API data

        # Prompts for Gemini based on IPO analysis structure from requirements
        # 1. Business Model, Competitive Landscape, Industry Health
        # These would ideally come from prospectus (S-1). Since we don't parse S-1s here,
        # we rely on Gemini's general knowledge or if API provides some description.
        company_description = ipo_data_from_fetch.get("raw_data", {}).get("companyDescription",
                                                                          "Not available from IPO calendar API.")  # FMP might have it for some

        prompt1_text = (
            f"For the upcoming IPO of '{ipo_name}', describe its likely business model. "
            f"Also, briefly outline the competitive landscape it operates in and the general health of its industry. "
            f"Use general knowledge if specific details are not provided. Company description if available: '{company_description}'"
        )
        response1 = self.gemini.generate_text(prompt1_text)
        # Naive split - Gemini might not follow this structure perfectly.
        parts = response1.split("Competitive Landscape:")
        analysis_payload["business_model_summary"] = parts[0].replace("Business Model:", "").strip()
        if len(parts) > 1:
            sub_parts = parts[1].split("Industry Health:")
            analysis_payload["competitive_landscape_summary"] = sub_parts[0].strip()
            if len(sub_parts) > 1:
                analysis_payload["industry_health_summary"] = sub_parts[1].strip()

        # 2. Use of Proceeds, Risk Factors (Typically from Prospectus)
        # This is very hard without S-1. We'll ask Gemini for *typical* uses and risks.
        prompt2_text = (
            f"For a company like '{ipo_name}' in its likely industry, what are typical uses of IPO proceeds? "
            f"And what are common major risk factors disclosed in prospectuses for such companies?"
        )
        response2 = self.gemini.generate_text(prompt2_text)
        parts2 = response2.split("Risk Factors:")
        analysis_payload["use_of_proceeds_summary"] = "General (Not from Prospectus): " + parts2[0].replace(
            "Typical Uses of IPO Proceeds:", "").strip()
        if len(parts2) > 1:
            analysis_payload["risk_factors_summary"] = "General (Not from Prospectus): " + parts2[1].strip()

        # 3. Financial Health and Valuation (Pre-IPO)
        # APIs for IPO calendars rarely give pre-IPO financials. FMP *might* for some if they filed.
        # We'll ask Gemini for what to look for.
        prompt3_text = (
            f"What key financial health indicators (e.g., revenue growth, profitability path) should an investor look for in the prospectus of '{ipo_name}'? "
            f"How would one typically approach valuing such an IPO against its public peers (e.g., relevant ratios like P/S if it's a growth company)?"
        )
        analysis_payload["pre_ipo_financials_summary"] = "Guidance: " + self.gemini.generate_text(prompt3_text)
        # Actual valuation requires peer data and IPO pricing details.

        # 4. IPO Specifics (Underwriter, OFS, Lock-up)
        # This information IS sometimes in good IPO calendar APIs.
        underwriters = ipo_data_from_fetch.get("raw_data", {}).get("underwriters", "N/A from calendar API")
        analysis_payload[
            "underwriter_quality"] = f"Underwriters: {underwriters}. (Note: Quality assessment requires research on their track record)."

        # Offer for Sale (OFS) vs Fresh Issue - Not typically in basic calendar data.
        analysis_payload[
            "fresh_issue_vs_ofs"] = "N/A from calendar API. Check prospectus for details (important for where money goes)."

        # Lock-up periods - Not typically in basic calendar data.
        analysis_payload["lock_up_periods_info"] = "N/A from calendar API. Check prospectus (typically 90-180 days)."

        # 5. Market Sentiment and Investor Demand (Anchor, Subscription)
        # This is dynamic information available closer to/during IPO.
        analysis_payload[
            "investor_demand_summary"] = "To be assessed closer to IPO date (Anchor investors, subscription levels). High demand is generally positive."

        # 6. Investment Decision and Reasoning (Synthesized by Gemini)
        synthesis_prompt = (
            f"Based on the general understanding of the IPO for '{ipo_name}' (business model: {analysis_payload.get('business_model_summary', 'N/A')}, "
            f"industry: {analysis_payload.get('industry_health_summary', 'N/A')}), and considering typical IPO factors "
            f"(use of proceeds: {analysis_payload.get('use_of_proceeds_summary', 'N/A')}, risks: {analysis_payload.get('risk_factors_summary', 'N/A')}), "
            f"provide a brief, cautious investment perspective. What are the key things an investor should verify in the actual prospectus? "
            f"Suggest a preliminary stance (e.g., 'Interesting, research further', 'Approach with caution', 'Potentially attractive if fundamentals are strong in S-1'). "
            f"Do not give financial advice."
        )
        gemini_synthesis = self.gemini.generate_text(synthesis_prompt)

        final_decision = "Research Further / Cautious"  # Default
        if "attractive" in gemini_synthesis.lower() or "interesting" in gemini_synthesis.lower():
            final_decision = "Interesting / Research Further"
        elif "caution" in gemini_synthesis.lower() or "high risk" in gemini_synthesis.lower():
            final_decision = "High Caution / Skeptical"

        analysis_payload["investment_decision"] = final_decision
        analysis_payload["reasoning"] = gemini_synthesis

        # Store analysis
        ipo_analysis_entry = IPOAnalysis(
            ipo_id=ipo_db_entry.id,
            business_model_summary=analysis_payload.get("business_model_summary"),
            competitive_landscape_summary=analysis_payload.get("competitive_landscape_summary"),
            industry_health_summary=analysis_payload.get("industry_health_summary"),
            use_of_proceeds_summary=analysis_payload.get("use_of_proceeds_summary"),
            risk_factors_summary=analysis_payload.get("risk_factors_summary"),
            pre_ipo_financials_summary=analysis_payload.get("pre_ipo_financials_summary"),
            valuation_comparison_summary=analysis_payload.get("valuation_comparison_summary",
                                                              "Requires peer data and IPO pricing from prospectus."),
            # Placeholder
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
            logger.info(f"Successfully analyzed and saved IPO: {ipo_name}")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving IPO analysis for {ipo_name}: {e}", exc_info=True)
            return None
        finally:
            self.db_session.close()  # Close session after each IPO analysis or batch

        return ipo_analysis_entry

    def run_ipo_analysis_pipeline(self):
        all_upcoming_ipos = self.fetch_upcoming_ipos()
        analyzed_ipos_results = []
        if not all_upcoming_ipos:
            logger.info("No upcoming IPOs found to analyze.")
            return []

        for ipo_data in all_upcoming_ipos:
            # Re-initialize session for each IPO if needed, or manage one session for the batch.
            # Current setup uses one session per IPOAnalyzer instance, then closes.
            # If run_ipo_analysis_pipeline is the main entry, session management might need adjustment
            # For now, let's assume self.db_session is valid or re-acquired.
            # If it was closed by analyze_single_ipo, we need a new one.
            if self.db_session.is_active:  # type: ignore
                pass  # it's fine
            else:  # It was closed
                self.db_session = next(get_db_session())

            result = self.analyze_single_ipo(ipo_data)
            if result:
                analyzed_ipos_results.append(result)
            time.sleep(2)  # Basic courtesy delay between extensive Gemini calls for multiple IPOs

        logger.info(f"IPO analysis pipeline completed. Analyzed {len(analyzed_ipos_results)} IPOs.")
        if self.db_session.is_active:  # type: ignore
            self.db_session.close()
        return analyzed_ipos_results


# Example usage:
if __name__ == '__main__':
    from database import init_db

    try:
        init_db()
        logger.info("Starting standalone IPO analysis pipeline test...")
        analyzer = IPOAnalyzer()
        results = analyzer.run_ipo_analysis_pipeline()
        if results:
            logger.info(f"Processed {len(results)} IPOs.")
            for res in results:
                logger.info(f"IPO: {res.ipo.company_name}, Decision: {res.investment_decision}")
        else:
            logger.info("No IPOs were processed or found.")
    except Exception as e:
        logger.error(f"Error during IPO analysis test: {e}", exc_info=True)