# services/ipo_analyzer/ipo_analyzer.py
import time
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timedelta, timezone
import concurrent.futures

from api_clients import FinnhubClient, GeminiAPIClient, SECEDGARClient
from database import SessionLocal, IPO, IPOAnalysis  # Removed get_db_session, manage session per thread
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError
from core.config import IPO_ANALYSIS_REANALYZE_DAYS

# Import functions from submodules
from .helpers import parse_ipo_date_string
from .data_fetcher import fetch_upcoming_ipo_data, fetch_s1_filing_data
from .db_handler import get_or_create_ipo_db_entry
from .ai_analyzer import perform_ai_analysis_for_ipo

MAX_IPO_ANALYSIS_WORKERS = 1  # Module-level constant, adjust as needed based on API limits and system resources


class IPOAnalyzer:
    def __init__(self):
        self.finnhub = FinnhubClient()
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()
        # Note: IPOAnalyzer orchestrates tasks, each task/thread will get its own DB session.

    def _analyze_single_ipo_task(self, db_session, ipo_data_from_fetch):
        """
        Analyzes a single IPO. This method is called by the thread worker.
        It uses the provided db_session.
        """
        ipo_identifier = ipo_data_from_fetch.get("company_name") or ipo_data_from_fetch.get("symbol") or "Unknown IPO"
        logger.info(
            f"Task: Starting analysis for IPO: {ipo_identifier} from source {ipo_data_from_fetch.get('source_api')}")

        ipo_db_entry = get_or_create_ipo_db_entry(self, db_session, ipo_data_from_fetch)
        if not ipo_db_entry:
            logger.error(
                f"Task: Could not get/create DB entry for IPO {ipo_identifier}. Aborting analysis for this item.")
            return None

        # Check if recent analysis exists and if significant data has changed
        reanalyze_threshold = datetime.now(timezone.utc) - timedelta(days=IPO_ANALYSIS_REANALYZE_DAYS)
        existing_analysis = db_session.query(IPOAnalysis).filter(IPOAnalysis.ipo_id == ipo_db_entry.id).order_by(
            IPOAnalysis.analysis_date.desc()).first()

        significant_change_detected = False
        if existing_analysis and existing_analysis.key_data_snapshot:
            # Compare key fields from the snapshot with current ipo_db_entry
            snap = existing_analysis.key_data_snapshot  # This is the raw_data from previous fetch
            snap_parsed_date = parse_ipo_date_string(snap.get("date"))  # Parse date from snapshot for comparison

            if (ipo_db_entry.ipo_date != snap_parsed_date or
                    ipo_db_entry.status != snap.get("status") or
                    ipo_db_entry.expected_price_range_low != snap.get(
                        "price_range_low") or  # Assuming snapshot keys match
                    ipo_db_entry.expected_price_range_high != snap.get("price_range_high")):
                significant_change_detected = True
                logger.info(f"Task: Significant data change detected for {ipo_identifier}. Re-analyzing.")

        if existing_analysis and not significant_change_detected and existing_analysis.analysis_date >= reanalyze_threshold:
            logger.info(
                f"Task: Recent analysis for {ipo_identifier} exists (Date: {existing_analysis.analysis_date}) and no significant changes. Skipping re-analysis.")
            return existing_analysis  # Return existing analysis if up-to-date and no major changes

        # Fetch S-1 data
        s1_text, s1_url = fetch_s1_filing_data(self, db_session, ipo_db_entry)
        # s1_url is already saved to ipo_db_entry by fetch_s1_filing_data if found

        # Perform AI analysis
        # ipo_data_from_fetch contains 'raw_data' which is the original API response
        analysis_payload = perform_ai_analysis_for_ipo(self, ipo_db_entry, s1_text,
                                                       ipo_data_from_fetch.get("raw_data", {}))

        current_time = datetime.now(timezone.utc)

        if existing_analysis:  # Update existing analysis entry
            logger.info(f"Task: Updating existing analysis for {ipo_identifier} (ID: {existing_analysis.id})")
            for key, value in analysis_payload.items():
                setattr(existing_analysis, key, value)
            existing_analysis.analysis_date = current_time
            entry_to_save = existing_analysis
        else:  # Create new analysis entry
            logger.info(f"Task: Creating new analysis for {ipo_identifier}")
            entry_to_save = IPOAnalysis(
                ipo_id=ipo_db_entry.id,
                analysis_date=current_time,
                **analysis_payload
            )
            db_session.add(entry_to_save)

        ipo_db_entry.last_analysis_date = current_time  # Update parent IPO's last analysis date

        try:
            db_session.commit()
            logger.info(f"Task: Saved IPO analysis for {ipo_identifier} (Analysis ID: {entry_to_save.id})")
        except SQLAlchemyError as e:
            db_session.rollback()
            logger.error(f"Task: DB error saving IPO analysis for {ipo_identifier}: {e}", exc_info=True)
            return None  # Indicate failure

        return entry_to_save

    def run_ipo_analysis_pipeline(self):
        all_upcoming_ipos_from_api = fetch_upcoming_ipo_data(self)
        analyzed_results = []

        if not all_upcoming_ipos_from_api:
            logger.info("No upcoming IPOs found to analyze.")
            return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_IPO_ANALYSIS_WORKERS) as executor:
            future_to_ipo_data = {}
            for ipo_data in all_upcoming_ipos_from_api:
                # Filter IPOs that are relevant for analysis (e.g., not withdrawn, has a name)
                status = ipo_data.get("status", "").lower()
                # Define statuses that are worth analyzing
                relevant_statuses = ["expected", "filed", "priced", "upcoming", "active"]  # Adjust as needed

                if status not in relevant_statuses or not ipo_data.get("company_name"):
                    logger.debug(
                        f"Skipping IPO '{ipo_data.get('company_name', 'N/A')}' due to status '{status}' or missing name.")
                    continue

                # Submit the task to the executor
                future = executor.submit(self._thread_worker_analyze_ipo, ipo_data)
                future_to_ipo_data[future] = ipo_data.get("company_name", "Unknown IPO")

            for future in concurrent.futures.as_completed(future_to_ipo_data):
                ipo_name = future_to_ipo_data[future]
                try:
                    result = future.result()  # This will block until the future is complete
                    if result:
                        analyzed_results.append(result)
                except Exception as exc:
                    logger.error(f"IPO analysis for '{ipo_name}' generated an exception in thread: {exc}",
                                 exc_info=True)

        logger.info(
            f"IPO analysis pipeline completed. Processed {len(analyzed_results)} IPOs that required new/updated analysis.")
        return analyzed_results

    def _thread_worker_analyze_ipo(self, ipo_data_from_fetch):
        """
        Worker function for each thread. Manages its own DB session.
        """
        db_session = SessionLocal()  # Create a new session for this thread
        try:
            return self._analyze_single_ipo_task(db_session, ipo_data_from_fetch)
        finally:
            SessionLocal.remove()  # Remove the session, effectively closing it for this thread