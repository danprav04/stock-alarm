# services/ipo_analyzer/ipo_analyzer.py
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
            snap = existing_analysis.key_data_snapshot
            snap_parsed_date = parse_ipo_date_string(snap.get("date"))

            # Compare key fields for significant changes
            if (ipo_db_entry.ipo_date != snap_parsed_date or
                    ipo_db_entry.status != snap.get("status") or
                    ipo_db_entry.expected_price_range_low != (
                            parse_ipo_date_string(snap.get("price").split('-')[0]) if isinstance(snap.get("price"),
                                                                                                 str) and '-' in snap.get(
                                "price") else snap.get("price")) or
                    # Crude price parsing for snapshot; assumes a simple number or "low-high"
                    ipo_db_entry.expected_price_range_high != (
                            parse_ipo_date_string(snap.get("price").split('-')[1]) if isinstance(snap.get("price"),
                                                                                                 str) and '-' in snap.get(
                                "price") and len(snap.get("price").split('-')) > 1 else snap.get("price"))):
                significant_change_detected = True
                logger.info(f"Task: Significant data change detected for {ipo_identifier}. Re-analyzing.")

        if existing_analysis and not significant_change_detected and existing_analysis.analysis_date >= reanalyze_threshold:
            logger.info(
                f"Task: Recent analysis for {ipo_identifier} exists (Date: {existing_analysis.analysis_date}) and no significant changes. Skipping re-analysis.")
            return existing_analysis

        s1_text, s1_url = fetch_s1_filing_data(self, db_session, ipo_db_entry)

        analysis_payload = perform_ai_analysis_for_ipo(self, ipo_db_entry, s1_text,
                                                       ipo_data_from_fetch.get("raw_data", {}))

        current_time = datetime.now(timezone.utc)

        if existing_analysis:
            logger.info(f"Task: Updating existing analysis for {ipo_identifier} (ID: {existing_analysis.id})")
            for key, value in analysis_payload.items():
                setattr(existing_analysis, key, value)
            existing_analysis.analysis_date = current_time
            entry_to_save = existing_analysis
        else:
            logger.info(f"Task: Creating new analysis for {ipo_identifier}")
            entry_to_save = IPOAnalysis(
                ipo_id=ipo_db_entry.id,
                analysis_date=current_time,
                **analysis_payload
            )
            db_session.add(entry_to_save)

        ipo_db_entry.last_analysis_date = current_time

        try:
            db_session.commit()
            logger.info(f"Task: Saved IPO analysis for {ipo_identifier} (Analysis ID: {entry_to_save.id})")
        except SQLAlchemyError as e:
            db_session.rollback()
            logger.error(f"Task: DB error saving IPO analysis for {ipo_identifier}: {e}", exc_info=True)
            return None

        return entry_to_save

    def run_ipo_analysis_pipeline(self, upcoming_only=False, max_to_analyze=None):
        all_ipos_from_api = fetch_upcoming_ipo_data(self)
        analyzed_results = []

        if not all_ipos_from_api:
            logger.info("No upcoming IPOs found from data fetcher to analyze.")
            return []

        # 1. Pre-filter for essential data (e.g., company name)
        pre_filtered_ipos = []
        for ipo_data in all_ipos_from_api:
            if not ipo_data.get("company_name"):
                logger.debug(f"Skipping IPO due to missing company name: {ipo_data.get('symbol', 'N/A')}")
                continue
            pre_filtered_ipos.append(ipo_data)

        if not pre_filtered_ipos:
            logger.info("No IPOs remain after pre-filtering for essential data.")
            return []

        # 2. Sort by IPO date (earliest first). Parsed 'ipo_date' (date object) is used.
        #    None dates or unparseable dates are pushed to the end.
        pre_filtered_ipos.sort(key=lambda x: x.get("ipo_date") or datetime.max.date())

        # 3. Apply 'upcoming_only' and status filtering
        relevant_ipos_to_process = []
        today_date = datetime.now(timezone.utc).date()
        # Finnhub statuses: "Expected", "Priced", "Filed", "Withdrawn"
        # "Priced" can be upcoming if its date is in the future.
        # "Filed" and "Expected" are generally upcoming.
        valid_statuses_for_analysis = ["expected", "filed", "priced", "upcoming", "active"]  # Generic upcoming statuses

        if upcoming_only:
            for ipo_data in pre_filtered_ipos:
                ipo_date_obj = ipo_data.get("ipo_date")  # This is a date object
                status = ipo_data.get("status", "").lower()
                if ipo_date_obj and ipo_date_obj >= today_date and \
                        status in valid_statuses_for_analysis and status != "withdrawn":
                    relevant_ipos_to_process.append(ipo_data)
            logger.info(
                f"Filtered for upcoming IPOs only: {len(relevant_ipos_to_process)} IPOs remain for potential analysis.")
        else:
            # If not 'upcoming_only', still filter out "withdrawn" and ensure a valid status
            for ipo_data in pre_filtered_ipos:
                status = ipo_data.get("status", "").lower()
                if status in valid_statuses_for_analysis and status != "withdrawn":
                    relevant_ipos_to_process.append(ipo_data)
            logger.info(
                f"Not filtering for upcoming only. {len(relevant_ipos_to_process)} IPOs with valid status remain for potential analysis.")

        if not relevant_ipos_to_process:
            logger.info("No IPOs left after upcoming/status filtering.")
            return []

        # 4. Apply 'max_to_analyze' limit
        if max_to_analyze is not None and isinstance(max_to_analyze, int) and max_to_analyze > 0:
            if len(relevant_ipos_to_process) > max_to_analyze:
                logger.info(
                    f"Limiting IPOs to analyze to the earliest {max_to_analyze} from {len(relevant_ipos_to_process)} relevant IPOs.")
                relevant_ipos_to_process = relevant_ipos_to_process[:max_to_analyze]
            else:
                logger.info(
                    f"Number of relevant IPOs ({len(relevant_ipos_to_process)}) is within or equal to max_to_analyze ({max_to_analyze}). Analyzing all {len(relevant_ipos_to_process)}.")
        else:
            logger.info(
                f"No limit set for max_to_analyze, proceeding with {len(relevant_ipos_to_process)} relevant IPOs.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_IPO_ANALYSIS_WORKERS) as executor:
            future_to_ipo_data = {}
            for ipo_data_for_task in relevant_ipos_to_process:  # Use the filtered list
                future = executor.submit(self._thread_worker_analyze_ipo, ipo_data_for_task)
                future_to_ipo_data[future] = ipo_data_for_task.get("company_name", "Unknown IPO")

            for future in concurrent.futures.as_completed(future_to_ipo_data):
                ipo_name = future_to_ipo_data[future]
                try:
                    result = future.result()
                    if result:
                        analyzed_results.append(result)
                except Exception as exc:
                    logger.error(f"IPO analysis for '{ipo_name}' generated an exception in thread: {exc}",
                                 exc_info=True)

        logger.info(
            f"IPO analysis pipeline completed. Processed {len(analyzed_results)} IPOs that required new/updated analysis from the filtered set.")
        return analyzed_results

    def _thread_worker_analyze_ipo(self, ipo_data_from_fetch):
        """
        Worker function for each thread. Manages its own DB session.
        """
        db_session = SessionLocal()
        try:
            return self._analyze_single_ipo_task(db_session, ipo_data_from_fetch)
        finally:
            SessionLocal.remove()