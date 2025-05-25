# services/stock_analyzer/stock_analyzer.py
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone
import math
import time
import warnings
from bs4 import XMLParsedAsHTMLWarning
import json

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from api_clients import (
    FinnhubClient, FinancialModelingPrepClient, AlphaVantageClient,
    EODHDClient, GeminiAPIClient, SECEDGARClient
)
from database import SessionLocal, get_db_session, Stock, StockAnalysis
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError

# Import functions from submodules
from .data_fetcher import fetch_financial_statements_data, fetch_key_metrics_and_profile_data
from .metrics_calculator import calculate_all_derived_metrics
from .dcf_analyzer import perform_dcf_analysis
from .qualitative_analyzer import fetch_and_summarize_10k_data, fetch_and_analyze_competitors
from .ai_synthesis import synthesize_investment_thesis


class StockAnalyzer:
    def __init__(self, ticker):
        self.ticker = ticker.upper()
        self.finnhub = FinnhubClient()
        self.fmp = FinancialModelingPrepClient()
        self.alphavantage = AlphaVantageClient()
        self.eodhd = EODHDClient()  # Currently unused but kept for potential future use
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()

        self.db_session = next(get_db_session())
        self.stock_db_entry = None
        self._financial_data_cache = {}  # Holds all fetched and calculated data for the analysis run
        self.data_quality_warnings = []  # Collects warnings during data processing

        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"CRITICAL: Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            self._close_session_if_active()  # Ensure session is closed on error
            # Re-raise as a more specific error or allow main handler to catch
            raise RuntimeError(
                f"StockAnalyzer for {self.ticker} could not be initialized due to DB/API issues during stock entry setup.") from e

    def _close_session_if_active(self):
        if self.db_session and self.db_session.is_active:
            try:
                self.db_session.close()
                logger.debug(f"DB session closed for {self.ticker}.")
            except Exception as e_close:
                logger.warning(f"Error closing session for {self.ticker}: {e_close}")

    def _get_or_create_stock_entry(self):
        # Ensure session is active before starting
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} inactive in _get_or_create. Re-establishing.")
            self._close_session_if_active()  # Close old one just in case
            self.db_session = next(get_db_session())

        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()

        company_name, industry, sector, cik = None, None, None, None

        # Try FMP first for profile data
        profile_fmp_list = self.fmp.get_company_profile(self.ticker)
        time.sleep(1.5)  # API call delay
        if profile_fmp_list and isinstance(profile_fmp_list, list) and len(profile_fmp_list) > 0 and profile_fmp_list[
            0]:
            data = profile_fmp_list[0]
            self._financial_data_cache['profile_fmp'] = data  # Cache for later use
            company_name = data.get('companyName')
            industry = data.get('industry')
            sector = data.get('sector')
            cik_val = data.get('cik')
            if cik_val: cik = str(cik_val).zfill(10)  # Pad CIK
            logger.info(f"Fetched profile from FMP for {self.ticker}.")
        else:
            logger.warning(f"FMP profile failed or empty for {self.ticker}. Trying Finnhub.")
            profile_fh = self.finnhub.get_company_profile2(self.ticker)
            time.sleep(1.5)
            if profile_fh:
                self._financial_data_cache['profile_finnhub'] = profile_fh
                company_name = profile_fh.get('name')
                industry = profile_fh.get('finnhubIndustry')
                # Finnhub doesn't typically provide sector or CIK directly in profile2
                logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
            else:
                logger.warning(f"Finnhub profile failed for {self.ticker}. Trying Alpha Vantage Overview.")
                overview_av = self.alphavantage.get_company_overview(self.ticker)
                time.sleep(2)  # AV can be slow/rate-limited
                if overview_av and overview_av.get("Symbol") == self.ticker:  # Ensure it's the correct ticker
                    self._financial_data_cache['overview_alphavantage'] = overview_av
                    company_name = overview_av.get('Name')
                    industry = overview_av.get('Industry')
                    sector = overview_av.get('Sector')
                    cik_val = overview_av.get('CIK')
                    if cik_val: cik = str(cik_val).zfill(10)
                    logger.info(f"Fetched overview from Alpha Vantage for {self.ticker}.")
                else:
                    logger.warning(
                        f"All primary profile fetches (FMP, Finnhub, AV) failed or incomplete for {self.ticker}.")

        if not company_name: company_name = self.ticker  # Fallback if no name found

        # If CIK is still missing, try SEC EDGAR map
        if not cik and self.ticker:
            logger.info(f"CIK not found from profiles for {self.ticker}. Querying SEC EDGAR CIK map.")
            cik_from_edgar = self.sec_edgar.get_cik_by_ticker(self.ticker)
            time.sleep(0.5)
            if cik_from_edgar:
                cik = str(cik_from_edgar).zfill(10)
                logger.info(f"Fetched CIK {cik} from SEC EDGAR CIK map for {self.ticker}.")
            else:
                logger.warning(f"Could not fetch CIK from SEC EDGAR CIK map for {self.ticker}.")

        if not self.stock_db_entry:
            logger.info(f"Stock {self.ticker} not found in DB, creating new entry.")
            self.stock_db_entry = Stock(
                ticker=self.ticker,
                company_name=company_name,
                industry=industry,
                sector=sector,
                cik=cik
            )
            self.db_session.add(self.stock_db_entry)
            try:
                self.db_session.commit()
                self.db_session.refresh(self.stock_db_entry)
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating stock entry for {self.ticker}: {e}", exc_info=True)
                raise  # Re-raise to indicate critical failure
        else:
            # Update existing entry if new data is available and different
            updated = False
            if company_name and self.stock_db_entry.company_name != company_name:
                self.stock_db_entry.company_name = company_name;
                updated = True
            if industry and self.stock_db_entry.industry != industry:
                self.stock_db_entry.industry = industry;
                updated = True
            if sector and self.stock_db_entry.sector != sector:
                self.stock_db_entry.sector = sector;
                updated = True
            if cik and self.stock_db_entry.cik != cik:  # CIK found and is different
                self.stock_db_entry.cik = cik;
                updated = True
            elif not self.stock_db_entry.cik and cik:  # CIK was missing, now found
                self.stock_db_entry.cik = cik;
                updated = True

            if updated:
                try:
                    self.db_session.commit()
                    self.db_session.refresh(self.stock_db_entry)
                    logger.info(f"Updated stock entry for {self.ticker} with new profile data.")
                except SQLAlchemyError as e:
                    self.db_session.rollback()
                    logger.error(f"Error updating stock entry for {self.ticker}: {e}")

        logger.info(
            f"Stock entry for {self.ticker} (ID: {self.stock_db_entry.id if self.stock_db_entry else 'N/A'}, CIK: {self.stock_db_entry.cik if self.stock_db_entry and self.stock_db_entry.cik else 'N/A'}) ready.")

    def _ensure_stock_db_entry_is_bound(self):
        if not self.stock_db_entry:
            raise RuntimeError(
                f"Stock entry for {self.ticker} is None during binding check. Prior initialization failure.")

        if not self.db_session.is_active:
            logger.warning(f"DB Session for {self.ticker} was INACTIVE before operation. Re-establishing.")
            self._close_session_if_active()  # Close old session
            self.db_session = next(get_db_session())  # Get a new session

            # Re-fetch the stock entry with the new session
            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock:
                # This is a critical state, as the stock entry should exist
                raise RuntimeError(
                    f"Failed to re-fetch stock {self.ticker} for new session after inactivity. Critical state.")
            self.stock_db_entry = re_fetched_stock
            logger.info(
                f"Re-fetched and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new active session.")
            return  # Successfully re-bound

        # Check if the current stock_db_entry is associated with the current db_session
        instance_state = sa_inspect(self.stock_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            obj_id_log = self.stock_db_entry.id if instance_state.has_identity else 'Transient/No ID'
            logger.warning(
                f"Stock {self.ticker} (ID: {obj_id_log}) DETACHED or bound to DIFFERENT session. Attempting to merge.")
            try:
                # Merge the detached instance into the current session
                self.stock_db_entry = self.db_session.merge(self.stock_db_entry)
                self.db_session.flush()  # Ensure it's actually in the session's identity map
                logger.info(
                    f"Successfully merged stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session.")
            except Exception as e_merge:
                logger.error(f"Failed to merge stock {self.ticker} into session: {e_merge}. Re-fetching as a fallback.",
                             exc_info=True)
                # Fallback: try to load it directly from the DB with the current session
                re_fetched_from_db_after_merge_fail = self.db_session.query(Stock).filter(
                    Stock.ticker == self.ticker).first()
                if re_fetched_from_db_after_merge_fail:
                    self.stock_db_entry = re_fetched_from_db_after_merge_fail
                    logger.info(
                        f"Successfully re-fetched stock {self.ticker} (ID: {self.stock_db_entry.id}) after merge failure.")
                else:
                    # This is a critical failure if the stock cannot be associated with the session
                    raise RuntimeError(
                        f"Failed to bind stock {self.ticker} to session after merge failure and re-fetch attempt. Analysis cannot proceed.")

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_data_for_db = {}
        try:
            if not self.stock_db_entry:
                logger.error(f"Stock DB entry for {self.ticker} not initialized properly. Aborting analysis.")
                return None

            self._ensure_stock_db_entry_is_bound()  # Ensure stock object is session-bound

            # Step 1: Fetch all raw data
            fetch_financial_statements_data(self)
            fetch_key_metrics_and_profile_data(self)

            # Step 2: Calculate quantitative metrics
            final_data_for_db.update(calculate_all_derived_metrics(self))
            final_data_for_db.update(perform_dcf_analysis(self))

            # Step 3: Perform qualitative analysis (10-K, Competitors)
            qual_summaries = fetch_and_summarize_10k_data(self)
            final_data_for_db.update(qual_summaries)  # This includes business_summary, risk_factors_summary etc.

            # fetch_and_analyze_competitors returns the summary string directly
            final_data_for_db["competitive_landscape_summary"] = fetch_and_analyze_competitors(self)

            # Step 4: AI Synthesis for Investment Thesis
            final_data_for_db.update(synthesize_investment_thesis(self))

            # Step 5: Save to Database
            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id, analysis_date=datetime.now(timezone.utc))

            model_fields = [c.key for c in StockAnalysis.__table__.columns if
                            c.key not in ['id', 'stock_id', 'analysis_date']]

            for field_name in model_fields:
                if field_name in final_data_for_db:
                    value_to_set = final_data_for_db[field_name]
                    target_column_type = getattr(StockAnalysis, field_name).type.python_type

                    # Type checking and conversion for float
                    if target_column_type == float:
                        if isinstance(value_to_set, str):
                            try:
                                value_to_set = float(value_to_set)
                            except ValueError:
                                value_to_set = None  # Cannot convert string to float
                        if isinstance(value_to_set, float) and (math.isnan(value_to_set) or math.isinf(value_to_set)):
                            value_to_set = None  # SQLAlchemy typically handles None for nullable float fields
                    elif target_column_type == dict and not isinstance(value_to_set, dict):
                        # If it's supposed to be JSON/dict but isn't, set to None or handle as error
                        logger.warning(f"Field {field_name} expected dict, got {type(value_to_set)}. Setting to None.")
                        value_to_set = None
                    elif target_column_type == str and not isinstance(value_to_set, str):
                        value_to_set = str(value_to_set) if value_to_set is not None else None

                    setattr(analysis_entry, field_name, value_to_set)

            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = analysis_entry.analysis_date  # Update parent stock
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved stock data: {self.ticker} (Analysis ID: {analysis_entry.id})")
            return analysis_entry

        except RuntimeError as rt_err:  # Catch specific init error or binding error
            logger.critical(f"Runtime error during full analysis for {self.ticker}: {rt_err}", exc_info=True)
            # Session might already be closed by the raiser or needs to be handled by caller
            return None
        except Exception as e:
            logger.error(f"CRITICAL error in full analysis pipeline for {self.ticker}: {e}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try:
                    self.db_session.rollback()
                    logger.info(f"Rolled back DB transaction for {self.ticker} due to error.")
                except Exception as e_rb:
                    logger.error(f"Rollback error for {self.ticker}: {e_rb}")
            return None
        finally:
            self._close_session_if_active()  # Ensure session is closed at the end of analysis