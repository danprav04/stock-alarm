# services/stock_analyzer/stock_analyzer.py
from sqlalchemy import inspect as sa_inspect
from datetime import datetime, timezone
import math
import time
import warnings
from bs4 import XMLParsedAsHTMLWarning
import json
import sqlalchemy  # Added import for sqlalchemy

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from api_clients import (
    FinnhubClient, FinancialModelingPrepClient, AlphaVantageClient,
    EODHDClient, GeminiAPIClient, SECEDGARClient
)
from database import SessionLocal, get_db_session, Stock, StockAnalysis
from core.logging_setup import logger
from sqlalchemy.exc import SQLAlchemyError

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
        self.eodhd = EODHDClient()
        self.gemini = GeminiAPIClient()
        self.sec_edgar = SECEDGARClient()

        self.db_session = next(get_db_session())
        self.stock_db_entry = None
        self._financial_data_cache = {}
        self.data_quality_warnings = []

        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"CRITICAL: Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            self._close_session_if_active()
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
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} inactive in _get_or_create. Re-establishing.")
            self._close_session_if_active()
            self.db_session = next(get_db_session())

        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()

        company_name, industry, sector, cik = None, None, None, None
        profile_source_preference = ["fmp", "finnhub", "alphavantage"]

        for source in profile_source_preference:
            if source == "fmp":
                profile_fmp_list = self.fmp.get_company_profile(self.ticker)
                time.sleep(1)
                if profile_fmp_list and isinstance(profile_fmp_list, list) and profile_fmp_list[0]:
                    data = profile_fmp_list[0]
                    self._financial_data_cache['profile_fmp'] = data
                    company_name = data.get('companyName')
                    industry = data.get('industry')
                    sector = data.get('sector')
                    cik_val = data.get('cik')
                    if cik_val: cik = str(cik_val).zfill(10)
                    logger.info(f"Fetched profile from FMP for {self.ticker}.")
                    break
            elif source == "finnhub" and not company_name:
                profile_fh = self.finnhub.get_company_profile2(self.ticker)
                time.sleep(1)
                if profile_fh:
                    self._financial_data_cache['profile_finnhub'] = profile_fh
                    company_name = profile_fh.get('name')
                    industry = profile_fh.get('finnhubIndustry') or industry
                    logger.info(f"Fetched profile from Finnhub for {self.ticker}.")
                    break
            elif source == "alphavantage" and not company_name:
                overview_av = self.alphavantage.get_company_overview(self.ticker)

                if overview_av and overview_av.get("Symbol") == self.ticker:
                    self._financial_data_cache['overview_alphavantage'] = overview_av
                    company_name = overview_av.get('Name')
                    industry = overview_av.get('Industry') or industry
                    sector = overview_av.get('Sector') or sector
                    cik_val = overview_av.get('CIK')
                    if cik_val: cik = str(cik_val).zfill(10)
                    logger.info(f"Fetched overview from Alpha Vantage for {self.ticker}.")
                    break

        if not company_name:
            company_name = self.ticker
            logger.warning(f"All primary profile fetches failed or incomplete for {self.ticker}. Using ticker as name.")

        if not cik and self.ticker:
            logger.info(f"CIK not found from profiles for {self.ticker}. Querying SEC EDGAR CIK map.")
            cik_from_edgar = self.sec_edgar.get_cik_by_ticker(self.ticker)
            time.sleep(0.2)
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
                raise
        else:
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
            if cik and (self.stock_db_entry.cik != cik or not self.stock_db_entry.cik):
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
            self._close_session_if_active()
            self.db_session = next(get_db_session())
            re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
            if not re_fetched_stock:
                raise RuntimeError(
                    f"Failed to re-fetch stock {self.ticker} for new session after inactivity. Critical state.")
            self.stock_db_entry = re_fetched_stock
            logger.info(
                f"Re-fetched and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new active session.")
            return

        instance_state = sa_inspect(self.stock_db_entry)
        if not instance_state.session or instance_state.session is not self.db_session:
            obj_id_log = self.stock_db_entry.id if instance_state.has_identity else 'Transient/No ID'
            logger.warning(
                f"Stock {self.ticker} (ID: {obj_id_log}) DETACHED or bound to DIFFERENT session. Attempting to merge.")
            try:
                self.stock_db_entry = self.db_session.merge(self.stock_db_entry)
                self.db_session.flush()
                logger.info(
                    f"Successfully merged stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session.")
            except Exception as e_merge:
                logger.error(f"Failed to merge stock {self.ticker} into session: {e_merge}. Re-fetching as a fallback.",
                             exc_info=True)
                re_fetched_from_db_after_merge_fail = self.db_session.query(Stock).filter(
                    Stock.ticker == self.ticker).first()
                if re_fetched_from_db_after_merge_fail:
                    self.stock_db_entry = re_fetched_from_db_after_merge_fail
                    logger.info(
                        f"Successfully re-fetched stock {self.ticker} (ID: {self.stock_db_entry.id}) after merge failure.")
                else:
                    raise RuntimeError(
                        f"Failed to bind stock {self.ticker} to session after merge failure and re-fetch attempt. Analysis cannot proceed.")

    def analyze(self):
        logger.info(f"Full analysis pipeline started for {self.ticker}...")
        final_data_for_db = {}
        try:
            if not self.stock_db_entry:
                logger.error(f"Stock DB entry for {self.ticker} not initialized properly. Aborting analysis.")
                return None

            self._ensure_stock_db_entry_is_bound()

            fetch_financial_statements_data(self)
            fetch_key_metrics_and_profile_data(self)

            final_data_for_db.update(calculate_all_derived_metrics(self))
            final_data_for_db.update(perform_dcf_analysis(self))

            qual_summaries_data = fetch_and_summarize_10k_data(self)

            final_data_for_db["business_summary"] = qual_summaries_data.get("business_summary_data", {}).get("summary",
                                                                                                             "N/A")
            final_data_for_db["risk_factors_summary"] = qual_summaries_data.get("risk_factors_summary_data", {}).get(
                "summary", "N/A")
            final_data_for_db["management_assessment_summary"] = qual_summaries_data.get(
                "management_assessment_summary_data", {}).get("summary", "N/A")
            final_data_for_db["economic_moat_summary"] = qual_summaries_data.get("economic_moat_summary_data", {}).get(
                "overallAssessment", "N/A")
            final_data_for_db["industry_trends_summary"] = qual_summaries_data.get("industry_trends_summary_data",
                                                                                   {}).get("overallOutlook", "N/A")

            final_data_for_db["qualitative_sources_summary"] = qual_summaries_data.get("qualitative_sources_summary",
                                                                                       {})
            if "key_metrics_snapshot" not in final_data_for_db: final_data_for_db["key_metrics_snapshot"] = {}
            final_data_for_db["key_metrics_snapshot"]["10k_business_summary_data"] = qual_summaries_data.get(
                "business_summary_data")
            final_data_for_db["key_metrics_snapshot"]["10k_risk_factors_data"] = qual_summaries_data.get(
                "risk_factors_summary_data")
            final_data_for_db["key_metrics_snapshot"]["10k_mda_data"] = qual_summaries_data.get(
                "management_assessment_summary_data")
            final_data_for_db["key_metrics_snapshot"]["10k_economic_moat_data"] = qual_summaries_data.get(
                "economic_moat_summary_data")
            final_data_for_db["key_metrics_snapshot"]["10k_industry_trends_data"] = qual_summaries_data.get(
                "industry_trends_summary_data")

            final_data_for_db["competitive_landscape_summary"] = fetch_and_analyze_competitors(self)
            competitor_data_cache = self._financial_data_cache.get('competitor_analysis', {})
            if "key_metrics_snapshot" not in final_data_for_db: final_data_for_db["key_metrics_snapshot"] = {}
            final_data_for_db["key_metrics_snapshot"]["competitor_analysis_data"] = competitor_data_cache

            final_data_for_db.update(synthesize_investment_thesis(self))

            analysis_entry = StockAnalysis(stock_id=self.stock_db_entry.id, analysis_date=datetime.now(timezone.utc))
            model_fields = [c.key for c in StockAnalysis.__table__.columns if
                            c.key not in ['id', 'stock_id', 'analysis_date']]

            for field_name in model_fields:
                if field_name in final_data_for_db:
                    value_to_set = final_data_for_db[field_name]
                    target_column = getattr(StockAnalysis, field_name)
                    target_column_type = target_column.type.python_type if hasattr(target_column.type,
                                                                                   'python_type') else type(None)

                    if target_column_type == float:
                        if isinstance(value_to_set, str):
                            try:
                                value_to_set = float(value_to_set)
                            except ValueError:
                                value_to_set = None
                        if isinstance(value_to_set, float) and (math.isnan(value_to_set) or math.isinf(value_to_set)):
                            value_to_set = None
                    elif target_column_type == dict or isinstance(target_column.type, (
                    sqlalchemy.dialects.postgresql.JSONB, sqlalchemy.dialects.postgresql.JSON,
                    sqlalchemy.types.JSON)):  # Check for JSON types
                        if not isinstance(value_to_set, dict) and value_to_set is not None:
                            try:
                                parsed_json = json.loads(value_to_set) if isinstance(value_to_set, str) else None
                                if isinstance(parsed_json, dict):
                                    value_to_set = parsed_json
                                else:
                                    logger.warning(
                                        f"Field {field_name} expected dict/JSON, got {type(value_to_set)}. Value: '{str(value_to_set)[:100]}...'. Setting to error dict.")
                                    value_to_set = {"error": "Invalid data type received",
                                                    "original_value": str(value_to_set)[
                                                                      :200]} if value_to_set is not None else None
                            except json.JSONDecodeError:
                                logger.warning(
                                    f"Field {field_name} expected dict/JSON but failed to parse string: '{str(value_to_set)[:100]}...'. Setting to error dict.")
                                value_to_set = {"error": "Failed to parse JSON string",
                                                "original_value": str(value_to_set)[
                                                                  :200]} if value_to_set is not None else None
                    elif target_column_type == str and not isinstance(value_to_set, str) and value_to_set is not None:
                        value_to_set = str(value_to_set)

                    setattr(analysis_entry, field_name, value_to_set)

            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = analysis_entry.analysis_date
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved stock data: {self.ticker} (Analysis ID: {analysis_entry.id})")
            return analysis_entry

        except RuntimeError as rt_err:
            logger.critical(f"Runtime error during full analysis for {self.ticker}: {rt_err}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"CRITICAL error in full analysis pipeline for {self.ticker}: {e}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try:
                    self.db_session.rollback(); logger.info(
                        f"Rolled back DB transaction for {self.ticker} due to error.")
                except Exception as e_rb:
                    logger.error(f"Rollback error for {self.ticker}: {e_rb}")
            return None
        finally:
            self._close_session_if_active()