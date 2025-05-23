# stock_analyzer.py
import pandas as pd
from sqlalchemy import inspect as sa_inspect  # Make sure this import is at the top
from api_clients import FinnhubClient, FinancialModelingPrepClient, EODHDClient, GeminiAPIClient
from database import SessionLocal, get_db_session
from models import Stock, StockAnalysis
from error_handler import logger
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime


class StockAnalyzer:
    def __init__(self, ticker):
        self.ticker = ticker.upper()
        self.finnhub = FinnhubClient()
        self.fmp = FinancialModelingPrepClient()
        self.eodhd = EODHDClient()
        self.gemini = GeminiAPIClient()
        self.db_session = next(get_db_session())  # Get a session

        self.stock_db_entry = None
        try:
            self._get_or_create_stock_entry()
        except Exception as e:
            logger.error(f"Failed during _get_or_create_stock_entry for {self.ticker}: {e}", exc_info=True)
            # Ensure session is closed if initialization fails badly
            if self.db_session and self.db_session.is_active:
                self.db_session.close()
            raise  # Re-raise to prevent using a potentially broken analyzer object

    def _get_or_create_stock_entry(self):
        # Ensure session is active before query
        if not self.db_session.is_active:
            logger.warning(f"Session for {self.ticker} in _get_or_create_stock_entry was inactive. Re-establishing.")
            try:
                self.db_session.close()  # Close old one if possible
            except Exception as e:
                logger.warning(f"Error closing inactive session in _get_or_create_stock_entry: {e}")
            self.db_session = next(get_db_session())  # Get new one

        self.stock_db_entry = self.db_session.query(Stock).filter_by(ticker=self.ticker).first()
        if not self.stock_db_entry:
            logger.info(f"Stock {self.ticker} not found in DB, creating new entry.")
            # Try to get company name from an API
            profile_fmp = self.fmp.get_company_profile(self.ticker)
            company_name = profile_fmp[0].get('companyName', self.ticker) if profile_fmp and isinstance(profile_fmp,
                                                                                                        list) and \
                                                                             profile_fmp[0] else self.ticker

            self.stock_db_entry = Stock(ticker=self.ticker, company_name=company_name)
            self.db_session.add(self.stock_db_entry)
            try:
                self.db_session.commit()
                # After commit, self.stock_db_entry is persistent.
                # If expire_on_commit=True (default), its attributes are expired but it's still session-bound.
                # It's good practice to refresh or re-query if further operations need fresh state immediately
                # or if you want to be absolutely sure of its state. For now, commit is enough.
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating stock entry for {self.ticker}: {e}", exc_info=True)
                raise
        else:
            logger.info(f"Found existing stock entry for {self.ticker} (ID: {self.stock_db_entry.id}).")
            # Ensure the found entry is part of the current session state.
            # query() already makes it part of the session. If the session is fresh, this is fine.
            # If this method is called multiple times on the same StockAnalyzer instance without closing
            # and reopening session, this could lead to issues if the object was modified elsewhere or expired.
            # However, given __init__ gets a fresh session, this should generally be safe.

    def _fetch_financial_data(self):
        logger.info(f"Fetching financial data for {self.ticker}...")
        data = {"profile": None, "financials_fmp": {}, "key_metrics_fmp": None, "financials_finnhub": None,
                "basic_financials_finnhub": None}

        # FMP Profile
        profile_fmp = self.fmp.get_company_profile(self.ticker)
        if profile_fmp and isinstance(profile_fmp, list) and profile_fmp[0]:
            data["profile"] = profile_fmp[0]
            # Update company name in DB if it's missing or different
            # This requires the self.stock_db_entry to be session-bound and active.
            # Let's assume it is, based on _get_or_create_stock_entry and merge logic in analyze()
            if self.stock_db_entry and (
                    not self.stock_db_entry.company_name or self.stock_db_entry.company_name == self.ticker):
                new_company_name = data["profile"].get('companyName', self.ticker)
                if new_company_name != self.stock_db_entry.company_name:
                    logger.info(
                        f"Updating company name for {self.ticker} from '{self.stock_db_entry.company_name}' to '{new_company_name}'.")
                    self.stock_db_entry.company_name = new_company_name
                    # Commit this small change immediately or batch with main analysis commit
                    try:
                        self.db_session.commit()
                    except SQLAlchemyError as e:
                        self.db_session.rollback()
                        logger.error(f"Error updating company name for {self.ticker} in DB: {e}")

        # FMP Financial Statements (Annual and Quarterly for trends)
        for statement_type in ["income-statement", "balance-sheet-statement", "cash-flow-statement"]:
            annual = self.fmp.get_financial_statements(self.ticker, statement_type, period="annual", limit=5)
            quarterly = self.fmp.get_financial_statements(self.ticker, statement_type, period="quarter", limit=8)
            data["financials_fmp"][statement_type] = {"annual": annual, "quarterly": quarterly}

        # FMP Key Metrics
        data["key_metrics_fmp"] = self.fmp.get_key_metrics(self.ticker, period="annual", limit=5)  # Annual for ratios
        data["key_metrics_fmp_quarterly"] = self.fmp.get_key_metrics(self.ticker, period="quarter",
                                                                     limit=5)  # Quarterly too

        # Finnhub Basic Financials (often has real-time or very recent ratios)
        data["basic_financials_finnhub"] = self.finnhub.get_basic_financials(self.ticker)

        return data

    def _calculate_metrics(self, raw_data):
        logger.info(f"Calculating metrics for {self.ticker}...")
        metrics = {"key_metrics_snapshot": {}}  # For email raw data

        if raw_data.get("key_metrics_fmp") and raw_data["key_metrics_fmp"]:
            latest_metrics = raw_data["key_metrics_fmp"][0]
            metrics["pe_ratio"] = latest_metrics.get("peRatio")
            metrics["pb_ratio"] = latest_metrics.get("pbRatio")
            metrics["dividend_yield"] = latest_metrics.get("dividendYield")
            metrics["debt_to_equity"] = latest_metrics.get("debtToEquity")
            metrics["net_profit_margin"] = latest_metrics.get("netProfitMargin")
            metrics["roe"] = latest_metrics.get("roe")
            metrics["key_metrics_snapshot"]["FMP_peRatio"] = metrics["pe_ratio"]
            metrics["key_metrics_snapshot"]["FMP_pbRatio"] = metrics["pb_ratio"]
            metrics["key_metrics_snapshot"]["FMP_dividendYield"] = metrics["dividend_yield"]
            metrics["key_metrics_snapshot"]["FMP_debtToEquity"] = metrics["debt_to_equity"]

        if raw_data.get("basic_financials_finnhub") and raw_data["basic_financials_finnhub"].get("metric"):
            fin_metrics = raw_data["basic_financials_finnhub"]["metric"]
            if not metrics.get("pe_ratio") and fin_metrics.get("peAnnual"):
                metrics["pe_ratio"] = fin_metrics.get("peAnnual")
            if not metrics.get("pb_ratio") and fin_metrics.get("pbAnnual"):
                metrics["pb_ratio"] = fin_metrics.get("pbAnnual")
            metrics["eps"] = fin_metrics.get("epsAnnual")
            metrics["key_metrics_snapshot"]["Finnhub_peAnnual"] = fin_metrics.get("peAnnual")
            metrics["key_metrics_snapshot"]["Finnhub_pbAnnual"] = fin_metrics.get("pbAnnual")
            metrics["key_metrics_snapshot"]["Finnhub_epsAnnual"] = metrics["eps"]

        income_annual = raw_data.get("financials_fmp", {}).get("income-statement", {}).get("annual")
        if income_annual and len(income_annual) > 1:
            metrics["key_metrics_snapshot"]["FMP_Revenue_Recent_Annual"] = income_annual[0].get("revenue")
            rev_curr = income_annual[0].get("revenue")
            rev_prev = income_annual[1].get("revenue")
            if rev_curr and rev_prev and rev_prev != 0:
                metrics["revenue_growth"] = f"{((rev_curr - rev_prev) / rev_prev * 100):.2f}%"
            metrics["key_metrics_snapshot"]["FMP_Revenue_Growth_YoY"] = metrics.get("revenue_growth")

            if not metrics.get("eps"): metrics["eps"] = income_annual[0].get("eps")
            if not metrics.get("net_profit_margin"): metrics["net_profit_margin"] = income_annual[0].get(
                "netProfitMargin")

            ebitda = income_annual[0].get("ebitda")
            interest_expense = income_annual[0].get("interestExpense")
            if ebitda and interest_expense and interest_expense != 0:
                metrics["interest_coverage_ratio"] = ebitda / abs(interest_expense)
            metrics["key_metrics_snapshot"]["FMP_interestCoverageRatio (EBITDA based)"] = metrics.get(
                "interest_coverage_ratio")

        balance_annual = raw_data.get("financials_fmp", {}).get("balance-sheet-statement", {}).get("annual")
        if balance_annual and len(balance_annual) > 0:
            latest_balance = balance_annual[0]
            current_assets = latest_balance.get("totalCurrentAssets")
            current_liabilities = latest_balance.get("totalCurrentLiabilities")
            if current_assets and current_liabilities and current_liabilities != 0:
                metrics["current_ratio"] = current_assets / current_liabilities
            metrics["key_metrics_snapshot"]["FMP_currentRatio"] = metrics.get("current_ratio")

            if not metrics.get("debt_to_equity"):
                total_debt = latest_balance.get("totalDebt")
                total_equity = latest_balance.get("totalStockholdersEquity")
                if total_debt and total_equity and total_equity != 0:
                    metrics["debt_to_equity"] = total_debt / total_equity

            if len(balance_annual) > 2:
                re_curr = balance_annual[0].get("retainedEarnings")
                re_prev1 = balance_annual[1].get("retainedEarnings")
                re_prev2 = balance_annual[2].get("retainedEarnings")
                if re_curr is not None and re_prev1 is not None and re_prev2 is not None:  # Ensure not None before comparison
                    if re_curr > re_prev1 > re_prev2:
                        metrics["retained_earnings_trend"] = "Growing"
                    elif re_curr < re_prev1 < re_prev2:
                        metrics["retained_earnings_trend"] = "Declining"
                    else:
                        metrics["retained_earnings_trend"] = "Mixed/Stable"
            metrics["key_metrics_snapshot"]["FMP_retainedEarnings_Recent"] = balance_annual[0].get("retainedEarnings")

        cashflow_annual = raw_data.get("financials_fmp", {}).get("cash-flow-statement", {}).get("annual")
        if cashflow_annual and len(cashflow_annual) > 2:
            fcf_curr = cashflow_annual[0].get("freeCashFlow")
            fcf_prev1 = cashflow_annual[1].get("freeCashFlow")
            fcf_prev2 = cashflow_annual[2].get("freeCashFlow")
            if fcf_curr is not None and fcf_prev1 is not None and fcf_prev2 is not None:  # Ensure not None
                if fcf_curr > fcf_prev1 > fcf_prev2:
                    metrics["free_cash_flow_trend"] = "Growing"
                elif fcf_curr < fcf_prev1 < fcf_prev2:
                    metrics["free_cash_flow_trend"] = "Declining"
                else:
                    metrics["free_cash_flow_trend"] = "Mixed/Stable"
            metrics["key_metrics_snapshot"]["FMP_FCF_Recent"] = fcf_curr

        for key in ["pe_ratio", "pb_ratio", "eps", "roe", "dividend_yield", "debt_to_equity", "interest_coverage_ratio",
                    "current_ratio", "net_profit_margin"]:
            if key not in metrics or metrics[key] is None:
                metrics[key] = None
        for key_trend in ["retained_earnings_trend", "revenue_growth", "free_cash_flow_trend"]:
            if key_trend not in metrics or metrics[key_trend] is None:
                metrics[key_trend] = "Data N/A"

        logger.info(
            f"Calculated metrics for {self.ticker}: { {k: v for k, v in metrics.items() if k != 'key_metrics_snapshot'} }")  # Log without huge snapshot
        return metrics

    def _analyze_qualitative_factors(self, raw_data):
        # This method uses self.stock_db_entry.company_name, which should be up-to-date and session-bound
        # due to logic in __init__ and analyze()
        logger.info(f"Analyzing qualitative factors for {self.ticker} using Gemini...")
        qual_analysis = {"qualitative_sources": {}}

        # Ensure self.stock_db_entry.company_name is available and not None
        company_name_for_prompt = self.stock_db_entry.company_name if self.stock_db_entry and self.stock_db_entry.company_name else self.ticker

        profile = raw_data.get("profile")
        description = profile.get("description", "") if profile else ""
        industry = profile.get("industry", "") if profile else ""
        sector = profile.get("sector", "") if profile else ""

        moat_prompt = (
            f"Based on the company description for {company_name_for_prompt} ({self.ticker}): \"{description}\", "
            f"and its industry '{industry}', what are its likely competitive advantages (economic moat)? "
            f"Consider brand strength, network effects, switching costs, intangible assets (patents, licenses), and cost advantages. "
            f"Provide a concise summary."
        )
        qual_analysis["economic_moat_summary"] = self.gemini.summarize_text(moat_prompt,
                                                                            context="Summarizing economic moat")
        qual_analysis["qualitative_sources"][
            "moat_prompt_context"] = f"Company Description: {description[:200]}..., Industry: {industry}"

        industry_prompt = (
            f"What are the current key trends, opportunities, and risks for the '{industry}' industry and '{sector}' sector? "
            f"How might {company_name_for_prompt} be positioned regarding these trends? Provide a concise summary."
        )
        qual_analysis["industry_trends_summary"] = self.gemini.summarize_text(industry_prompt,
                                                                              context="Summarizing industry trends")
        qual_analysis["qualitative_sources"]["industry_prompt_context"] = f"Industry: {industry}, Sector: {sector}"

        management_prompt = (
            f"Given the general information about {company_name_for_prompt} ({self.ticker}), "
            f"what are generic positive and negative indicators to look for in its management team? "
            f"This is a general query, not based on specific named executives of this company unless provided. "
            f"Provide a brief summary of factors."
        )
        qual_analysis["management_assessment_summary"] = self.gemini.summarize_text(management_prompt,
                                                                                    context="Generic management assessment factors")
        qual_analysis["qualitative_sources"]["management_prompt_context"] = "General management quality factors query."

        logger.info(f"Qualitative analysis for {self.ticker} complete.")
        return qual_analysis

    def _determine_investment_strategy_and_conclusion(self, fund_metrics, qual_analysis):
        logger.info(f"Determining investment strategy for {self.ticker}...")
        decision_parts = []
        company_name_for_prompt = self.stock_db_entry.company_name if self.stock_db_entry and self.stock_db_entry.company_name else self.ticker

        pe_interpretation = "P/E Ratio: "
        if fund_metrics.get("pe_ratio") is not None:
            pe_val = fund_metrics["pe_ratio"]
            pe_interpretation += f"{pe_val:.2f}. "
            if pe_val < 0:
                pe_interpretation += "Negative P/E (company is loss-making or unusual data). "
            elif pe_val < 15:
                pe_interpretation += "Potentially undervalued or low growth expectations. "
            elif pe_val <= 25:
                pe_interpretation += "Considered fair for many industries. "
            else:
                pe_interpretation += "Potentially overvalued or high growth expectations. "
        else:
            pe_interpretation += "N/A. "
        decision_parts.append(pe_interpretation)

        roe_interpretation = "ROE: "
        if fund_metrics.get("roe") is not None:
            roe_val = fund_metrics["roe"] * 100
            roe_interpretation += f"{roe_val:.2f}%. "
            if roe_val > 20:
                roe_interpretation += "Strong ROE, efficient use of shareholder equity. "
            elif roe_val > 15:
                roe_interpretation += "Good ROE. "
            else:
                roe_interpretation += "Subpar or low ROE. "
        else:
            roe_interpretation += "N/A. "
        decision_parts.append(roe_interpretation)

        de_interpretation = "Debt-to-Equity: "
        if fund_metrics.get("debt_to_equity") is not None:
            de_val = fund_metrics["debt_to_equity"]
            de_interpretation += f"{de_val:.2f}. "
            if de_val > 1.0:
                de_interpretation += "High leverage, be cautious. "
            elif de_val > 0.5:
                de_interpretation += "Moderate leverage. "
            else:
                de_interpretation += "Low leverage. "
        else:
            de_interpretation += "N/A. "
        decision_parts.append(de_interpretation)

        decision_parts.append(f"Revenue Growth (YoY): {fund_metrics.get('revenue_growth', 'N/A')}.")
        decision_parts.append(f"Free Cash Flow Trend: {fund_metrics.get('free_cash_flow_trend', 'N/A')}.")
        decision_parts.append(f"Economic Moat: {qual_analysis.get('economic_moat_summary', 'N/A')[:150]}...")
        decision_parts.append(f"Industry Trends: {qual_analysis.get('industry_trends_summary', 'N/A')[:150]}...")

        synthesis_prompt = (
            f"Synthesize the following financial metrics and qualitative analysis for {company_name_for_prompt} ({self.ticker}) "
            f"into an investment thesis. Metrics: {pe_interpretation}, {roe_interpretation}, {de_interpretation}, "
            f"Revenue Growth: {fund_metrics.get('revenue_growth', 'N/A')}, FCF Trend: {fund_metrics.get('free_cash_flow_trend', 'N/A')}. "
            f"Qualitative: Moat: {qual_analysis.get('economic_moat_summary', 'N/A')}, "
            f"Industry: {qual_analysis.get('industry_trends_summary', 'N/A')}. "
            f"Based on these, suggest a general investment decision (e.g., 'Consider for Value', 'Monitor for Growth', 'Exercise Caution', 'Potential Buy', 'Leaning Negative') and a brief reasoning. "
            f"Do not give financial advice. This is for informational purposes."
        )
        gemini_synthesis = self.gemini.generate_text(synthesis_prompt)

        final_decision = "Neutral/Monitor"
        if "potential buy" in gemini_synthesis.lower() or "consider for" in gemini_synthesis.lower() or "favorable" in gemini_synthesis.lower():
            final_decision = "Potential Buy/Consider"
        elif "caution" in gemini_synthesis.lower() or "negative" in gemini_synthesis.lower() or "avoid" in gemini_synthesis.lower():
            final_decision = "Caution/Avoid"

        strategy_type = "Undetermined"
        if "value" in gemini_synthesis.lower():
            strategy_type = "Value"
        elif "growth" in gemini_synthesis.lower():
            strategy_type = "Growth"

        return {
            "investment_decision": final_decision,
            "reasoning": f"Rule-based checks: {' '.join(decision_parts)}\n\nAI Synthesis: {gemini_synthesis}",
            "strategy_type": strategy_type
        }

    def analyze(self):
        logger.info(f"Starting analysis for {self.ticker}...")
        final_analysis_entry = None  # To hold the result

        try:
            if not self.stock_db_entry:
                logger.error(f"Stock entry for {self.ticker} was not initialized. Aborting analysis.")
                return None

            # --- Defensive checks for session and instance state ---
            if not self.db_session.is_active:
                logger.warning(
                    f"Session for {self.ticker} is INACTIVE at the start of analyze method. Re-establishing.")
                try:
                    self.db_session.close()
                except Exception as e_close:
                    logger.warning(f"Error closing inactive session in analyze(): {e_close}")
                self.db_session = next(get_db_session())
                # After re-establishing session, we need to ensure self.stock_db_entry is associated with this new session
                logger.info(f"Re-querying stock {self.ticker} for new session.")
                re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
                if not re_fetched_stock:
                    logger.error(f"Could not re-fetch stock {self.ticker} after session re-establishment. Aborting.")
                    return None
                self.stock_db_entry = re_fetched_stock
                logger.info(
                    f"Successfully re-fetched and bound stock {self.ticker} (ID: {self.stock_db_entry.id}) to new session.")

            instance_state = sa_inspect(self.stock_db_entry)
            if not instance_state.session or instance_state.session is not self.db_session:
                log_msg_prefix = "DETACHED" if not instance_state.session else f"bound to a DIFFERENT session (expected {id(self.db_session)}, got {id(instance_state.session)})"
                object_id_for_log = self.stock_db_entry.id if instance_state.has_identity else 'Unknown ID'
                logger.warning(
                    f"Stock entry {self.ticker} (ID: {object_id_for_log}) is {log_msg_prefix}. Attempting to merge into current session.")
                try:
                    # Ensure the instance has its primary key loaded if it's detached & new
                    if not instance_state.has_identity and self.stock_db_entry.id is None:
                        logger.info(
                            f"Re-querying {self.ticker} by ticker as PK is not available on detached instance before merge.")
                        re_fetched_stock = self.db_session.query(Stock).filter(Stock.ticker == self.ticker).first()
                        if not re_fetched_stock:
                            logger.error(
                                f"Could not re-fetch {self.ticker} by ticker to merge (PK was missing). Aborting.")
                            return None
                        self.stock_db_entry = re_fetched_stock  # Now it has PK and is in session
                        logger.info(f"Re-fetched {self.ticker} (ID: {self.stock_db_entry.id}) before potential merge.")

                    # If stock_db_entry was re-fetched above, it's already in session. Merge will just return it.
                    # If it was from an old session but had its PK, merge will load/update it.
                    merged_stock = self.db_session.merge(self.stock_db_entry)
                    self.stock_db_entry = merged_stock
                    logger.info(
                        f"Successfully merged/re-associated stock {self.ticker} (ID: {self.stock_db_entry.id}) into current session.")
                except Exception as e_merge:
                    logger.error(f"Failed to merge stock {self.ticker} into session: {e_merge}. Aborting analysis.",
                                 exc_info=True)
                    return None
            # --- End of defensive checks ---

            raw_data = self._fetch_financial_data()  # This might commit company name changes
            if not raw_data.get("profile") and not raw_data.get("key_metrics_fmp"):
                logger.error(f"Failed to fetch significant data for {self.ticker}. Aborting analysis.")
                return None

            calculated_metrics = self._calculate_metrics(raw_data)
            qualitative_summary = self._analyze_qualitative_factors(raw_data)  # Uses self.stock_db_entry.company_name
            strategy_and_conclusion = self._determine_investment_strategy_and_conclusion(calculated_metrics,
                                                                                         qualitative_summary)

            analysis_entry = StockAnalysis(
                stock_id=self.stock_db_entry.id,
                pe_ratio=calculated_metrics.get("pe_ratio"),
                pb_ratio=calculated_metrics.get("pb_ratio"),
                eps=calculated_metrics.get("eps"),
                roe=calculated_metrics.get("roe"),
                dividend_yield=calculated_metrics.get("dividend_yield"),
                debt_to_equity=calculated_metrics.get("debt_to_equity"),
                interest_coverage_ratio=calculated_metrics.get("interest_coverage_ratio"),
                current_ratio=calculated_metrics.get("current_ratio"),
                retained_earnings_trend=calculated_metrics.get("retained_earnings_trend"),
                revenue_growth=calculated_metrics.get("revenue_growth"),
                net_profit_margin=calculated_metrics.get("net_profit_margin"),
                free_cash_flow_trend=calculated_metrics.get("free_cash_flow_trend"),
                economic_moat_summary=qualitative_summary.get("economic_moat_summary"),
                industry_trends_summary=qualitative_summary.get("industry_trends_summary"),
                management_assessment_summary=qualitative_summary.get("management_assessment_summary"),
                investment_decision=strategy_and_conclusion.get("investment_decision"),
                reasoning=strategy_and_conclusion.get("reasoning"),
                strategy_type=strategy_and_conclusion.get("strategy_type"),
                key_metrics_snapshot=calculated_metrics.get("key_metrics_snapshot"),
                qualitative_sources=qualitative_summary.get("qualitative_sources")
            )
            self.db_session.add(analysis_entry)
            self.stock_db_entry.last_analysis_date = datetime.utcnow()

            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved stock: {self.ticker} (Analysis ID: {analysis_entry.id})")
            final_analysis_entry = analysis_entry

        except Exception as e_outer:
            logger.error(f"Outer exception in analyze() for {self.ticker}: {e_outer}", exc_info=True)
            if self.db_session and self.db_session.is_active:
                try:
                    self.db_session.rollback()
                except Exception as e_rollback:
                    logger.error(f"Error during rollback for {self.ticker}: {e_rollback}")
            return None  # Indicate failure
        finally:
            if self.db_session and self.db_session.is_active:
                self.db_session.close()
                logger.debug(f"Session closed for {self.ticker} at end of analyze method.")

        return final_analysis_entry


# Example usage:
if __name__ == '__main__':
    from database import init_db

    try:
        init_db()
        logger.info("Starting standalone stock analysis test...")
        analyzer = StockAnalyzer(ticker="AAPL")
        analysis_result = analyzer.analyze()
        if analysis_result:
            logger.info(
                f"Analysis for {analysis_result.stock.ticker} completed. Decision: {analysis_result.investment_decision}")
            logger.info(f"Reasoning snippet: {analysis_result.reasoning[:300]}...")
        else:
            logger.error("Stock analysis failed for AAPL.")

        # Test with another stock to ensure session handling is robust per instance
        analyzer_msft = StockAnalyzer(ticker="MSFT")
        analysis_result_msft = analyzer_msft.analyze()
        if analysis_result_msft:
            logger.info(
                f"Analysis for {analysis_result_msft.stock.ticker} completed. Decision: {analysis_result_msft.investment_decision}")
        else:
            logger.error("Stock analysis failed for MSFT.")

    except Exception as e:
        logger.error(f"Error during stock analysis test in __main__: {e}", exc_info=True)