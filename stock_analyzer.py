# stock_analyzer.py
import pandas as pd
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
        self.eodhd = EODHDClient()  # EODHD expects ticker with exchange, e.g., AAPL.US
        self.gemini = GeminiAPIClient()
        self.db_session = next(get_db_session())  # Get a session

        self.stock_db_entry = None
        self._get_or_create_stock_entry()

    def _get_or_create_stock_entry(self):
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
            except SQLAlchemyError as e:
                self.db_session.rollback()
                logger.error(f"Error creating stock entry for {self.ticker}: {e}")
                raise
        else:
            logger.info(f"Found existing stock entry for {self.ticker}.")

    def _fetch_financial_data(self):
        logger.info(f"Fetching financial data for {self.ticker}...")
        data = {"profile": None, "financials_fmp": {}, "key_metrics_fmp": None, "financials_finnhub": None,
                "basic_financials_finnhub": None}

        # FMP Profile
        profile_fmp = self.fmp.get_company_profile(self.ticker)
        if profile_fmp and isinstance(profile_fmp, list) and profile_fmp[0]:
            data["profile"] = profile_fmp[0]
            if self.stock_db_entry and not self.stock_db_entry.company_name:  # Update if missing
                self.stock_db_entry.company_name = data["profile"].get('companyName', self.ticker)

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

        # Finnhub Financials Reported (alternative source, might have different structure/detail)
        # data["financials_finnhub"] = self.finnhub.get_financials_reported(self.ticker, freq="annual")

        # EODHD requires ticker.EXCHANGE, e.g. AAPL.US. This needs to be known or discovered.
        # For simplicity, we'll assume FMP/Finnhub are primary for now unless exchange is provided.
        # If exchange is known:
        # eodhd_ticker = f"{self.ticker}.US" # Example, needs logic to find exchange
        # data["eodhd_fundamentals"] = self.eodhd.get_fundamental_data(eodhd_ticker)

        return data

    def _calculate_metrics(self, raw_data):
        logger.info(f"Calculating metrics for {self.ticker}...")
        metrics = {"key_metrics_snapshot": {}}  # For email raw data

        # From FMP Key Metrics (annual, most recent)
        if raw_data.get("key_metrics_fmp") and raw_data["key_metrics_fmp"]:
            latest_metrics = raw_data["key_metrics_fmp"][0]
            metrics["pe_ratio"] = latest_metrics.get("peRatio")
            metrics["pb_ratio"] = latest_metrics.get("pbRatio")
            metrics["dividend_yield"] = latest_metrics.get("dividendYield")
            metrics["debt_to_equity"] = latest_metrics.get("debtToEquity")
            metrics["net_profit_margin"] = latest_metrics.get("netProfitMargin")
            metrics["roe"] = latest_metrics.get("roe")
            # Add to snapshot for email
            metrics["key_metrics_snapshot"]["FMP_peRatio"] = metrics["pe_ratio"]
            metrics["key_metrics_snapshot"]["FMP_pbRatio"] = metrics["pb_ratio"]
            metrics["key_metrics_snapshot"]["FMP_dividendYield"] = metrics["dividend_yield"]
            metrics["key_metrics_snapshot"]["FMP_debtToEquity"] = metrics["debt_to_equity"]

        # From Finnhub Basic Financials (might be more up-to-date for P/E)
        if raw_data.get("basic_financials_finnhub") and raw_data["basic_financials_finnhub"].get("metric"):
            fin_metrics = raw_data["basic_financials_finnhub"]["metric"]
            if not metrics.get("pe_ratio") and fin_metrics.get("peAnnual"):  # Prioritize FMP if available
                metrics["pe_ratio"] = fin_metrics.get("peAnnual")
            if not metrics.get("pb_ratio") and fin_metrics.get("pbAnnual"):
                metrics["pb_ratio"] = fin_metrics.get("pbAnnual")
            # Finnhub often has direct EPS
            metrics["eps"] = fin_metrics.get("epsAnnual")  # Or epsTTM
            metrics["key_metrics_snapshot"]["Finnhub_peAnnual"] = fin_metrics.get("peAnnual")
            metrics["key_metrics_snapshot"]["Finnhub_pbAnnual"] = fin_metrics.get("pbAnnual")
            metrics["key_metrics_snapshot"]["Finnhub_epsAnnual"] = metrics["eps"]

        # Income Statement Analysis (from FMP)
        income_annual = raw_data.get("financials_fmp", {}).get("income-statement", {}).get("annual")
        if income_annual and len(income_annual) > 1:
            metrics["key_metrics_snapshot"]["FMP_Revenue_Recent_Annual"] = income_annual[0].get("revenue")
            # Revenue Growth (YoY)
            rev_curr = income_annual[0].get("revenue")
            rev_prev = income_annual[1].get("revenue")
            if rev_curr and rev_prev and rev_prev != 0:
                metrics["revenue_growth"] = f"{((rev_curr - rev_prev) / rev_prev * 100):.2f}%"
            metrics["key_metrics_snapshot"]["FMP_Revenue_Growth_YoY"] = metrics["revenue_growth"]

            # EPS (if not from Finnhub) / Net Profit Margin
            if not metrics.get("eps"): metrics["eps"] = income_annual[0].get("eps")
            if not metrics.get("net_profit_margin"): metrics["net_profit_margin"] = income_annual[0].get(
                "netProfitMargin")

            # Retained Earnings Trend (from Balance Sheet)
            # Interest Coverage Ratio (EBIT / Interest Expense)
            ebitda = income_annual[0].get("ebitda")  # FMP often provides ebitda, sometimes ebit.
            interest_expense = income_annual[0].get("interestExpense")
            if ebitda and interest_expense and interest_expense != 0:  # Use EBITDA as proxy for EBIT if EBIT not avail.
                metrics["interest_coverage_ratio"] = ebitda / abs(interest_expense)  # Interest expense can be negative
            metrics["key_metrics_snapshot"]["FMP_interestCoverageRatio (EBITDA based)"] = metrics.get(
                "interest_coverage_ratio")

        # Balance Sheet Analysis (from FMP)
        balance_annual = raw_data.get("financials_fmp", {}).get("balance-sheet-statement", {}).get("annual")
        if balance_annual and len(balance_annual) > 0:
            latest_balance = balance_annual[0]
            current_assets = latest_balance.get("totalCurrentAssets")
            current_liabilities = latest_balance.get("totalCurrentLiabilities")
            if current_assets and current_liabilities and current_liabilities != 0:
                metrics["current_ratio"] = current_assets / current_liabilities
            metrics["key_metrics_snapshot"]["FMP_currentRatio"] = metrics.get("current_ratio")

            # Debt to Equity (if not from key metrics)
            if not metrics.get("debt_to_equity"):
                total_debt = latest_balance.get("totalDebt")
                total_equity = latest_balance.get("totalStockholdersEquity")
                if total_debt and total_equity and total_equity != 0:
                    metrics["debt_to_equity"] = total_debt / total_equity

            # Retained Earnings Trend
            if len(balance_annual) > 2:
                re_curr = balance_annual[0].get("retainedEarnings")
                re_prev1 = balance_annual[1].get("retainedEarnings")
                re_prev2 = balance_annual[2].get("retainedEarnings")
                if re_curr and re_prev1 and re_prev2:
                    if re_curr > re_prev1 > re_prev2:
                        metrics["retained_earnings_trend"] = "Growing"
                    elif re_curr < re_prev1 < re_prev2:
                        metrics["retained_earnings_trend"] = "Declining"
                    else:
                        metrics["retained_earnings_trend"] = "Mixed/Stable"
            metrics["key_metrics_snapshot"]["FMP_retainedEarnings_Recent"] = balance_annual[0].get("retainedEarnings")

        # Cash Flow Statement Analysis (from FMP)
        cashflow_annual = raw_data.get("financials_fmp", {}).get("cash-flow-statement", {}).get("annual")
        if cashflow_annual and len(cashflow_annual) > 2:
            fcf_curr = cashflow_annual[0].get("freeCashFlow")
            fcf_prev1 = cashflow_annual[1].get("freeCashFlow")
            fcf_prev2 = cashflow_annual[2].get("freeCashFlow")
            if fcf_curr and fcf_prev1 and fcf_prev2:
                if fcf_curr > fcf_prev1 > fcf_prev2:
                    metrics["free_cash_flow_trend"] = "Growing"
                elif fcf_curr < fcf_prev1 < fcf_prev2:
                    metrics["free_cash_flow_trend"] = "Declining"
                else:
                    metrics["free_cash_flow_trend"] = "Mixed/Stable"
            metrics["key_metrics_snapshot"]["FMP_FCF_Recent"] = fcf_curr

        # Placeholder for missing metrics
        for key in ["pe_ratio", "pb_ratio", "eps", "roe", "dividend_yield", "debt_to_equity", "interest_coverage_ratio",
                    "current_ratio", "net_profit_margin"]:
            if key not in metrics or metrics[key] is None:
                metrics[key] = None  # Explicitly set to None if not found/calculated
        for key_trend in ["retained_earnings_trend", "revenue_growth", "free_cash_flow_trend"]:
            if key_trend not in metrics or metrics[key_trend] is None:
                metrics[key_trend] = "Data N/A"

        logger.info(f"Calculated metrics for {self.ticker}: {metrics}")
        return metrics

    def _analyze_qualitative_factors(self, raw_data):
        logger.info(f"Analyzing qualitative factors for {self.ticker} using Gemini...")
        qual_analysis = {"qualitative_sources": {}}  # For email snippets
        profile = raw_data.get("profile")
        description = profile.get("description", "") if profile else ""
        industry = profile.get("industry", "") if profile else ""
        sector = profile.get("sector", "") if profile else ""

        # Economic Moat
        moat_prompt = (
            f"Based on the company description for {self.stock_db_entry.company_name} ({self.ticker}): \"{description}\", "
            f"and its industry '{industry}', what are its likely competitive advantages (economic moat)? "
            f"Consider brand strength, network effects, switching costs, intangible assets (patents, licenses), and cost advantages. "
            f"Provide a concise summary."
        )
        qual_analysis["economic_moat_summary"] = self.gemini.summarize_text(moat_prompt,
                                                                            context="Summarizing economic moat")
        qual_analysis["qualitative_sources"][
            "moat_prompt_context"] = f"Company Description: {description[:200]}..., Industry: {industry}"

        # Industry Trends
        industry_prompt = (
            f"What are the current key trends, opportunities, and risks for the '{industry}' industry and '{sector}' sector? "
            f"How might {self.stock_db_entry.company_name} be positioned regarding these trends? Provide a concise summary."
        )
        # Augment with news if available (TODO: integrate news_analyzer results or fetch fresh news)
        # For now, general knowledge based.
        qual_analysis["industry_trends_summary"] = self.gemini.summarize_text(industry_prompt,
                                                                              context="Summarizing industry trends")
        qual_analysis["qualitative_sources"]["industry_prompt_context"] = f"Industry: {industry}, Sector: {sector}"

        # Management Assessment (very high level without specific news/data)
        # Actual management assessment requires deep dives into exec bios, tenure, insider trading, capital allocation history.
        # This is a placeholder for what Gemini *could* infer or what could be fed from other sources.
        management_prompt = (
            f"Given the general information about {self.stock_db_entry.company_name} ({self.ticker}), "
            f"what are generic positive and negative indicators to look for in its management team? "
            f"This is a general query, not based on specific named executives of this company unless provided. "
            f"Provide a brief summary of factors."
        )
        qual_analysis["management_assessment_summary"] = self.gemini.summarize_text(management_prompt,
                                                                                    context="Generic management assessment factors")
        # In a real scenario, you'd feed specific news about CEO changes, insider trades, etc.
        qual_analysis["qualitative_sources"]["management_prompt_context"] = "General management quality factors query."

        logger.info(f"Qualitative analysis for {self.ticker} complete.")
        return qual_analysis

    def _determine_investment_strategy_and_conclusion(self, fund_metrics, qual_analysis):
        logger.info(f"Determining investment strategy for {self.ticker}...")
        decision_parts = []

        # Fundamental checks based on provided strategy document
        # P/E
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
            # TODO: Add comparison to industry average (needs industry P/E data)
        else:
            pe_interpretation += "N/A. "
        decision_parts.append(pe_interpretation)

        # ROE
        roe_interpretation = "ROE: "
        if fund_metrics.get("roe") is not None:
            roe_val = fund_metrics["roe"] * 100  # Assuming ROE from API is decimal
            roe_interpretation += f"{roe_val:.2f}%. "
            if roe_val > 20:
                roe_interpretation += "Strong ROE, efficient use of shareholder equity. "
            elif roe_val > 15:
                roe_interpretation += "Good ROE. "
            else:
                roe_interpretation += "Subpar or low ROE. "
            # TODO: Check if driven by high debt
        else:
            roe_interpretation += "N/A. "
        decision_parts.append(roe_interpretation)

        # Debt-to-Equity
        de_interpretation = "Debt-to-Equity: "
        if fund_metrics.get("debt_to_equity") is not None:
            de_val = fund_metrics["debt_to_equity"]
            de_interpretation += f"{de_val:.2f}. "
            if de_val > 1.0:
                de_interpretation += "High leverage, be cautious. "  # General rule, industry specific
            elif de_val > 0.5:
                de_interpretation += "Moderate leverage. "
            else:
                de_interpretation += "Low leverage. "
        else:
            de_interpretation += "N/A. "
        decision_parts.append(de_interpretation)

        # Revenue Growth
        decision_parts.append(f"Revenue Growth (YoY): {fund_metrics.get('revenue_growth', 'N/A')}.")
        # Free Cash Flow Trend
        decision_parts.append(f"Free Cash Flow Trend: {fund_metrics.get('free_cash_flow_trend', 'N/A')}.")

        # Qualitative Summaries
        decision_parts.append(f"Economic Moat: {qual_analysis.get('economic_moat_summary', 'N/A')[:150]}...")  # Snippet
        decision_parts.append(f"Industry Trends: {qual_analysis.get('industry_trends_summary', 'N/A')[:150]}...")

        # Gemini for overall synthesis
        synthesis_prompt = (
            f"Synthesize the following financial metrics and qualitative analysis for {self.stock_db_entry.company_name} ({self.ticker}) "
            f"into an investment thesis. Metrics: {pe_interpretation}, {roe_interpretation}, {de_interpretation}, "
            f"Revenue Growth: {fund_metrics.get('revenue_growth', 'N/A')}, FCF Trend: {fund_metrics.get('free_cash_flow_trend', 'N/A')}. "
            f"Qualitative: Moat: {qual_analysis.get('economic_moat_summary', 'N/A')}, "
            f"Industry: {qual_analysis.get('industry_trends_summary', 'N/A')}. "
            f"Based on these, suggest a general investment decision (e.g., 'Consider for Value', 'Monitor for Growth', 'Exercise Caution', 'Potential Buy', 'Leaning Negative') and a brief reasoning. "
            f"Do not give financial advice. This is for informational purposes."
        )

        gemini_synthesis = self.gemini.generate_text(synthesis_prompt)

        # Simple keyword-based decision extraction (can be improved)
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
        if not self.stock_db_entry:
            logger.error(f"Stock entry for {self.ticker} could not be established. Aborting analysis.")
            return None

        raw_data = self._fetch_financial_data()
        if not raw_data.get("profile") and not raw_data.get("key_metrics_fmp"):  # Basic check if any data came through
            logger.error(f"Failed to fetch significant data for {self.ticker}. Aborting analysis.")
            return None

        calculated_metrics = self._calculate_metrics(raw_data)
        qualitative_summary = self._analyze_qualitative_factors(raw_data)
        strategy_and_conclusion = self._determine_investment_strategy_and_conclusion(calculated_metrics,
                                                                                     qualitative_summary)

        # Store analysis in DB
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
        self.stock_db_entry.last_analysis_date = datetime.utcnow()  # Or func.now() if DB supports it well

        try:
            self.db_session.commit()
            logger.info(f"Successfully analyzed and saved stock: {self.ticker}")
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error saving analysis for {self.ticker}: {e}", exc_info=True)
            return None  # Indicate failure
        finally:
            self.db_session.close()

        return analysis_entry  # Return the saved analysis


# Example usage:
if __name__ == '__main__':
    # This part is for testing and won't run when imported.
    # Ensure DB is initialized and config is correct before running.
    from database import init_db

    try:
        init_db()  # Make sure tables are created
        logger.info("Starting standalone stock analysis test...")
        # You need to provide a ticker symbol, e.g., AAPL, MSFT
        # Ensure API keys in config.py are valid and have entitlements for these tickers.
        analyzer = StockAnalyzer(ticker="AAPL")
        analysis_result = analyzer.analyze()
        if analysis_result:
            logger.info(
                f"Analysis for {analysis_result.stock.ticker} completed. Decision: {analysis_result.investment_decision}")
            logger.info(f"Reasoning: {analysis_result.reasoning[:300]}...")  # Print snippet
        else:
            logger.error("Stock analysis failed.")

    except Exception as e:
        logger.error(f"Error during stock analysis test: {e}", exc_info=True)