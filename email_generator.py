# email_generator.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import (
    EMAIL_HOST, EMAIL_PORT, EMAIL_USE_TLS, EMAIL_HOST_USER,
    EMAIL_HOST_PASSWORD, EMAIL_SENDER, EMAIL_RECIPIENT
)
from error_handler import logger
from models import StockAnalysis, IPOAnalysis, NewsEventAnalysis  # Ensure these are imported
from datetime import datetime, timezone  # Added timezone
import json
from markdown2 import Markdown


class EmailGenerator:
    def __init__(self):
        self.markdowner = Markdown(extras=["tables", "fenced-code-blocks", "break-on-newline"])

    def _md_to_html(self, md_text):
        if md_text is None: return "<p>N/A</p>"
        if isinstance(md_text, (dict, list)):  # Handle JSON data directly if passed
            return f"<pre>{json.dumps(md_text, indent=2)}</pre>"
        if not isinstance(md_text, str): md_text = str(md_text)

        # Basic check for existing HTML tags
        if "<" in md_text and ">" in md_text and ("<p>" in md_text.lower() or "<div>" in md_text.lower()):
            return md_text  # Assume it's already HTMLish
        return self.markdowner.convert(md_text)

    def _format_stock_analysis_html(self, analysis: StockAnalysis):
        if not analysis: return ""
        stock = analysis.stock

        # Helper for formatting numbers (percentage, decimal)
        def fmt_num(val, type="decimal", na_val="N/A"):
            if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))): return na_val
            if type == "percent": return f"{val * 100:.2f}%"
            if type == "decimal": return f"{val:.2f}"
            return str(val)

        # Qualitative summaries from potentially Markdown content
        business_summary_html = self._md_to_html(analysis.business_summary)
        economic_moat_html = self._md_to_html(analysis.economic_moat_summary)
        industry_trends_html = self._md_to_html(analysis.industry_trends_summary)
        competitive_landscape_html = self._md_to_html(analysis.competitive_landscape_summary)
        management_assessment_html = self._md_to_html(analysis.management_assessment_summary)
        risk_factors_html = self._md_to_html(analysis.risk_factors_summary)
        investment_thesis_html = self._md_to_html(analysis.investment_thesis_full)  # Full thesis
        reasoning_points_html = self._md_to_html(analysis.reasoning)  # Key reasoning points

        html = f"""
        <div class="analysis-block stock-analysis">
            <h2>Stock Analysis: {stock.company_name} ({stock.ticker})</h2>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M %Z')}</p>
            <p><strong>Industry:</strong> {stock.industry or 'N/A'}, <strong>Sector:</strong> {stock.sector or 'N/A'}</p>
            <p><strong>Investment Decision:</strong> {analysis.investment_decision or 'N/A'}</p>
            <p><strong>Strategy Type:</strong> {analysis.strategy_type or 'N/A'}</p>
            <p><strong>Confidence Level:</strong> {analysis.confidence_level or 'N/A'}</p>

            <details>
                <summary><strong>Investment Thesis & Reasoning (Click to expand)</strong></summary>
                <h4>Full Thesis:</h4>
                <div class="markdown-content">{investment_thesis_html}</div>
                <h4>Key Reasoning Points:</h4>
                <div class="markdown-content">{reasoning_points_html}</div>
            </details>

            <details>
                <summary><strong>Key Financial Metrics (Click to expand)</strong></summary>
                <ul>
                    <li>P/E Ratio: {fmt_num(analysis.pe_ratio)}</li>
                    <li>P/B Ratio: {fmt_num(analysis.pb_ratio)}</li>
                    <li>P/S Ratio: {fmt_num(analysis.ps_ratio)}</li>
                    <li>EV/Sales: {fmt_num(analysis.ev_to_sales)}</li>
                    <li>EV/EBITDA: {fmt_num(analysis.ev_to_ebitda)}</li>
                    <li>EPS: {fmt_num(analysis.eps)}</li>
                    <li>ROE: {fmt_num(analysis.roe, 'percent')}</li>
                    <li>ROA: {fmt_num(analysis.roa, 'percent')}</li>
                    <li>ROIC: {fmt_num(analysis.roic, 'percent')}</li>
                    <li>Dividend Yield: {fmt_num(analysis.dividend_yield, 'percent')}</li>
                    <li>Debt-to-Equity: {fmt_num(analysis.debt_to_equity)}</li>
                    <li>Debt-to-EBITDA: {fmt_num(analysis.debt_to_ebitda)}</li>
                    <li>Interest Coverage: {fmt_num(analysis.interest_coverage_ratio)}x</li>
                    <li>Current Ratio: {fmt_num(analysis.current_ratio)}</li>
                    <li>Quick Ratio: {fmt_num(analysis.quick_ratio)}</li>
                    <li>Gross Profit Margin: {fmt_num(analysis.gross_profit_margin, 'percent')}</li>
                    <li>Operating Profit Margin: {fmt_num(analysis.operating_profit_margin, 'percent')}</li>
                    <li>Net Profit Margin: {fmt_num(analysis.net_profit_margin, 'percent')}</li>
                    <li>Revenue Growth YoY: {fmt_num(analysis.revenue_growth_yoy, 'percent')} (QoQ: {fmt_num(analysis.revenue_growth_qoq, 'percent')})</li>
                    <li>Revenue Growth CAGR (3yr/5yr): {fmt_num(analysis.revenue_growth_cagr_3yr, 'percent')} / {fmt_num(analysis.revenue_growth_cagr_5yr, 'percent')}</li>
                    <li>EPS Growth YoY: {fmt_num(analysis.eps_growth_yoy, 'percent')}</li>
                    <li>EPS Growth CAGR (3yr/5yr): {fmt_num(analysis.eps_growth_cagr_3yr, 'percent')} / {fmt_num(analysis.eps_growth_cagr_5yr, 'percent')}</li>
                    <li>FCF per Share: {fmt_num(analysis.free_cash_flow_per_share)}</li>
                    <li>FCF Yield: {fmt_num(analysis.free_cash_flow_yield, 'percent')}</li>
                    <li>FCF Trend: {analysis.free_cash_flow_trend or 'N/A'}</li>
                    <li>Retained Earnings Trend: {analysis.retained_earnings_trend or 'N/A'}</li>
                </ul>
            </details>

            <details>
                <summary><strong>DCF Analysis (Simplified) (Click to expand)</strong></summary>
                <ul>
                    <li>Intrinsic Value per Share: {fmt_num(analysis.dcf_intrinsic_value)}</li>
                    <li>Upside/Downside: {fmt_num(analysis.dcf_upside_percentage, 'percent')}</li>
                </ul>
                <p><em>Assumptions:</em></p>
                <div class="markdown-content">{self._md_to_html(analysis.dcf_assumptions)}</div>
            </details>

            <details>
                <summary><strong>Qualitative Analysis (from 10-K/Profile & AI) (Click to expand)</strong></summary>
                <p><strong>Business Summary:</strong></p><div class="markdown-content">{business_summary_html}</div>
                <p><strong>Economic Moat:</strong></p><div class="markdown-content">{economic_moat_html}</div>
                <p><strong>Industry Trends & Position:</strong></p><div class="markdown-content">{industry_trends_html}</div>
                <p><strong>Competitive Landscape:</strong></p><div class="markdown-content">{competitive_landscape_html}</div>
                <p><strong>Management Discussion Highlights (MD&A/Assessment):</strong></p><div class="markdown-content">{management_assessment_html}</div>
                <p><strong>Key Risk Factors:</strong></p><div class="markdown-content">{risk_factors_html}</div>
            </details>

            <details>
                <summary><strong>Supporting Data Snapshots (Click to expand)</strong></summary>
                <p><em>Key Metrics Data Points Used:</em></p>
                <div class="markdown-content">{self._md_to_html(analysis.key_metrics_snapshot)}</div>
                <p><em>Qualitative Analysis Sources Summary:</em></p>
                <div class="markdown-content">{self._md_to_html(analysis.qualitative_sources_summary)}</div>
            </details>
        </div>
        """
        return html

    def _format_ipo_analysis_html(self, analysis: IPOAnalysis):
        if not analysis: return ""
        ipo = analysis.ipo

        # Helper for formatting numbers/price ranges
        def fmt_price(val_low, val_high, currency="USD"):
            if val_low is None and val_high is None: return "N/A"
            if val_low is not None and val_high is not None:
                if val_low == val_high: return f"{val_low:.2f} {currency}"
                return f"{val_low:.2f} - {val_high:.2f} {currency}"
            if val_low is not None: return f"{val_low:.2f} {currency}"
            if val_high is not None: return f"{val_high:.2f} {currency}"  # Should ideally have low if high exists
            return "N/A"

        reasoning_html = self._md_to_html(analysis.reasoning)
        s1_business_summary_html = self._md_to_html(
            analysis.s1_business_summary or analysis.business_model_summary)  # Fallback to old field
        s1_risk_factors_summary_html = self._md_to_html(
            analysis.s1_risk_factors_summary or analysis.risk_factors_summary)
        s1_mda_summary_html = self._md_to_html(analysis.s1_mda_summary)
        s1_financial_health_summary_html = self._md_to_html(
            analysis.s1_financial_health_summary or analysis.pre_ipo_financials_summary)
        competitive_landscape_html = self._md_to_html(analysis.competitive_landscape_summary)
        industry_outlook_html = self._md_to_html(analysis.industry_outlook_summary)  # was industry_health_summary
        use_of_proceeds_html = self._md_to_html(analysis.use_of_proceeds_summary)
        management_team_html = self._md_to_html(analysis.management_team_assessment)
        underwriter_html = self._md_to_html(analysis.underwriter_quality_assessment)
        valuation_html = self._md_to_html(analysis.valuation_comparison_summary)

        html = f"""
        <div class="analysis-block ipo-analysis">
            <h2>IPO Analysis: {ipo.company_name} ({ipo.symbol or 'N/A'})</h2>
            <p><strong>Expected IPO Date:</strong> {ipo.ipo_date.strftime('%Y-%m-%d') if ipo.ipo_date else ipo.ipo_date_str or 'N/A'}</p>
            <p><strong>Expected Price Range:</strong> {fmt_price(ipo.expected_price_range_low, ipo.expected_price_range_high, ipo.expected_price_currency)}</p>
            <p><strong>Exchange:</strong> {ipo.exchange or 'N/A'}, <strong>Status:</strong> {ipo.status or 'N/A'}</p>
            <p><strong>S-1 Filing URL:</strong> {f'<a href="{ipo.s1_filing_url}">{ipo.s1_filing_url}</a>' if ipo.s1_filing_url else 'Not Found'}</p>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M %Z')}</p>
            <p><strong>Preliminary Stance:</strong> {analysis.investment_decision or 'N/A'}</p>

            <details>
                <summary><strong>AI Synthesized Reasoning & Critical Verification Points (Click to expand)</strong></summary>
                <div class="markdown-content">{reasoning_html}</div>
            </details>

            <details>
                <summary><strong>S-1 Based Summaries (if available) & AI Analysis (Click to expand)</strong></summary>
                <p><strong>Business Summary (from S-1 or inferred):</strong></p><div class="markdown-content">{s1_business_summary_html}</div>
                <p><strong>Competitive Landscape:</strong></p><div class="markdown-content">{competitive_landscape_html}</div>
                <p><strong>Industry Outlook:</strong></p><div class="markdown-content">{industry_outlook_html}</div>
                <p><strong>Risk Factors Summary (from S-1 or inferred):</strong></p><div class="markdown-content">{s1_risk_factors_summary_html}</div>
                <p><strong>Use of Proceeds (from S-1 or inferred):</strong></p><div class="markdown-content">{use_of_proceeds_html}</div>
                <p><strong>MD&A / Financial Health Summary (from S-1 or inferred):</strong></p><div class="markdown-content">{s1_mda_summary_html if s1_mda_summary_html and not s1_mda_summary_html.startswith("Section not found") else s1_financial_health_summary_html}</div>
                <p><strong>Management Team Assessment (Placeholder):</strong></p><div class="markdown-content">{management_team_html}</div>
                <p><strong>Underwriter Quality Assessment (Placeholder):</strong></p><div class="markdown-content">{underwriter_html}</div>
                <p><strong>Valuation Comparison Guidance (Generic):</strong></p><div class="markdown-content">{valuation_html}</div>
            </details>

            <details>
                <summary><strong>Supporting Data (Click to expand)</strong></summary>
                <p><em>Raw data from IPO calendar API:</em></p>
                <div class="markdown-content">{self._md_to_html(analysis.key_data_snapshot)}</div>
                <p><em>S-1 Sections Used for Analysis (True if found & used):</em></p>
                <div class="markdown-content">{self._md_to_html(analysis.s1_sections_used)}</div>
            </details>
        </div>
        """
        return html

    def _format_news_event_analysis_html(self, analysis: NewsEventAnalysis):
        if not analysis: return ""
        news_event = analysis.news_event

        sentiment_html = self._md_to_html(
            f"**Sentiment:** {analysis.sentiment or 'N/A'}\n**Reasoning:** {analysis.sentiment_reasoning or 'N/A'}")
        news_summary_detailed_html = self._md_to_html(analysis.news_summary_detailed)
        impact_companies_html = self._md_to_html(analysis.potential_impact_on_companies)
        impact_sectors_html = self._md_to_html(analysis.potential_impact_on_sectors)
        mechanism_html = self._md_to_html(analysis.mechanism_of_impact)
        timing_duration_html = self._md_to_html(analysis.estimated_timing_duration)
        magnitude_direction_html = self._md_to_html(analysis.estimated_magnitude_direction)
        confidence_html = self._md_to_html(analysis.confidence_of_assessment)
        investor_summary_html = self._md_to_html(analysis.summary_for_email)

        html = f"""
        <div class="analysis-block news-analysis">
            <h2>News/Event Analysis: {news_event.event_title}</h2>
            <p><strong>Event Date:</strong> {news_event.event_date.strftime('%Y-%m-%d %H:%M %Z') if news_event.event_date else 'N/A'}</p>
            <p><strong>Source:</strong> <a href="{news_event.source_url}">{news_event.source_name or news_event.source_url}</a></p>
            <p><strong>Full Article Scraped:</strong> {'Yes' if news_event.full_article_text else 'No (Analysis based on headline/summary if available)'}</p>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M %Z')}</p>

            <p><strong>Investor Summary:</strong></p>
            <div class="markdown-content">{investor_summary_html}</div>

            <details>
                <summary><strong>Detailed AI Analysis (Click to expand)</strong></summary>
                <p><strong>Sentiment Analysis:</strong></p>
                <div class="markdown-content">{sentiment_html}</div>
                <p><strong>Detailed News Summary:</strong></p>
                <div class="markdown-content">{news_summary_detailed_html}</div>
                <p><strong>Potentially Affected Companies/Stocks:</strong></p>
                <div class="markdown-content">{impact_companies_html}</div>
                <p><strong>Potentially Affected Sectors:</strong></p>
                <div class="markdown-content">{impact_sectors_html}</div>
                <p><strong>Mechanism of Impact:</strong></p>
                <div class="markdown-content">{mechanism_html}</div>
                <p><strong>Estimated Timing & Duration:</strong></p>
                <div class="markdown-content">{timing_duration_html}</div>
                <p><strong>Estimated Magnitude & Direction:</strong></p>
                <div class="markdown-content">{magnitude_direction_html}</div>
                <p><strong>Confidence of Assessment:</strong></p>
                <div class="markdown-content">{confidence_html}</div>
            </details>
             <details>
                <summary><strong>Key Snippets Used for Analysis (Click to expand)</strong></summary>
                <div class="markdown-content">{self._md_to_html(analysis.key_news_snippets)}</div>
            </details>
        </div>
        """
        return html

    def create_summary_email(self, stock_analyses=None, ipo_analyses=None, news_analyses=None):
        if not any([stock_analyses, ipo_analyses, news_analyses]):
            logger.info("No analyses provided to create an email.")
            return None

        subject_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        subject = f"Financial Analysis Summary - {subject_date}"

        html_body = f"""
        <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; line-height: 1.6; color: #333; }}
                    .container {{ background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 0 15px rgba(0,0,0,0.1); max-width: 900px; margin: auto; }}
                    .analysis-block {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 25px; border-radius: 5px; background-color: #fdfdfd; box-shadow: 0 2px 4px rgba(0,0,0,0.05);}}
                    .stock-analysis {{ border-left: 5px solid #4CAF50; }} /* Green for stocks */
                    .ipo-analysis {{ border-left: 5px solid #2196F3; }}   /* Blue for IPOs */
                    .news-analysis {{ border-left: 5px solid #FFC107; }} /* Yellow for News */
                    h1 {{ color: #2c3e50; text-align: center; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                    h2 {{ color: #34495e; border-bottom: 1px solid #eee; padding-bottom: 5px; margin-top: 0; }}
                    h4 {{ color: #555; margin-top: 15px; margin-bottom: 5px; }}
                    details > summary {{ cursor: pointer; font-weight: bold; margin-bottom: 10px; color: #2980b9; padding: 5px; background-color: #ecf0f1; border-radius:3px; }}
                    details[open] > summary {{ background-color: #dde5e8; }}
                    pre {{ background-color: #eee; padding: 10px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; word-wrap: break-word; font-size: 0.85em; border: 1px solid #ccc; }}
                    ul {{ list-style-type: disc; margin-left: 20px; padding-left: 5px; }}
                    li {{ margin-bottom: 8px; }}
                    .markdown-content {{ padding: 5px 0; }}
                    .markdown-content p {{ margin: 0.5em 0; }}
                    .markdown-content ul, .markdown-content ol {{ margin-left: 20px; }}
                    .markdown-content table {{ border-collapse: collapse; width: 100%; margin-bottom: 1em;}}
                    .markdown-content th, .markdown-content td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    .markdown-content th {{ background-color: #f2f2f2; }}
                    .report-footer {{ text-align: center; font-size: 0.9em; color: #777; margin-top: 30px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Financial Analysis Report</h1>
                    <p style="text-align:center; font-style:italic; color:#555;">Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}</p>
                    <p style="text-align:center; font-style:italic; color:#7f8c8d;"><em>This email contains automated analysis. Always do your own research before making investment decisions.</em></p>
        """

        if stock_analyses:
            html_body += "<h2>Individual Stock Analyses</h2>"
            for sa in stock_analyses:
                html_body += self._format_stock_analysis_html(sa)

        if ipo_analyses:
            html_body += "<h2>Upcoming IPO Analyses</h2>"
            for ia in ipo_analyses:
                html_body += self._format_ipo_analysis_html(ia)

        if news_analyses:
            html_body += "<h2>Recent News & Event Analyses</h2>"
            for na in news_analyses:
                html_body += self._format_news_event_analysis_html(na)

        html_body += """
                    <div class="report-footer">
                        <p>© Automated Financial Analysis System</p>
                    </div>
                </div>
            </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECIPIENT

        msg.attach(MIMEText(html_body, 'html', 'utf-8'))  # Ensure UTF-8
        return msg

    def send_email(self, message: MIMEMultipart):
        if not message:
            logger.error("No message object provided to send_email.")
            return False
        try:
            # Ensure SMTP settings are correctly fetched from config
            smtp_server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=20)  # Added timeout
            if EMAIL_USE_TLS:
                smtp_server.starttls()
            smtp_server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
            smtp_server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, message.as_string())
            smtp_server.quit()
            logger.info(f"Email sent successfully to {EMAIL_RECIPIENT}")
            return True
        except smtplib.SMTPException as e_smtp:
            logger.error(f"SMTP error sending email: {e_smtp}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"General error sending email: {e}", exc_info=True)
            return False


if __name__ == '__main__':
    import math  # For mock data with potential NaN/inf

    logger.info("Starting email generator test with new model fields...")


    # --- Mock Stock Data ---
    class MockStock:
        def __init__(self, ticker, company_name, industry="Tech", sector="Software"):
            self.ticker = ticker
            self.company_name = company_name
            self.industry = industry
            self.sector = sector


    class MockStockAnalysis:
        def __init__(self, stock):
            self.stock = stock
            self.analysis_date = datetime.now(timezone.utc)
            self.investment_decision = "Buy"
            self.strategy_type = "GARP (Growth at a Reasonable Price)"
            self.confidence_level = "Medium"
            self.investment_thesis_full = "This is a **compelling** investment due to *strong growth* and reasonable valuation. AI suggests `monitoring` market conditions."
            self.reasoning = "- Strong revenue growth.\n- Improving margins.\n- Reasonable valuation compared to peers."

            self.pe_ratio = 18.5;
            self.pb_ratio = 3.2;
            self.ps_ratio = 2.5;
            self.ev_to_sales = 2.8;
            self.ev_to_ebitda = 12.0
            self.eps = 2.50;
            self.roe = 0.22;
            self.roa = 0.10;
            self.roic = 0.15;
            self.dividend_yield = 0.015
            self.debt_to_equity = 0.5;
            self.debt_to_ebitda = 2.1;
            self.interest_coverage_ratio = 8.0
            self.current_ratio = 1.8;
            self.quick_ratio = 1.2;
            self.revenue_growth_yoy = 0.15;
            self.revenue_growth_qoq = 0.04;
            self.revenue_growth_cagr_3yr = 0.12;
            self.revenue_growth_cagr_5yr = 0.10
            self.eps_growth_yoy = 0.20;
            self.eps_growth_cagr_3yr = 0.18;
            self.eps_growth_cagr_5yr = 0.15
            self.net_profit_margin = 0.12;
            self.gross_profit_margin = 0.60;
            self.operating_profit_margin = 0.20
            self.free_cash_flow_per_share = 1.80;
            self.free_cash_flow_yield = 0.05;
            self.free_cash_flow_trend = "Growing"
            self.retained_earnings_trend = "Growing"

            self.dcf_intrinsic_value = 120.50;
            self.dcf_upside_percentage = 0.205
            self.dcf_assumptions = {"discount_rate": 0.09, "perpetual_growth_rate": 0.025, "start_fcf": 100000000}

            self.business_summary = "MockCorp is a leading provider of cloud solutions."
            self.economic_moat_summary = "Strong brand recognition and high switching costs."
            self.industry_trends_summary = "Industry is rapidly growing with tailwinds from AI adoption."
            self.competitive_landscape_summary = "Competitive but MockCorp has a differentiated product."
            self.management_assessment_summary = "Experienced management team with a clear vision (from MD&A)."
            self.risk_factors_summary = "Key risks include talent retention and cybersecurity threats."

            self.key_metrics_snapshot = {"price": 100, "marketCap": 1000000000, "latest_revenue_q": 25000000}
            self.qualitative_sources_summary = {"10k_filing_url_used": "http://example.com/10k",
                                                "business_10k_source_length": 5000}


    # --- Mock IPO Data ---
    class MockIPO:
        def __init__(self, company_name, symbol, ipo_date_str="2025-07-15"):
            self.company_name = company_name
            self.symbol = symbol
            self.ipo_date_str = ipo_date_str
            self.ipo_date = datetime.strptime(ipo_date_str, "%Y-%m-%d").date() if ipo_date_str else None
            self.expected_price_range_low = 18.00
            self.expected_price_range_high = 22.00
            self.expected_price_currency = "USD"
            self.exchange = "NASDAQ"
            self.status = "Filed"
            self.s1_filing_url = "http://example.com/s1_filing"


    class MockIPOAnalysis:
        def __init__(self, ipo):
            self.ipo = ipo
            self.analysis_date = datetime.now(timezone.utc)
            self.investment_decision = "Potentially Interesting, S-1 Review Critical"
            self.reasoning = "Promising industry, but financial details from S-1 are key. S-1 summary indicates good initial traction."
            self.s1_business_summary = "NewIPO Inc. is a disruptive player in the fintech space, offering innovative payment solutions."
            self.s1_risk_factors_summary = "Primary risks include regulatory changes and competition from established banks."
            self.s1_mda_summary = "MD&A shows rapid revenue growth but increasing operating losses due to R&D and S&M."
            self.s1_financial_health_summary = "Strong top-line growth, negative FCF, well-funded post-IPO."
            self.competitive_landscape_summary = "Competes with traditional banks and other fintech startups."
            self.industry_outlook_summary = "Fintech industry has strong tailwinds but is becoming crowded."
            self.use_of_proceeds_summary = "Proceeds to be used for product development and market expansion."
            self.management_team_assessment = "Founders have prior startup success. Full team review in S-1 needed."
            self.underwriter_quality_assessment = "Lead underwriters are reputable (e.g., Goldman Sachs, Morgan Stanley - from S-1)."
            self.key_data_snapshot = {"name": ipo.company_name, "symbol": ipo.symbol, "price": "18.00-22.00"}
            self.s1_sections_used = {"business": True, "risk_factors": True, "mda": True}
            # Fallback fields for demonstration
            self.business_model_summary = self.s1_business_summary
            self.risk_factors_summary = self.s1_risk_factors_summary
            self.pre_ipo_financials_summary = self.s1_financial_health_summary
            self.valuation_comparison_summary = "Peer valuation suggests a range of X to Y based on P/S multiples."


    # --- Mock News Data ---
    class MockNewsEvent:
        def __init__(self, title, url, event_date_str="2025-05-25 10:00:00"):
            self.event_title = title
            self.source_url = url
            self.source_name = "Mock News Source"
            self.event_date = datetime.strptime(event_date_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc) if event_date_str else datetime.now(timezone.utc)
            self.full_article_text = "This is the full article text which is much longer and provides more context than just a summary. It discusses market trends and company X's new product."


    class MockNewsEventAnalysis:
        def __init__(self, news_event):
            self.news_event = news_event
            self.analysis_date = datetime.now(timezone.utc)
            self.sentiment = "Positive"
            self.sentiment_reasoning = "The article highlights strong growth and innovation."
            self.news_summary_detailed = "A detailed summary of the news focusing on key impacts."
            self.potential_impact_on_companies = "Company X (TICK) likely to benefit from new product launch. Competitor Y (COMP) may face pressure."
            self.potential_impact_on_sectors = "Technology sector, specifically software services, will see increased activity."
            self.mechanism_of_impact = "New product addresses a key market need, potentially increasing revenue for Company X."
            self.estimated_timing_duration = "Short-term positive sentiment, Medium-term revenue impact."
            self.estimated_magnitude_direction = "Medium Positive for Company X."
            self.confidence_of_assessment = "High"
            self.summary_for_email = "Company X launched a new product, expecting positive revenue impact in the tech sector."
            self.key_news_snippets = {"headline": news_event.event_title, "snippet_used": "Company X's new product..."}


    # Create mock instances
    mock_sa = MockStockAnalysis(MockStock("MOCK", "MockCorp Inc."))
    mock_ipo_a = MockIPOAnalysis(MockIPO("NewIPO Inc.", "NIPO"))
    mock_news_a = MockNewsEventAnalysis(MockNewsEvent("Major Tech Breakthrough Announced", "http://example.com/news1"))

    email_gen = EmailGenerator()
    email_message = email_gen.create_summary_email(
        stock_analyses=[mock_sa],
        ipo_analyses=[mock_ipo_a],
        news_analyses=[mock_news_a]
    )

    if email_message:
        logger.info("Email message created successfully with new fields.")
        # Save to file for inspection
        output_filename = f"test_email_summary_refactored_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(output_filename, "w", encoding="utf-8") as f:
            # MIMEText part is the first payload if it's html only, or might be nested.
            # For 'alternative', payload is a list of MIMEMultipart/MIMEText.
            # We expect the HTML part.
            payload_html = ""
            if email_message.is_multipart():
                for part in email_message.get_payload():
                    if part.get_content_type() == "text/html":
                        payload_html = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8')
                        break
            else:  # Not multipart, should be text/html directly
                payload_html = email_message.get_payload(decode=True).decode(
                    email_message.get_content_charset() or 'utf-8')

            if payload_html:
                f.write(payload_html)
                logger.info(f"Test email HTML saved to {output_filename}")
            else:
                logger.error("Could not extract HTML payload from email message.")

        # To actually send, uncomment and ensure config.py is correct:
        # logger.info("Attempting to send test email...")
        # if email_gen.send_email(email_message):
        #     logger.info("Test email sent successfully (check your inbox).")
        # else:
        #     logger.error("Failed to send test email.")

    else:
        logger.error("Failed to create email message.")