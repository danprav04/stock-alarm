# email_generator.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import EMAIL_HOST, EMAIL_PORT, EMAIL_USE_TLS, EMAIL_HOST_USER, EMAIL_HOST_PASSWORD, EMAIL_SENDER, \
    EMAIL_RECIPIENT
from error_handler import logger
from models import StockAnalysis, IPOAnalysis, NewsEventAnalysis
from datetime import datetime
import json
from markdown2 import Markdown  # Import the markdown library


class EmailGenerator:
    def __init__(self):
        self.markdowner = Markdown()  # Create a Markdown converter instance

    def _md_to_html(self, md_text):
        """Converts a markdown string to HTML. Handles None input."""
        if md_text is None:
            return "<p>N/A</p>"  # Or an empty string, or some other placeholder
        # Basic check to see if it's already HTML (e.g. if AI starts returning HTML directly)
        if md_text.strip().startswith("<") and md_text.strip().endswith(">"):
            return md_text
        return self.markdowner.convert(md_text)

    def _format_stock_analysis_html(self, analysis: StockAnalysis):
        if not analysis: return ""
        stock = analysis.stock
        # Apply markdown conversion to fields that might contain markdown from Gemini
        reasoning_html = self._md_to_html(analysis.reasoning)
        economic_moat_html = self._md_to_html(analysis.economic_moat_summary)
        industry_trends_html = self._md_to_html(analysis.industry_trends_summary)
        management_assessment_html = self._md_to_html(analysis.management_assessment_summary)

        html = f"""
        <div class="analysis-block">
            <h2>Stock Analysis: {stock.company_name} ({stock.ticker})</h2>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M')}</p>
            <p><strong>Decision:</strong> {analysis.investment_decision}</p>
            <p><strong>Strategy Type:</strong> {analysis.strategy_type}</p>

            <details>
                <summary><strong>Reasoning & AI Synthesis (Click to expand)</strong></summary>
                <div class="markdown-content">{reasoning_html}</div>
            </details>

            <details>
                <summary><strong>Key Financial Metrics (Click to expand)</strong></summary>
                <ul>
                    <li>P/E Ratio: {analysis.pe_ratio if analysis.pe_ratio is not None else 'N/A'}</li>
                    <li>P/B Ratio: {analysis.pb_ratio if analysis.pb_ratio is not None else 'N/A'}</li>
                    <li>EPS: {analysis.eps if analysis.eps is not None else 'N/A'}</li>
                    <li>ROE: {f"{analysis.roe * 100:.2f}%" if analysis.roe is not None else 'N/A'}</li>
                    <li>Dividend Yield: {f"{analysis.dividend_yield * 100:.2f}%" if analysis.dividend_yield is not None else 'N/A'}</li>
                    <li>Debt-to-Equity: {analysis.debt_to_equity if analysis.debt_to_equity is not None else 'N/A'}</li>
                    <li>Interest Coverage Ratio: {analysis.interest_coverage_ratio if analysis.interest_coverage_ratio is not None else 'N/A'}</li>
                    <li>Current Ratio: {analysis.current_ratio if analysis.current_ratio is not None else 'N/A'}</li>
                    <li>Net Profit Margin: {f"{analysis.net_profit_margin * 100:.2f}%" if analysis.net_profit_margin is not None else 'N/A'}</li>
                    <li>Revenue Growth (YoY): {analysis.revenue_growth}</li>
                    <li>Retained Earnings Trend: {analysis.retained_earnings_trend}</li>
                    <li>Free Cash Flow Trend: {analysis.free_cash_flow_trend}</li>
                </ul>
                <p><em>Raw data points used for metrics:</em></p>
                <pre>{json.dumps(analysis.key_metrics_snapshot, indent=2) if analysis.key_metrics_snapshot else 'N/A'}</pre>
            </details>

            <details>
                <summary><strong>Qualitative Analysis (Click to expand)</strong></summary>
                <p><strong>Economic Moat:</strong></p>
                <div class="markdown-content">{economic_moat_html}</div>
                <p><strong>Industry Trends:</strong></p>
                <div class="markdown-content">{industry_trends_html}</div>
                <p><strong>Management Assessment (General Factors):</strong></p>
                <div class="markdown-content">{management_assessment_html}</div>
                <p><em>Context for qualitative prompts:</em></p>
                <pre>{json.dumps(analysis.qualitative_sources, indent=2) if analysis.qualitative_sources else 'N/A'}</pre>
            </details>
        </div>
        """
        return html

    def _format_ipo_analysis_html(self, analysis: IPOAnalysis):
        if not analysis: return ""
        ipo = analysis.ipo
        # Apply markdown conversion
        reasoning_html = self._md_to_html(analysis.reasoning)
        business_model_html = self._md_to_html(analysis.business_model_summary)
        competitive_landscape_html = self._md_to_html(analysis.competitive_landscape_summary)
        industry_health_html = self._md_to_html(analysis.industry_health_summary)
        use_of_proceeds_html = self._md_to_html(analysis.use_of_proceeds_summary)
        risk_factors_html = self._md_to_html(analysis.risk_factors_summary)
        pre_ipo_financials_html = self._md_to_html(analysis.pre_ipo_financials_summary)
        # valuation_comparison_summary is likely also markdown
        valuation_comparison_html = self._md_to_html(analysis.valuation_comparison_summary)

        html = f"""
        <div class="analysis-block">
            <h2>IPO Analysis: {ipo.company_name}</h2>
            <p><strong>Expected IPO Date:</strong> {ipo.ipo_date}</p>
            <p><strong>Expected Price Range:</strong> {ipo.expected_price_range}</p>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M')}</p>
            <p><strong>Preliminary Stance:</strong> {analysis.investment_decision}</p>

            <details>
                <summary><strong>AI Synthesized Reasoning & Key Verification Points (Click to expand)</strong></summary>
                <div class="markdown-content">{reasoning_html}</div>
            </details>

            <details>
                <summary><strong>IPO Details & Summaries (Click to expand)</strong></summary>
                <p><strong>Business Model:</strong></p><div class="markdown-content">{business_model_html}</div>
                <p><strong>Competitive Landscape:</strong></p><div class="markdown-content">{competitive_landscape_html}</div>
                <p><strong>Industry Health:</strong></p><div class="markdown-content">{industry_health_html}</div>
                <p><strong>Use of Proceeds (General):</strong></p><div class="markdown-content">{use_of_proceeds_html}</div>
                <p><strong>Risk Factors (General):</strong></p><div class="markdown-content">{risk_factors_html}</div>
                <p><strong>Pre-IPO Financials (Guidance):</strong></p><div class="markdown-content">{pre_ipo_financials_html}</div>
                <p><strong>Valuation Comparison (Guidance):</strong></p><div class="markdown-content">{valuation_comparison_html}</div>
                <p><strong>Underwriter Quality:</strong> {analysis.underwriter_quality}</p>
                <p><strong>Fresh Issue vs OFS:</strong> {analysis.fresh_issue_vs_ofs}</p>
                <p><strong>Lock-up Periods:</strong> {analysis.lock_up_periods_info}</p>
                <p><strong>Investor Demand:</strong> {analysis.investor_demand_summary}</p>
                <p><em>Raw data from IPO calendar API:</em></p>
                <pre>{json.dumps(analysis.key_data_snapshot, indent=2) if analysis.key_data_snapshot else 'N/A'}</pre>
            </details>
        </div>
        """
        return html

    def _format_news_event_analysis_html(self, analysis: NewsEventAnalysis):
        if not analysis: return ""
        news_event = analysis.news_event
        # Apply markdown conversion
        summary_email_html = self._md_to_html(analysis.summary_for_email)
        scope_relevance_html = self._md_to_html(analysis.scope_relevance)
        # affected_stocks_sectors is JSON, handle its 'text_analysis' part if it contains markdown
        affected_stocks_text_analysis = analysis.affected_stocks_sectors.get('text_analysis',
                                                                             'N/A') if analysis.affected_stocks_sectors else 'N/A'
        affected_stocks_html = self._md_to_html(affected_stocks_text_analysis)
        mechanism_of_impact_html = self._md_to_html(analysis.mechanism_of_impact)
        estimated_timing_html = self._md_to_html(analysis.estimated_timing)
        estimated_magnitude_direction_html = self._md_to_html(analysis.estimated_magnitude_direction)
        countervailing_factors_html = self._md_to_html(analysis.countervailing_factors)

        html = f"""
        <div class="analysis-block">
            <h2>News/Event Analysis: {news_event.event_title}</h2>
            <p><strong>Event Date:</strong> {news_event.event_date.strftime('%Y-%m-%d %H:%M') if news_event.event_date else 'N/A'}</p>
            <p><strong>Source:</strong> <a href="{news_event.source_url}">{news_event.source_url}</a></p>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M')}</p>
            <p><strong>Investor Summary:</strong></p>
            <div class="markdown-content">{summary_email_html}</div>

            <details>
                <summary><strong>Detailed Analysis (Click to expand)</strong></summary>
                <p><strong>Scope & Relevance:</strong></p>
                <div class="markdown-content">{scope_relevance_html}</div>
                <p><strong>Affected Stocks/Sectors (AI Analysis & API):</strong></p>
                <div class="markdown-content">
                    <p><em>API Related:</em> {analysis.affected_stocks_sectors.get('api_related', 'N/A') if analysis.affected_stocks_sectors else 'N/A'}</p>
                    <p><em>AI Analysis:</em></p>
                    {affected_stocks_html}
                </div>
                <p><strong>Mechanism of Impact:</strong></p>
                <div class="markdown-content">{mechanism_of_impact_html}</div>
                <p><strong>Estimated Timing & Duration:</strong></p>
                <div class="markdown-content">{estimated_timing_html}</div>
                <p><strong>Estimated Magnitude & Direction:</strong></p>
                <div class="markdown-content">{estimated_magnitude_direction_html}</div>
                <p><strong>Countervailing Factors:</strong></p>
                <div class="markdown-content">{countervailing_factors_html}</div>
                <p><em>Key snippets used for analysis:</em></p>
                <pre>{json.dumps(analysis.key_news_snippets, indent=2) if analysis.key_news_snippets else 'N/A'}</pre>
            </details>
        </div>
        """
        return html

    def create_summary_email(self, stock_analyses=None, ipo_analyses=None, news_analyses=None):
        if not any([stock_analyses, ipo_analyses, news_analyses]):
            logger.info("No analyses provided to create an email.")
            return None

        subject_date = datetime.now().strftime("%Y-%m-%d")
        subject = f"Financial Analysis Summary - {subject_date}"

        # Added .markdown-content style
        html_body = """
        <html>
            <head>
                <style>
                    body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; line-height: 1.6; }
                    .container { background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
                    .analysis-block { border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; border-radius: 5px; background-color: #f9f9f9; }
                    h1 { color: #333; }
                    h2 { color: #555; border-bottom: 1px solid #eee; padding-bottom: 5px;}
                    details > summary { cursor: pointer; font-weight: bold; margin-bottom: 10px; color: #0056b3; }
                    pre { background-color: #eee; padding: 10px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; word-wrap: break-word; font-size: 0.9em; }
                    ul { list-style-type: disc; margin-left: 20px; }
                    li { margin-bottom: 5px; }
                    .markdown-content { padding: 5px 0; }
                    .markdown-content p { margin: 0.5em 0; } /* Add some margin to paragraphs generated from markdown */
                    .markdown-content ul, .markdown-content ol { margin-left: 20px; }
                    .markdown-content strong { font-weight: bold; }
                    .markdown-content em { font-style: italic; }
                    .markdown-content h1, .markdown-content h2, .markdown-content h3 { margin-top: 1em; margin-bottom: 0.5em; color: #444; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Financial Analysis Report</h1>
                    <p><em>This email contains automated analysis. Always do your own research before making investment decisions.</em></p>
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
                </div>
            </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECIPIENT

        msg.attach(MIMEText(html_body, 'html'))
        return msg

    def send_email(self, message: MIMEMultipart):
        if not message:
            logger.error("No message object provided to send_email.")
            return False
        try:
            with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                if EMAIL_USE_TLS:
                    server.starttls()
                server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
                server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, message.as_string())
            logger.info(f"Email sent successfully to {EMAIL_RECIPIENT}")
            return True
        except Exception as e:
            logger.error(f"Failed to send email: {e}", exc_info=True)
            return False


# Example Usage (remains the same, but output HTML will be richer):
if __name__ == '__main__':
    logger.info("Starting email generator test...")


    class MockStock:
        def __init__(self, ticker, company_name):
            self.ticker = ticker
            self.company_name = company_name


    class MockStockAnalysis:
        def __init__(self, stock):
            self.stock = stock
            self.analysis_date = datetime.now()
            self.investment_decision = "Hold"
            self.strategy_type = "Value"
            self.reasoning = "The stock appears **fairly valued** with *moderate* growth prospects. AI suggests `monitoring`."
            self.pe_ratio = 15.5;
            self.pb_ratio = 1.2;
            self.eps = 2.5;
            self.roe = 0.16
            self.dividend_yield = 0.025;
            self.debt_to_equity = 0.5;
            self.interest_coverage_ratio = 5
            self.current_ratio = 2.0;
            self.net_profit_margin = 0.10;
            self.revenue_growth = "5.00%"
            self.retained_earnings_trend = "Growing";
            self.free_cash_flow_trend = "Stable"
            self.key_metrics_snapshot = {"price": 100, "marketCap": 1000000000}
            self.economic_moat_summary = "Moderate moat due to **brand recognition** but *increasing competition*."
            self.industry_trends_summary = "Industry is mature with `slow growth`. Shift towards **new tech** is a key trend."
            self.management_assessment_summary = "Management team seems **stable**. Need to check *insider activity*."
            self.qualitative_sources = {"description_snippet": "A leading company in its sector..."}


    mock_sa = MockStockAnalysis(MockStock("MOCK", "MockCorp"))

    email_gen = EmailGenerator()
    email_message = email_gen.create_summary_email(stock_analyses=[mock_sa])

    if email_message:
        logger.info("Email message created. To actually send, uncomment send_email and configure SMTP in config.py")

        with open("test_email_summary_markdown_rendered.html", "w", encoding="utf-8") as f:
            f.write(email_message.get_payload(0).get_payload(decode=True).decode())
        logger.info("Test email HTML saved to test_email_summary_markdown_rendered.html")
    else:
        logger.error("Failed to create email message.")