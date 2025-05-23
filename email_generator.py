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


class EmailGenerator:
    def __init__(self):
        pass

    def _format_stock_analysis_html(self, analysis: StockAnalysis):
        if not analysis: return ""
        stock = analysis.stock
        html = f"""
        <div class="analysis-block">
            <h2>Stock Analysis: {stock.company_name} ({stock.ticker})</h2>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M')}</p>
            <p><strong>Decision:</strong> {analysis.investment_decision}</p>
            <p><strong>Strategy Type:</strong> {analysis.strategy_type}</p>

            <details>
                <summary><strong>Reasoning & AI Synthesis (Click to expand)</strong></summary>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.reasoning}</pre>
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
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.economic_moat_summary}</pre>
                <p><strong>Industry Trends:</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.industry_trends_summary}</pre>
                <p><strong>Management Assessment (General Factors):</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.management_assessment_summary}</pre>
                <p><em>Context for qualitative prompts:</em></p>
                <pre>{json.dumps(analysis.qualitative_sources, indent=2) if analysis.qualitative_sources else 'N/A'}</pre>
            </details>
        </div>
        """
        return html

    def _format_ipo_analysis_html(self, analysis: IPOAnalysis):
        if not analysis: return ""
        ipo = analysis.ipo
        html = f"""
        <div class="analysis-block">
            <h2>IPO Analysis: {ipo.company_name}</h2>
            <p><strong>Expected IPO Date:</strong> {ipo.ipo_date}</p>
            <p><strong>Expected Price Range:</strong> {ipo.expected_price_range}</p>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M')}</p>
            <p><strong>Preliminary Stance:</strong> {analysis.investment_decision}</p>

            <details>
                <summary><strong>AI Synthesized Reasoning & Key Verification Points (Click to expand)</strong></summary>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.reasoning}</pre>
            </details>

            <details>
                <summary><strong>IPO Details & Summaries (Click to expand)</strong></summary>
                <p><strong>Business Model:</strong> {analysis.business_model_summary}</p>
                <p><strong>Competitive Landscape:</strong> {analysis.competitive_landscape_summary}</p>
                <p><strong>Industry Health:</strong> {analysis.industry_health_summary}</p>
                <p><strong>Use of Proceeds (General):</strong> {analysis.use_of_proceeds_summary}</p>
                <p><strong>Risk Factors (General):</strong> {analysis.risk_factors_summary}</p>
                <p><strong>Pre-IPO Financials (Guidance):</strong> {analysis.pre_ipo_financials_summary}</p>
                <p><strong>Valuation Comparison (Guidance):</strong> {analysis.valuation_comparison_summary}</p>
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
        html = f"""
        <div class="analysis-block">
            <h2>News/Event Analysis: {news_event.event_title}</h2>
            <p><strong>Event Date:</strong> {news_event.event_date.strftime('%Y-%m-%d %H:%M') if news_event.event_date else 'N/A'}</p>
            <p><strong>Source:</strong> <a href="{news_event.source_url}">{news_event.source_url}</a></p>
            <p><strong>Analysis Date:</strong> {analysis.analysis_date.strftime('%Y-%m-%d %H:%M')}</p>
            <p><strong>Investor Summary:</strong> {analysis.summary_for_email}</p>

            <details>
                <summary><strong>Detailed Analysis (Click to expand)</strong></summary>
                <p><strong>Scope & Relevance:</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.scope_relevance}</pre>
                <p><strong>Affected Stocks/Sectors (AI Analysis & API):</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">API Related: {analysis.affected_stocks_sectors.get('api_related', 'N/A') if analysis.affected_stocks_sectors else 'N/A'}\nAI Analysis: {analysis.affected_stocks_sectors.get('text_analysis', 'N/A') if analysis.affected_stocks_sectors else 'N/A'}</pre>
                <p><strong>Mechanism of Impact:</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.mechanism_of_impact}</pre>
                <p><strong>Estimated Timing & Duration:</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.estimated_timing}</pre>
                <p><strong>Estimated Magnitude & Direction:</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.estimated_magnitude_direction}</pre>
                <p><strong>Countervailing Factors:</strong></p>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{analysis.countervailing_factors}</pre>
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

        html_body = """
        <html>
            <head>
                <style>
                    body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }
                    .container { background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
                    .analysis-block { border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; border-radius: 5px; background-color: #f9f9f9; }
                    h1 { color: #333; }
                    h2 { color: #555; border-bottom: 1px solid #eee; padding-bottom: 5px;}
                    details > summary { cursor: pointer; font-weight: bold; margin-bottom: 5px; }
                    pre { background-color: #eee; padding: 10px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; word-wrap: break-word; }
                    ul { list-style-type: disc; margin-left: 20px; }
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


# Example Usage:
if __name__ == '__main__':
    # This part is for testing and won't run when imported.
    # It requires mock data or fetching real data from DB.
    logger.info("Starting email generator test...")


    # Mock data (replace with actual data fetched from DB for a real test)
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
            self.reasoning = "The stock appears fairly valued with moderate growth prospects. AI suggests monitoring."
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
            self.economic_moat_summary = "Moderate moat due to brand recognition but increasing competition."
            self.industry_trends_summary = "Industry is mature with slow growth. Shift towards new tech is a key trend."
            self.management_assessment_summary = "Management team seems stable. Need to check insider activity."
            self.qualitative_sources = {"description_snippet": "A leading company in its sector..."}


    mock_sa = MockStockAnalysis(MockStock("MOCK", "MockCorp"))

    email_gen = EmailGenerator()
    email_message = email_gen.create_summary_email(stock_analyses=[mock_sa])

    if email_message:
        logger.info("Email message created. To actually send, uncomment send_email and configure SMTP in config.py")
        # success = email_gen.send_email(email_message)
        # logger.info(f"Test email send status: {success}")

        # For testing, save HTML to a file:
        with open("test_email_summary.html", "w", encoding="utf-8") as f:
            f.write(email_message.get_payload(0).get_payload(decode=True).decode())  # type: ignore
        logger.info("Test email HTML saved to test_email_summary.html")
    else:
        logger.error("Failed to create email message.")