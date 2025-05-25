import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
import json
import math
from markdown2 import Markdown

from core.config import (
    EMAIL_HOST, EMAIL_PORT, EMAIL_USE_TLS, EMAIL_HOST_USER,
    EMAIL_HOST_PASSWORD, EMAIL_SENDER, EMAIL_RECIPIENT
)
from core.logging_setup import logger
from database.models import StockAnalysis, IPOAnalysis, NewsEventAnalysis


class EmailService:
    def __init__(self):
        self.markdowner = Markdown(extras=["tables", "fenced-code-blocks", "break-on-newline"])

    def _md_to_html(self, md_text):
        if md_text is None: return "<p>N/A</p>"
        if isinstance(md_text, (dict, list)):
            return f"<pre>{json.dumps(md_text, indent=2)}</pre>"
        if not isinstance(md_text, str): md_text = str(md_text)
        if "<" in md_text and ">" in md_text and ("<p>" in md_text.lower() or "<div>" in md_text.lower()):
            return md_text
        return self.markdowner.convert(md_text)

    def _format_stock_analysis_html(self, analysis: StockAnalysis):
        if not analysis: return ""
        stock = analysis.stock

        def fmt_num(val, type="decimal", na_val="N/A"):
            if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))): return na_val
            if type == "percent": return f"{val * 100:.2f}%"
            if type == "decimal": return f"{val:.2f}"
            return str(val)

        business_summary_html = self._md_to_html(analysis.business_summary)
        economic_moat_html = self._md_to_html(analysis.economic_moat_summary)
        industry_trends_html = self._md_to_html(analysis.industry_trends_summary)
        competitive_landscape_html = self._md_to_html(analysis.competitive_landscape_summary)
        management_assessment_html = self._md_to_html(analysis.management_assessment_summary)
        risk_factors_html = self._md_to_html(analysis.risk_factors_summary)
        investment_thesis_html = self._md_to_html(analysis.investment_thesis_full)
        reasoning_points_html = self._md_to_html(analysis.reasoning)

        dcf_assumptions_html = "<ul>"
        if analysis.dcf_assumptions and isinstance(analysis.dcf_assumptions, dict):
            assumptions_data = analysis.dcf_assumptions
            dcf_assumptions_html += f"<li>Discount Rate: {fmt_num(assumptions_data.get('discount_rate'), 'percent')}</li>"
            dcf_assumptions_html += f"<li>Perpetual Growth Rate: {fmt_num(assumptions_data.get('perpetual_growth_rate'), 'percent')}</li>"
            dcf_assumptions_html += f"<li>FCF Projection Years: {assumptions_data.get('projection_years', 'N/A')}</li>"
            dcf_assumptions_html += f"<li>Starting FCF: {fmt_num(assumptions_data.get('start_fcf'))}</li>"
            fcf_growth_proj = assumptions_data.get('fcf_growth_rates_projection')
            if fcf_growth_proj and isinstance(fcf_growth_proj, list):
                 dcf_assumptions_html += f"<li>Projected FCF Growth Rates: {', '.join([fmt_num(r, 'percent') for r in fcf_growth_proj])}</li>"
        else:
            dcf_assumptions_html += "<li>N/A</li>"
        dcf_assumptions_html += "</ul>"

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
                <h4>Full Thesis:</h4><div class="markdown-content">{investment_thesis_html}</div>
                <h4>Key Reasoning Points:</h4><div class="markdown-content">{reasoning_points_html}</div>
            </details>
            <details>
                <summary><strong>Key Financial Metrics (Click to expand)</strong></summary>
                <ul>
                    <li>P/E Ratio: {fmt_num(analysis.pe_ratio)}</li><li>P/B Ratio: {fmt_num(analysis.pb_ratio)}</li>
                    <li>P/S Ratio: {fmt_num(analysis.ps_ratio)}</li><li>EV/Sales: {fmt_num(analysis.ev_to_sales)}</li>
                    <li>EV/EBITDA: {fmt_num(analysis.ev_to_ebitda)}</li><li>EPS: {fmt_num(analysis.eps)}</li>
                    <li>ROE: {fmt_num(analysis.roe, 'percent')}</li><li>ROA: {fmt_num(analysis.roa, 'percent')}</li>
                    <li>ROIC: {fmt_num(analysis.roic, 'percent')}</li><li>Dividend Yield: {fmt_num(analysis.dividend_yield, 'percent')}</li>
                    <li>Debt-to-Equity: {fmt_num(analysis.debt_to_equity)}</li><li>Debt-to-EBITDA: {fmt_num(analysis.debt_to_ebitda)}</li>
                    <li>Interest Coverage: {fmt_num(analysis.interest_coverage_ratio)}x</li><li>Current Ratio: {fmt_num(analysis.current_ratio)}</li>
                    <li>Quick Ratio: {fmt_num(analysis.quick_ratio)}</li>
                    <li>Gross Profit Margin: {fmt_num(analysis.gross_profit_margin, 'percent')}</li>
                    <li>Operating Profit Margin: {fmt_num(analysis.operating_profit_margin, 'percent')}</li>
                    <li>Net Profit Margin: {fmt_num(analysis.net_profit_margin, 'percent')}</li>
                    <li>Revenue Growth YoY: {fmt_num(analysis.revenue_growth_yoy, 'percent')} (QoQ: {fmt_num(analysis.revenue_growth_qoq, 'percent')})</li>
                    <li>Revenue Growth CAGR (3yr/5yr): {fmt_num(analysis.revenue_growth_cagr_3yr, 'percent')} / {fmt_num(analysis.revenue_growth_cagr_5yr, 'percent')}</li>
                    <li>EPS Growth YoY: {fmt_num(analysis.eps_growth_yoy, 'percent')}</li>
                    <li>EPS Growth CAGR (3yr/5yr): {fmt_num(analysis.eps_growth_cagr_3yr, 'percent')} / {fmt_num(analysis.eps_growth_cagr_5yr, 'percent')}</li>
                    <li>FCF per Share: {fmt_num(analysis.free_cash_flow_per_share)}</li><li>FCF Yield: {fmt_num(analysis.free_cash_flow_yield, 'percent')}</li>
                    <li>FCF Trend: {analysis.free_cash_flow_trend or 'N/A'}</li><li>Retained Earnings Trend: {analysis.retained_earnings_trend or 'N/A'}</li>
                </ul>
            </details>
            <details>
                <summary><strong>DCF Analysis (Simplified) (Click to expand)</strong></summary>
                <ul>
                    <li>Intrinsic Value per Share: {fmt_num(analysis.dcf_intrinsic_value)}</li>
                    <li>Upside/Downside: {fmt_num(analysis.dcf_upside_percentage, 'percent')}</li>
                </ul>
                <p><em>Key Assumptions Used:</em></p>
                {dcf_assumptions_html}
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
                <p><em>Key Metrics Data Points Used:</em></p><div class="markdown-content">{self._md_to_html(analysis.key_metrics_snapshot)}</div>
                <p><em>Qualitative Analysis Sources Summary:</em></p><div class="markdown-content">{self._md_to_html(analysis.qualitative_sources_summary)}</div>
            </details>
        </div>
        """
        return html

    def _format_ipo_analysis_html(self, analysis: IPOAnalysis):
        if not analysis: return ""
        ipo = analysis.ipo
        def fmt_price(val_low, val_high, currency="USD"):
            if val_low is None and val_high is None: return "N/A"
            if val_low is not None and val_high is not None:
                if val_low == val_high: return f"{val_low:.2f} {currency}"
                return f"{val_low:.2f} - {val_high:.2f} {currency}"
            if val_low is not None: return f"{val_low:.2f} {currency}"
            if val_high is not None: return f"{val_high:.2f} {currency}"
            return "N/A"
        reasoning_html = self._md_to_html(analysis.reasoning)
        s1_business_summary_html = self._md_to_html(analysis.s1_business_summary or analysis.business_model_summary)
        s1_risk_factors_summary_html = self._md_to_html(analysis.s1_risk_factors_summary or analysis.risk_factors_summary)
        s1_mda_summary_html = self._md_to_html(analysis.s1_mda_summary)
        s1_financial_health_summary_html = self._md_to_html(analysis.s1_financial_health_summary or analysis.pre_ipo_financials_summary)
        competitive_landscape_html = self._md_to_html(analysis.competitive_landscape_summary)
        industry_outlook_html = self._md_to_html(analysis.industry_outlook_summary)
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
            <details><summary><strong>AI Synthesized Reasoning & Critical Verification Points (Click to expand)</strong></summary><div class="markdown-content">{reasoning_html}</div></details>
            <details>
                <summary><strong>S-1 Based Summaries (if available) & AI Analysis (Click to expand)</strong></summary>
                <p><strong>Business Summary (S-1/inferred):</strong></p><div class="markdown-content">{s1_business_summary_html}</div>
                <p><strong>Competitive Landscape:</strong></p><div class="markdown-content">{competitive_landscape_html}</div>
                <p><strong>Industry Outlook:</strong></p><div class="markdown-content">{industry_outlook_html}</div>
                <p><strong>Risk Factors Summary (S-1/inferred):</strong></p><div class="markdown-content">{s1_risk_factors_summary_html}</div>
                <p><strong>Use of Proceeds (S-1/inferred):</strong></p><div class="markdown-content">{use_of_proceeds_html}</div>
                <p><strong>MD&A / Financial Health Summary (S-1/inferred):</strong></p><div class="markdown-content">{s1_mda_summary_html if s1_mda_summary_html and not s1_mda_summary_html.startswith("Section not found") else s1_financial_health_summary_html}</div>
                <p><strong>Management Team Assessment:</strong></p><div class="markdown-content">{management_team_html}</div>
                <p><strong>Underwriter Quality Assessment:</strong></p><div class="markdown-content">{underwriter_html}</div>
                <p><strong>Valuation Comparison Guidance:</strong></p><div class="markdown-content">{valuation_html}</div>
            </details>
            <details><summary><strong>Supporting Data (Click to expand)</strong></summary>
                <p><em>Raw IPO calendar API data:</em></p><div class="markdown-content">{self._md_to_html(analysis.key_data_snapshot)}</div>
                <p><em>S-1 Sections Used (True if found & used):</em></p><div class="markdown-content">{self._md_to_html(analysis.s1_sections_used)}</div>
            </details>
        </div>"""
        return html

    def _format_news_event_analysis_html(self, analysis: NewsEventAnalysis):
        if not analysis: return ""
        news_event = analysis.news_event
        sentiment_html = self._md_to_html(f"**Sentiment:** {analysis.sentiment or 'N/A'}\n**Reasoning:** {analysis.sentiment_reasoning or 'N/A'}")
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
            <p><strong>Investor Summary:</strong></p><div class="markdown-content">{investor_summary_html}</div>
            <details><summary><strong>Detailed AI Analysis (Click to expand)</strong></summary>
                <p><strong>Sentiment Analysis:</strong></p><div class="markdown-content">{sentiment_html}</div>
                <p><strong>Detailed News Summary:</strong></p><div class="markdown-content">{news_summary_detailed_html}</div>
                <p><strong>Potentially Affected Companies/Stocks:</strong></p><div class="markdown-content">{impact_companies_html}</div>
                <p><strong>Potentially Affected Sectors:</strong></p><div class="markdown-content">{impact_sectors_html}</div>
                <p><strong>Mechanism of Impact:</strong></p><div class="markdown-content">{mechanism_html}</div>
                <p><strong>Estimated Timing & Duration:</strong></p><div class="markdown-content">{timing_duration_html}</div>
                <p><strong>Estimated Magnitude & Direction:</strong></p><div class="markdown-content">{magnitude_direction_html}</div>
                <p><strong>Confidence of Assessment:</strong></p><div class="markdown-content">{confidence_html}</div>
            </details>
            <details><summary><strong>Key Snippets Used for Analysis (Click to expand)</strong></summary><div class="markdown-content">{self._md_to_html(analysis.key_news_snippets)}</div></details>
        </div>"""
        return html

    def create_summary_email(self, stock_analyses=None, ipo_analyses=None, news_analyses=None):
        if not any([stock_analyses, ipo_analyses, news_analyses]):
            logger.info("No analyses provided to create an email."); return None
        subject_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        subject = f"Financial Analysis Summary - {subject_date}"
        html_body = f"""
        <html><head><style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; line-height: 1.6; color: #333; }}
            .container {{ background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 0 15px rgba(0,0,0,0.1); max-width: 900px; margin: auto; }}
            .analysis-block {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 25px; border-radius: 5px; background-color: #fdfdfd; box-shadow: 0 2px 4px rgba(0,0,0,0.05);}}
            .stock-analysis {{ border-left: 5px solid #4CAF50; }} .ipo-analysis {{ border-left: 5px solid #2196F3; }} .news-analysis {{ border-left: 5px solid #FFC107; }}
            h1 {{ color: #2c3e50; text-align: center; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
            h2 {{ color: #34495e; border-bottom: 1px solid #eee; padding-bottom: 5px; margin-top: 0; }}
            h4 {{ color: #555; margin-top: 15px; margin-bottom: 5px; }}
            details > summary {{ cursor: pointer; font-weight: bold; margin-bottom: 10px; color: #2980b9; padding: 5px; background-color: #ecf0f1; border-radius:3px; }}
            details[open] > summary {{ background-color: #dde5e8; }}
            pre {{ background-color: #eee; padding: 10px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; word-wrap: break-word; font-size: 0.85em; border: 1px solid #ccc; }}
            ul {{ list-style-type: disc; margin-left: 20px; padding-left: 5px; }} li {{ margin-bottom: 8px; }}
            .markdown-content p {{ margin: 0.5em 0; }} .markdown-content ul, .markdown-content ol {{ margin-left: 20px; }}
            .markdown-content table {{ border-collapse: collapse; width: 100%; margin-bottom: 1em;}}
            .markdown-content th, .markdown-content td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }} .markdown-content th {{ background-color: #f2f2f2; }}
            .report-footer {{ text-align: center; font-size: 0.9em; color: #777; margin-top: 30px; }}
        </style></head><body><div class="container">
            <h1>Financial Analysis Report</h1>
            <p style="text-align:center; font-style:italic; color:#555;">Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}</p>
            <p style="text-align:center; font-style:italic; color:#7f8c8d;"><em>This email contains automated analysis. Always do your own research before making investment decisions.</em></p>"""
        if stock_analyses: html_body += "<h2>Individual Stock Analyses</h2>"; [html_body := html_body + self._format_stock_analysis_html(sa) for sa in stock_analyses]
        if ipo_analyses: html_body += "<h2>Upcoming IPO Analyses</h2>"; [html_body := html_body + self._format_ipo_analysis_html(ia) for ia in ipo_analyses]
        if news_analyses: html_body += "<h2>Recent News & Event Analyses</h2>"; [html_body := html_body + self._format_news_event_analysis_html(na) for na in news_analyses]
        html_body += """<div class="report-footer"><p>© Automated Financial Analysis System</p></div></div></body></html>"""
        msg = MIMEMultipart('alternative'); msg['Subject'], msg['From'], msg['To'] = subject, EMAIL_SENDER, EMAIL_RECIPIENT
        msg.attach(MIMEText(html_body, 'html', 'utf-8')); return msg

    def send_email(self, message: MIMEMultipart):
        if not message: logger.error("No message object provided to send_email."); return False
        try:
            smtp_server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=20)
            if EMAIL_USE_TLS: smtp_server.starttls()
            smtp_server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
            smtp_server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, message.as_string())
            smtp_server.quit(); logger.info(f"Email sent successfully to {EMAIL_RECIPIENT}"); return True
        except smtplib.SMTPException as e_smtp: logger.error(f"SMTP error sending email: {e_smtp}", exc_info=True); return False
        except Exception as e: logger.error(f"General error sending email: {e}", exc_info=True); return False

if __name__ == '__main__':
    logger.info("Starting email service test...")
    class MockStock: __init__ = lambda self, ticker, company_name, industry="Tech", sector="Software": setattr(self, 'ticker', ticker) or setattr(self, 'company_name', company_name) or setattr(self, 'industry', industry) or setattr(self, 'sector', sector)
    class MockIPO: __init__ = lambda self, company_name, symbol, ipo_date_str="2025-07-15": setattr(self, 'company_name', company_name) or setattr(self, 'symbol', symbol) or setattr(self, 'ipo_date_str', ipo_date_str) or setattr(self, 'ipo_date', datetime.strptime(ipo_date_str, "%Y-%m-%d").date() if ipo_date_str else None) or setattr(self, 'expected_price_range_low', 18.00) or setattr(self, 'expected_price_range_high', 22.00) or setattr(self, 'expected_price_currency', "USD") or setattr(self, 'exchange', "NASDAQ") or setattr(self, 'status', "Filed") or setattr(self, 's1_filing_url', "http://example.com/s1")
    class MockNewsEvent: __init__ = lambda self, title, url, event_date_str="2025-05-25 10:00:00": setattr(self, 'event_title', title) or setattr(self, 'source_url', url) or setattr(self, 'source_name', "Mock News") or setattr(self, 'event_date', datetime.strptime(event_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)) or setattr(self, 'full_article_text', "Full article text...")
    class MockStockAnalysis:
        def __init__(self, stock): self.stock, self.analysis_date, self.investment_decision, self.strategy_type, self.confidence_level, self.investment_thesis_full, self.reasoning = stock, datetime.now(timezone.utc), "Buy", "GARP", "Medium", "Thesis...", "Reasons..."
        self.dcf_assumptions = {"discount_rate": 0.095, "perpetual_growth_rate": 0.025, "projection_years": 5, "start_fcf": 1.2e9, "fcf_growth_rates_projection": [0.08, 0.07, 0.06, 0.05, 0.04]}
        self.pe_ratio, self.pb_ratio, self.ps_ratio, self.ev_to_sales, self.ev_to_ebitda, self.eps, self.roe, self.roa, self.roic, self.dividend_yield, self.debt_to_equity, self.debt_to_ebitda, self.interest_coverage_ratio, self.current_ratio, self.quick_ratio, self.gross_profit_margin, self.operating_profit_margin, self.net_profit_margin, self.revenue_growth_yoy, self.revenue_growth_qoq, self.revenue_growth_cagr_3yr, self.revenue_growth_cagr_5yr, self.eps_growth_yoy, self.eps_growth_cagr_3yr, self.eps_growth_cagr_5yr, self.free_cash_flow_per_share, self.free_cash_flow_yield, self.free_cash_flow_trend, self.retained_earnings_trend, self.dcf_intrinsic_value, self.dcf_upside_percentage, self.business_summary, self.economic_moat_summary, self.industry_trends_summary, self.competitive_landscape_summary, self.management_assessment_summary, self.risk_factors_summary, self.key_metrics_snapshot, self.qualitative_sources_summary = (18.5, 3.2, 2.5, 2.8, 12.0, 2.50, 0.22, 0.10, 0.15, 0.015, 0.5, 2.1, 8.0, 1.8, 1.2, 0.60, 0.20, 0.12, 0.15, 0.04, 0.12, 0.10, 0.20, 0.18, 0.15, 1.80, 0.05, "Growing", "Growing", 120.50, 0.205, "Biz Sum.", "Moat Sum.", "Ind Sum.", "Comp Sum.", "Mgmt Sum.", "Risk Sum.", {"price": 100}, {"10k_url": "url"})
    class MockIPOAnalysis: __init__ = lambda self, ipo: setattr(self, 'ipo', ipo) or setattr(self, 'analysis_date', datetime.now(timezone.utc))
    class MockNewsEventAnalysis: __init__ = lambda self, news_event: setattr(self, 'news_event', news_event) or setattr(self, 'analysis_date', datetime.now(timezone.utc))
    mock_sa = MockStockAnalysis(MockStock("MOCK", "MockCorp Inc."))
    mock_ipo_a = MockIPOAnalysis(MockIPO("NewIPO Inc.", "NIPO"))
    mock_news_a = MockNewsEventAnalysis(MockNewsEvent("Major Tech Breakthrough", "http://example.com/news1"))
    email_svc = EmailService()
    email_message = email_svc.create_summary_email(stock_analyses=[mock_sa], ipo_analyses=[mock_ipo_a], news_analyses=[mock_news_a])
    if email_message:
        logger.info("Email message created successfully.")
        output_filename = f"test_email_summary_refactored_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(output_filename, "w", encoding="utf-8") as f:
            payload_html = "";
            if email_message.is_multipart():
                for part in email_message.get_payload():
                    if part.get_content_type() == "text/html": payload_html = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8'); break
            else: payload_html = email_message.get_payload(decode=True).decode(email_message.get_content_charset() or 'utf-8')
            if payload_html: f.write(payload_html); logger.info(f"Test email HTML saved to {output_filename}")
            else: logger.error("Could not extract HTML payload.")
        # if email_svc.send_email(email_message): logger.info("Test email sent.") else: logger.error("Failed to send test email.")
    else: logger.error("Failed to create email message.")