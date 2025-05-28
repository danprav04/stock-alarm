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
        self.markdowner = Markdown(extras=["tables", "fenced-code-blocks", "break-on-newline", "smarty-pants"])

    def _md_to_html(self, md_text):
        if md_text is None: return "<p>N/A</p>"
        if isinstance(md_text, (dict, list)):
            # Pretty print JSON, then wrap in pre for email
            try:
                pretty_json = json.dumps(md_text, indent=2, ensure_ascii=False)
                # Basic HTML escaping for JSON string to be safe inside <pre>
                escaped_json = pretty_json.replace("&", "&").replace("<", "<").replace(">", ">")
                return f"<pre>{escaped_json}</pre>"
            except Exception:
                return f"<pre>{str(md_text)}</pre>" # Fallback for non-serializable
        if not isinstance(md_text, str): md_text = str(md_text)
        # Avoid re-processing if it looks like HTML already
        if "<" in md_text and ">" in md_text and ("<p>" in md_text.lower() or "<div>" in md_text.lower() or "<ul>" in md_text.lower()):
            return md_text
        return self.markdowner.convert(md_text)

    def _format_stock_analysis_html(self, analysis: StockAnalysis):
        if not analysis: return ""
        stock = analysis.stock

        def fmt_num(val, type="decimal", na_val="N/A"):
            if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))): return na_val
            if type == "percent": return f"{val * 100:.2f}%"
            if type == "decimal": return f"{val:.2f}"
            if type == "currency": return f"${val:,.2f}" if isinstance(val, (int,float)) else str(val)
            if type == "large_number": return f"{val:,.0f}" if isinstance(val, (int,float)) else str(val)
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
            dcf_assumptions_html += f"<li>Starting FCF ({assumptions_data.get('start_fcf_basis','N/A')}): {fmt_num(assumptions_data.get('start_fcf'), 'large_number')}</li>"
            dcf_assumptions_html += f"<li>Initial FCF Growth ({assumptions_data.get('initial_fcf_growth_rate_basis','N/A')}): {fmt_num(assumptions_data.get('initial_fcf_growth_rate_used'), 'percent')}</li>"

            fcf_growth_proj = assumptions_data.get('fcf_growth_rates_projection')
            if fcf_growth_proj and isinstance(fcf_growth_proj, list):
                 dcf_assumptions_html += f"<li>Projected FCF Growth Rates (annual): {', '.join([fmt_num(r, 'percent') for r in fcf_growth_proj])}</li>"

            sensitivity_analysis = assumptions_data.get("sensitivity_analysis")
            if sensitivity_analysis and isinstance(sensitivity_analysis, list):
                dcf_assumptions_html += "<li>Sensitivity Analysis Highlights:<ul>"
                for sens_item in sensitivity_analysis[:3]: # Show a few
                    dcf_assumptions_html += f"<li>{sens_item.get('scenario', 'N/A')}: IV {fmt_num(sens_item.get('intrinsic_value'), 'currency')} (Upside: {fmt_num(sens_item.get('upside'), 'percent')})</li>"
                if len(sensitivity_analysis) > 3:
                    dcf_assumptions_html += "<li>... more available in full data.</li>"
                dcf_assumptions_html += "</ul></li>"
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
                    <li>EV/EBITDA: {fmt_num(analysis.ev_to_ebitda)}</li><li>EPS: {fmt_num(analysis.eps, 'currency')}</li>
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
                    <li>FCF per Share: {fmt_num(analysis.free_cash_flow_per_share, 'currency')}</li><li>FCF Yield: {fmt_num(analysis.free_cash_flow_yield, 'percent')}</li>
                    <li>FCF Trend: {analysis.free_cash_flow_trend or 'N/A'}</li><li>Retained Earnings Trend: {analysis.retained_earnings_trend or 'N/A'}</li>
                </ul>
            </details>
            <details>
                <summary><strong>DCF Analysis (Simplified) (Click to expand)</strong></summary>
                <ul>
                    <li>Intrinsic Value per Share: {fmt_num(analysis.dcf_intrinsic_value, 'currency')}</li>
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
                <p><em>Key Metrics Data Points Used (Snapshot):</em></p><div class="markdown-content">{self._md_to_html(analysis.key_metrics_snapshot)}</div>
                <p><em>Qualitative Analysis Sources Summary (e.g., 10-K URL):</em></p><div class="markdown-content">{self._md_to_html(analysis.qualitative_sources_summary)}</div>
            </details>
        </div>
        """
        return html

    def _format_ipo_analysis_html(self, analysis: IPOAnalysis):
        if not analysis: return ""
        ipo = analysis.ipo
        def fmt_price(val_low, val_high, currency="USD"):
            if val_low is None and val_high is None: return "N/A"
            try:
                if val_low is not None and val_high is not None:
                    if float(val_low) == float(val_high): return f"{float(val_low):.2f} {currency}"
                    return f"{float(val_low):.2f} - {float(val_high):.2f} {currency}"
                if val_low is not None: return f"{float(val_low):.2f} {currency}"
                if val_high is not None: return f"{float(val_high):.2f} {currency}"
            except (ValueError, TypeError):
                return f"{val_low} - {val_high} {currency}" # Fallback for non-numeric
            return "N/A"

        reasoning_html = self._md_to_html(analysis.reasoning)
        # Prefer more specific summaries if available
        business_summary_html = self._md_to_html(analysis.business_model_summary or analysis.s1_business_summary)
        risk_factors_summary_html = self._md_to_html(analysis.risk_factors_summary or analysis.s1_risk_factors_summary)
        financial_health_summary_html = self._md_to_html(analysis.pre_ipo_financials_summary or analysis.s1_financial_health_summary or analysis.s1_mda_summary)

        competitive_landscape_html = self._md_to_html(analysis.competitive_landscape_summary)
        industry_outlook_html = self._md_to_html(analysis.industry_outlook_summary)
        use_of_proceeds_html = self._md_to_html(analysis.use_of_proceeds_summary)
        management_team_html = self._md_to_html(analysis.management_team_assessment)
        underwriter_html = self._md_to_html(analysis.underwriter_quality_assessment)
        valuation_html = self._md_to_html(analysis.valuation_comparison_summary)

        s1_disclaimer = ""
        if analysis.s1_sections_used and isinstance(analysis.s1_sections_used, dict):
            s1_found_and_used = any(analysis.s1_sections_used.values())
            if not s1_found_and_used:
                s1_disclaimer = """
                <p style="color:orange; border: 1px solid orange; padding: 5px; margin-bottom:10px;">
                <strong>Important Note:</strong> The S-1 filing (prospectus) for this IPO could not be retrieved or its key sections could not be automatically processed.
                The following summaries are based on general knowledge of this type of IPO (e.g., SPAC characteristics if applicable) and publicly available IPO calendar data.
                A thorough review of the actual S-1 filing is critical before making any investment decisions.
                </p>
                """
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
            {s1_disclaimer}
            <details>
                <summary><strong>Summaries (S-1 Inferred/AI Synthesized) & AI Analysis (Click to expand)</strong></summary>
                <p><strong>Business Summary (Inferred/AI Synthesized):</strong></p><div class="markdown-content">{business_summary_html}</div>
                <p><strong>Competitive Landscape (Inferred/AI Synthesized):</strong></p><div class="markdown-content">{competitive_landscape_html}</div>
                <p><strong>Industry Outlook (Inferred/AI Synthesized):</strong></p><div class="markdown-content">{industry_outlook_html}</div>
                <p><strong>Risk Factors Summary (Inferred/AI Synthesized):</strong></p><div class="markdown-content">{risk_factors_summary_html}</div>
                <p><strong>Use of Proceeds (Inferred/AI Synthesized):</strong></p><div class="markdown-content">{use_of_proceeds_html}</div>
                <p><strong>MD&A / Financial Health Summary (Inferred/AI Synthesized):</strong></p><div class="markdown-content">{financial_health_summary_html}</div>
                <p><strong>Management Team Assessment (AI Synthesized):</strong></p><div class="markdown-content">{management_team_html}</div>
                <p><strong>Underwriter Quality Assessment (AI Synthesized):</strong></p><div class="markdown-content">{underwriter_html}</div>
                <p><strong>Valuation Comparison Guidance (AI Synthesized):</strong></p><div class="markdown-content">{valuation_html}</div>
            </details>
            <details><summary><strong>Supporting Data (Click to expand)</strong></summary>
                <p><em>Raw IPO calendar API data:</em></p><div class="markdown-content">{self._md_to_html(analysis.key_data_snapshot)}</div>
                <p><em>S-1 Sections Used (True if found & used by automated system):</em></p><div class="markdown-content">{self._md_to_html(analysis.s1_sections_used)}</div>
            </details>
        </div>"""
        return html

    def _format_news_event_analysis_html(self, analysis: NewsEventAnalysis):
        if not analysis: return ""
        news_event = analysis.news_event
        sentiment_html = self._md_to_html(f"**Sentiment:** {analysis.sentiment or 'N/A'}\n**Reasoning:** {analysis.sentiment_reasoning or 'N/A'}")
        news_summary_detailed_html = self._md_to_html(analysis.news_summary_detailed)
        impact_companies_html = self._md_to_html(analysis.potential_impact_on_companies) # Assumed to be JSON string, will be pre-formatted
        impact_sectors_html = self._md_to_html(analysis.potential_impact_on_sectors) # Assumed to be JSON string
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
        # Enhanced CSS for better readability
        html_body = f"""
        <html><head><style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"; margin: 0; padding: 20px; background-color: #f0f2f5; line-height: 1.65; color: #333; }}
            .container {{ background-color: #ffffff; padding: 25px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); max-width: 950px; margin: 20px auto; }}
            .analysis-block {{ border: 1px solid #e0e0e0; padding: 20px; margin-bottom: 30px; border-radius: 8px; background-color: #fcfcfc; box-shadow: 0 2px 5px rgba(0,0,0,0.07);}}
            .stock-analysis {{ border-left: 6px solid #2ecc71; }} /* Green */
            .ipo-analysis {{ border-left: 6px solid #3498db; }}   /* Blue */
            .news-analysis {{ border-left: 6px solid #f39c12; }} /* Orange */
            h1 {{ color: #2c3e50; text-align: center; border-bottom: 3px solid #3498db; padding-bottom: 15px; margin-bottom:20px; font-size:2em; }}
            h2 {{ color: #2980b9; border-bottom: 2px solid #eaeff2; padding-bottom: 8px; margin-top: 10px; font-size:1.6em; }}
            h4 {{ color: #34495e; margin-top: 18px; margin-bottom: 8px; font-size:1.1em; }}
            details > summary {{ cursor: pointer; font-weight: bold; margin-bottom: 12px; color: #2c3e50; padding: 10px 15px; background-color: #eaf1f4; border-radius:5px; transition: background-color 0.2s ease; }}
            details > summary:hover {{ background-color: #dce7ec; }}
            details[open] > summary {{ background-color: #d1dde2; }}
            pre {{ background-color: #f5f7fa; padding: 12px; border-radius: 5px; font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace; white-space: pre-wrap; word-wrap: break-word; font-size: 0.9em; border: 1px solid #d1d5da; overflow-x: auto; }}
            ul {{ list-style-type: disc; margin-left: 25px; padding-left: 5px; }} li {{ margin-bottom: 10px; }}
            .markdown-content p {{ margin: 0.7em 0; }} .markdown-content ul, .markdown-content ol {{ margin-left: 20px; }}
            .markdown-content table {{ border-collapse: collapse; width: 100%; margin-bottom: 1em; font-size:0.95em;}}
            .markdown-content th, .markdown-content td {{ border: 1px solid #d1d5da; padding: 10px; text-align: left; }} .markdown-content th {{ background-color: #f1f5f8; font-weight:bold; }}
            .report-footer {{ text-align: center; font-size: 0.85em; color: #888; margin-top: 35px; padding-top:15px; border-top:1px solid #eee; }}
            a {{ color: #3498db; text-decoration:none; }} a:hover {{ text-decoration:underline; }}
        </style></head><body><div class="container">
            <h1>Financial Analysis Report</h1>
            <p style="text-align:center; font-style:italic; color:#555;">Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}</p>
            <p style="text-align:center; font-style:italic; color:#7f8c8d; margin-bottom:25px;"><em>This email contains automated analysis. Always do your own research before making investment decisions.</em></p>"""
        if stock_analyses: html_body += "<h2>Individual Stock Analyses</h2>"; [html_body := html_body + self._format_stock_analysis_html(sa) for sa in stock_analyses]
        if ipo_analyses: html_body += "<h2>Upcoming IPO Analyses</h2>"; [html_body := html_body + self._format_ipo_analysis_html(ia) for ia in ipo_analyses]
        if news_analyses: html_body += "<h2>Recent News & Event Analyses</h2>"; [html_body := html_body + self._format_news_event_analysis_html(na) for na in news_analyses]
        html_body += """<div class="report-footer"><p>(c) Automated Financial Analysis System</p></div></div></body></html>""" # Corrected typo in (c)
        msg = MIMEMultipart('alternative'); msg['Subject'], msg['From'], msg['To'] = subject, EMAIL_SENDER, EMAIL_RECIPIENT
        msg.attach(MIMEText(html_body, 'html', 'utf-8')); return msg

    def send_email(self, message: MIMEMultipart):
        if not message: logger.error("No message object provided to send_email."); return False
        try:
            smtp_server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=30) # Increased timeout
            if EMAIL_USE_TLS: smtp_server.starttls()
            smtp_server.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
            smtp_server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENT, message.as_string())
            smtp_server.quit(); logger.info(f"Email sent successfully to {EMAIL_RECIPIENT}"); return True
        except smtplib.SMTPException as e_smtp: logger.error(f"SMTP error sending email: {e_smtp}", exc_info=True); return False
        except Exception as e: logger.error(f"General error sending email: {e}", exc_info=True); return False

if __name__ == '__main__':
    logger.info("Starting email service test...")
    class MockStock: __init__ = lambda self, ticker, company_name, industry="Tech", sector="Software": setattr(self, 'ticker', ticker) or setattr(self, 'company_name', company_name) or setattr(self, 'industry', industry) or setattr(self, 'sector', sector)
    class MockIPO:
        def __init__(self, company_name, symbol, ipo_date_str="2025-07-15", s1_url="http://example.com/s1"):
            self.company_name = company_name; self.symbol = symbol; self.ipo_date_str = ipo_date_str;
            self.ipo_date = datetime.strptime(ipo_date_str, "%Y-%m-%d").date() if ipo_date_str else None;
            self.expected_price_range_low = 18.00; self.expected_price_range_high = 22.00;
            self.expected_price_currency = "USD"; self.exchange = "NASDAQ"; self.status = "Filed";
            self.s1_filing_url = s1_url

    class MockNewsEvent: __init__ = lambda self, title, url, event_date_str="2025-05-25 10:00:00": setattr(self, 'event_title', title) or setattr(self, 'source_url', url) or setattr(self, 'source_name', "Mock News") or setattr(self, 'event_date', datetime.strptime(event_date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)) or setattr(self, 'full_article_text', "Full article text...")
    class MockStockAnalysis:
        def __init__(self, stock): self.stock, self.analysis_date, self.investment_decision, self.strategy_type, self.confidence_level, self.investment_thesis_full, self.reasoning = stock, datetime.now(timezone.utc), "Buy", "GARP", "Medium", "This is a **bold** thesis with _italic_ parts and a list:\n\n* Point 1\n* Point 2\n\n```python\nprint('hello')\n```\n\n| Header 1 | Header 2 |\n|----------|----------|\n| Cell 1   | Cell 2   |\n", "Reasons involving multiple lines\nand points for GARP strategy."
        self.dcf_assumptions = {"discount_rate": 0.095, "perpetual_growth_rate": 0.025, "projection_years": 5, "start_fcf": 1.2e9, "fcf_growth_rates_projection": [0.08, 0.07, 0.06, 0.05, 0.04], "start_fcf_basis":"Latest Annual TTM", "initial_fcf_growth_rate_basis":"3yr Hist. CAGR", "initial_fcf_growth_rate_used":0.075, "sensitivity_analysis": [{"scenario":"Best Case", "intrinsic_value":150, "upside":0.5}]}
        self.pe_ratio, self.pb_ratio, self.ps_ratio, self.ev_to_sales, self.ev_to_ebitda, self.eps, self.roe, self.roa, self.roic, self.dividend_yield, self.debt_to_equity, self.debt_to_ebitda, self.interest_coverage_ratio, self.current_ratio, self.quick_ratio, self.gross_profit_margin, self.operating_profit_margin, self.net_profit_margin, self.revenue_growth_yoy, self.revenue_growth_qoq, self.revenue_growth_cagr_3yr, self.revenue_growth_cagr_5yr, self.eps_growth_yoy, self.eps_growth_cagr_3yr, self.eps_growth_cagr_5yr, self.free_cash_flow_per_share, self.free_cash_flow_yield, self.free_cash_flow_trend, self.retained_earnings_trend, self.dcf_intrinsic_value, self.dcf_upside_percentage, self.business_summary, self.economic_moat_summary, self.industry_trends_summary, self.competitive_landscape_summary, self.management_assessment_summary, self.risk_factors_summary, self.key_metrics_snapshot, self.qualitative_sources_summary = (18.5, 3.2, 2.5, 2.8, 12.0, 2.50, 0.22, 0.10, 0.15, 0.015, 0.5, 2.1, 8.0, 1.8, 1.2, 0.60, 0.20, 0.12, 0.15, 0.04, 0.12, 0.10, 0.20, 0.18, 0.15, 1.80, 0.05, "Growing", "Growing", 120.50, 0.205, "Biz Sum with **markdown**.", "Moat Sum.", "Ind Sum.", "Comp Sum.", "Mgmt Sum.", "Risk Sum.", {"price": 100, "latest_q_revenue": 5000000000, "q_revenue_source": "FMP"}, {"10k_url": "url", "10k_business_summary_data":{"summary":"Test"}})
    class MockIPOAnalysis:
        def __init__(self, ipo, s1_used_all_false=False):
            self.ipo = ipo; self.analysis_date = datetime.now(timezone.utc);
            self.investment_decision = "High Risk/Speculative";
            self.reasoning = "SPACs are speculative. Need to see target. S-1 was NOT available for this mock."
            self.business_model_summary = "Typical SPAC seeking acquisition."
            self.risk_factors_summary = "Standard SPAC risks: finding target, dilution."
            self.pre_ipo_financials_summary = "No operating history as a SPAC."
            self.s1_mda_summary = "Not applicable pre-merger."
            self.competitive_landscape_summary = "Competes with other capital pools."
            self.industry_outlook_summary = "SPAC market conditions vary."
            self.use_of_proceeds_summary = "Fund trust for future acquisition."
            self.management_team_assessment = "Sponsor track record is key (details in S-1)."
            self.underwriter_quality_assessment = "Underwriters not specified here (details in S-1)."
            self.valuation_comparison_summary = "SPACs IPO at $10; real valuation post-merger."
            self.key_data_snapshot = {"name": ipo.company_name, "symbol": ipo.symbol, "price": "10.00"}
            if s1_used_all_false:
                self.s1_sections_used = {"business": False, "risk_factors": False, "mda": False, "financial_statements": False}
            else:
                self.s1_sections_used = {"business": True, "risk_factors": True, "mda": False, "financial_statements": False} # Simulate some partial use
            # For s1_summaries, let's assume they are similar to the ones above for this test
            self.s1_business_summary = self.business_model_summary
            self.s1_risk_factors_summary = self.risk_factors_summary
            self.s1_financial_health_summary = self.pre_ipo_financials_summary


    class MockNewsEventAnalysis:
        def __init__(self, news_event):
            self.news_event = news_event; self.analysis_date = datetime.now(timezone.utc)
            self.sentiment = "Neutral"; self.sentiment_reasoning = "Factual report."
            self.summary_for_email = "Tech company X announces Y, potential impact on Z."
            self.news_summary_detailed = "Detailed summary of the news event about X, Y, Z."
            self.potential_impact_on_companies = json.dumps([{"entityName": "Competitor A", "tickerSymbol": "COMPA", "explanation": "May face new competition."}])
            self.potential_impact_on_sectors = json.dumps([{"sectorName": "Semiconductors", "explanation": "Overall positive for innovation."}])
            self.mechanism_of_impact = "Introduction of new technology."
            self.estimated_timing_duration = "Timing: Medium-term (3-12mo), Duration: Extended"
            self.estimated_magnitude_direction = "Magnitude: Medium, Direction: Positive"
            self.confidence_of_assessment = "Medium (Based on initial reports)"
            self.key_news_snippets = {"headline": news_event.event_title, "source_type_used":"full article"}


    mock_sa = MockStockAnalysis(MockStock("MOCK", "MockCorp Inc."))
    mock_ipo_a_s1_used = MockIPOAnalysis(MockIPO("FutureTech IPO", "FTIP"))
    mock_ipo_a_s1_not_used = MockIPOAnalysis(MockIPO("BlankCheck SPAC", "BCSC", s1_url=None), s1_used_all_false=True) # Simulate S1 not found
    mock_news_a = MockNewsEventAnalysis(MockNewsEvent("Major Tech Breakthrough by Innovate Inc.", "http://example.com/news1"))

    email_svc = EmailService()
    email_message = email_svc.create_summary_email(
        stock_analyses=[mock_sa],
        ipo_analyses=[mock_ipo_a_s1_used, mock_ipo_a_s1_not_used],
        news_analyses=[mock_news_a]
    )

    if email_message:
        logger.info("Email message created successfully.")
        output_filename = f"test_email_summary_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        payload_html = "";
        if email_message.is_multipart():
            for part in email_message.get_payload():
                if part.get_content_type() == "text/html":
                    payload_html = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8')
                    break
        else:
            payload_html = email_message.get_payload(decode=True).decode(email_message.get_content_charset() or 'utf-8')

        if payload_html:
            with open(output_filename, "w", encoding="utf-8") as f:
                f.write(payload_html)
            logger.info(f"Test email HTML saved to {output_filename}")
        else:
            logger.error("Could not extract HTML payload from email message.")

        # Uncomment to actually send the test email
        # if email_svc.send_email(email_message):
        #     logger.info("Test email sent successfully.")
        # else:
        #     logger.error("Failed to send test email.")
    else:
        logger.error("Failed to create email message.")