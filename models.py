# models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, ForeignKey, Boolean, Date, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
from datetime import datetime, timezone  # Ensure timezone is available


class Stock(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, unique=True, index=True, nullable=False)
    company_name = Column(String)
    industry = Column(String, nullable=True)  # New field
    sector = Column(String, nullable=True)  # New field
    # Ensure last_analysis_date uses timezone-aware datetime
    last_analysis_date = Column(DateTime(timezone=True), server_default=lambda: datetime.now(timezone.utc),
                                onupdate=lambda: datetime.now(timezone.utc))
    cik = Column(String, nullable=True, index=True)  # Store CIK for EDGAR lookups

    analyses = relationship("StockAnalysis", back_populates="stock", cascade="all, delete-orphan")
    # If storing 10-K summaries:
    # latest_10k_summary = relationship("TenKSummary", back_populates="stock", uselist=False, cascade="all, delete-orphan")


class StockAnalysis(Base):
    __tablename__ = "stock_analyses"
    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    # Ensure analysis_date uses timezone-aware datetime
    analysis_date = Column(DateTime(timezone=True), server_default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    # Fundamental Analysis - Quantitative
    pe_ratio = Column(Float, nullable=True)
    pb_ratio = Column(Float, nullable=True)
    ps_ratio = Column(Float, nullable=True)  # Price to Sales
    ev_to_sales = Column(Float, nullable=True)  # Enterprise Value to Sales
    ev_to_ebitda = Column(Float, nullable=True)  # Enterprise Value to EBITDA
    eps = Column(Float, nullable=True)
    roe = Column(Float, nullable=True)  # Return on Equity
    roa = Column(Float, nullable=True)  # Return on Assets
    roic = Column(Float, nullable=True)  # Return on Invested Capital
    dividend_yield = Column(Float, nullable=True)
    debt_to_equity = Column(Float, nullable=True)
    debt_to_ebitda = Column(Float, nullable=True)  # Total Debt to EBITDA
    interest_coverage_ratio = Column(Float, nullable=True)
    current_ratio = Column(Float, nullable=True)
    quick_ratio = Column(Float, nullable=True)  # Acid-test ratio

    revenue_growth_yoy = Column(Float, nullable=True)  # Year-over-Year
    revenue_growth_qoq = Column(Float, nullable=True)  # Quarter-over-Quarter
    revenue_growth_cagr_3yr = Column(Float, nullable=True)  # 3-year CAGR
    revenue_growth_cagr_5yr = Column(Float, nullable=True)  # 5-year CAGR

    eps_growth_yoy = Column(Float, nullable=True)
    eps_growth_cagr_3yr = Column(Float, nullable=True)
    eps_growth_cagr_5yr = Column(Float, nullable=True)

    net_profit_margin = Column(Float, nullable=True)
    gross_profit_margin = Column(Float, nullable=True)
    operating_profit_margin = Column(Float, nullable=True)

    free_cash_flow_per_share = Column(Float, nullable=True)
    free_cash_flow_yield = Column(Float, nullable=True)
    free_cash_flow_trend = Column(String, nullable=True)  # Growing, Stable, Declining
    retained_earnings_trend = Column(String, nullable=True)  # Growing, Stable, Declining

    # DCF Analysis results
    dcf_intrinsic_value = Column(Float, nullable=True)
    dcf_upside_percentage = Column(Float, nullable=True)
    dcf_assumptions = Column(JSON, nullable=True)  # Store key assumptions like WACC, growth rates

    # Qualitative Analysis (Summaries from LLM, potentially based on 10-K)
    business_summary = Column(Text, nullable=True)  # From 10-K Item 1
    economic_moat_summary = Column(Text, nullable=True)
    industry_trends_summary = Column(Text, nullable=True)  # Consider industry outlook
    competitive_landscape_summary = Column(Text, nullable=True)  # From 10-K or LLM
    management_assessment_summary = Column(Text, nullable=True)  # Generic factors, or 10-K MD&A insights
    risk_factors_summary = Column(Text, nullable=True)  # From 10-K Item 1A

    # Investment Strategy/Conclusion
    investment_decision = Column(String, nullable=True)  # e.g., Buy, Hold, Sell, Monitor, Under Review
    reasoning = Column(Text, nullable=True)  # Detailed reasoning for the decision
    strategy_type = Column(String, nullable=True)  # e.g., Value, Growth, GARP, Dividend Income
    confidence_level = Column(String, nullable=True)  # e.g., High, Medium, Low

    key_metrics_snapshot = Column(JSON, nullable=True)  # Raw data points used
    qualitative_sources_summary = Column(JSON, nullable=True)  # Summary of prompts or 10-K sections used
    # Removed revenue_growth (string) as we have specific growth floats

    stock = relationship("Stock", back_populates="analyses")


class IPO(Base):
    __tablename__ = "ipos"
    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String, index=True, nullable=False)
    symbol = Column(String, index=True, nullable=True)
    ipo_date_str = Column(String, nullable=True)  # Original string date
    ipo_date = Column(Date, nullable=True)  # Parsed date object
    expected_price_range_low = Column(Float, nullable=True)
    expected_price_range_high = Column(Float, nullable=True)
    expected_price_currency = Column(String, nullable=True, default="USD")
    offered_shares = Column(Integer, nullable=True)
    total_shares_value = Column(Float, nullable=True)  # IPO deal size
    exchange = Column(String, nullable=True)
    status = Column(String, nullable=True)  # e.g., expected, filed, priced, withdrawn
    cik = Column(String, nullable=True, index=True)  # For S-1 lookup
    # Ensure last_analysis_date uses timezone-aware datetime
    last_analysis_date = Column(DateTime(timezone=True), server_default=lambda: datetime.now(timezone.utc),
                                onupdate=lambda: datetime.now(timezone.utc))
    s1_filing_url = Column(String, nullable=True)  # Direct link to S-1/F-1 if found

    analyses = relationship("IPOAnalysis", back_populates="ipo", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint('company_name', 'ipo_date_str', 'symbol', name='uq_ipo_name_date_symbol'),)


class IPOAnalysis(Base):
    __tablename__ = "ipo_analyses"
    id = Column(Integer, primary_key=True, index=True)
    ipo_id = Column(Integer, ForeignKey("ipos.id"), nullable=False)
    # Ensure analysis_date uses timezone-aware datetime
    analysis_date = Column(DateTime(timezone=True), server_default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    # Summaries based on S-1/F-1 (if available) and LLM
    s1_business_summary = Column(Text, nullable=True)
    s1_risk_factors_summary = Column(Text, nullable=True)
    s1_mda_summary = Column(Text, nullable=True)  # Management Discussion & Analysis
    s1_financial_health_summary = Column(Text, nullable=True)  # LLM summary of S-1 financials

    competitive_landscape_summary = Column(Text, nullable=True)  # LLM or S-1 based
    industry_outlook_summary = Column(Text, nullable=True)  # LLM or S-1 based
    management_team_assessment = Column(Text, nullable=True)  # LLM based on S-1 if details are parsed
    use_of_proceeds_summary = Column(Text, nullable=True)  # From S-1
    underwriter_quality_assessment = Column(String, nullable=True)  # LLM based on S-1 underwriters list

    # Fields from previous model, can be populated by LLM if S-1 not available
    business_model_summary = Column(Text, nullable=True)  # Kept for generic case
    # competitive_landscape_summary = Column(Text, nullable=True) # Duplicate, consolidated above
    industry_health_summary = Column(Text, nullable=True)  # Renamed to industry_outlook_summary
    # use_of_proceeds_summary = Column(Text, nullable=True) # Duplicate, consolidated above
    risk_factors_summary = Column(Text, nullable=True)  # Kept for generic case, s1_ is preferred
    pre_ipo_financials_summary = Column(Text, nullable=True)  # Kept for generic, s1_financial_health is preferred
    valuation_comparison_summary = Column(Text, nullable=True)  # General guidance if no S-1 financials

    # Lock-up periods, investor demand etc. are hard to get systematically for free
    # lock_up_periods_info = Column(String, nullable=True) # From S-1
    # investor_demand_summary = Column(Text, nullable=True) # From news closer to date

    investment_decision = Column(String, nullable=True)  # e.g., Avoid, Monitor, Cautious Buy, Strong Buy (Post-IPO)
    reasoning = Column(Text, nullable=True)
    key_data_snapshot = Column(JSON, nullable=True)  # Raw calendar data
    s1_sections_used = Column(JSON, nullable=True)  # e.g. {"business": true, "risk_factors": false}

    ipo = relationship("IPO", back_populates="analyses")


class NewsEvent(Base):
    __tablename__ = "news_events"
    id = Column(Integer, primary_key=True, index=True)
    event_title = Column(String, index=True)
    # Ensure event_date and processed_date use timezone-aware datetime
    event_date = Column(DateTime(timezone=True), nullable=True)  # Date of the news
    source_url = Column(String, unique=True, nullable=False)
    source_name = Column(String, nullable=True)  # e.g., Reuters, CNBC
    category = Column(String, nullable=True)  # From API or LLM
    # processed_date is when our system first saw/processed it
    processed_date = Column(DateTime(timezone=True), server_default=lambda: datetime.now(timezone.utc))
    last_analyzed_date = Column(DateTime(timezone=True), nullable=True)  # When analysis was last run

    full_article_text = Column(Text, nullable=True)  # Store scraped full text

    analyses = relationship("NewsEventAnalysis", back_populates="news_event", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint('source_url', name='uq_news_source_url'),)


class NewsEventAnalysis(Base):
    __tablename__ = "news_event_analyses"
    id = Column(Integer, primary_key=True, index=True)
    news_event_id = Column(Integer, ForeignKey("news_events.id"), nullable=False)
    # Ensure analysis_date uses timezone-aware datetime
    analysis_date = Column(DateTime(timezone=True), server_default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    sentiment = Column(String, nullable=True)  # Positive, Negative, Neutral
    sentiment_reasoning = Column(Text, nullable=True)

    affected_stocks_explicit = Column(JSON, nullable=True)  # Tickers explicitly mentioned or strongly implied
    affected_sectors_explicit = Column(JSON, nullable=True)  # Sectors explicitly mentioned

    # LLM generated detailed analysis based on full text
    news_summary_detailed = Column(Text, nullable=True)  # Longer, more nuanced summary
    potential_impact_on_market = Column(Text, nullable=True)
    potential_impact_on_companies = Column(Text, nullable=True)  # Specific companies if identifiable
    potential_impact_on_sectors = Column(Text, nullable=True)  # Specific sectors

    mechanism_of_impact = Column(Text, nullable=True)
    estimated_timing_duration = Column(String, nullable=True)  # e.g. Short-term, Medium-term
    estimated_magnitude_direction = Column(String, nullable=True)  # e.g. Small Positive, Large Negative
    confidence_of_assessment = Column(String, nullable=True)  # e.g. High, Medium, Low on the impact assessment

    summary_for_email = Column(Text, nullable=True)  # Short, investor-focused summary
    key_news_snippets = Column(JSON, nullable=True)  # Snippets used by LLM or important quotes

    news_event = relationship("NewsEvent", back_populates="analyses")


class CachedAPIData(Base):
    __tablename__ = "cached_api_data"
    id = Column(Integer, primary_key=True, index=True)
    api_source = Column(String, index=True, nullable=False)
    request_url_or_params = Column(String, unique=True, nullable=False, index=True)  # Index for faster lookups
    response_data = Column(JSON, nullable=False)
    # Ensure timestamp and expires_at use timezone-aware datetime
    timestamp = Column(DateTime(timezone=True), server_default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)  # Index for cleaning old cache