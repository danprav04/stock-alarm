from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, ForeignKey, Boolean, Date, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from .connection import Base  # Import Base from connection.py


class Stock(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, unique=True, index=True, nullable=False)
    company_name = Column(String)
    industry = Column(String, nullable=True)
    sector = Column(String, nullable=True)
    last_analysis_date = Column(DateTime(timezone=True),
                                default=lambda: datetime.now(timezone.utc),
                                onupdate=lambda: datetime.now(timezone.utc))
    cik = Column(String, nullable=True, index=True)

    analyses = relationship("StockAnalysis", back_populates="stock", cascade="all, delete-orphan")


class StockAnalysis(Base):
    __tablename__ = "stock_analyses"
    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id", ondelete="CASCADE"), nullable=False)
    analysis_date = Column(DateTime(timezone=True),
                           default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    pe_ratio = Column(Float, nullable=True)
    pb_ratio = Column(Float, nullable=True)
    ps_ratio = Column(Float, nullable=True)
    ev_to_sales = Column(Float, nullable=True)
    ev_to_ebitda = Column(Float, nullable=True)
    eps = Column(Float, nullable=True)
    roe = Column(Float, nullable=True)
    roa = Column(Float, nullable=True)
    roic = Column(Float, nullable=True)
    dividend_yield = Column(Float, nullable=True)
    debt_to_equity = Column(Float, nullable=True)
    debt_to_ebitda = Column(Float, nullable=True)
    interest_coverage_ratio = Column(Float, nullable=True)
    current_ratio = Column(Float, nullable=True)
    quick_ratio = Column(Float, nullable=True)
    revenue_growth_yoy = Column(Float, nullable=True)
    revenue_growth_qoq = Column(Float, nullable=True)
    revenue_growth_cagr_3yr = Column(Float, nullable=True)
    revenue_growth_cagr_5yr = Column(Float, nullable=True)
    eps_growth_yoy = Column(Float, nullable=True)
    eps_growth_cagr_3yr = Column(Float, nullable=True)
    eps_growth_cagr_5yr = Column(Float, nullable=True)
    net_profit_margin = Column(Float, nullable=True)
    gross_profit_margin = Column(Float, nullable=True)
    operating_profit_margin = Column(Float, nullable=True)
    free_cash_flow_per_share = Column(Float, nullable=True)
    free_cash_flow_yield = Column(Float, nullable=True)
    free_cash_flow_trend = Column(String, nullable=True)
    retained_earnings_trend = Column(String, nullable=True)
    dcf_intrinsic_value = Column(Float, nullable=True)
    dcf_upside_percentage = Column(Float, nullable=True)
    dcf_assumptions = Column(JSON, nullable=True)
    business_summary = Column(Text, nullable=True)
    economic_moat_summary = Column(Text, nullable=True)
    industry_trends_summary = Column(Text, nullable=True)
    competitive_landscape_summary = Column(Text, nullable=True)
    management_assessment_summary = Column(Text, nullable=True)
    risk_factors_summary = Column(Text, nullable=True)
    investment_thesis_full = Column(Text, nullable=True)
    investment_decision = Column(String, nullable=True)
    reasoning = Column(Text, nullable=True)
    strategy_type = Column(String, nullable=True)
    confidence_level = Column(String, nullable=True)
    key_metrics_snapshot = Column(JSON, nullable=True)
    qualitative_sources_summary = Column(JSON, nullable=True)

    stock = relationship("Stock", back_populates="analyses")


class IPO(Base):
    __tablename__ = "ipos"
    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String, index=True, nullable=False)
    symbol = Column(String, index=True, nullable=True)
    ipo_date_str = Column(String, nullable=True)
    ipo_date = Column(Date, nullable=True)
    expected_price_range_low = Column(Float, nullable=True)
    expected_price_range_high = Column(Float, nullable=True)
    expected_price_currency = Column(String, nullable=True, default="USD")
    offered_shares = Column(Float, nullable=True)
    total_shares_value = Column(Float, nullable=True)
    exchange = Column(String, nullable=True)
    status = Column(String, nullable=True)
    cik = Column(String, nullable=True, index=True)
    last_analysis_date = Column(DateTime(timezone=True),
                                default=lambda: datetime.now(timezone.utc),
                                onupdate=lambda: datetime.now(timezone.utc))
    s1_filing_url = Column(String, nullable=True)

    analyses = relationship("IPOAnalysis", back_populates="ipo", cascade="all, delete-orphan")
    __table_args__ = (UniqueConstraint('company_name', 'ipo_date_str', 'symbol', name='uq_ipo_name_date_symbol'),)


class IPOAnalysis(Base):
    __tablename__ = "ipo_analyses"
    id = Column(Integer, primary_key=True, index=True)
    ipo_id = Column(Integer, ForeignKey("ipos.id", ondelete="CASCADE"), nullable=False)
    analysis_date = Column(DateTime(timezone=True),
                           default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    s1_business_summary = Column(Text, nullable=True)
    s1_risk_factors_summary = Column(Text, nullable=True)
    s1_mda_summary = Column(Text, nullable=True)
    s1_financial_health_summary = Column(Text, nullable=True)
    competitive_landscape_summary = Column(Text, nullable=True)
    industry_outlook_summary = Column(Text, nullable=True)
    management_team_assessment = Column(Text, nullable=True)
    use_of_proceeds_summary = Column(Text, nullable=True)
    underwriter_quality_assessment = Column(String, nullable=True)
    business_model_summary = Column(Text, nullable=True)
    risk_factors_summary = Column(Text, nullable=True)
    pre_ipo_financials_summary = Column(Text, nullable=True)
    valuation_comparison_summary = Column(Text, nullable=True)
    investment_decision = Column(String, nullable=True)
    reasoning = Column(Text, nullable=True)
    key_data_snapshot = Column(JSON, nullable=True)
    s1_sections_used = Column(JSON, nullable=True)

    ipo = relationship("IPO", back_populates="analyses")


class NewsEvent(Base):
    __tablename__ = "news_events"
    id = Column(Integer, primary_key=True, index=True)
    event_title = Column(String, index=True)
    event_date = Column(DateTime(timezone=True), nullable=True)
    source_url = Column(String, unique=True, nullable=False, index=True)
    source_name = Column(String, nullable=True)
    category = Column(String, nullable=True)
    processed_date = Column(DateTime(timezone=True),
                            default=lambda: datetime.now(timezone.utc))
    last_analyzed_date = Column(DateTime(timezone=True), nullable=True,
                                onupdate=lambda: datetime.now(timezone.utc))
    full_article_text = Column(Text, nullable=True)

    analyses = relationship("NewsEventAnalysis", back_populates="news_event", cascade="all, delete-orphan")
    __table_args__ = (UniqueConstraint('source_url', name='uq_news_source_url'),)


class NewsEventAnalysis(Base):
    __tablename__ = "news_event_analyses"
    id = Column(Integer, primary_key=True, index=True)
    news_event_id = Column(Integer, ForeignKey("news_events.id", ondelete="CASCADE"), nullable=False)
    analysis_date = Column(DateTime(timezone=True),
                           default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    sentiment = Column(String, nullable=True)
    sentiment_reasoning = Column(Text, nullable=True)
    affected_stocks_explicit = Column(JSON, nullable=True)
    affected_sectors_explicit = Column(JSON, nullable=True)
    news_summary_detailed = Column(Text, nullable=True)
    potential_impact_on_market = Column(Text, nullable=True)
    potential_impact_on_companies = Column(Text, nullable=True)
    potential_impact_on_sectors = Column(Text, nullable=True)
    mechanism_of_impact = Column(Text, nullable=True)
    estimated_timing_duration = Column(String, nullable=True)
    estimated_magnitude_direction = Column(String, nullable=True)
    confidence_of_assessment = Column(String, nullable=True)
    summary_for_email = Column(Text, nullable=True)
    key_news_snippets = Column(JSON, nullable=True)

    news_event = relationship("NewsEvent", back_populates="analyses")


class CachedAPIData(Base):
    __tablename__ = "cached_api_data"
    id = Column(Integer, primary_key=True, index=True)
    api_source = Column(String, index=True, nullable=False)
    request_url_or_params = Column(String, unique=True, nullable=False, index=True)
    response_data = Column(JSON, nullable=False)
    timestamp = Column(DateTime(timezone=True),
                       default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)