# models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class Stock(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, unique=True, index=True, nullable=False)
    company_name = Column(String)
    last_analysis_date = Column(DateTime(timezone=True), server_default=func.now())

    analyses = relationship("StockAnalysis", back_populates="stock")


class StockAnalysis(Base):
    __tablename__ = "stock_analyses"
    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    analysis_date = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Fundamental Analysis
    pe_ratio = Column(Float, nullable=True)
    pb_ratio = Column(Float, nullable=True)
    eps = Column(Float, nullable=True)
    roe = Column(Float, nullable=True)
    dividend_yield = Column(Float, nullable=True)
    debt_to_equity = Column(Float, nullable=True)
    interest_coverage_ratio = Column(Float, nullable=True)
    current_ratio = Column(Float, nullable=True)
    retained_earnings_trend = Column(String, nullable=True)  # e.g., "Growing", "Stable", "Declining"
    revenue_growth = Column(String, nullable=True)
    net_profit_margin = Column(Float, nullable=True)
    free_cash_flow_trend = Column(String, nullable=True)

    # Qualitative Analysis
    economic_moat_summary = Column(Text, nullable=True)
    industry_trends_summary = Column(Text, nullable=True)
    management_assessment_summary = Column(Text, nullable=True)

    # Investment Strategy/Conclusion
    investment_decision = Column(String, nullable=True)  # e.g., "Buy", "Hold", "Sell", "Avoid"
    reasoning = Column(Text, nullable=True)
    strategy_type = Column(String, nullable=True)  # e.g., "Value", "Growth", "Income"
    key_metrics_snapshot = Column(JSON, nullable=True)  # Store raw data for email
    qualitative_sources = Column(JSON, nullable=True)  # Store text snippets for email

    stock = relationship("Stock", back_populates="analyses")


class IPO(Base):
    __tablename__ = "ipos"
    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String, unique=True, index=True, nullable=False)
    ipo_date = Column(String, nullable=True)  # Store as string if format varies, or use Date type
    expected_price_range = Column(String, nullable=True)
    last_analysis_date = Column(DateTime(timezone=True), server_default=func.now())

    analyses = relationship("IPOAnalysis", back_populates="ipo")


class IPOAnalysis(Base):
    __tablename__ = "ipo_analyses"
    id = Column(Integer, primary_key=True, index=True)
    ipo_id = Column(Integer, ForeignKey("ipos.id"), nullable=False)
    analysis_date = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    business_model_summary = Column(Text, nullable=True)
    competitive_landscape_summary = Column(Text, nullable=True)
    industry_health_summary = Column(Text, nullable=True)
    use_of_proceeds_summary = Column(Text, nullable=True)
    risk_factors_summary = Column(Text, nullable=True)
    pre_ipo_financials_summary = Column(Text, nullable=True)  # Key trends
    valuation_comparison_summary = Column(Text, nullable=True)  # P/S to peers etc.
    underwriter_quality = Column(String, nullable=True)
    fresh_issue_vs_ofs = Column(String, nullable=True)
    lock_up_periods_info = Column(String, nullable=True)
    investor_demand_summary = Column(Text, nullable=True)  # Anchor, subscription

    investment_decision = Column(String, nullable=True)  # e.g., "Apply", "Avoid", "Wait & Watch"
    reasoning = Column(Text, nullable=True)
    key_data_snapshot = Column(JSON, nullable=True)  # Store raw data for email

    ipo = relationship("IPO", back_populates="analyses")


class NewsEvent(Base):
    __tablename__ = "news_events"
    id = Column(Integer, primary_key=True, index=True)
    event_title = Column(String, index=True)
    event_date = Column(DateTime(timezone=True), nullable=True)
    source_url = Column(String, unique=True)  # If applicable
    category = Column(String)  # Macro, Industry, Company-Specific, Geopolitical
    processed_date = Column(DateTime(timezone=True), server_default=func.now())

    analyses = relationship("NewsEventAnalysis", back_populates="news_event")


class NewsEventAnalysis(Base):
    __tablename__ = "news_event_analyses"
    id = Column(Integer, primary_key=True, index=True)
    news_event_id = Column(Integer, ForeignKey("news_events.id"), nullable=False)
    analysis_date = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    affected_stocks_sectors = Column(JSON)  # List of tickers/sectors with reasoning
    scope_relevance = Column(Text, nullable=True)
    mechanism_of_impact = Column(Text, nullable=True)
    estimated_timing = Column(String, nullable=True)  # Immediate, Short-term, etc.
    estimated_magnitude_direction = Column(String, nullable=True)  # e.g., "Large Negative", "Small Positive"
    countervailing_factors = Column(Text, nullable=True)

    summary_for_email = Column(Text, nullable=True)
    key_news_snippets = Column(JSON, nullable=True)

    news_event = relationship("NewsEvent", back_populates="analyses")


class CachedAPIData(Base):
    __tablename__ = "cached_api_data"
    id = Column(Integer, primary_key=True, index=True)
    api_source = Column(String, index=True, nullable=False)  # e.g., "finnhub", "fmp"
    request_url_or_params = Column(String, unique=True, nullable=False)  # A unique key for the request
    response_data = Column(JSON, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)

# Add more models as needed, e.g., for User Preferences if the script becomes multi-user