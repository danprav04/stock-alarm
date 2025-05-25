from datetime import datetime, timedelta, timezone
from .base_client import APIClient
from core.config import FINNHUB_API_KEY


class FinnhubClient(APIClient):
    def __init__(self):
        super().__init__("https://finnhub.io/api/v1", api_key_name="token", api_key_value=FINNHUB_API_KEY)

    def get_market_news(self, category="general", min_id=0):
        params = {"category": category}
        if min_id > 0: params["minId"] = min_id
        return self.request("GET", "/news", params=params, api_source_name="finnhub_news")

    def get_company_profile2(self, ticker):
        return self.request("GET", "/stock/profile2", params={"symbol": ticker}, api_source_name="finnhub_profile")

    def get_financials_reported(self, ticker, freq="quarterly", count=20): # count specifies number of periods
        params = {"symbol": ticker, "freq": freq, "count": count}
        return self.request("GET", "/stock/financials-reported", params=params,
                            api_source_name="finnhub_financials_reported")

    def get_basic_financials(self, ticker, metric_type="all"):
        return self.request("GET", "/stock/metric", params={"symbol": ticker, "metric": metric_type},
                            api_source_name="finnhub_metrics")

    def get_ipo_calendar(self, from_date=None, to_date=None):
        if from_date is None: from_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        if to_date is None: to_date = (datetime.now(timezone.utc) + timedelta(days=90)).strftime('%Y-%m-%d')
        params = {"from": from_date, "to": to_date}
        return self.request("GET", "/calendar/ipo", params=params, api_source_name="finnhub_ipo_calendar")

    def get_sec_filings(self, ticker, from_date=None, to_date=None):
        if from_date is None: from_date = (datetime.now(timezone.utc) - timedelta(days=365 * 2)).strftime('%Y-%m-%d')
        if to_date is None: to_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        params = {"symbol": ticker, "from": from_date, "to": to_date}
        return self.request("GET", "/stock/filings", params=params, api_source_name="finnhub_filings")

    def get_company_peers(self, ticker):
        """Gets a list of company peers."""
        return self.request("GET", "/stock/peers", params={"symbol": ticker}, api_source_name="finnhub_peers")
