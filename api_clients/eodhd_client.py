from .base_client import APIClient
from core.config import EODHD_API_KEY
from core.logging_setup import logger


class EODHDClient(APIClient):
    def __init__(self):
        super().__init__("https://eodhistoricaldata.com/api", api_key_name="api_token", api_key_value=EODHD_API_KEY)
        self.params["fmt"] = "json" # Default format

    def get_fundamental_data(self, ticker_with_exchange): # e.g., AAPL.US
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}", api_source_name="eodhd_fundamentals")

    def get_ipo_calendar(self, from_date=None, to_date=None):
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        logger.info("EODHDClient.get_ipo_calendar called. Data quality/availability may vary by subscription.")
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd_ipo_calendar")