# api_clients/fmp_client.py
from .base_client import APIClient
from core.config import FINANCIAL_MODELING_PREP_API_KEY
from core.logging_setup import logger


class FinancialModelingPrepClient(APIClient):
    def __init__(self):
        super().__init__("https://financialmodelingprep.com/api/v3", api_key_name="apikey",
                         api_key_value=FINANCIAL_MODELING_PREP_API_KEY)

    def get_ipo_calendar(self, from_date=None, to_date=None):
        # Note: FMP's free tier might not support this well or at all.
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        logger.info("FinancialModelingPrepClient.get_ipo_calendar called. Availability depends on FMP subscription.")
        return self.request("GET", "/ipo_calendar", params=params, api_source_name="fmp_ipo_calendar")

    def get_financial_statements(self, ticker, statement_type="income-statement", period="quarter", limit=40):
        actual_limit = limit
        if period == "annual": actual_limit = min(limit, 15)
        elif period == "quarter": actual_limit = min(limit, 60)

        return self.request("GET", f"/{statement_type}/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_{statement_type.replace('-', '_')}_{period}")

    def get_income_statement_growth(self, ticker, period="quarter", limit=40):
        actual_limit = limit
        if period == "annual": actual_limit = min(limit, 15)
        elif period == "quarter": actual_limit = min(limit, 60)
        return self.request("GET", f"/income-statement-growth/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_income_statement_growth_{period}")

    def get_key_metrics(self, ticker, period="quarter", limit=40):
        actual_limit = limit
        if period == "annual": actual_limit = min(limit, 15)
        elif period == "quarter": actual_limit = min(limit, 60)
        return self.request("GET", f"/key-metrics/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_key_metrics_{period}")

    def get_ratios(self, ticker, period="quarter", limit=40):
        actual_limit = limit
        if period == "annual": actual_limit = min(limit, 15)
        elif period == "quarter": actual_limit = min(limit, 60)
        return self.request("GET", f"/ratios/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_ratios_{period}")

    def get_company_profile(self, ticker):
        return self.request("GET", f"/profile/{ticker}", params={}, api_source_name="fmp_profile")

    def get_analyst_estimates(self, ticker, period="annual"):
        logger.info(f"FMP get_analyst_estimates for {ticker} called. Availability depends on FMP subscription.")
        return self.request("GET", f"/analyst-estimates/{ticker}", params={"period": period},
                            api_source_name="fmp_analyst_estimates")