from .base_client import APIClient
from core.config import ALPHA_VANTAGE_API_KEY


class AlphaVantageClient(APIClient):
    def __init__(self):
        super().__init__("https://www.alphavantage.co", api_key_name="apikey", api_key_value=ALPHA_VANTAGE_API_KEY)

    def get_company_overview(self, ticker):
        params = {"function": "OVERVIEW", "symbol": ticker}
        return self.request("GET", "/query", params=params, api_source_name="alphavantage_overview")

    def get_income_statement_quarterly(self, ticker):
        params = {"function": "INCOME_STATEMENT", "symbol": ticker}
        data = self.request("GET", "/query", params=params, api_source_name="alphavantage_income_quarterly")
        if data and isinstance(data.get("quarterlyReports"), list):
            data["quarterlyReports"].reverse()
        return data

    def get_balance_sheet_quarterly(self, ticker):
        params = {"function": "BALANCE_SHEET", "symbol": ticker}
        data = self.request("GET", "/query", params=params, api_source_name="alphavantage_balance_quarterly")
        if data and isinstance(data.get("quarterlyReports"), list):
            data["quarterlyReports"].reverse()
        return data

    def get_cash_flow_quarterly(self, ticker):
        params = {"function": "CASH_FLOW", "symbol": ticker}
        data = self.request("GET", "/query", params=params, api_source_name="alphavantage_cashflow_quarterly")
        if data and isinstance(data.get("quarterlyReports"), list):
            data["quarterlyReports"].reverse()
        return data