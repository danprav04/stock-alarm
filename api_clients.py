# api_clients.py
import requests
import time
import json
from datetime import datetime, timedelta
from config import (
    GOOGLE_API_KEYS, FINNHUB_API_KEY, FINANCIAL_MODELING_PREP_API_KEY,
    EODHD_API_KEY, RAPIDAPI_UPCOMING_IPO_KEY, API_REQUEST_TIMEOUT,
    API_RETRY_ATTEMPTS, API_RETRY_DELAY, CACHE_EXPIRY_SECONDS
)
from error_handler import logger
from database import SessionLocal
from models import CachedAPIData

# --- Global state for Google API Key Rotation ---
# This is a simple in-memory rotation. For multiple script instances or long-running services,
# this state might need to be managed in a database or a shared cache.
current_google_api_key_index = 0


# ---

class APIClient:
    def __init__(self, base_url, api_key_name=None, api_key_value=None, headers=None):
        self.base_url = base_url
        self.api_key_name = api_key_name
        self.api_key_value = api_key_value
        self.headers = headers or {}
        if api_key_name and api_key_value:
            self.params = {api_key_name: api_key_value}
        else:
            self.params = {}

    def _get_cached_response(self, request_url_or_params_str):
        session = SessionLocal()
        try:
            cache_entry = session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str,
                CachedAPIData.expires_at > datetime.now(CachedAPIData.expires_at.expression.type.timezone)
                # Ensure timezone aware comparison
            ).first()
            if cache_entry:
                logger.info(f"Cache hit for: {request_url_or_params_str}")
                return cache_entry.response_data
        except Exception as e:
            logger.error(f"Error reading from cache: {e}", exc_info=True)
        finally:
            session.close()
        return None

    def _cache_response(self, request_url_or_params_str, response_data, api_source):
        session = SessionLocal()
        try:
            expires_at = datetime.now() + timedelta(seconds=CACHE_EXPIRY_SECONDS)
            # For timezone-aware datetime, if your DB expects it
            # from sqlalchemy.sql import func; expires_at = func.now() + timedelta(seconds=CACHE_EXPIRY_SECONDS)

            cache_entry = CachedAPIData(
                api_source=api_source,
                request_url_or_params=request_url_or_params_str,
                response_data=response_data,
                expires_at=expires_at
            )
            session.add(cache_entry)
            session.commit()
            logger.info(f"Cached response for: {request_url_or_params_str}")
        except Exception as e:
            logger.error(f"Error writing to cache: {e}", exc_info=True)
            session.rollback()
        finally:
            session.close()

    def request(self, method, endpoint, params=None, data=None, json_data=None, use_cache=True,
                api_source_name="unknown"):
        url = f"{self.base_url}{endpoint}"
        full_params = self.params.copy()
        if params:
            full_params.update(params)

        # Create a unique string for caching based on URL and params
        # Sort params for consistent key generation
        sorted_params = sorted(full_params.items()) if full_params else []
        cache_key_str = f"{method.upper()}:{url}?{json.dumps(sorted_params)}"

        if use_cache:
            cached_data = self_get_cached_response(cache_key_str)
            if cached_data:
                return cached_data

        for attempt in range(API_RETRY_ATTEMPTS):
            try:
                response = requests.request(
                    method, url, params=full_params, data=data, json=json_data,
                    headers=self.headers, timeout=API_REQUEST_TIMEOUT
                )
                response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)

                response_json = response.json()
                if use_cache:
                    self._cache_response(cache_key_str, response_json, api_source_name)
                return response_json

            except requests.exceptions.HTTPError as e:
                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:  # Rate limit
                    logger.info(f"Rate limit hit. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))  # Exponential backoff can be better
                elif 500 <= e.response.status_code < 600:  # Server-side error
                    logger.info(f"Server error. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))
                else:  # Other client errors (4xx) usually don't benefit from retries
                    logger.error(f"Client error for {url}: {e}", exc_info=True)
                    return None  # Or raise custom error
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")

            if attempt < API_RETRY_ATTEMPTS - 1:
                time.sleep(API_RETRY_DELAY)
            else:
                logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}.")
                return None  # Or raise custom error
        return None


class FinnhubClient(APIClient):
    def __init__(self):
        super().__init__("https://finnhub.io/api/v1", api_key_name="token", api_key_value=FINNHUB_API_KEY)

    def get_market_news(self, category="general", min_id=0):
        # `min_id` can be used for pagination or fetching newer news.
        params = {"category": category}
        if min_id > 0:
            params["minId"] = min_id
        return self.request("GET", "/news", params=params, api_source_name="finnhub")

    def get_company_profile2(self, ticker):  # profile2 includes more details
        return self.request("GET", "/stock/profile2", params={"symbol": ticker}, api_source_name="finnhub")

    def get_financials_reported(self, ticker, freq="quarterly"):  # annual, quarterly
        # Options: bs (balance sheet), ic (income statement), cf (cash flow)
        return self.request("GET", "/stock/financials-reported", params={"symbol": ticker, "freq": freq},
                            api_source_name="finnhub")

    def get_basic_financials(self, ticker, metric_type="all"):
        # Provides key metrics like P/E, P/B, DividendYield, etc.
        return self.request("GET", "/stock/metric", params={"symbol": ticker, "metric": metric_type},
                            api_source_name="finnhub")

    # Add more Finnhub endpoints as needed


class FinancialModelingPrepClient(APIClient):
    def __init__(self):
        super().__init__("https://financialmodelingprep.com/api/v3", api_key_name="apikey",
                         api_key_value=FINANCIAL_MODELING_PREP_API_KEY)

    def get_ipo_calendar(self, from_date=None, to_date=None):
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        return self.request("GET", "/ipo_calendar", params=params, api_source_name="fmp")

    def get_financial_statements(self, ticker, statement_type="income-statement", period="quarter", limit=20):
        # statement_type: income-statement, balance-sheet-statement, cash-flow-statement
        # period: quarter or annual
        return self.request("GET", f"/{statement_type}/{ticker}", params={"period": period, "limit": limit},
                            api_source_name="fmp")

    def get_key_metrics(self, ticker, period="quarter", limit=20):  # TTM available with different endpoint
        # Provides P/E, P/B, ROE, Debt to Equity, etc.
        return self.request("GET", f"/key-metrics/{ticker}", params={"period": period, "limit": limit},
                            api_source_name="fmp")

    def get_company_profile(self, ticker):
        return self.request("GET", f"/profile/{ticker}", params={}, api_source_name="fmp")

    # Add more FMP endpoints


class EODHDClient(APIClient):
    def __init__(self):
        # EODHD often needs 'fmt=json' and api_token in query params
        super().__init__("https://eodhistoricaldata.com/api", api_key_name="api_token", api_key_value=EODHD_API_KEY)
        self.base_params = {"fmt": "json", "api_token": EODHD_API_KEY}  # Add token to base_params for EODHD

    def request(self, method, endpoint, params=None, data=None, json_data=None, use_cache=True,
                api_source_name="eodhd"):  # Override to merge base_params
        url = f"{self.base_url}{endpoint}"
        request_specific_params = params.copy() if params else {}

        # ticker is usually part of endpoint path for EODHD, not a param for the base call
        # e.g. /fundamentals/{ticker_symbol.US}
        # Ensure token and fmt are always there
        final_params = self.base_params.copy()
        final_params.update(request_specific_params)

        # Rebuild cache key for EODHD since its param structure is a bit different
        sorted_params = sorted(final_params.items()) if final_params else []
        cache_key_str = f"{method.upper()}:{url}?{json.dumps(sorted_params)}"

        if use_cache:
            cached_data = self._get_cached_response(cache_key_str)
            if cached_data:
                return cached_data

        # Actual request logic (copied and adapted from parent to ensure correct param usage)
        for attempt in range(API_RETRY_ATTEMPTS):
            try:
                response = requests.request(
                    method, url, params=final_params, data=data, json=json_data,
                    headers=self.headers, timeout=API_REQUEST_TIMEOUT
                )
                response.raise_for_status()
                response_json = response.json()
                if use_cache:
                    self._cache_response(cache_key_str, response_json, api_source_name)
                return response_json
            except requests.exceptions.HTTPError as e:
                # (Same error handling as parent)
                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url} with params {final_params}: {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429 or 500 <= e.response.status_code < 600:
                    time.sleep(API_RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"Client error for {url}: {e}", exc_info=True)
                    return None
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")

            if attempt < API_RETRY_ATTEMPTS - 1:
                time.sleep(API_RETRY_DELAY)
            else:
                logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}.")
                return None
        return None

    def get_fundamental_data(self, ticker_with_exchange):  # e.g., "AAPL.US"
        # Contains financial statements, ratios, company info
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}", api_source_name="eodhd")

    def get_ipo_calendar(self, from_date=None, to_date=None):  # Dates YYYY-MM-DD
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd")

    # Add more EODHD endpoints


class RapidAPIUpcomingIPOCalendarClient(APIClient):
    def __init__(self):
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_UPCOMING_IPO_KEY,
            "X-RapidAPI-Host": "upcoming-ipo-calendar.p.rapidapi.com"  # Verify host from RapidAPI docs
        }
        super().__init__("https://upcoming-ipo-calendar.p.rapidapi.com", headers=headers)

    def get_ipo_calendar(self):  # Endpoint details may vary
        # RapidAPI endpoints are often specific, e.g., /ipocalendar
        # Check RapidAPI documentation for the exact endpoint path and params
        # For example, if it's just the base URL that gives the calendar:
        return self.request("GET", "/", api_source_name="rapidapi_ipo")  # Assuming base URL gives calendar

    # Add more specific methods if the API has more features


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"  # Adjust if different
        # Key rotation is handled per call
        self.current_key_index = 0

    def _get_next_api_key(self):
        key = GOOGLE_API_KEYS[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(GOOGLE_API_KEYS)
        return key

    def generate_text(self, prompt, model="gemini-pro"):  # or gemini-1.5-flash, etc.
        api_key = self._get_next_api_key()
        url = f"{self.base_url}/{model}:generateContent?key={api_key}"

        # Gemini API has a specific request structure
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            # Optional: Add generationConfig, safetySettings here
            # "generationConfig": {
            #   "temperature": 0.7,
            #   "maxOutputTokens": 1024,
            # },
            # "safetySettings": [ ... ]
        }

        for attempt in range(API_RETRY_ATTEMPTS):  # Simplified retry for Gemini
            try:
                response = requests.post(url, json=payload,
                                         timeout=API_REQUEST_TIMEOUT + 20)  # Longer timeout for generative models
                response.raise_for_status()

                # Check for content and candidates
                response_json = response.json()
                if "candidates" in response_json and response_json["candidates"]:
                    content_part = response_json["candidates"][0].get("content", {}).get("parts", [{}])[0]
                    if "text" in content_part:
                        return content_part["text"]
                    else:
                        logger.error(f"Gemini response missing text in content part: {response_json}")
                        return f"Error: No text found in Gemini response. Full response: {response_json}"
                elif "promptFeedback" in response_json and response_json["promptFeedback"].get("blockReason"):
                    block_reason = response_json["promptFeedback"]["blockReason"]
                    logger.error(
                        f"Gemini prompt blocked: {block_reason}. Details: {response_json.get('promptFeedback')}")
                    return f"Error: Prompt blocked by Gemini ({block_reason})."
                else:
                    logger.error(f"Gemini response malformed or missing candidates: {response_json}")
                    return f"Error: Malformed response from Gemini. Full response: {response_json}"

            except requests.exceptions.HTTPError as e:
                logger.warning(
                    f"Gemini API HTTP error on attempt {attempt + 1}: {e.response.status_code} - {e.response.text}")
                # Specific Gemini error handling if needed, e.g. quota exceeded, key invalid
                if e.response.status_code == 429:  # Quota or rate limit
                    logger.info(f"Gemini API rate limit/quota. Trying next key or waiting.")
                    api_key = self._get_next_api_key()  # Try next key immediately on rate limit
                    url = f"{self.base_url}/{model}:generateContent?key={api_key}"
                elif e.response.status_code == 400:  # Bad request, likely prompt issue
                    logger.error(
                        f"Gemini API bad request (400). Prompt: '{prompt[:200]}...'. Response: {e.response.text}")
                    return f"Error: Gemini API bad request. {e.response.text}"
                # Other errors might also benefit from key rotation or just retrying
                time.sleep(API_RETRY_DELAY * (attempt + 1))

            except requests.exceptions.RequestException as e:
                logger.warning(f"Gemini API request error on attempt {attempt + 1}: {e}")
                time.sleep(API_RETRY_DELAY)

        logger.error(f"All attempts to call Gemini API failed for prompt: {prompt[:100]}...")
        return "Error: Could not get response from Gemini API after multiple attempts."

    def summarize_text(self, text_to_summarize, context=""):
        prompt = f"Please summarize the following text. {context}\n\nText:\n\"\"\"\n{text_to_summarize}\n\"\"\"\n\nSummary:"
        return self.generate_text(prompt)

    def analyze_sentiment(self, text_to_analyze):
        prompt = f"Analyze the sentiment of the following text. Classify it as 'Positive', 'Negative', or 'Neutral' and provide a brief explanation. Text:\n\"\"\"\n{text_to_analyze}\n\"\"\"\n\nSentiment Analysis:"
        return self.generate_text(prompt)

    def answer_question_from_text(self, text_block, question):
        prompt = f"Based on the following text, please answer the question.\n\nText:\n\"\"\"\n{text_block}\n\"\"\"\n\nQuestion: {question}\n\nAnswer:"
        return self.generate_text(prompt)

    def interpret_financial_data(self, data_description, data_points, context_prompt):
        """
        Asks Gemini to interpret financial data.
        data_description: e.g., "P/E ratio trend for AAPL"
        data_points: e.g., "2020: 25, 2021: 28, 2022: 22. Industry average P/E: 20."
        context_prompt: e.g., "Is this P/E trend generally favorable or unfavorable for a growth-oriented tech company?"
        """
        prompt = f"Interpret the following financial data:\nDescription: {data_description}\nData: {data_points}\nContext/Question: {context_prompt}\n\nInterpretation:"
        return self.generate_text(prompt)


# Instantiate clients for use in other modules
# These can be singletons if managed carefully or instantiated as needed.
# For simplicity here, other modules will import and instantiate them.
# Example:
# finnhub_client = FinnhubClient()
# fmp_client = FinancialModelingPrepClient()
# eodhd_client = EODHDClient()
# rapidapi_ipo_client = RapidAPIUpcomingIPOCalendarClient()
# gemini_client = GeminiAPIClient()

# --- AlphaVantage and TickerTick ---
# As per prompt, these don't require keys. If they do, similar classes would be built.
# For now, if direct access is needed without a class structure:
def get_alphavantage_data(params):
    # Example: https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=YOUR_KEY
    # Since no key is "needed", their free tier might be very limited or require 'demo' key
    # This function would need to be fleshed out if used.
    # For now, let's assume if data is needed from here, it's via a more generic search or user provides URL.
    logger.info("AlphaVantage: Assuming no API key needed, but free tier may be limited. Not fully implemented here.")
    return None


def get_tickertick_data(params):
    # GitHub indicates it's a local API, so usage would depend on local setup.
    # Not directly callable as a public web API in the same way as others.
    logger.info("TickerTick-API appears to be a local setup. Not implemented as a cloud API client.")
    return None