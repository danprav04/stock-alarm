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
            # Ensure timezone aware comparison if database stores timezone-aware datetimes
            # For simplicity, assuming database stores naive UTC or comparison handles it.
            # If CachedAPIData.expires_at is timezone-aware, datetime.now() needs to be too.
            # Example: datetime.now(timezone.utc) if expires_at is tz-aware UTC.
            # For now, assuming direct comparison works or DB handles timezone conversion implicitly.

            # A more robust way for timezone-aware comparison if expires_at column type has timezone:
            # current_time = datetime.now(CachedAPIData.expires_at.expression.type.timezone_awareness)
            # However, if it's just a standard DateTime field that's naive UTC:
            current_time = datetime.utcnow()  # If storing naive UTC timestamps

            cache_entry = session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str,
                CachedAPIData.expires_at > current_time  # Use consistent datetime object for comparison
            ).first()
            if cache_entry:
                logger.info(f"Cache hit for: {request_url_or_params_str}")
                return cache_entry.response_data
        except Exception as e:
            logger.error(f"Error reading from cache for '{request_url_or_params_str}': {e}", exc_info=True)
        finally:
            session.close()
        return None

    def _cache_response(self, request_url_or_params_str, response_data, api_source):
        session = SessionLocal()
        try:
            # If storing naive UTC
            expires_at_utc = datetime.utcnow() + timedelta(seconds=CACHE_EXPIRY_SECONDS)

            # Delete any existing cache for this key to avoid unique constraint violations if we re-cache
            session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str).delete()
            session.commit()  # Commit delete before adding new one

            cache_entry = CachedAPIData(
                api_source=api_source,
                request_url_or_params=request_url_or_params_str,
                response_data=response_data,
                expires_at=expires_at_utc  # Use naive UTC
            )
            session.add(cache_entry)
            session.commit()
            logger.info(f"Cached response for: {request_url_or_params_str}")
        except Exception as e:
            logger.error(f"Error writing to cache for '{request_url_or_params_str}': {e}", exc_info=True)
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
        # Using json.dumps for params part of cache key to handle complex structures if any, though usually simple dicts.
        param_string = "&".join([f"{k}={v}" for k, v in sorted_params])  # More standard query string like for cache key
        cache_key_str = f"{method.upper()}:{url}?{param_string}"

        if use_cache:
            # CORRECTED LINE HERE:
            cached_data = self._get_cached_response(cache_key_str)
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
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url} (Params: {full_params}): {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:  # Rate limit
                    logger.info(f"Rate limit hit. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))  # Exponential backoff can be better
                elif 500 <= e.response.status_code < 600:  # Server-side error
                    logger.info(f"Server error. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))
                else:  # Other client errors (4xx) usually don't benefit from retries if not rate limit
                    logger.error(f"Non-retryable client error for {url}: {e}",
                                 exc_info=False)  # No need for full exc_info for typical 400/401/403/404
                    return None  # Or raise custom error
            except requests.exceptions.RequestException as e:  # Covers DNS, ConnectionTimeout, etc.
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")

            if attempt < API_RETRY_ATTEMPTS - 1:
                time.sleep(API_RETRY_DELAY)
            else:
                logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}. Params: {full_params}")
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
        # FMP IPO calendar can return empty list if no IPOs, which is valid JSON.
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
        # The base_url does not include the final / so endpoints should start with /
        super().__init__("https://eodhistoricaldata.com/api", api_key_name="api_token", api_key_value=EODHD_API_KEY)
        # EODHD requires token and fmt to be part of every request's parameters.
        # We'll ensure the parent class's params handling correctly uses self.params which holds the api_token.
        # And we'll add 'fmt':'json' to specific calls if not globally applied, or ensure it's in self.params.
        self.params["fmt"] = "json"  # Add fmt=json globally for this client

    def request(self, method, endpoint, params=None, data=None, json_data=None, use_cache=True,
                api_source_name="eodhd"):
        # EODHD's ticker often forms part of the path, e.g., /fundamentals/AAPL.US
        # The common params like api_token and fmt are already in self.params from __init__
        # So, the generic self.request method should work fine.
        # No need to override usually, unless EODHD has very specific parameter merging logic not covered by parent.
        # For EODHD, the ticker is usually part of the endpoint string itself.
        # Example: endpoint = f"/fundamentals/{ticker_symbol_with_exchange}"

        # Rebuild cache key for EODHD to ensure 'fmt=json' and 'api_token' are consistently ordered if params were dynamic
        # However, self.params ensures they are always present in full_params which is used for cache_key_str.
        # The parent's cache_key_str should be fine.

        # Let's simplify by calling parent's request directly.
        # If specific EODHD behavior for params or cache key is needed, this is where it would go.
        return super().request(method, endpoint, params=params, data=data, json_data=json_data, use_cache=use_cache,
                               api_source_name=api_source_name)

    def get_fundamental_data(self, ticker_with_exchange):  # e.g., "AAPL.US"
        # Contains financial statements, ratios, company info
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}.US",
                            api_source_name="eodhd")  # Added .US suffix for common case, adjust if needed

    def get_ipo_calendar(self, from_date=None, to_date=None):  # Dates YYYY-MM-DD
        params = {}  # api_token and fmt will be added by parent class
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        # Note: EODHD /calendar/ipos might require subscription beyond free tier.
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd")

    # Add more EODHD endpoints


class RapidAPIUpcomingIPOCalendarClient(APIClient):
    def __init__(self):
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_UPCOMING_IPO_KEY,
            "X-RapidAPI-Host": "upcoming-ipo-calendar.p.rapidapi.com"  # Verify host from RapidAPI docs
        }
        # For RapidAPI, the key is in headers, not params. So api_key_name/value are None for base class.
        super().__init__("https://upcoming-ipo-calendar.p.rapidapi.com", headers=headers)

    def get_ipo_calendar(self):  # Endpoint details may vary
        # RapidAPI endpoints are often specific, e.g., /ipocalendar
        # Check RapidAPI documentation for the exact endpoint path and params
        # Assuming base URL or a specific path like "/list" gives the calendar:
        return self.request("GET", "/", api_source_name="rapidapi_ipo")  # Adjust endpoint as needed

    # Add more specific methods if the API has more features


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"  # Adjust if different
        # Key rotation is handled per call
        self.current_key_index = 0

    def _get_next_api_key(self):
        # This simple rotation doesn't track which keys specifically failed, just cycles.
        # A more robust solution might involve removing known bad/over-limit keys from rotation temporarily.
        key = GOOGLE_API_KEYS[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(GOOGLE_API_KEYS)
        logger.debug(f"Using Google API Key index: {self.current_key_index} (Key ending: ...{key[-4:]})")
        return key

    def generate_text(self, prompt, model="gemini-2.5-flash-preview-05-20"):  # or gemini-1.5-flash, etc.

        initial_key_index = self.current_key_index  # To detect if all keys have been tried

        for attempt in range(len(GOOGLE_API_KEYS) * API_RETRY_ATTEMPTS):  # Max attempts across all keys
            api_key = self._get_next_api_key()  # Rotate key first
            url = f"{self.base_url}/{model}:generateContent?key={api_key}"

            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {  # Sensible defaults
                    "temperature": 0.7,
                    "maxOutputTokens": 2048,  # Increased for potentially longer analyses
                },
                "safetySettings": [  # Default safety settings
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                ]
            }

            try:
                response = requests.post(url, json=payload,
                                         timeout=API_REQUEST_TIMEOUT + 30)  # Longer timeout for generative models
                response.raise_for_status()  # Check for HTTP errors first

                response_json = response.json()

                if "promptFeedback" in response_json and response_json["promptFeedback"].get("blockReason"):
                    block_reason = response_json["promptFeedback"]["blockReason"]
                    block_details = response_json["promptFeedback"].get("safetyRatings", "")
                    logger.error(
                        f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {block_reason}. Details: {block_details}. Prompt snippet: '{prompt[:100]}...'")
                    # If blocked for safety, retrying with same prompt and different key might not help.
                    # Consider if this should immediately fail or try other keys. For now, it will try other keys.
                    # If it's a specific key issue (e.g. permissions), other keys might work.
                    if attempt % API_RETRY_ATTEMPTS == API_RETRY_ATTEMPTS - 1 and self.current_key_index == initial_key_index:  # All keys tried for this prompt with retries
                        return f"Error: Prompt blocked by Gemini for all keys ({block_reason})."
                    time.sleep(API_RETRY_DELAY)  # Wait before trying next key or retry
                    continue  # Try next key or retry current key

                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    if candidate.get("finishReason") not in [None, "STOP"]:  # Check for abnormal finish reasons
                        logger.warning(
                            f"Gemini candidate finished with reason: {candidate.get('finishReason')}. Prompt: '{prompt[:100]}...'")
                        # If finishReason is MAX_TOKENS, the response might be truncated.
                        # If SAFETY, it's similar to promptFeedback block.
                        # For now, we'll try to extract text if available.

                    content_part = candidate.get("content", {}).get("parts", [{}])[0]
                    if "text" in content_part:
                        return content_part["text"]
                    else:
                        logger.error(
                            f"Gemini response missing text in content part for key ...{api_key[-4:]}: {response_json}")
                else:  # No candidates or malformed
                    logger.error(
                        f"Gemini response malformed or missing candidates for key ...{api_key[-4:]}: {response_json}")

            except requests.exceptions.HTTPError as e:
                logger.warning(
                    f"Gemini API HTTP error for key ...{api_key[-4:]} on attempt {(attempt % API_RETRY_ATTEMPTS) + 1}/{API_RETRY_ATTEMPTS}: {e.response.status_code} - {e.response.text}. Prompt: '{prompt[:100]}...'")
                # 400 Bad Request (e.g. invalid model, malformed payload) - often not recoverable by retry with same params
                # 429 Resource Exhausted (quota) - good candidate for key rotation / backoff
                if e.response.status_code == 400:
                    logger.error(
                        f"Gemini API Bad Request (400). Check model name and payload structure. Response: {e.response.text}")
                    # This is likely a persistent issue with the request itself, not the key.
                    # Stop trying for this prompt to avoid burning through all keys for a bad request.
                    return f"Error: Gemini API bad request (400). {e.response.text}"
                # For other errors (like 429, 500s), the loop will continue to retry/rotate keys.

            except requests.exceptions.RequestException as e:  # Timeout, ConnectionError
                logger.warning(
                    f"Gemini API request error for key ...{api_key[-4:]} on attempt {(attempt % API_RETRY_ATTEMPTS) + 1}/{API_RETRY_ATTEMPTS}: {e}. Prompt: '{prompt[:100]}...'")

            # If all retries for current key are exhausted, or if all keys tried once and back to start
            if (attempt % API_RETRY_ATTEMPTS) == API_RETRY_ATTEMPTS - 1:  # Exhausted retries for current key
                logger.info(f"Exhausted retries for key ...{api_key[-4:]}. Moving to next key if available.")

            if self.current_key_index == initial_key_index and (attempt % API_RETRY_ATTEMPTS) == API_RETRY_ATTEMPTS - 1:
                logger.error(
                    f"All Google API keys have been tried {API_RETRY_ATTEMPTS} times for this prompt and failed. Prompt: '{prompt[:100]}...'")
                return f"Error: Could not get response from Gemini API after trying all keys with {API_RETRY_ATTEMPTS} retries each."

            time.sleep(API_RETRY_DELAY)  # Wait before next attempt (either retry on same key or next key)

        logger.error(f"All attempts to call Gemini API failed for prompt: {prompt[:100]}...")
        return "Error: Could not get response from Gemini API after multiple attempts across all keys."

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
        prompt = f"Interpret the following financial data:\nDescription: {data_description}\nData: {data_points}\nContext/Question: {context_prompt}\n\nInterpretation:"
        return self.generate_text(prompt)


# --- AlphaVantage and TickerTick ---
def get_alphavantage_data(params):
    logger.info("AlphaVantage: Not fully implemented. Assumed no API key or using 'demo'. Free tier is limited.")
    # Example base: url = "https://www.alphavantage.co/query"
    # full_params = {"apikey": "demo", **params} # Add 'demo' or your key
    # response = requests.get(url, params=full_params)
    return None


def get_tickertick_data(params):
    logger.info("TickerTick-API appears to be a local setup. Not implemented as a cloud API client.")
    return None
