# api_clients.py
import requests
import time
import json
from datetime import datetime, timedelta, timezone  # Added timezone
from config import (
    GOOGLE_API_KEYS, FINNHUB_API_KEY, FINANCIAL_MODELING_PREP_API_KEY,
    EODHD_API_KEY, RAPIDAPI_UPCOMING_IPO_KEY, API_REQUEST_TIMEOUT,
    API_RETRY_ATTEMPTS, API_RETRY_DELAY, CACHE_EXPIRY_SECONDS
)
from error_handler import logger
from database import SessionLocal
from models import CachedAPIData

current_google_api_key_index = 0


class APIClient:
    def __init__(self, base_url, api_key_name=None, api_key_value=None, headers=None):
        self.base_url = base_url
        self.api_key_name = api_key_name
        self.api_key_value = api_key_value
        self.headers = headers or {}
        if api_key_name and api_key_value:  # For APIs that pass key as query param
            self.params = {api_key_name: api_key_value}
        else:  # For APIs that pass key in headers (like RapidAPI) or have no key for base class
            self.params = {}

    def _get_cached_response(self, request_url_or_params_str):
        session = SessionLocal()
        try:
            current_time_utc = datetime.now(timezone.utc)

            cache_entry = session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str,
                CachedAPIData.expires_at > current_time_utc
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
            expires_at_utc = datetime.now(timezone.utc) + timedelta(seconds=CACHE_EXPIRY_SECONDS)

            session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str).delete()
            session.commit()

            cache_entry = CachedAPIData(
                api_source=api_source,
                request_url_or_params=request_url_or_params_str,
                response_data=response_data,
                timestamp=datetime.now(timezone.utc),
                expires_at=expires_at_utc
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
        current_call_params = params.copy() if params else {}
        full_query_params = self.params.copy()
        full_query_params.update(current_call_params)

        sorted_params = sorted(full_query_params.items()) if full_query_params else []
        param_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        cache_key_str = f"{method.upper()}:{url}?{param_string}"

        if use_cache:
            cached_data = self._get_cached_response(cache_key_str)
            if cached_data:
                return cached_data

        for attempt in range(API_RETRY_ATTEMPTS):
            try:
                response = requests.request(
                    method, url, params=full_query_params, data=data, json=json_data,
                    headers=self.headers, timeout=API_REQUEST_TIMEOUT
                )
                response.raise_for_status()

                response_json = response.json()
                if use_cache:
                    self._cache_response(cache_key_str, response_json, api_source_name)
                return response_json

            except requests.exceptions.HTTPError as e:
                log_params_for_error = {k: (v[:-6] + '******' if k == self.api_key_name and isinstance(v, str) and len(v) > 6 else v) for k,v in full_query_params.items()} # Obfuscate API key for logging
                if not full_query_params and self.headers.get("X-RapidAPI-Key"): # RapidAPI specific key obfuscation
                    log_params_for_error = {"X-RapidAPI-Key": self.headers["X-RapidAPI-Key"][-6:]}


                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url} (Details: {log_params_for_error}): {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429: # Rate limit
                    logger.info(f"Rate limit hit. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))
                elif 500 <= e.response.status_code < 600: # Server error
                    logger.info(f"Server error. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))
                else: # Non-retryable client error (like 403 Forbidden)
                    logger.error(f"Non-retryable client error for {url}: {e.response.status_code} {e.response.reason}",
                                 exc_info=False) # Set exc_info to False for client errors unless debugging specific ones
                    return None # Critical: return None for non-retryable client errors.
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")

            if attempt < API_RETRY_ATTEMPTS - 1:
                time.sleep(API_RETRY_DELAY)
            else:
                logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}. Params: {full_query_params}") # Log final failure
                return None
        return None


class FinnhubClient(APIClient):
    def __init__(self):
        super().__init__("https://finnhub.io/api/v1", api_key_name="token", api_key_value=FINNHUB_API_KEY)

    def get_market_news(self, category="general", min_id=0):
        params = {"category": category}
        if min_id > 0:
            params["minId"] = min_id
        return self.request("GET", "/news", params=params, api_source_name="finnhub")

    def get_company_profile2(self, ticker):
        return self.request("GET", "/stock/profile2", params={"symbol": ticker}, api_source_name="finnhub")

    def get_financials_reported(self, ticker, freq="quarterly"):
        return self.request("GET", "/stock/financials-reported", params={"symbol": ticker, "freq": freq},
                            api_source_name="finnhub")

    def get_basic_financials(self, ticker, metric_type="all"):
        return self.request("GET", "/stock/metric", params={"symbol": ticker, "metric": metric_type},
                            api_source_name="finnhub")

    def get_ipo_calendar(self, from_date=None, to_date=None):
        # Finnhub's IPO calendar endpoint requires date ranges.
        # Default to a sensible range if not provided.
        if from_date is None:
            from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if to_date is None:
            to_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        params = {"from": from_date, "to": to_date}
        return self.request("GET", "/calendar/ipo", params=params, api_source_name="finnhub_ipo")


class FinancialModelingPrepClient(APIClient):
    def __init__(self):
        super().__init__("https://financialmodelingprep.com/api/v3", api_key_name="apikey",
                         api_key_value=FINANCIAL_MODELING_PREP_API_KEY)

    def get_ipo_calendar(self, from_date=None, to_date=None):
        # This client will likely not be used for IPOs anymore due to subscription issues
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        # Log a warning if this is still called for IPOs
        logger.warning("FinancialModelingPrepClient.get_ipo_calendar called, but may be restricted by subscription.")
        return self.request("GET", "/ipo_calendar", params=params, api_source_name="fmp_ipo")

    def get_financial_statements(self, ticker, statement_type="income-statement", period="quarter", limit=20):
        return self.request("GET", f"/{statement_type}/{ticker}", params={"period": period, "limit": limit},
                            api_source_name="fmp")

    def get_key_metrics(self, ticker, period="quarter", limit=20):
        return self.request("GET", f"/key-metrics/{ticker}", params={"period": period, "limit": limit},
                            api_source_name="fmp")

    def get_company_profile(self, ticker):
        return self.request("GET", f"/profile/{ticker}", params={}, api_source_name="fmp")


class EODHDClient(APIClient):
    def __init__(self):
        super().__init__("https://eodhistoricaldata.com/api", api_key_name="api_token", api_key_value=EODHD_API_KEY)
        self.params["fmt"] = "json" # Default format for this client

    def get_fundamental_data(self, ticker_with_exchange):
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}",
                            api_source_name="eodhd")

    def get_ipo_calendar(self, from_date=None, to_date=None):
        # This client will likely not be used for IPOs anymore due to subscription issues
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        # Log a warning if this is still called for IPOs
        logger.warning("EODHDClient.get_ipo_calendar called, but may be restricted by subscription.")
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd_ipo")


class RapidAPIUpcomingIPOCalendarClient(APIClient):
    def __init__(self):
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_UPCOMING_IPO_KEY,
            "X-RapidAPI-Host": "upcoming-ipo-calendar.p.rapidapi.com"
        }
        super().__init__("https://upcoming-ipo-calendar.p.rapidapi.com", headers=headers)

    def get_ipo_calendar(self):
        # This client will likely not be used for IPOs anymore due to subscription issues
        logger.warning("RapidAPIUpcomingIPOCalendarClient.get_ipo_calendar called, but may be restricted by subscription.")
        return self.request("GET", "/ipo-calendar", params=None, api_source_name="rapidapi_ipo")


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        self.current_key_index = 0 # This will be managed per instance now for key rotation

    def _get_next_api_key_for_attempt(self, overall_attempt_num, max_attempts_per_key, total_keys):
        key_group_index = (overall_attempt_num // max_attempts_per_key) % total_keys
        api_key = GOOGLE_API_KEYS[key_group_index]
        current_retry_for_this_key = (overall_attempt_num % max_attempts_per_key) + 1
        logger.debug(
            f"Gemini: Using key ...{api_key[-4:]} (Index {key_group_index}), Attempt {current_retry_for_this_key}/{max_attempts_per_key}")
        return api_key, current_retry_for_this_key


    def generate_text(self, prompt, model="gemini-pro"):
        max_attempts_per_key = API_RETRY_ATTEMPTS
        total_keys = len(GOOGLE_API_KEYS)
        if total_keys == 0:
            logger.error("Gemini: No API keys configured for Google API.")
            return "Error: No Google API keys configured."

        for overall_attempt_num in range(total_keys * max_attempts_per_key):
            api_key, current_retry_for_this_key = self._get_next_api_key_for_attempt(
                overall_attempt_num, max_attempts_per_key, total_keys
            )
            url = f"{self.base_url}/{model}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.7,
                    "maxOutputTokens": 8192, # Adjusted from 65536, check API limits for gemini-pro
                },
                "safetySettings": [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                ]
            }

            try:
                response = requests.post(url, json=payload, timeout=API_REQUEST_TIMEOUT + 60) # Increased timeout for Gemini
                response.raise_for_status()
                response_json = response.json()

                if "promptFeedback" in response_json and response_json["promptFeedback"].get("blockReason"):
                    block_reason = response_json["promptFeedback"]["blockReason"]
                    block_details = response_json["promptFeedback"].get("safetyRatings", "")
                    logger.error(
                        f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {block_reason}. Details: {block_details}. Prompt snippet: '{prompt[:100]}...'")
                    # If blocked for safety, it's specific to this prompt and key combo. Loop will try next.
                    time.sleep(API_RETRY_DELAY) # Wait before next attempt (could be next key)
                    continue

                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    # Valid finish reasons: "STOP", "MAX_TOKENS", "MODEL_LENGTH" (or None if implicit stop)
                    # Other reasons like "SAFETY", "RECITATION", "OTHER" are problematic.
                    finish_reason = candidate.get("finishReason")
                    if finish_reason not in [None, "STOP", "MAX_TOKENS", "MODEL_LENGTH"]:
                        logger.warning(
                            f"Gemini candidate finished with unexpected reason: {finish_reason}. Prompt: '{prompt[:100]}...'")
                        # Depending on the reason, might need to retry or abort. For now, log and continue to extract text if possible.

                    content_part = candidate.get("content", {}).get("parts", [{}])[0]
                    if "text" in content_part:
                        return content_part["text"]
                    else:
                        logger.error(
                            f"Gemini response missing text in content part for key ...{api_key[-4:]}: {response_json}")
                else:
                    logger.error(
                        f"Gemini response malformed or missing candidates for key ...{api_key[-4:]}: {response_json}")

            except requests.exceptions.HTTPError as e:
                logger.warning(
                    f"Gemini API HTTP error for key ...{api_key[-4:]} on attempt {current_retry_for_this_key}/{max_attempts_per_key}: {e.response.status_code} - {e.response.text}. Prompt: '{prompt[:100]}...'")
                if e.response.status_code == 400: # Bad Request (often malformed prompt/payload)
                    logger.error(
                        f"Gemini API Bad Request (400). This is likely a persistent issue with the prompt/payload. Aborting Gemini for this call. Response: {e.response.text}")
                    return f"Error: Gemini API bad request (400). {e.response.text}"
                # Other HTTP errors (429, 5xx) will be retried with the next key/attempt by the loop
            except requests.exceptions.RequestException as e: # Timeout, ConnectionError
                logger.warning(
                    f"Gemini API request error for key ...{api_key[-4:]} on attempt {current_retry_for_this_key}/{max_attempts_per_key}: {e}. Prompt: '{prompt[:100]}...'")

            # Wait before next attempt (could be next retry for this key, or first retry for next key)
            if overall_attempt_num < (total_keys * max_attempts_per_key) - 1 : # If not the very last attempt of all
                 time.sleep(API_RETRY_DELAY * (current_retry_for_this_key)) # Exponential backoff for retries on same key


        logger.error(
            f"All attempts ({total_keys * max_attempts_per_key}) to call Gemini API failed for prompt: {prompt[:100]}...")
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


def get_alphavantage_data(params):
    # This function seems unused by the core logic. Placeholder.
    logger.info("AlphaVantage: Not fully implemented. Assumed no API key or using 'demo'. Free tier is limited.")
    return None


def get_tickertick_data(params):
    # This function seems unused by the core logic. Placeholder.
    logger.info("TickerTick-API appears to be a local setup. Not implemented as a cloud API client.")
    return None