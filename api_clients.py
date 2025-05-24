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
            # Assuming expires_at is stored as timezone-aware UTC in DB
            # or as naive UTC (which is less robust but common)
            # For this example, let's be explicit with UTC.
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

            # Delete any existing cache for this key to avoid unique constraint violations
            session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str).delete()
            # A commit here might be too chatty for DB if many deletes/adds happen.
            # Some DBs might handle "upsert" logic better, or commit once after add.
            # For simplicity, explicit delete then add. If performance is an issue, optimize.
            session.commit()

            cache_entry = CachedAPIData(
                api_source=api_source,
                request_url_or_params=request_url_or_params_str,
                response_data=response_data,
                # Ensure timestamp and expires_at are consistently timezone-aware or naive.
                # Storing as UTC is best practice.
                timestamp=datetime.now(timezone.utc),  # Add current timestamp for the cache entry itself
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

        # These are parameters specific to THIS request call
        current_call_params = params.copy() if params else {}

        # Merge with base params (like api_key if it's a query param type)
        # self.params usually holds the API key if passed as a query parameter.
        # For RapidAPI, self.params will be empty as key is in header.
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
                # Pass full_query_params to requests' params argument
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
                log_params_for_error = full_query_params if full_query_params else self.headers.get("X-RapidAPI-Key",
                                                                                                    "NoKeyInHeader")[
                                                                                   -6:]  # Log last 6 of key for RapidAPI for identification
                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url} (Details: {log_params_for_error}): {e.response.status_code} - {e.response.text}")
                if e.response.status_code == 429:
                    logger.info(f"Rate limit hit. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))
                elif 500 <= e.response.status_code < 600:
                    logger.info(f"Server error. Waiting for {API_RETRY_DELAY * (attempt + 1)} seconds.")
                    time.sleep(API_RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"Non-retryable client error for {url}: {e.response.status_code} {e.response.reason}",
                                 exc_info=False)
                    return None
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")

            if attempt < API_RETRY_ATTEMPTS - 1:
                time.sleep(API_RETRY_DELAY)
            else:
                logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}. Params: {full_query_params}")
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
        self.params["fmt"] = "json"

    def get_fundamental_data(self, ticker_with_exchange):
        # EODHD ticker is usually part of path, not a param for the base request
        # Example: AAPL.US - ensure .US (or other exchange) is appended correctly before calling
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}",
                            api_source_name="eodhd")  # No extra params typically

    def get_ipo_calendar(self, from_date=None, to_date=None):
        params = {}  # api_token and fmt are already in self.params
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd")


class RapidAPIUpcomingIPOCalendarClient(APIClient):
    def __init__(self):
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_UPCOMING_IPO_KEY,
            "X-RapidAPI-Host": "upcoming-ipo-calendar.p.rapidapi.com"
        }
        # Key is in headers, so no api_key_name/value for base class params
        super().__init__("https://upcoming-ipo-calendar.p.rapidapi.com", headers=headers)

    def get_ipo_calendar(self):
        # CORRECTED ENDPOINT based on your screenshot's cURL example
        # No additional query parameters are typically needed for this specific endpoint based on common RapidAPI patterns for "get all" lists.
        return self.request("GET", "/ipo-calendar", params=None, api_source_name="rapidapi_ipo")


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        self.current_key_index = 0

    def _get_next_api_key(self):
        key = GOOGLE_API_KEYS[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(GOOGLE_API_KEYS)
        logger.debug(f"Using Google API Key index: {self.current_key_index} (Key ending: ...{key[-4:]})")
        return key

    def generate_text(self, prompt, model="gemini-pro"):

        initial_key_index = self.current_key_index
        max_attempts_per_key = API_RETRY_ATTEMPTS
        total_keys = len(GOOGLE_API_KEYS)

        for overall_attempt_num in range(total_keys * max_attempts_per_key):
            current_key_attempt = (overall_attempt_num % max_attempts_per_key)

            # Rotate key only after all retries for the current key are exhausted
            if current_key_attempt == 0 and overall_attempt_num > 0:  # (and not the very first attempt)
                pass  # Key already rotated by previous iteration's end or _get_next_api_key will handle initial cycle

            # Always get key for this attempt cycle
            # If overall_attempt_num is a multiple of max_attempts_per_key, it's time to advance the key.
            # This logic might be tricky; simpler is to just call _get_next_api_key() IF current_key_attempt == 0 and overall_attempt_num > 0
            # For now, let's assume _get_next_api_key at start of loop is fine for general cycling.

            # More direct key cycling:
            # After (total_keys * max_attempts_per_key) it means we tried all keys, all retries.
            # current_key_idx_for_this_round = (overall_attempt_num // max_attempts_per_key) % total_keys
            # api_key = GOOGLE_API_KEYS[current_key_idx_for_this_round]
            # logger.debug(f"Using Google API Key index: {current_key_idx_for_this_round} (Key ending: ...{api_key[-4:]}) for attempt {current_key_attempt+1}")

            # Simpler: rely on _get_next_api_key to cycle, and the loop handles retries.
            # This means a key is tried, then next key, then next... then wrap around for retries.
            # Let's adjust to: try one key for all its retries, THEN switch.

            # Determine which key to use based on overall_attempt_num / max_attempts_per_key
            key_group_index = (overall_attempt_num // max_attempts_per_key)
            if key_group_index >= total_keys:  # Should not happen with loop range
                logger.error("Gemini key index out of bounds, aborting.")
                break

            api_key = GOOGLE_API_KEYS[key_group_index]
            current_retry_for_this_key = current_key_attempt + 1

            logger.debug(
                f"Gemini: Using key ...{api_key[-4:]} (Index {key_group_index}), Attempt {current_retry_for_this_key}/{max_attempts_per_key}")

            url = f"{self.base_url}/{model}:generateContent?key={api_key}"

            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.7,
                    "maxOutputTokens": 65536,  # Increased
                },
                "safetySettings": [
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                ]
            }

            try:
                response = requests.post(url, json=payload, timeout=API_REQUEST_TIMEOUT + 30)
                response.raise_for_status()

                response_json = response.json()

                if "promptFeedback" in response_json and response_json["promptFeedback"].get("blockReason"):
                    block_reason = response_json["promptFeedback"]["blockReason"]
                    block_details = response_json["promptFeedback"].get("safetyRatings", "")
                    logger.error(
                        f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {block_reason}. Details: {block_details}. Prompt snippet: '{prompt[:100]}...'")
                    # If blocked for safety, this key might be problematic for this prompt.
                    # The loop will move to the next key after retries for this key are done.
                    if current_retry_for_this_key == max_attempts_per_key and key_group_index == total_keys - 1:
                        return f"Error: Prompt blocked by Gemini, and all keys tried. Reason: ({block_reason})."  # Last key, last attempt
                    time.sleep(API_RETRY_DELAY)
                    continue  # Go to next attempt (which might be next retry for this key, or first retry for next key)

                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    if candidate.get("finishReason") not in [None, "STOP",
                                                             "MODEL_LENGTH"]:  # MAX_TOKENS changed to MODEL_LENGTH for some Gemini versions.
                        # Also allow MAX_TOKENS as a valid (though possibly truncated) finish.
                        # Let's treat MAX_TOKENS as potentially recoverable if we shorten prompt or increase output.
                        # For now, if it's not STOP or MODEL_LENGTH/MAX_TOKENS, it's more concerning.
                        if candidate.get("finishReason") not in ["MAX_TOKENS"]:  # Log others as warnings
                            logger.warning(
                                f"Gemini candidate finished with reason: {candidate.get('finishReason')}. Prompt: '{prompt[:100]}...'")
                        else:  # MAX_TOKENS / MODEL_LENGTH
                            logger.info(
                                f"Gemini candidate finished due to MAX_TOKENS/MODEL_LENGTH. Response might be truncated. Prompt: '{prompt[:100]}...'")

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
                if e.response.status_code == 400:
                    logger.error(
                        f"Gemini API Bad Request (400). This is likely a persistent issue with the prompt/payload. Aborting Gemini for this call. Response: {e.response.text}")
                    return f"Error: Gemini API bad request (400). {e.response.text}"

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Gemini API request error for key ...{api_key[-4:]} on attempt {current_retry_for_this_key}/{max_attempts_per_key}: {e}. Prompt: '{prompt[:100]}...'")

            # If this was the last retry for the current key, and it's not the last key overall, the outer loop will proceed to the next key group.
            # If this was the last retry for the last key, the loop will terminate.
            if current_retry_for_this_key < max_attempts_per_key:  # If more retries for current key
                time.sleep(API_RETRY_DELAY)
            # else: it will loop to next key group or finish

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
    logger.info("AlphaVantage: Not fully implemented. Assumed no API key or using 'demo'. Free tier is limited.")
    return None


def get_tickertick_data(params):
    logger.info("TickerTick-API appears to be a local setup. Not implemented as a cloud API client.")
    return None