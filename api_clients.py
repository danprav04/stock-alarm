# api_clients.py
import requests
import time
import json
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup  # For news scraping
import re  # For S-1/10-K parsing

from config import (
    GOOGLE_API_KEYS, FINNHUB_API_KEY, FINANCIAL_MODELING_PREP_API_KEY,
    EODHD_API_KEY, RAPIDAPI_UPCOMING_IPO_KEY, API_REQUEST_TIMEOUT,
    API_RETRY_ATTEMPTS, API_RETRY_DELAY, CACHE_EXPIRY_SECONDS, EDGAR_USER_AGENT
)
from error_handler import logger
from database import SessionLocal, get_db_session
from models import CachedAPIData

current_google_api_key_index = 0


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
            # Ensure all datetimes are offset-aware (UTC) for comparison
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
            # Ensure all datetimes are offset-aware (UTC)
            now_utc = datetime.now(timezone.utc)
            expires_at_utc = now_utc + timedelta(seconds=CACHE_EXPIRY_SECONDS)

            session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str).delete()
            session.commit()

            cache_entry = CachedAPIData(
                api_source=api_source,
                request_url_or_params=request_url_or_params_str,
                response_data=response_data,
                timestamp=now_utc,  # Store as UTC
                expires_at=expires_at_utc  # Store as UTC
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
                api_source_name="unknown", is_json_response=True):
        url = f"{self.base_url}{endpoint}"
        current_call_params = params.copy() if params else {}
        full_query_params = self.params.copy()
        full_query_params.update(current_call_params)

        # Create cache key string
        sorted_params = sorted(full_query_params.items()) if full_query_params else []
        param_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        cache_key_str = f"{method.upper()}:{url}?{param_string}"
        # Add json_data to cache key if it exists, ensuring order for consistency
        if json_data:
            sorted_json_data = json.dumps(json_data, sort_keys=True)
            cache_key_str += f"|BODY:{sorted_json_data}"

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

                if not is_json_response:  # For non-JSON responses like fetching file content
                    if use_cache:
                        self._cache_response(cache_key_str, response.text, api_source_name)
                    return response.text

                response_json = response.json()
                if use_cache:
                    self._cache_response(cache_key_str, response_json, api_source_name)
                return response_json

            except requests.exceptions.HTTPError as e:
                log_params_for_error = {
                    k: (v[:-6] + '******' if k == self.api_key_name and isinstance(v, str) and len(v) > 6 else v) for
                    k, v in full_query_params.items()}
                if not full_query_params and self.headers.get("X-RapidAPI-Key"):
                    log_params_for_error = {"X-RapidAPI-Key": self.headers["X-RapidAPI-Key"][-6:] + "******"}

                status_code = e.response.status_code
                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url} (Details: {log_params_for_error}): {status_code} - {e.response.text}")
                if status_code == 429:  # Rate limit
                    delay = API_RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Rate limit hit. Waiting for {delay} seconds.")
                    time.sleep(delay)
                elif 500 <= status_code < 600:  # Server error
                    delay = API_RETRY_DELAY * (2 ** attempt)
                    logger.info(f"Server error. Waiting for {delay} seconds.")
                    time.sleep(delay)
                else:  # Non-retryable client error
                    logger.error(f"Non-retryable client error for {url}: {status_code} {e.response.reason}",
                                 exc_info=False)
                    return None
            except requests.exceptions.RequestException as e:  # Timeout, ConnectionError
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")
                if attempt < API_RETRY_ATTEMPTS - 1:
                    delay = API_RETRY_DELAY * (2 ** attempt)
                    time.sleep(delay)
            except json.JSONDecodeError as e:
                logger.error(
                    f"JSON decode error for {url} on attempt {attempt + 1}. Response text: {response.text[:500]}... Error: {e}")
                if attempt < API_RETRY_ATTEMPTS - 1:
                    delay = API_RETRY_DELAY * (2 ** attempt)
                    time.sleep(delay)  # Wait before retrying what might be a transient issue
                else:
                    return None  # Failed after retries

        logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}. Params: {full_query_params}")
        return None


class FinnhubClient(APIClient):
    def __init__(self):
        super().__init__("https://finnhub.io/api/v1", api_key_name="token", api_key_value=FINNHUB_API_KEY)

    def get_market_news(self, category="general", min_id=0):
        params = {"category": category}
        if min_id > 0:
            params["minId"] = min_id
        return self.request("GET", "/news", params=params, api_source_name="finnhub_news")

    def get_company_profile2(self, ticker):
        return self.request("GET", "/stock/profile2", params={"symbol": ticker}, api_source_name="finnhub_profile")

    def get_financials_reported(self, ticker, freq="quarterly", years=None):
        # Finnhub's financials-reported can be quite large.
        # It doesn't directly support a 'years' or 'limit' parameter for number of periods in the same way FMP does.
        # We fetch all available and then filter if needed in the application logic.
        params = {"symbol": ticker, "freq": freq}
        return self.request("GET", "/stock/financials-reported", params=params, api_source_name="finnhub_financials")

    def get_basic_financials(self, ticker, metric_type="all"):
        return self.request("GET", "/stock/metric", params={"symbol": ticker, "metric": metric_type},
                            api_source_name="finnhub_metrics")

    def get_ipo_calendar(self, from_date=None, to_date=None):
        if from_date is None:
            from_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        if to_date is None:
            to_date = (datetime.now(timezone.utc) + timedelta(days=90)).strftime('%Y-%m-%d')
        params = {"from": from_date, "to": to_date}
        return self.request("GET", "/calendar/ipo", params=params, api_source_name="finnhub_ipo_calendar")

    def get_sec_filings(self, ticker, from_date=None, to_date=None):
        if from_date is None:
            from_date = (datetime.now(timezone.utc) - timedelta(days=365 * 2)).strftime(
                '%Y-%m-%d')  # Default to last 2 years
        if to_date is None:
            to_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        params = {"symbol": ticker, "from": from_date, "to": to_date}
        return self.request("GET", "/stock/filings", params=params, api_source_name="finnhub_filings")


class FinancialModelingPrepClient(APIClient):
    def __init__(self):
        super().__init__("https://financialmodelingprep.com/api/v3", api_key_name="apikey",
                         api_key_value=FINANCIAL_MODELING_PREP_API_KEY)

    def get_ipo_calendar(self, from_date=None, to_date=None):  # FMP's IPO calendar is often limited on free/low tiers
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        logger.warning("FinancialModelingPrepClient.get_ipo_calendar called, but may be restricted by subscription.")
        return self.request("GET", "/ipo_calendar", params=params, api_source_name="fmp_ipo_calendar")

    def get_financial_statements(self, ticker, statement_type="income-statement", period="quarter", limit=20):
        # FMP limit is often 5 for free tier, or 120 for annuals on paid.
        # Be mindful of API limits.
        actual_limit = limit
        if period == "annual" and limit > 10:  # Adjust if asking for many annuals
            actual_limit = 10  # Max reasonable for annuals usually
        elif period == "quarter" and limit > 40:  # Max reasonable for quarters
            actual_limit = 40

        return self.request("GET", f"/{statement_type}/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_{statement_type.split('-')[0]}")  # e.g. fmp_income

    def get_key_metrics(self, ticker, period="quarter", limit=20):
        actual_limit = limit
        if period == "annual" and limit > 10:
            actual_limit = 10
        elif period == "quarter" and limit > 40:
            actual_limit = 40
        return self.request("GET", f"/key-metrics/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name="fmp_key_metrics")

    def get_company_profile(self, ticker):
        return self.request("GET", f"/profile/{ticker}", params={}, api_source_name="fmp_profile")

    def get_analyst_estimates(self, ticker, period="annual"):  # FMP has analyst estimates
        return self.request("GET", f"/analyst-estimates/{ticker}", params={"period": period},
                            api_source_name="fmp_analyst_estimates")


class EODHDClient(APIClient):
    def __init__(self):
        super().__init__("https://eodhistoricaldata.com/api", api_key_name="api_token", api_key_value=EODHD_API_KEY)
        self.params["fmt"] = "json"

    def get_fundamental_data(self, ticker_with_exchange):  # e.g., AAPL.US
        # EODHD fundamentals include General, Highlights, Valuation, SharesStats, Technicals, SplitsDividends, Financials, etc.
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}", api_source_name="eodhd_fundamentals")

    def get_ipo_calendar(self, from_date=None, to_date=None):
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        logger.warning("EODHDClient.get_ipo_calendar called, but may be restricted or have limited data.")
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd_ipo_calendar")


class RapidAPIUpcomingIPOCalendarClient(APIClient):  # Generally not preferred, often out of date or limited
    def __init__(self):
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_UPCOMING_IPO_KEY,
            "X-RapidAPI-Host": "upcoming-ipo-calendar.p.rapidapi.com"
        }
        super().__init__("httpshttps://upcoming-ipo-calendar.p.rapidapi.com", headers=headers)

    def get_ipo_calendar(self):
        logger.warning(
            "RapidAPIUpcomingIPOCalendarClient.get_ipo_calendar called, but may be restricted by subscription or data quality.")
        return self.request("GET", "/ipo-calendar", params=None, api_source_name="rapidapi_ipo_calendar")


class SECEDGARClient(APIClient):
    def __init__(self):
        # Company CIK data source
        self.cik_lookup_url = "https://www.sec.gov/files/company_tickers.json"
        # Submissions API base
        super().__init__("https://data.sec.gov/submissions/")
        self.headers = {"User-Agent": EDGAR_USER_AGENT}
        self._cik_map = None  # Cache for CIK lookup

    def _get_cik_map(self):
        if self._cik_map is None:
            logger.info("Fetching CIK map from SEC...")
            response = requests.get(self.cik_lookup_url, headers=self.headers, timeout=API_REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            # The data is a dictionary where keys are indices and values are dicts with cik_str, ticker, title
            self._cik_map = {item['ticker']: str(item['cik_str']).zfill(10) for item in data.values() if
                             'ticker' in item and 'cik_str' in item}
            logger.info(f"CIK map loaded with {len(self._cik_map)} entries.")
        return self._cik_map

    def get_cik_by_ticker(self, ticker):
        ticker = ticker.upper()
        try:
            cik_map = self._get_cik_map()
            return cik_map.get(ticker)
        except Exception as e:
            logger.error(f"Error fetching or processing CIK map: {e}", exc_info=True)
            return None

    def get_company_filings(self, ticker=None, cik=None):
        if not cik and ticker:
            cik = self.get_cik_by_ticker(ticker)
        if not cik:
            logger.error(f"CIK not found for ticker {ticker} or not provided.")
            return None

        # The endpoint is /CIK##########.json (10 digits for CIK, zero-padded)
        formatted_cik = str(cik).zfill(10)
        return self.request("GET", f"CIK{formatted_cik}.json", api_source_name="edgar_filings_summary")

    def get_filing_document_url(self, ticker_or_cik, form_type="10-K", priordate=None, count=1):
        """
        Attempts to find the primary document URL for the most recent specified filing type.
        Returns the URL to the filing document itself (e.g., .htm or .txt).
        """
        cik = None
        if isinstance(ticker_or_cik, str) and not ticker_or_cik.isdigit():
            cik = self.get_cik_by_ticker(ticker_or_cik)
        elif isinstance(ticker_or_cik, (str, int)) and str(ticker_or_cik).isdigit():
            cik = str(ticker_or_cik).zfill(10)

        if not cik:
            logger.error(f"Could not determine CIK for {ticker_or_cik}")
            return None

        try:
            company_filings_json = self.get_company_filings(cik=cik)  # Uses the request method with caching
            if not company_filings_json or "filings" not in company_filings_json or "recent" not in \
                    company_filings_json["filings"]:
                logger.warning(f"No recent filings data found for CIK {cik}.")
                return None

            recent_filings = company_filings_json["filings"]["recent"]

            target_filings = []
            forms = recent_filings.get("form", [])
            accession_numbers = recent_filings.get("accessionNumber", [])
            primary_documents = recent_filings.get("primaryDocument", [])
            filing_dates = recent_filings.get("filingDate", [])

            for i, form in enumerate(forms):
                if form.upper() == form_type.upper():
                    if priordate:
                        filing_date_dt = datetime.strptime(filing_dates[i], '%Y-%m-%d').date()
                        if filing_date_dt > priordate:
                            continue  # Skip filings after the priordate if we want older ones

                    # accessionNumber is like 0001234567-23-000123 -> needs to be 000123456723000123 for URL
                    acc_num_no_hyphens = accession_numbers[i].replace('-', '')
                    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_num_no_hyphens}/{primary_documents[i]}"
                    target_filings.append({"url": doc_url, "date": filing_dates[i]})

            if not target_filings:
                logger.info(f"No '{form_type}' filings found for CIK {cik} recently.")
                return None

            # Sort by date descending to get the most recent
            target_filings.sort(key=lambda x: x["date"], reverse=True)

            return target_filings[0]["url"] if count == 1 and target_filings else [f["url"] for f in
                                                                                   target_filings[:count]]

        except Exception as e:
            logger.error(f"Error getting filing document URL for CIK {cik}, form {form_type}: {e}", exc_info=True)
            return None

    def get_filing_text(self, filing_url):
        if not filing_url:
            return None
        logger.info(f"Fetching filing text from: {filing_url}")
        # Use base request method for SEC documents (not JSON, no API key)
        # Cache key will be GET:filing_url?
        return self.request("GET", filing_url.replace(self.base_url, ""),  # Pass relative path
                            api_source_name="edgar_filing_text", is_json_response=False)


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        # Key rotation managed per call now

    def _get_next_api_key_for_attempt(self, overall_attempt_num, max_attempts_per_key, total_keys):
        key_group_index = (overall_attempt_num // max_attempts_per_key) % total_keys
        api_key = GOOGLE_API_KEYS[key_group_index]
        current_retry_for_this_key = (overall_attempt_num % max_attempts_per_key) + 1
        logger.debug(
            f"Gemini: Using key ...{api_key[-4:]} (Index {key_group_index}), Attempt {current_retry_for_this_key}/{max_attempts_per_key}")
        return api_key, current_retry_for_this_key

    def generate_text(self, prompt, model="gemini-1.5-flash-latest"):  # Updated model
        max_attempts_per_key = API_RETRY_ATTEMPTS
        total_keys = len(GOOGLE_API_KEYS)
        if total_keys == 0:
            logger.error("Gemini: No API keys configured for Google API.")
            return "Error: No Google API keys configured."

        # Ensure prompt is not excessively long before sending
        if len(prompt) > 30000:  # Gemini Pro has a limit of 32k tokens, text is different
            logger.warning(f"Gemini prompt length {len(prompt)} is very long. Truncating to 30000 characters.")
            prompt = prompt[:30000]

        for overall_attempt_num in range(total_keys * max_attempts_per_key):
            api_key, current_retry_for_this_key = self._get_next_api_key_for_attempt(
                overall_attempt_num, max_attempts_per_key, total_keys
            )
            url = f"{self.base_url}/{model}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.6,  # Slightly lower for more factual synthesis
                    "maxOutputTokens": 8192,  # Gemini 1.5 Flash typical output limit
                    "topP": 0.9,
                    "topK": 40
                },
                "safetySettings": [  # Stricter safety
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_ONLY_HIGH"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_ONLY_HIGH"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_ONLY_HIGH"},
                ]
            }

            try:
                response = requests.post(url, json=payload, timeout=API_REQUEST_TIMEOUT + 90)  # Increased timeout
                response.raise_for_status()
                response_json = response.json()

                if "promptFeedback" in response_json and response_json["promptFeedback"].get("blockReason"):
                    block_reason = response_json["promptFeedback"]["blockReason"]
                    block_details = response_json["promptFeedback"].get("safetyRatings", "")
                    logger.error(
                        f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {block_reason}. Details: {block_details}. Prompt snippet: '{prompt[:100]}...'")
                    time.sleep(API_RETRY_DELAY)
                    continue

                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    finish_reason = candidate.get("finishReason")
                    if finish_reason not in [None, "STOP", "MAX_TOKENS", "MODEL_LENGTH", "OK"]:  # Added OK
                        logger.warning(
                            f"Gemini candidate finished with unexpected reason: {finish_reason} for key ...{api_key[-4:]}. Prompt: '{prompt[:100]}...'. Response: {response_json}")
                        # Treat other reasons as potential issues and retry if possible
                        if finish_reason == "SAFETY":  # If safety blocked the *candidate*
                            logger.error(f"Gemini candidate blocked by safety settings. Key ...{api_key[-4:]}")
                            # This specific prompt/key is problematic. Loop will try next.
                            time.sleep(API_RETRY_DELAY)
                            continue

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
                        f"Gemini API Bad Request (400). Likely persistent issue with prompt/payload. Aborting. Response: {e.response.text}")
                    return f"Error: Gemini API bad request (400). {e.response.text}"
                # Other HTTP errors (429, 5xx) will be retried
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Gemini API request error for key ...{api_key[-4:]} on attempt {current_retry_for_this_key}/{max_attempts_per_key}: {e}. Prompt: '{prompt[:100]}...'")

            if overall_attempt_num < (total_keys * max_attempts_per_key) - 1:
                time.sleep(API_RETRY_DELAY * (current_retry_for_this_key // 2 + 1))  # Modified backoff

        logger.error(
            f"All attempts ({total_keys * max_attempts_per_key}) to call Gemini API failed for prompt: {prompt[:100]}...")
        return "Error: Could not get response from Gemini API after multiple attempts across all keys."

    def summarize_text_with_context(self, text_to_summarize, context_summary, max_length=None):
        if max_length and len(text_to_summarize) > max_length:
            text_to_summarize = text_to_summarize[:max_length] + "\n... [TRUNCATED FOR BREVITY] ..."
            logger.info(f"Truncated text for Gemini summary due to length: {max_length}")

        prompt = f"Context: {context_summary}\n\nPlease provide a concise and factual summary of the following text, focusing on key information relevant to the context:\n\nText:\n\"\"\"\n{text_to_summarize}\n\"\"\"\n\nSummary:"
        return self.generate_text(prompt)

    def analyze_sentiment_with_reasoning(self, text_to_analyze, context=""):
        prompt = (f"Analyze the sentiment of the following text. Classify it as 'Positive', 'Negative', or 'Neutral'. "
                  f"Provide a brief explanation for your classification (1-2 sentences), citing specific phrases or elements from the text. "
                  f"{f'Consider this context: {context}. ' if context else ''}"
                  f"Text:\n\"\"\"\n{text_to_analyze}\n\"\"\"\n\nSentiment Analysis (Classification and Reasoning):")
        return self.generate_text(prompt)

    def answer_question_from_text_detailed(self, text_block, question,
                                           instruction="Answer comprehensively but concisely, citing evidence from the text where possible."):
        prompt = (f"Based *only* on the following text, please answer the question. {instruction}\n\n"
                  f"Text:\n\"\"\"\n{text_block}\n\"\"\"\n\n"
                  f"Question: {question}\n\nAnswer:")
        return self.generate_text(prompt)

    def interpret_financial_data_with_context(self, data_description, data_points_str, context_prompt, company_name):
        # data_points_str should be a string representation of the data
        prompt = (f"Company: {company_name}\n"
                  f"Interpret the following financial data:\nDescription: {data_description}\n"
                  f"Data: {data_points_str}\n"
                  f"Context/Specific Question: {context_prompt}\n\n"
                  f"Provide an interpretation focusing on trends, implications, and comparisons where appropriate. Be factual and objective. "
                  f"If data suggests specific strengths or weaknesses, state them clearly.")
        return self.generate_text(prompt)


def scrape_article_content(url):
    """
    Basic function to scrape main article content from a URL.
    Returns text content or None if scraping fails.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers,
                                timeout=API_REQUEST_TIMEOUT - 10)  # Slightly less timeout for scraping
        response.raise_for_status()

        # Check content type
        content_type = response.headers.get('content-type', '').lower()
        if 'html' not in content_type:
            logger.warning(f"Content type for {url} is not HTML ({content_type}). Skipping scrape.")
            return None

        soup = BeautifulSoup(response.content, 'lxml')  # lxml is generally faster

        # Common tags for main content: article, main, divs with specific classes/ids
        # This is a very generic approach and will need refinement for specific sites or a more robust library

        # Try to find common main content containers
        main_content = None
        selectors = [
            'article', 'main',
            'div[class*="content"]', 'div[class*="article"]', 'div[id*="content"]', 'div[id*="article"]',
            'div[class*="post"]', 'div[id*="post"]', 'div[class*="body"]', 'div[id*="body"]'
        ]
        for selector in selectors:
            main_content_tag = soup.select_one(selector)
            if main_content_tag:
                # Further refine by removing script, style, nav, header, footer, ads, comments
                for unwanted_tag_name in ['script', 'style', 'nav', 'header', 'footer', 'aside', 'form', 'iframe']:
                    for tag_to_remove in main_content_tag.find_all(unwanted_tag_name):
                        tag_to_remove.decompose()

                # Attempt to remove common ad/social share/related links sections by class/id heuristics
                for unwanted_class_pattern in ['ad', 'social', 'related', 'share', 'comment', 'promo', 'sidebar',
                                               'newsletter']:
                    for tag_to_remove in main_content_tag.find_all(
                            lambda tag: any(unwanted_class_pattern in c for c in tag.get('class', [])) or \
                                        any(unwanted_class_pattern in i for i in tag.get('id', []))):
                        tag_to_remove.decompose()

                text_parts = [p.get_text(separator=' ', strip=True) for p in
                              main_content_tag.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'li'])]
                article_text = '\n'.join(filter(None, text_parts))

                if len(article_text) > 200:  # Arbitrary threshold for meaningful content
                    main_content = article_text
                    break

        if not main_content:  # Fallback if specific selectors fail, grab all p tags
            paragraphs = soup.find_all('p')
            article_text = "\n".join(
                [p.get_text(separator=' ', strip=True) for p in paragraphs if p.get_text(strip=True)])
            if len(article_text) > 200:
                main_content = article_text

        if main_content:
            logger.info(f"Successfully scraped ~{len(main_content)} characters from {url}")
            return main_content.strip()
        else:
            logger.warning(f"Could not extract significant main content from {url} using generic selectors.")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching URL {url} for scraping: {e}")
        return None
    except Exception as e:
        logger.error(f"Error scraping article content from {url}: {e}", exc_info=True)
        return None


def extract_S1_text_sections(filing_text, sections_map=None):
    """
    Very basic extraction of text from S-1 or 10-K based on common item headers.
    This is a simplified approach and might not work for all filings or formats.
    `sections_map` should be like: {"business": ["Item 1.", "Business"], ...}
    """
    if not filing_text or not sections_map:
        return {}

    extracted_sections = {}
    # Normalize text: remove excessive newlines and leading/trailing whitespace
    normalized_text = re.sub(r'\n\s*\n', '\n\n', filing_text.strip())
    # Remove form feed characters and other non-printables that can mess up regex
    normalized_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', normalized_text)

    # Attempt to split by major "ITEM" sections first as a rough guide
    # This regex tries to find "ITEM X." or "Item X." followed by a space or newline
    # It's case-insensitive for "Item"
    item_splits = re.split(r'(^\s*(?:ITEM|Item)\s+\d+[A-Z]?\.?\s+)', normalized_text,
                           flags=re.MULTILINE | re.IGNORECASE)

    current_section_key = None
    accumulated_text = ""

    for i, part in enumerate(item_splits):
        is_header = (i % 2 == 1)  # Headers are at odd indices due to regex capture group

        if is_header:
            # If we were accumulating for a previous section, try to match and store it
            if current_section_key and accumulated_text.strip():
                extracted_sections[current_section_key] = accumulated_text.strip()
                logger.debug(
                    f"Extracted section '{current_section_key}' with length {len(extracted_sections[current_section_key])}")

            accumulated_text = ""  # Reset for the new section
            current_section_key = None  # Reset key

            # Try to identify which section this header corresponds to
            # Normalize header: uppercase, remove trailing dot
            normalized_header_part = part.upper().replace('.', '').strip()
            for key, patterns in sections_map.items():
                # patterns are like ["Item 1.", "Business"]
                # We check if the normalized form of "Item X" is in our header part
                item_pattern_normalized = patterns[0].upper().replace('.', '').strip()
                if item_pattern_normalized in normalized_header_part:
                    # Further check for the descriptive name if provided and if header is short (just "ITEM X")
                    if len(normalized_header_part.split()) <= 2 and len(patterns) > 1:  # e.g. "ITEM 1"
                        # We need to look ahead in the *original* text to see if the descriptive name follows.
                        # This is hard with pre-split text.
                        # For now, we'll primarily rely on the ITEM X match.
                        pass  # Simple match is enough for now
                    current_section_key = key
                    break
        else:  # This is the text content part
            accumulated_text += part

    # Store the last accumulated section if any
    if current_section_key and accumulated_text.strip():
        extracted_sections[current_section_key] = accumulated_text.strip()
        logger.debug(
            f"Extracted final section '{current_section_key}' with length {len(extracted_sections[current_section_key])}")

    # Fallback/Refinement: If ITEM splitting was not effective, or for sections without clear ITEM markers
    # This part needs more sophisticated logic, e.g., using regex for section titles directly
    # For now, the ITEM-based splitting is the primary mechanism implemented here.
    # A true parser would handle table of contents, varying header formats, etc.

    if not extracted_sections:
        logger.warning(
            "SEC filing text extraction using ITEM splits yielded no sections. The filing might be structured differently or very short.")
        # As a last resort, return the whole text for a general overview if no sections found
        # extracted_sections['full_text_fallback'] = normalized_text[:100000] # Limit length

    return extracted_sections