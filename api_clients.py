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
    API_RETRY_ATTEMPTS, API_RETRY_DELAY, CACHE_EXPIRY_SECONDS, EDGAR_USER_AGENT,
    ALPHA_VANTAGE_API_KEY  # Added Alpha Vantage Key
)
from error_handler import logger
from database import SessionLocal  # Direct import for SessionLocal
from models import CachedAPIData


# Removed unused global variable: current_google_api_key_index

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
            current_time_utc = datetime.now(timezone.utc)
            cache_entry = session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str,
                CachedAPIData.expires_at > current_time_utc
            ).first()
            if cache_entry:
                logger.info(f"Cache hit for: {request_url_or_params_str[:100]}...")
                return cache_entry.response_data
        except Exception as e:
            logger.error(f"Error reading from cache for '{request_url_or_params_str[:100]}...': {e}", exc_info=True)
        finally:
            session.close()
        return None

    def _cache_response(self, request_url_or_params_str, response_data, api_source):
        session = SessionLocal()
        try:
            now_utc = datetime.now(timezone.utc)
            expires_at_utc = now_utc + timedelta(seconds=CACHE_EXPIRY_SECONDS)

            session.query(CachedAPIData).filter(
                CachedAPIData.request_url_or_params == request_url_or_params_str).delete(synchronize_session=False)

            new_cache_entry = CachedAPIData(
                api_source=api_source,
                request_url_or_params=request_url_or_params_str,
                response_data=response_data,
                timestamp=now_utc,
                expires_at=expires_at_utc
            )
            session.add(new_cache_entry)
            session.commit()
            logger.info(f"Cached response for: {request_url_or_params_str[:100]}...")
        except Exception as e:
            logger.error(f"Error writing to cache for '{request_url_or_params_str[:100]}...': {e}", exc_info=True)
            session.rollback()
        finally:
            session.close()

    def request(self, method, endpoint, params=None, data=None, json_data=None, use_cache=True,
                api_source_name="unknown", is_json_response=True):
        url = f"{self.base_url}{endpoint}"
        current_call_params = params.copy() if params else {}
        full_query_params = self.params.copy()
        full_query_params.update(current_call_params)

        # Create a string representation for caching, ensuring order for params
        # and including JSON body if present.
        sorted_params = sorted(full_query_params.items()) if full_query_params else []
        param_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        cache_key_str = f"{method.upper()}:{url}?{param_string}"
        if json_data:
            # Sort keys in json_data to ensure consistent cache key for same data
            sorted_json_data_str = json.dumps(json_data, sort_keys=True)
            cache_key_str += f"|BODY:{sorted_json_data_str}"

        if use_cache:
            cached_data = self._get_cached_response(cache_key_str)
            if cached_data is not None:
                return cached_data

        for attempt in range(API_RETRY_ATTEMPTS):
            try:
                response = requests.request(
                    method, url, params=full_query_params, data=data, json=json_data,
                    headers=self.headers, timeout=API_REQUEST_TIMEOUT
                )
                response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)

                # If response is not expected to be JSON (e.g., raw text, HTML)
                if not is_json_response:
                    response_content = response.text
                    if use_cache:
                        self._cache_response(cache_key_str, response_content, api_source_name)
                    return response_content

                # Process as JSON
                response_json = response.json()
                if use_cache:
                    self._cache_response(cache_key_str, response_json, api_source_name)
                return response_json

            except requests.exceptions.HTTPError as e:
                # Log params, masking API key if it's in params
                log_params_for_error = {k: (
                    str(v)[:-6] + '******' if k == self.api_key_name and isinstance(v, str) and len(str(v)) > 6 else v)
                    for k, v in full_query_params.items()}
                # Log headers, masking sensitive headers
                log_headers_for_error = self.headers.copy()
                sensitive_header_keys = ["X-RapidAPI-Key", "Authorization", "Token"]  # Add more if needed
                for h_key in sensitive_header_keys:
                    if h_key in log_headers_for_error and isinstance(log_headers_for_error[h_key], str) and len(
                            log_headers_for_error[h_key]) > 6:
                        log_headers_for_error[h_key] = log_headers_for_error[h_key][:4] + "******" + \
                                                       log_headers_for_error[h_key][-4:]

                status_code = e.response.status_code
                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url} "
                    f"(Params: {log_params_for_error}, Headers: {log_headers_for_error}): "
                    f"{status_code} - {e.response.text[:200]}..."
                )
                # Specific handling for Alpha Vantage rate limit note
                if api_source_name.startswith(
                        "alphavantage") and "Our standard API call frequency is 25 requests per day." in e.response.text:
                    logger.error(
                        f"Alpha Vantage API daily limit likely reached for key. Params: {log_params_for_error}")
                    return None  # Do not retry if daily limit message is present

                if status_code == 429:  # Too Many Requests
                    delay = API_RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Rate limit hit. Waiting for {delay} seconds.")
                    time.sleep(delay)
                elif 500 <= status_code < 600:  # Server-side errors
                    delay = API_RETRY_DELAY * (2 ** attempt)
                    logger.info(f"Server error. Waiting for {delay} seconds.")
                    time.sleep(delay)
                else:  # Other client errors (400, 401, 403, 404 etc.) - typically not retryable
                    logger.error(f"Non-retryable client error for {url}: {status_code} {e.response.reason}",
                                 exc_info=False)  # No exc_info for cleaner log for non-retryable
                    return None
            except requests.exceptions.RequestException as e:  # Other request issues (timeout, connection error)
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")
                if attempt < API_RETRY_ATTEMPTS - 1:
                    delay = API_RETRY_DELAY * (2 ** attempt)
                    time.sleep(delay)
            except json.JSONDecodeError as e_json:  # If response.json() fails
                logger.error(
                    f"JSON decode error for {url} on attempt {attempt + 1}. Response text: {response.text[:500]}... Error: {e_json}")
                if attempt < API_RETRY_ATTEMPTS - 1:
                    delay = API_RETRY_DELAY * (2 ** attempt)  # Basic retry for transient decode issues
                    time.sleep(delay)
                else:
                    return None  # Failed all retries

        logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}. Last query params: {full_query_params}")
        return None


class FinnhubClient(APIClient):
    def __init__(self):
        super().__init__("https://finnhub.io/api/v1", api_key_name="token", api_key_value=FINNHUB_API_KEY)

    def get_market_news(self, category="general", min_id=0):
        params = {"category": category}
        if min_id > 0: params["minId"] = min_id
        return self.request("GET", "/news", params=params, api_source_name="finnhub_news")

    def get_company_profile2(self, ticker):
        return self.request("GET", "/stock/profile2", params={"symbol": ticker}, api_source_name="finnhub_profile")

    def get_financials_reported(self, ticker, freq="quarterly"):
        # freq can be 'annual', 'quarterly', or 'ttm'
        params = {"symbol": ticker, "freq": freq}
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


class FinancialModelingPrepClient(APIClient):
    def __init__(self):
        super().__init__("https://financialmodelingprep.com/api/v3", api_key_name="apikey",
                         api_key_value=FINANCIAL_MODELING_PREP_API_KEY)

    def get_ipo_calendar(self, from_date=None, to_date=None):  # Note: FMP IPO Calendar is often premium
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        logger.warning("FinancialModelingPrepClient.get_ipo_calendar called, but may be restricted by subscription.")
        return self.request("GET", "/ipo_calendar", params=params, api_source_name="fmp_ipo_calendar")

    def get_financial_statements(self, ticker, statement_type="income-statement", period="quarter", limit=40):
        actual_limit = limit
        # FMP has different historical data limits for free/paid plans and by period
        if period == "annual" and limit > 15:  # Max for annual usually around 5-10 for free, more for paid
            actual_limit = 15  # Adjust based on typical observed limits if known
            logger.debug(f"FMP {statement_type} for {ticker} (annual): limit adjusted to {actual_limit}.")
        elif period == "quarter" and limit > 60:  # Max for quarterly typically more
            actual_limit = 60
            logger.debug(f"FMP {statement_type} for {ticker} (quarterly): limit adjusted to {actual_limit}.")
        return self.request("GET", f"/{statement_type}/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_{statement_type.replace('-', '_')}_{period}")

    def get_key_metrics(self, ticker, period="quarter", limit=40):
        actual_limit = limit
        if period == "annual" and limit > 15:
            actual_limit = 15
        elif period == "quarter" and limit > 60:
            actual_limit = 60
        return self.request("GET", f"/key-metrics/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_key_metrics_{period}")

    def get_ratios(self, ticker, period="quarter", limit=40):
        actual_limit = limit
        if period == "annual" and limit > 15:
            actual_limit = 15
        elif period == "quarter" and limit > 60:
            actual_limit = 60
        return self.request("GET", f"/ratios/{ticker}", params={"period": period, "limit": actual_limit},
                            api_source_name=f"fmp_ratios_{period}")

    def get_company_profile(self, ticker):
        return self.request("GET", f"/profile/{ticker}", params={}, api_source_name="fmp_profile")

    def get_analyst_estimates(self, ticker, period="annual"):
        return self.request("GET", f"/analyst-estimates/{ticker}", params={"period": period},
                            api_source_name="fmp_analyst_estimates")


class AlphaVantageClient(APIClient):
    def __init__(self):
        super().__init__("https://www.alphavantage.co", api_key_name="apikey", api_key_value=ALPHA_VANTAGE_API_KEY)
        # Alpha Vantage has a specific call pattern, usually /query?function=FUNCTION_NAME&symbol=TICKER&apikey=KEY

    def get_company_overview(self, ticker):
        params = {"function": "OVERVIEW", "symbol": ticker}
        return self.request("GET", "/query", params=params, api_source_name="alphavantage_overview")

    def get_income_statement_quarterly(self, ticker):
        params = {"function": "INCOME_STATEMENT", "symbol": ticker}
        # Data contains "annualReports" and "quarterlyReports". We are interested in quarterlyReports.
        # Each report has fiscalDateEnding, reportedCurrency, totalRevenue, netIncome etc.
        # Reports are typically sorted oldest to newest.
        return self.request("GET", "/query", params=params, api_source_name="alphavantage_income_quarterly")

    def get_balance_sheet_quarterly(self, ticker):
        params = {"function": "BALANCE_SHEET", "symbol": ticker}
        return self.request("GET", "/query", params=params, api_source_name="alphavantage_balance_quarterly")

    def get_cash_flow_quarterly(self, ticker):
        params = {"function": "CASH_FLOW", "symbol": ticker}
        return self.request("GET", "/query", params=params, api_source_name="alphavantage_cashflow_quarterly")


class EODHDClient(APIClient):
    def __init__(self):
        super().__init__("https://eodhistoricaldata.com/api", api_key_name="api_token", api_key_value=EODHD_API_KEY)
        self.params["fmt"] = "json"  # Common param for EODHD

    def get_fundamental_data(self, ticker_with_exchange):  # e.g., AAPL.US
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}", api_source_name="eodhd_fundamentals")

    def get_ipo_calendar(self, from_date=None, to_date=None):  # EODHD IPO data might be limited
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        logger.warning("EODHDClient.get_ipo_calendar called, but may be restricted or have limited data.")
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd_ipo_calendar")


class SECEDGARClient(APIClient):
    def __init__(self):
        self.company_tickers_url = "https://www.sec.gov/files/company_tickers.json"
        super().__init__("https://data.sec.gov/submissions/")  # Base for submissions API
        self.headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
        self._cik_map = None
        self._archives_base = "https://www.sec.gov/Archives/edgar/data/"

    def _load_cik_map(self):
        if self._cik_map is None:
            logger.info("Fetching CIK map from SEC...")
            try:
                # Direct request, not using self.request to avoid base_url prepending for this specific URL
                response = requests.get(self.company_tickers_url, headers=self.headers, timeout=API_REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()
                self._cik_map = {item['ticker']: str(item['cik_str']).zfill(10)
                                 for item in data.values() if 'ticker' in item and 'cik_str' in item}
                logger.info(f"CIK map loaded with {len(self._cik_map)} entries.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching CIK map from SEC: {e}", exc_info=True);
                self._cik_map = {}
            except json.JSONDecodeError as e_json:
                logger.error(f"Error decoding CIK map JSON from SEC: {e_json}", exc_info=True);
                self._cik_map = {}
        return self._cik_map

    def get_cik_by_ticker(self, ticker):
        ticker = ticker.upper()
        try:
            cik_map = self._load_cik_map()
            return cik_map.get(ticker)
        except Exception as e:
            logger.error(f"Unexpected error in get_cik_by_ticker for {ticker}: {e}", exc_info=True)
            return None

    def get_company_filings_summary(self, cik):
        if not cik: return None
        formatted_cik_for_api = str(cik).zfill(10)
        # Uses self.request from base APIClient class
        return self.request("GET", f"CIK{formatted_cik_for_api}.json", api_source_name="edgar_filings_summary")

    def get_filing_document_url(self, cik, form_type="10-K", priordate_str=None, count=1):
        # Fetches summary, then constructs URL. Summary uses self.request.
        if not cik: return None if count == 1 else []
        company_summary = self.get_company_filings_summary(cik)  # Uses self.request
        if not company_summary or "filings" not in company_summary or "recent" not in company_summary["filings"]:
            logger.warning(f"No recent filings data for CIK {cik} in company summary.")
            return None if count == 1 else []

        recent_filings = company_summary["filings"]["recent"]
        target_filings_info = []
        forms = recent_filings.get("form", [])
        accession_numbers = recent_filings.get("accessionNumber", [])
        primary_documents = recent_filings.get("primaryDocument", [])
        filing_dates = recent_filings.get("filingDate", [])

        priordate_dt = None
        if priordate_str:
            try:
                priordate_dt = datetime.strptime(priordate_str, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"Invalid priordate_str: {priordate_str}. Ignoring.")

        for i, form in enumerate(forms):
            if form.upper() == form_type.upper():
                current_filing_date = datetime.strptime(filing_dates[i], '%Y-%m-%d').date()
                if priordate_dt and current_filing_date > priordate_dt: continue

                acc_num_no_hyphens = accession_numbers[i].replace('-', '')
                # Ensure CIK is int for URL construction if it was fetched as string and padded
                try:
                    cik_int_for_url = int(cik)
                except ValueError:
                    logger.error(f"CIK '{cik}' for URL construction is not a valid integer.")
                    continue  # Skip this filing if CIK format is bad for URL

                doc_url = f"{self._archives_base}{cik_int_for_url}/{acc_num_no_hyphens}/{primary_documents[i]}"
                target_filings_info.append({"url": doc_url, "date": current_filing_date, "form": form})

        if not target_filings_info:
            logger.info(f"No '{form_type}' filings for CIK {cik} matching criteria.")
            return None if count == 1 else []

        target_filings_info.sort(key=lambda x: x["date"], reverse=True)  # Get most recent first
        return target_filings_info[0]["url"] if count == 1 else [f_info["url"] for f_info in
                                                                 target_filings_info[:count]]

    def get_filing_text(self, filing_url):
        if not filing_url: return None
        logger.info(f"Fetching filing text from: {filing_url}")
        cache_key_str = f"GET_SEC_DOC:{filing_url}"  # Specific cache key for raw doc fetches
        cached_text = self._get_cached_response(cache_key_str)
        if cached_text is not None: return cached_text

        try:
            # Direct request, not using self.request's base_url logic for SEC Archives
            response = requests.get(filing_url, headers=self.headers,
                                    timeout=API_REQUEST_TIMEOUT + 30)  # Longer timeout for large docs
            response.raise_for_status()
            try:
                text_content = response.content.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f"UTF-8 decode failed for {filing_url}, trying latin-1.")
                text_content = response.content.decode('latin-1', errors='replace')  # Replace errors to avoid crash

            self._cache_response(cache_key_str, text_content, "edgar_filing_text_content")
            return text_content
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching SEC filing text from {filing_url}: {e}")
            return None


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        # self.current_api_key_index = 0 # Managed by _get_next_api_key_for_attempt

    def _get_next_api_key_for_attempt(self, overall_attempt_num, max_attempts_per_key, total_keys):
        # Determine which key to use based on overall_attempt_num
        # This rotates through keys if previous ones fail or hit limits within their retry window.
        key_group_index = (overall_attempt_num // max_attempts_per_key) % total_keys
        api_key = GOOGLE_API_KEYS[key_group_index]
        current_retry_for_this_key = (overall_attempt_num % max_attempts_per_key) + 1  # 1-based for logging
        logger.debug(
            f"Gemini: Using key ...{api_key[-4:]} (Index {key_group_index}), Attempt {current_retry_for_this_key}/{max_attempts_per_key}")
        return api_key, current_retry_for_this_key

    def generate_text(self, prompt, model="gemini-1.5-flash-latest"):
        max_attempts_per_key = API_RETRY_ATTEMPTS  # How many times to try each key
        total_keys = len(GOOGLE_API_KEYS)
        if total_keys == 0:
            logger.error("Gemini: No API keys configured.");
            return "Error: No Google API keys."

        # Prompt length check (crude character count, Gemini has token limits)
        # This is a safeguard, actual token limits are more complex.
        if len(prompt) > 250000:  # Using a general large char limit
            logger.warning(f"Gemini prompt length {len(prompt)} very long. Truncating to 250000 chars for safety.")
            # Truncate in a way that keeps the structure if possible, or just head + tail
            prompt = prompt[:250000] + "\n...[PROMPT TRUNCATED DUE TO EXCESSIVE LENGTH]..."

        # Total attempts will be total_keys * max_attempts_per_key
        for overall_attempt_num in range(total_keys * max_attempts_per_key):
            api_key, current_retry_for_this_key = self._get_next_api_key_for_attempt(
                overall_attempt_num, max_attempts_per_key, total_keys
            )

            url = f"{self.base_url}/{model}:generateContent?key={api_key}"
            # Standard payload for Gemini API
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {  # Adjust these as needed
                    "temperature": 0.5,  # Controls randomness, lower is more deterministic
                    "maxOutputTokens": 8192,  # Max tokens in the response
                    "topP": 0.9,  # Nucleus sampling: considers tokens with cumulative probability >= topP
                    "topK": 35  # Considers top K tokens
                },
                "safetySettings": [  # Standard safety settings
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                ]
            }
            try:
                response = requests.post(url, json=payload,
                                         timeout=API_REQUEST_TIMEOUT + 120)  # Longer timeout for GenAI
                response.raise_for_status()
                response_json = response.json()

                # Check for prompt feedback (blocked prompt)
                if response_json.get("promptFeedback", {}).get("blockReason"):
                    reason = response_json["promptFeedback"]["blockReason"]
                    logger.error(
                        f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {reason}. Prompt: '{prompt[:100]}...'")
                    time.sleep(API_RETRY_DELAY);  # Wait before trying next key/attempt
                    continue  # Move to next attempt (potentially next key)

                # Check for candidates and content
                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    # Check for finishReason (e.g., "SAFETY", "MAX_TOKENS")
                    finish_reason = candidate.get("finishReason")
                    if finish_reason not in [None, "STOP", "MAX_TOKENS", "MODEL_LENGTH", "OK"]:  # OK is sometimes seen
                        logger.warning(
                            f"Gemini unexpected finish: {finish_reason} for key ...{api_key[-4:]}. Prompt: '{prompt[:100]}...'")
                        if finish_reason == "SAFETY":  # Candidate content blocked by safety
                            logger.error(f"Gemini candidate blocked by safety settings for key ...{api_key[-4:]}.")
                            time.sleep(API_RETRY_DELAY);  # Wait
                            continue  # Move to next attempt

                    content_part = candidate.get("content", {}).get("parts", [{}])[0]
                    if "text" in content_part:
                        return content_part["text"]
                    else:  # Should not happen if finishReason is STOP
                        logger.error(f"Gemini response missing text for key ...{api_key[-4:]}: {response_json}")
                else:  # No candidates, malformed response
                    logger.error(f"Gemini response malformed for key ...{api_key[-4:]}: {response_json}")

            except requests.exceptions.HTTPError as e:
                logger.warning(
                    f"Gemini API HTTP error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {e.response.status_code} - {e.response.text[:200]}. Prompt: '{prompt[:100]}...'")
                if e.response.status_code == 400:  # Bad Request (e.g. malformed prompt for API)
                    logger.error(
                        f"Gemini API Bad Request (400). Aborting for this prompt. Response: {e.response.text[:500]}")
                    return f"Error: Gemini API bad request (400). {e.response.text[:200]}"
                # Other errors like 429 (rate limit), 500 (server error) will cause a retry with delay (handled below)
            except requests.exceptions.RequestException as e:  # Timeout, connection error
                logger.warning(
                    f"Gemini API request error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {e}. Prompt: '{prompt[:100]}...'")
            except json.JSONDecodeError as e_json_gemini:
                logger.error(
                    f"Gemini API JSON decode error key ...{api_key[-4:]} attempt {current_retry_for_this_key}. Resp: {response.text[:500]}. Err: {e_json_gemini}")

            # If not the absolute last attempt, sleep before retrying (with current or next key)
            if overall_attempt_num < (total_keys * max_attempts_per_key) - 1:
                # Exponential backoff based on retries for THIS KEY, reset for new key
                time.sleep(API_RETRY_DELAY * (
                    current_retry_for_this_key))  # (overall_attempt_num % max_attempts_per_key) is retry for current key group

        logger.error(
            f"All attempts ({total_keys * max_attempts_per_key}) for Gemini API failed for prompt: {prompt[:100]}...")
        return "Error: Could not get response from Gemini API after multiple attempts."

    def summarize_text_with_context(self, text_to_summarize, context_summary, max_length=None):
        if max_length and len(text_to_summarize) > max_length:
            text_to_summarize = text_to_summarize[:max_length] + "\n... [TRUNCATED FOR BREVITY] ..."
            logger.info(f"Truncated text for Gemini summary to length: {max_length}")
        prompt = f"Context: {context_summary}\n\nPlease provide a concise and factual summary of the following text, focusing on key information relevant to the context:\n\nText:\n\"\"\"\n{text_to_summarize}\n\"\"\"\n\nSummary:"
        return self.generate_text(prompt)

    def analyze_sentiment_with_reasoning(self, text_to_analyze, context=""):
        # Updated prompt for clearer, structured sentiment output
        prompt = (
            f"Analyze the sentiment of the following text. "
            f"Context for analysis (if any): '{context}'.\n\n"
            f"Text to Analyze:\n\"\"\"\n{text_to_analyze}\n\"\"\"\n\n"
            f"Instructions: Respond with the sentiment classification and reasoning, structured as follows:\n"
            f"Sentiment: [Choose one: Positive, Negative, Neutral]\n"
            f"Reasoning: [Provide a brief 1-2 sentence explanation, citing specific phrases from the text if possible to justify the sentiment.]"
        )
        return self.generate_text(prompt)


# --- Helper functions for scraping and parsing ---
def scrape_article_content(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            # Common user agent
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive'
        }
        response = requests.get(url, headers=headers, timeout=API_REQUEST_TIMEOUT - 10,
                                allow_redirects=True)  # Slightly less timeout for scrape
        response.raise_for_status()

        # Check content type - if not HTML, probably not worth parsing with BeautifulSoup
        if 'html' not in response.headers.get('content-type', '').lower():
            logger.warning(
                f"Content type for {url} is not HTML ('{response.headers.get('content-type')}'). Skipping scrape.");
            return None

        soup = BeautifulSoup(response.content, 'lxml')  # lxml is generally faster

        # Remove common unwanted tags
        for tag_name in ['script', 'style', 'nav', 'header', 'footer', 'aside', 'form', 'iframe', 'noscript', 'link',
                         'meta']:  # Consider 'figure', 'figcaption' if they become noisy
            for tag in soup.find_all(tag_name): tag.decompose()

        # Try to find main content using common semantic tags or class/id patterns
        main_content_html = None
        selectors = ['article', 'main', 'div[role="main"]', 'div[class*="article-content"]', 'div[id="content"]',
                     'div[class*="post-content"]', 'div[class*="entry-content"]', 'div[class*="story-body"]']
        for selector in selectors:
            tag = soup.select_one(selector)
            if tag:
                # Further clean common noisy elements within the selected main content
                for unwanted_pattern in ['ad', 'social', 'related', 'share', 'comment', 'promo', 'sidebar', 'popup',
                                         'banner', 'meta-info', 'byline-container']:
                    # Find by class, id, or aria attributes
                    for sub_tag in tag.find_all(lambda t: any(unwanted_pattern in c for c in t.get('class', [])) or \
                                                          any(unwanted_pattern in i for i in t.get('id', [])) or \
                                                          t.get('aria-label', '').lower().count(unwanted_pattern) > 0):
                        sub_tag.decompose()
                main_content_html = tag;
                break

        article_text = ""
        if main_content_html:
            # Extract text from relevant tags within the main content
            # Favor 'p', 'h1-h6', 'li'. Include 'div' and 'span' if they are direct text containers.
            text_parts = [p.get_text(separator=' ', strip=True) for p in
                          main_content_html.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'span', 'div'],
                                                     recursive=True)  # Recursive needed for nested structures
                          if p.get_text(strip=True) and not p.find_all(['p', 'div'])
                          # Avoid double counting from parent div/span if it also has 'p's
                          ]
            article_text = '\n'.join(filter(None, text_parts))  # Join non-empty parts
            article_text = re.sub(r'\s+\n\s*', '\n', article_text)  # Consolidate multiple newlines around spaces
            article_text = re.sub(r'\n{3,}', '\n\n', article_text)  # Max 2 consecutive newlines
        elif soup.body:  # Fallback: try to get text from the whole body if main content selectors fail
            logger.info(f"Main content selectors failed for {url}, trying body text. This might be noisy.")
            article_text = soup.body.get_text(separator='\n', strip=True)
            article_text = re.sub(r'\n{3,}', '\n\n', article_text)  # Max 2 consecutive newlines
        else:
            logger.warning(f"Could not extract main content or body text from {url}.");
            return None

        if len(article_text) < 150:  # Arbitrary threshold for "very short"
            logger.info(
                f"Extracted text from {url} is very short ({len(article_text)} chars). Might be a stub or paywall.")

        logger.info(f"Successfully scraped ~{len(article_text)} chars from {url}")
        return article_text.strip()

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error scraping {url}: {e}");
        return None
    except Exception as e:  # Catch any other BeautifulSoup or general error
        logger.error(f"General error scraping {url}: {e}", exc_info=True);
        return None


def extract_S1_text_sections(filing_text, sections_map):
    # sections_map: e.g., {"business": ["Item 1.", "Business"], "risk_factors": ["Item 1A.", "Risk Factors"]}
    if not filing_text or not sections_map: return {}
    extracted_sections = {}

    # Attempt to parse as HTML first, then get text. This helps with HTML entities.
    # Use 'lxml' for robustness if available, otherwise 'html.parser'.
    try:
        soup_text = BeautifulSoup(filing_text, 'lxml').get_text(separator='\n')
    except Exception:  # Fallback if lxml fails or not installed
        try:
            soup_text = BeautifulSoup(filing_text, 'html.parser').get_text(separator='\n')
        except Exception as e_bs_parse:
            logger.error(f"BeautifulSoup failed to parse filing text: {e_bs_parse}. Using raw text.")
            soup_text = filing_text  # Use raw text if parsing fails badly

    # Normalize text: replace multiple newlines, strip leading/trailing whitespace
    # Remove null characters and other problematic control characters
    normalized_text = re.sub(r'\n\s*\n', '\n\n', soup_text.strip())
    normalized_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\xff]', '', normalized_text)  # Keep \t, \n, \r

    section_patterns = []
    for key, patterns_list in sections_map.items():
        # ITEM 1. or Item 1A. (optional period after number/letter)
        item_num_pattern_str = patterns_list[0].replace('.', r'\.?')  # Make period optional in "Item X."
        # Regex for "ITEM X." or "Item X. Business"
        # Using \s+ for spaces, re.IGNORECASE
        # Ensure word boundaries around ITEM to avoid matching "ITEMS"
        # Allow for optional colon after item number/letter
        start_regex_str = r"(?:\bITEM\b|\bItem\b)\s*" + item_num_pattern_str.split()[-1] + r"\.?\s*:?\s*"
        if len(patterns_list) > 1:  # If descriptive name is also provided
            start_regex_str += r"\s*" + re.escape(patterns_list[1])
        section_patterns.append({"key": key, "start_regex": re.compile(start_regex_str, re.IGNORECASE)})

    found_sections = []
    # First pass: find sections by "ITEM X." patterns
    for pattern_info in section_patterns:
        for match in pattern_info["start_regex"].finditer(normalized_text):
            found_sections.append(
                {"key": pattern_info["key"], "start": match.start(), "header_text": match.group(0).strip()})

    # Fallback: If no "ITEM X." found, try matching by descriptive names only (more carefully)
    if not found_sections:
        logger.warning(f"No primary ITEM X. headers found in SEC filing. Trying descriptive names as section headers.")
        for key, patterns_list in sections_map.items():
            if len(patterns_list) > 1:  # Must have a descriptive name
                # Match descriptive name at the start of a line, possibly with some leading whitespace.
                # This is less precise and might match sub-headers if not careful.
                # Look for lines that *primarily* consist of the descriptive name.
                desc_name_pattern = re.compile(r"^\s*" + re.escape(patterns_list[1]) + r"\s*$",
                                               re.IGNORECASE | re.MULTILINE)
                for match in desc_name_pattern.finditer(normalized_text):
                    # Heuristic: check if line before is empty or very short, or if line after starts itemization.
                    # This is to avoid picking up mentions of the phrase deep within another section.
                    # For now, accept if found, but this could be refined.
                    found_sections.append({"key": key, "start": match.start(), "header_text": match.group(0).strip()})

    if not found_sections:
        logger.warning("No sections extracted from SEC filing based on any patterns.");
        return {}

    # Sort found sections by their start index
    found_sections.sort(key=lambda x: x["start"])

    # Extract text between sections
    for i, current_sec_info in enumerate(found_sections):
        start_index = current_sec_info["start"] + len(current_sec_info["header_text"])
        end_index = found_sections[i + 1]["start"] if i + 1 < len(found_sections) else None

        section_text = normalized_text[start_index:end_index].strip()

        if section_text:
            # If a section (e.g. "Business") is found multiple times, take the longest one,
            # or the first one. Current logic takes the one that results in longest section_text.
            if current_sec_info["key"] not in extracted_sections or len(section_text) > len(
                    extracted_sections.get(current_sec_info["key"], "")):
                extracted_sections[current_sec_info["key"]] = section_text
                logger.debug(
                    f"Extracted section '{current_sec_info['key']}' (header: '{current_sec_info['header_text']}') len {len(section_text)}")

    if not extracted_sections:
        logger.warning("No text content could be extracted for any identified section headers.")

    return extracted_sections