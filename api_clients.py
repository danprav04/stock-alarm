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
    ALPHA_VANTAGE_API_KEY, GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE
)
from error_handler import logger
from database import SessionLocal
from models import CachedAPIData


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

        # Create a canonical representation for the cache key
        sorted_params = sorted(full_query_params.items()) if full_query_params else []
        param_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        cache_key_str = f"{method.upper()}:{url}?{param_string}"
        if json_data: # For POST requests with JSON body
            try:
                # Attempt to create a sorted, compact JSON string for the body part of the cache key
                sorted_json_data_str = json.dumps(json_data, sort_keys=True, separators=(',', ':'))
                cache_key_str += f"|BODY:{sorted_json_data_str}"
            except TypeError as e:
                logger.warning(f"Could not serialize json_data for cache key for {url}: {e}. Cache key may be less effective.")
                cache_key_str += f"|BODY_UNSERIALIZED:{str(json_data)}"


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
                response.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)

                if not is_json_response:
                    response_content = response.text
                    if use_cache:
                        self._cache_response(cache_key_str, response_content, api_source_name)
                    return response_content

                response_json = response.json()
                if use_cache:
                    self._cache_response(cache_key_str, response_json, api_source_name)
                return response_json

            except requests.exceptions.HTTPError as e:
                # Log params securely, redacting API key if it's in params
                log_params_for_error = {k: (str(v)[:4] + '******' + str(v)[-4:] if k == self.api_key_name and isinstance(v, str) and len(str(v)) > 8 else v) for k,v in full_query_params.items()}
                log_headers_for_error = self.headers.copy()
                sensitive_header_keys = ["X-RapidAPI-Key", "Authorization", "Token", self.api_key_name] # Add api_key_name if it's a header key
                for h_key in sensitive_header_keys:
                    if h_key in log_headers_for_error and isinstance(log_headers_for_error[h_key], str) and len(log_headers_for_error[h_key]) > 8:
                        log_headers_for_error[h_key] = log_headers_for_error[h_key][:4] + "******" + log_headers_for_error[h_key][-4:]

                status_code = e.response.status_code if e.response is not None else "Unknown"
                response_text_preview = e.response.text[:200] if e.response is not None else "No response body"

                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {method} {url} "
                    f"(Params: {log_params_for_error}, Headers: {log_headers_for_error}): "
                    f"{status_code} - {response_text_preview}..."
                )
                if api_source_name.startswith("alphavantage") and e.response is not None and "Our standard API call frequency is 25 requests per day." in e.response.text:
                    logger.error(f"Alpha Vantage API daily limit likely reached. Params: {log_params_for_error}")
                    return None # Don't retry if it's a known daily limit issue

                if e.response is not None:
                    if status_code == 429: # Rate limit
                        delay = API_RETRY_DELAY * (2 ** attempt) # Exponential backoff
                        logger.info(f"Rate limit hit (429). Waiting for {delay} seconds.")
                        time.sleep(delay)
                    elif 500 <= status_code < 600: # Server-side errors
                        delay = API_RETRY_DELAY * (2 ** attempt)
                        logger.info(f"Server error ({status_code}). Waiting for {delay} seconds before retry.")
                        time.sleep(delay)
                    elif status_code == 401 or status_code == 403: # Unauthorized or Forbidden
                        logger.error(f"Client error {status_code} (Unauthorized/Forbidden) for {url}. API key may be invalid or permissions lacking. No retry. Params: {log_params_for_error}")
                        return None # Do not retry on auth errors
                    else: # Other client errors
                        logger.error(f"Non-retryable client error {status_code} for {url}: {e.response.reason if e.response else 'Unknown reason'}", exc_info=False)
                        return None
                else: # If e.response is None for some reason
                    logger.error(f"HTTPError without response object for {url}. Cannot retry effectively.")
                    return None

            except requests.exceptions.RequestException as e: # Catch other request exceptions (timeout, connection error)
                logger.warning(f"Request error on attempt {attempt + 1}/{API_RETRY_ATTEMPTS} for {url}: {e}")
                if attempt < API_RETRY_ATTEMPTS - 1:
                    delay = API_RETRY_DELAY * (2 ** attempt)
                    time.sleep(delay)
            except json.JSONDecodeError as e_json:
                logger.error(f"JSON decode error for {url} on attempt {attempt + 1}. Response text: {response.text[:500] if 'response' in locals() else 'Response object not available'}... Error: {e_json}")
                if attempt < API_RETRY_ATTEMPTS - 1:
                    delay = API_RETRY_DELAY * (2 ** attempt)
                    time.sleep(delay)
                else:
                    return None # Failed all attempts

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
        # FMP limits historical data on free/lower tiers.
        # For 'annual', limit is often around 5-10 years. For 'quarter', it might be more.
        actual_limit = limit
        if period == "annual": actual_limit = min(limit, 15) # Adjust based on typical FMP limits
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
        # Analyst estimates are typically premium.
        logger.info(f"FMP get_analyst_estimates for {ticker} called. Availability depends on FMP subscription.")
        return self.request("GET", f"/analyst-estimates/{ticker}", params={"period": period},
                            api_source_name="fmp_analyst_estimates")


class AlphaVantageClient(APIClient):
    def __init__(self):
        super().__init__("https://www.alphavantage.co", api_key_name="apikey", api_key_value=ALPHA_VANTAGE_API_KEY)

    def get_company_overview(self, ticker):
        params = {"function": "OVERVIEW", "symbol": ticker}
        return self.request("GET", "/query", params=params, api_source_name="alphavantage_overview")

    def get_income_statement_quarterly(self, ticker):
        # AV free tier usually provides last 5 years of annual and quarterly.
        params = {"function": "INCOME_STATEMENT", "symbol": ticker}
        data = self.request("GET", "/query", params=params, api_source_name="alphavantage_income_quarterly")
        # AV data for quarterly reports is often in chronological order. Reverse to have most recent first.
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


class EODHDClient(APIClient):
    def __init__(self):
        super().__init__("https://eodhistoricaldata.com/api", api_key_name="api_token", api_key_value=EODHD_API_KEY)
        self.params["fmt"] = "json" # Default format

    def get_fundamental_data(self, ticker_with_exchange): # e.g., AAPL.US
        # EODHD fundamentals can be extensive.
        return self.request("GET", f"/fundamentals/{ticker_with_exchange}", api_source_name="eodhd_fundamentals")

    def get_ipo_calendar(self, from_date=None, to_date=None):
        params = {}
        if from_date: params["from"] = from_date
        if to_date: params["to"] = to_date
        logger.info("EODHDClient.get_ipo_calendar called. Data quality/availability may vary by subscription.")
        return self.request("GET", "/calendar/ipos", params=params, api_source_name="eodhd_ipo_calendar")


class SECEDGARClient(APIClient):
    def __init__(self):
        self.company_tickers_url = "https://www.sec.gov/files/company_tickers.json"
        # Base URL for submissions API (CIK{cik_number}.json)
        super().__init__("https://data.sec.gov/submissions/")
        self.headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
        self._cik_map = None # Lazy loaded CIK map
        self._archives_base = "https://www.sec.gov/Archives/edgar/data/" # For constructing document URLs

    def _load_cik_map(self):
        if self._cik_map is None:
            logger.info("Fetching CIK map from SEC...")
            cache_key_str = f"GET:{self.company_tickers_url}" # Unique key for this specific resource
            cached_map = self._get_cached_response(cache_key_str)
            if cached_map:
                self._cik_map = cached_map
                logger.info(f"CIK map loaded from cache with {len(self._cik_map)} entries.")
                return self._cik_map

            try:
                # Not using self.request here as it's a one-off setup call with specific caching needs
                response = requests.get(self.company_tickers_url, headers=self.headers, timeout=API_REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()
                # CIKs from company_tickers.json are integers, pad with zeros to 10 digits for consistency
                self._cik_map = {item['ticker']: str(item['cik_str']).zfill(10)
                                 for item in data.values() if 'ticker' in item and 'cik_str' in item}
                self._cache_response(cache_key_str, self._cik_map, "sec_cik_map")
                logger.info(f"CIK map fetched and cached with {len(self._cik_map)} entries.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching CIK map from SEC: {e}", exc_info=True)
                self._cik_map = {} # Avoid repeated failed attempts by setting to empty dict
            except json.JSONDecodeError as e_json:
                logger.error(f"Error decoding CIK map JSON from SEC: {e_json}", exc_info=True)
                self._cik_map = {}
        return self._cik_map

    def get_cik_by_ticker(self, ticker):
        ticker = ticker.upper()
        try:
            cik_map = self._load_cik_map()
            return cik_map.get(ticker)
        except Exception as e: # Catch any unexpected error during CIK map loading or lookup
            logger.error(f"Unexpected error in get_cik_by_ticker for {ticker}: {e}", exc_info=True)
            return None

    def get_company_filings_summary(self, cik):
        if not cik: return None
        # The API expects CIK without leading zeros in the path, but needs to be 10 digits for some other uses.
        # The SEC submissions API uses CIK padded to 10 digits.
        formatted_cik_for_api = str(cik).zfill(10)
        return self.request("GET", f"CIK{formatted_cik_for_api}.json", api_source_name="edgar_filings_summary")

    def get_filing_document_url(self, cik, form_type="10-K", priordate_str=None, count=1):
        if not cik: return None if count == 1 else []
        company_summary = self.get_company_filings_summary(cik) # Uses self.request with caching

        if not company_summary or "filings" not in company_summary or "recent" not in company_summary["filings"]:
            logger.warning(f"No recent filings data for CIK {cik} in company summary.")
            return None if count == 1 else []

        recent_filings = company_summary["filings"]["recent"]
        target_filings_info = []

        # Ensure all necessary lists exist and have the same length
        required_keys = ["form", "accessionNumber", "primaryDocument", "filingDate"]
        min_len = float('inf')
        for key in required_keys:
            if key not in recent_filings or not isinstance(recent_filings[key], list):
                logger.warning(f"Missing or invalid '{key}' in recent filings for CIK {cik}.")
                return None if count == 1 else []
            min_len = min(min_len, len(recent_filings[key]))

        if min_len == float('inf') or min_len == 0:
             logger.warning(f"No usable filing entries for CIK {cik}.")
             return None if count == 1 else []

        forms = recent_filings["form"][:min_len]
        accession_numbers = recent_filings["accessionNumber"][:min_len]
        primary_documents = recent_filings["primaryDocument"][:min_len]
        filing_dates = recent_filings["filingDate"][:min_len]

        priordate_dt = None
        if priordate_str:
            try:
                priordate_dt = datetime.strptime(priordate_str, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"Invalid priordate_str format: {priordate_str}. Should be YYYY-MM-DD. Ignoring.")

        for i, form_val in enumerate(forms):
            if form_val.upper() == form_type.upper():
                try:
                    current_filing_date = datetime.strptime(filing_dates[i], '%Y-%m-%d').date()
                except ValueError:
                    logger.warning(f"Invalid filingDate format '{filing_dates[i]}' for CIK {cik}, entry {i}. Skipping.")
                    continue

                if priordate_dt and current_filing_date > priordate_dt:
                    continue # Skip filings newer than priordate

                acc_num_no_hyphens = accession_numbers[i].replace('-', '')
                # CIK for URL construction should be the raw CIK (potentially with leading zeros removed if that's how SEC structures it)
                # The data.sec.gov uses full CIK. Archives uses CIK stripped of leading zeros.
                try:
                    # Ensure CIK is an integer for constructing the path, as per SEC structure
                    cik_int_for_url = int(cik)
                except ValueError:
                    logger.error(f"CIK '{cik}' for URL construction is not a valid integer. Skipping filing.")
                    continue

                doc_url = f"{self._archives_base}{cik_int_for_url}/{acc_num_no_hyphens}/{primary_documents[i]}"
                target_filings_info.append({"url": doc_url, "date": current_filing_date, "form": form_val})

        if not target_filings_info:
            logger.info(f"No '{form_type}' filings found for CIK {cik} matching criteria.")
            return None if count == 1 else []

        # Sort by date descending to get the most recent ones first
        target_filings_info.sort(key=lambda x: x["date"], reverse=True)

        if count == 1:
            return target_filings_info[0]["url"]
        else:
            return [f_info["url"] for f_info in target_filings_info[:count]]

    def get_filing_text(self, filing_url):
        if not filing_url: return None
        logger.info(f"Fetching filing text from: {filing_url}")
        # Caching for SEC filing text is done by self.request if use_cache=True (default)
        # The cache key includes the full URL.
        try:
            # Use the parent's request method, ensuring api_source_name is descriptive
            text_content = self.request("GET", filing_url, use_cache=True,
                                        api_source_name="edgar_filing_text_content",
                                        is_json_response=False) # Important: response is text/html

            if text_content:
                 # Attempt to decode, assuming self.request doesn't handle this for non-JSON
                if isinstance(text_content, bytes): # Should be string from response.text
                    try:
                        text_content = text_content.decode('utf-8')
                    except UnicodeDecodeError:
                        logger.warning(f"UTF-8 decode failed for {filing_url}, trying latin-1.")
                        text_content = text_content.decode('latin-1', errors='replace')
            return text_content

        except requests.exceptions.RequestException as e: # Should be caught by self.request, but defensive
            logger.error(f"Error fetching SEC filing text from {filing_url}: {e}")
            return None


class GeminiAPIClient:
    def __init__(self):
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"
        self.model_name = "gemini-1.5-flash-latest" # Using a generally available model

    def _get_next_api_key_for_attempt(self, overall_attempt_num, max_attempts_per_key, total_keys):
        if total_keys == 0: return None, 0 # No keys available
        key_group_index = (overall_attempt_num // max_attempts_per_key) % total_keys
        api_key = GOOGLE_API_KEYS[key_group_index]
        current_retry_for_this_key = (overall_attempt_num % max_attempts_per_key) + 1
        logger.debug(f"Gemini: Using key ...{api_key[-4:]} (Index {key_group_index}), Attempt {current_retry_for_this_key}/{max_attempts_per_key}")
        return api_key, current_retry_for_this_key

    def generate_text(self, prompt, model=None):
        if model is None: model = self.model_name

        max_attempts_per_key = API_RETRY_ATTEMPTS # Using global retry attempts
        total_keys = len(GOOGLE_API_KEYS)
        if total_keys == 0:
            logger.error("Gemini: No API keys configured in GOOGLE_API_KEYS."); return "Error: No Google API keys."

        if len(prompt) > GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE:
            original_len = len(prompt)
            prompt = prompt[:GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE]
            logger.warning(
                f"Gemini prompt (original length {original_len}) exceeded hard limit {GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE}. "
                f"Truncated to {len(prompt)} chars."
            )
            # Append a note about truncation if space allows, or just truncate.
            trunc_note = "\n...[PROMPT TRUNCATED DUE TO EXCESSIVE LENGTH]..."
            if len(prompt) + len(trunc_note) <= GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE:
                prompt += trunc_note
            else: # If even the note makes it too long, just use the truncated prompt
                prompt = prompt[:GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE - len(trunc_note)] + trunc_note


        for overall_attempt_num in range(total_keys * max_attempts_per_key):
            api_key, current_retry_for_this_key = self._get_next_api_key_for_attempt(
                overall_attempt_num, max_attempts_per_key, total_keys
            )
            if api_key is None: break # No more keys to try

            url = f"{self.base_url}/{model}:generateContent?key={api_key}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": { # Sensible defaults
                    "temperature": 0.6, # Slightly higher for more nuanced summaries/analysis
                    "maxOutputTokens": 8192, # Max for Flash model
                    "topP": 0.9,
                    "topK": 40
                },
                "safetySettings": [ # Standard safety settings
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                ]
            }
            try:
                # Note: Gemini requests are not typically cached by default APIClient due to dynamic nature of prompts
                # If caching is desired for identical prompts, it can be added here or by passing use_cache=True
                response = requests.post(url, json=payload, timeout=API_REQUEST_TIMEOUT + 120) # Longer timeout for LLM
                response.raise_for_status()
                response_json = response.json()

                if response_json.get("promptFeedback", {}).get("blockReason"):
                    reason = response_json["promptFeedback"]["blockReason"]
                    logger.error(f"Gemini prompt blocked for key ...{api_key[-4:]}. Reason: {reason}. Prompt: '{prompt[:150]}...'")
                    # This is a prompt issue, likely won't be fixed by retrying with same prompt/different key immediately.
                    # However, if it's a temporary safety filter glitch, another key might pass.
                    time.sleep(API_RETRY_DELAY); continue # Try next key or attempt

                if "candidates" in response_json and response_json["candidates"]:
                    candidate = response_json["candidates"][0]
                    finish_reason = candidate.get("finishReason")
                    if finish_reason not in [None, "STOP", "MAX_TOKENS", "MODEL_LENGTH", "OK", "OTHER"]: # "OTHER" can be normal
                        logger.warning(f"Gemini unusual finish reason: {finish_reason} for key ...{api_key[-4:]}. Prompt: '{prompt[:150]}...'")
                        if finish_reason == "SAFETY":
                            logger.error(f"Gemini candidate content blocked by safety settings for key ...{api_key[-4:]}.")
                            time.sleep(API_RETRY_DELAY); continue # Try next

                    content_part = candidate.get("content", {}).get("parts", [{}])[0]
                    if "text" in content_part:
                        return content_part["text"]
                    else:
                        logger.error(f"Gemini response missing 'text' in content part for key ...{api_key[-4:]}: {response_json}")
                else:
                    logger.error(f"Gemini response malformed or no candidates for key ...{api_key[-4:]}: {response_json}")

            except requests.exceptions.HTTPError as e:
                response_text = e.response.text[:200] if e.response is not None else "N/A"
                status_code = e.response.status_code if e.response is not None else "N/A"
                logger.warning(
                    f"Gemini API HTTP error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {status_code} - {response_text}. Prompt: '{prompt[:150]}...'")
                if e.response is not None and e.response.status_code == 400: # Bad Request (often invalid API key or malformed request)
                    if "API key not valid" in e.response.text or "API_KEY_INVALID" in e.response.text:
                        logger.error(f"Gemini API key ...{api_key[-4:]} reported as invalid. Skipping further retries with this key for this call.")
                        # Effectively, this moves to the next key group faster if a key is truly bad.
                        overall_attempt_num = ( (overall_attempt_num // max_attempts_per_key) + 1) * max_attempts_per_key -1 # fast-forward to next key group start minus 1 for loop increment
                        # The -1 is because the loop will increment overall_attempt_num
                        # If it's the last key, this loop will naturally end.
                        continue
                    else: # Other 400 errors might be prompt-related
                        logger.error(f"Gemini API Bad Request (400). Aborting for this prompt. Response: {e.response.text[:500]}")
                        return f"Error: Gemini API bad request (400). {e.response.text[:200]}"
                # Other HTTP errors will fall through to the general retry delay
            except requests.exceptions.RequestException as e: # Timeout, connection error
                logger.warning(f"Gemini API request error key ...{api_key[-4:]} attempt {current_retry_for_this_key}: {e}. Prompt: '{prompt[:150]}...'")
            except json.JSONDecodeError as e_json_gemini:
                resp_text_for_log = response.text[:500] if 'response' in locals() and hasattr(response, 'text') else "N/A"
                logger.error(f"Gemini API JSON decode error key ...{api_key[-4:]} attempt {current_retry_for_this_key}. Resp: {resp_text_for_log}. Err: {e_json_gemini}")

            if overall_attempt_num < (total_keys * max_attempts_per_key) - 1:
                time.sleep(API_RETRY_DELAY * current_retry_for_this_key) # Exponential backoff for this key's attempts

        logger.error(f"All attempts ({total_keys * max_attempts_per_key}) for Gemini API failed for prompt: {prompt[:150]}...")
        return "Error: Could not get response from Gemini API after multiple attempts."


    def summarize_text_with_context(self, text_to_summarize, context_summary, desired_output_instruction):
        prompt = (
            f"Context: {context_summary}\n\n"
            f"Text to Analyze:\n\"\"\"\n{text_to_summarize}\n\"\"\"\n\n"
            f"Instructions: {desired_output_instruction}\n\n"
            f"Provide a concise and factual summary based on the text and guided by the context and instructions."
        )
        return self.generate_text(prompt)


    def analyze_sentiment_with_reasoning(self, text_to_analyze, context=""):
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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9', 'Connection': 'keep-alive'
        }
        response = requests.get(url, headers=headers, timeout=API_REQUEST_TIMEOUT - 10, allow_redirects=True)
        response.raise_for_status()
        content_type = response.headers.get('content-type', '').lower()
        if 'html' not in content_type:
            logger.warning(f"Content type for {url} is not HTML ('{content_type}'). Skipping scrape."); return None

        soup = BeautifulSoup(response.content, 'lxml') # lxml is generally faster and more robust

        # Remove common non-content tags
        for tag_name in ['script', 'style', 'nav', 'header', 'footer', 'aside', 'form', 'iframe', 'noscript', 'link', 'meta', 'button', 'input', 'select', 'textarea', 'figure', 'figcaption']:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        # Try to find main content area using common selectors
        main_content_html = None
        selectors = [
            'article', 'main', 'div[role="main"]',
            'div[class*="article-body"]', 'div[class*="article-content"]', 'div[id*="article-body"]', 'div[id*="article-content"]',
            'div[class*="post-content"]', 'div[class*="entry-content"]',
            'div[class*="story-body"]', 'div[class*="main-content"]', 'section[class*="content"]'
        ]
        for selector in selectors:
            tag = soup.select_one(selector)
            if tag:
                # Further clean common clutter within the selected main content
                for unwanted_pattern in ['ad', 'social', 'related', 'share', 'comment', 'promo', 'sidebar', 'popup', 'banner', 'meta-info', 'byline', 'author', 'timestamp', 'tags', 'breadcrumb', 'pagination', 'tools', 'print-button', 'advertisement', 'figcaption', 'read-more', 'newsletter', 'modal']:
                    # Find by class, id, or aria-label
                    for sub_tag in tag.find_all(lambda t: any(unwanted_pattern in c.lower() for c in t.get('class', [])) or \
                                                              any(unwanted_pattern in i.lower() for i in t.get('id', [])) or \
                                                              unwanted_pattern in t.get('role', '').lower() or \
                                                              unwanted_pattern in t.get('aria-label', '').lower()):
                        sub_tag.decompose()
                main_content_html = tag
                break

        article_text = ""
        if main_content_html:
            # Extract text from meaningful tags, preserving some structure with newlines
            text_parts = []
            for element in main_content_html.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'div', 'span', 'td', 'th']):
                text = element.get_text(separator=' ', strip=True)
                if text:
                    # Avoid adding text from deeply nested divs that might be UI elements missed by decomposition
                    if element.name == 'div' and element.find(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
                        continue # Likely a container, let child elements handle their text
                    text_parts.append(text)
            article_text = '\n'.join(filter(None, text_parts))
        elif soup.body:
            logger.info(f"Main content selectors failed for {url}, trying body text. This might be noisy.")
            article_text = soup.body.get_text(separator='\n', strip=True)
        else:
            logger.warning(f"Could not extract main content or body text from {url}."); return None

        # Clean up excessive newlines and whitespace
        article_text = re.sub(r'[ \t]+', ' ', article_text) # Consolidate multiple spaces/tabs
        article_text = re.sub(r'\n\s*\n', '\n\n', article_text) # Consolidate multiple newlines
        article_text = re.sub(r'\n{3,}', '\n\n', article_text).strip() # Ensure max two newlines

        if len(article_text) < 200: # Increased threshold
            logger.info(f"Extracted text from {url} is very short ({len(article_text)} chars). Might be a stub, paywall, or primarily non-text content.")
        logger.info(f"Successfully scraped ~{len(article_text)} chars from {url}")
        return article_text

    except requests.exceptions.Timeout:
        logger.error(f"Timeout error scraping {url}."); return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error scraping {url}: {e}"); return None
    except Exception as e:
        logger.error(f"General error scraping {url}: {e}", exc_info=True); return None

def extract_S1_text_sections(filing_text, sections_map):
    if not filing_text or not sections_map: return {}
    extracted_sections = {}
    # Try parsing with lxml, fallback to html.parser, then to raw text if all fail
    try:
        soup = BeautifulSoup(filing_text, 'lxml')
    except Exception:
        try:
            logger.warning("lxml parsing failed for SEC filing, trying html.parser.")
            soup = BeautifulSoup(filing_text, 'html.parser')
        except Exception as e_bs_parse:
            logger.error(f"BeautifulSoup failed to parse filing text with lxml and html.parser: {e_bs_parse}. Using raw text and regex matching might be less accurate.")
            # Basic normalization for raw text
            normalized_text = re.sub(r'\s*\n\s*', '\n', filing_text.strip())
            normalized_text = ''.join(filter(lambda x: x.isprintable() or x.isspace(), normalized_text)) # Keep printable and whitespace
            soup = None # Indicate that soup parsing failed

    if soup: # If BeautifulSoup parsing was successful
        # Remove potentially problematic elements before text extraction
        for invisible_element_name in ['style', 'script', 'head', 'title', 'meta', 'link', 'noscript']:
            for element in soup.find_all(invisible_element_name):
                element.decompose()
        # Attempt to improve text extraction for HTML documents
        # Get text, trying to preserve paragraphs and some structure
        page_text = []
        for element in soup.find_all(['p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'td', 'tr', 'table', 'body']):
            text = element.get_text(separator='\n', strip=True)
            if text:
                page_text.append(text)
        normalized_text = '\n\n'.join(page_text) # Join major blocks with double newlines
        normalized_text = re.sub(r'\s*\n\s*', '\n', normalized_text) # Consolidate internal newlines
        normalized_text = re.sub(r'\n{3,}', '\n\n', normalized_text) # Max two newlines between blocks
        normalized_text = ''.join(filter(lambda x: x.isprintable() or x.isspace(), normalized_text))
    else: # soup is None, normalized_text was already prepared from raw text
        pass


    section_patterns = []
    # Prepare regex patterns for finding sections
    for key, patterns_list in sections_map.items():
        # Pattern for "Item X." or "Item X"
        item_num_pattern_str = patterns_list[0].replace('.', r'\.?') # Make dot optional
        # Regex to match "Item X." possibly followed by the section name
        # This tries to capture the header more accurately, including common variations
        # Using non-capturing group (?:...) for flexibility
        # Allowing for variations in spacing and optional colons
        # Making the descriptive name part optional for broader matching of "Item X."
        base_item_regex = r"(?:ITEM|Item)\s*" + item_num_pattern_str.split()[-1] + r"\.?\s*:?\s*"
        if len(patterns_list) > 1: # If a descriptive name is also provided
            # Escape special characters in descriptive name for regex
            descriptive_name_regex = re.escape(patterns_list[1])
            # Allow for variations in how descriptive name is presented (e.g., wrapped in newlines, different casing)
            # This pattern looks for "Item X. Business" or "Item X.\nBusiness" etc.
            # It also tries to match if the descriptive name appears standalone as a header
            # Case 1: Item X. Descriptive Name
            start_regex_str_item_desc = base_item_regex + descriptive_name_regex
            section_patterns.append({"key": key, "start_regex": re.compile(start_regex_str_item_desc, re.IGNORECASE)})
            # Case 2: Just Descriptive Name (as a potential fallback or primary header)
            # This regex tries to match the descriptive name if it appears as a clear header
            # ^\s* ensures it's at the beginning of a line (potentially after some whitespace)
            # \s*$ ensures it's at the end of a line (potentially before some whitespace)
            start_regex_str_desc_only = r"^\s*" + descriptive_name_regex + r"\s*$"
            section_patterns.append({"key": key, "start_regex": re.compile(start_regex_str_desc_only, re.IGNORECASE | re.MULTILINE)})
        else: # Only "Item X."
            section_patterns.append({"key": key, "start_regex": re.compile(base_item_regex, re.IGNORECASE)})


    found_sections_matches = []
    for pattern_info in section_patterns:
        for match in pattern_info["start_regex"].finditer(normalized_text):
            found_sections_matches.append({
                "key": pattern_info["key"],
                "start": match.start(),
                "end_of_header": match.end(), # Position after the matched header
                "header_text": match.group(0).strip()
            })

    if not found_sections_matches:
        logger.warning("No sections extracted from SEC filing based on ITEM X or descriptive name patterns."); return {}

    # Sort found sections by their start position to process them in order
    found_sections_matches.sort(key=lambda x: x["start"])

    # Deduplicate, preferring more specific matches if overlaps occur (e.g. "Item 1. Business" over just "Item 1.")
    # This is a simple deduplication; more complex logic might be needed for tricky documents.
    # For now, if multiple patterns match the same section key, the one appearing earliest or being longer might be chosen later.
    # The primary goal here is to define the boundaries.

    for i, current_sec_info in enumerate(found_sections_matches):
        # Start extracting text right after the identified header
        start_index = current_sec_info["end_of_header"]

        # Determine the end index: it's the start of the *next different* section's header,
        # or end of the document if this is the last section.
        end_index = len(normalized_text) # Default to end of document
        for j in range(i + 1, len(found_sections_matches)):
            next_sec_info = found_sections_matches[j]
            # If the next found section header is for a *different* key, it marks the end of the current section
            if next_sec_info["key"] != current_sec_info["key"]: # Or even if it's the same key but a new distinct match
                end_index = next_sec_info["start"] # End before the next section's header starts
                break
            # If it's the same key but a new match, we might want to combine or choose the best one.
            # For now, a simple approach: the first match for a key defines its start.

        section_text = normalized_text[start_index:end_index].strip()

        # Clean section_text: remove table of contents remnants, page numbers, etc.
        # This requires more sophisticated pattern matching.
        # Example: remove lines that look like "Table of Contents................... X"
        section_text = re.sub(r'(?i)\btable\s+of\s+contents\b.*?\n', '', section_text, flags=re.MULTILINE)
        # Example: remove lines that are just page numbers or short headers like "PART I" if they are isolated
        section_text = re.sub(r'^\s*(?:Page\s+\d+|\d+|PART\s+[IVXLCDM]+)\s*$', '', section_text, flags=re.MULTILINE)
        section_text = re.sub(r'\n{3,}', '\n\n', section_text).strip()


        if section_text:
            # If section already exists, append or replace based on length (prefer longer, more complete)
            if current_sec_info["key"] not in extracted_sections or len(section_text) > len(extracted_sections.get(current_sec_info["key"], "")):
                extracted_sections[current_sec_info["key"]] = section_text
                logger.debug(f"Extracted section '{current_sec_info['key']}' (header: '{current_sec_info['header_text']}') len {len(section_text)}")

    if not extracted_sections:
        logger.warning("No text content could be extracted for any identified section headers after processing.")
    return extracted_sections