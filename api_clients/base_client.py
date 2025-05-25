# api_clients/base_client.py
import requests
import time
import json
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
import re

from core.config import (
    API_REQUEST_TIMEOUT, API_RETRY_ATTEMPTS, API_RETRY_DELAY,
    CACHE_EXPIRY_SECONDS
)
from core.logging_setup import logger
from database.connection import SessionLocal
from database.models import CachedAPIData


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

        sorted_params = sorted(full_query_params.items()) if full_query_params else []
        param_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        cache_key_str = f"{method.upper()}:{url}?{param_string}"
        if json_data:
            try:
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
                response.raise_for_status()

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
                log_params_for_error = {k: (str(v)[:4] + '******' + str(v)[-4:] if k == self.api_key_name and isinstance(v, str) and len(str(v)) > 8 else v) for k,v in full_query_params.items()}
                log_headers_for_error = self.headers.copy()
                sensitive_header_keys = ["X-RapidAPI-Key", "Authorization", "Token", self.api_key_name]
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
                    return None
                if e.response is not None:
                    if status_code == 429:
                        delay = API_RETRY_DELAY * (2 ** attempt)
                        logger.info(f"Rate limit hit (429). Waiting for {delay} seconds.")
                        time.sleep(delay)
                    elif 500 <= status_code < 600:
                        delay = API_RETRY_DELAY * (2 ** attempt)
                        logger.info(f"Server error ({status_code}). Waiting for {delay} seconds before retry.")
                        time.sleep(delay)
                    elif status_code == 401 or status_code == 403:
                        logger.error(f"Client error {status_code} (Unauthorized/Forbidden) for {url}. API key may be invalid or permissions lacking. No retry. Params: {log_params_for_error}")
                        return None
                    else:
                        logger.error(f"Non-retryable client error {status_code} for {url}: {e.response.reason if e.response else 'Unknown reason'}", exc_info=False)
                        return None
                else:
                    logger.error(f"HTTPError without response object for {url}. Cannot retry effectively.")
                    return None
            except requests.exceptions.RequestException as e:
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
                    return None
        logger.error(f"All {API_RETRY_ATTEMPTS} attempts failed for {url}. Last query params: {full_query_params}")
        return None


# --- Helper functions for scraping and parsing (moved from old api_clients.py) ---
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

        soup = BeautifulSoup(response.content, 'lxml')

        for tag_name in ['script', 'style', 'nav', 'header', 'footer', 'aside', 'form', 'iframe', 'noscript', 'link', 'meta', 'button', 'input', 'select', 'textarea', 'figure', 'figcaption']:
            for tag in soup.find_all(tag_name):
                tag.decompose()

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
                for unwanted_pattern in ['ad', 'social', 'related', 'share', 'comment', 'promo', 'sidebar', 'popup', 'banner', 'meta-info', 'byline', 'author', 'timestamp', 'tags', 'breadcrumb', 'pagination', 'tools', 'print-button', 'advertisement', 'figcaption', 'read-more', 'newsletter', 'modal']:
                    for sub_tag in tag.find_all(lambda t: any(unwanted_pattern in c.lower() for c in t.get('class', [])) or \
                                                              any(unwanted_pattern in i.lower() for i in t.get('id', [])) or \
                                                              unwanted_pattern in t.get('role', '').lower() or \
                                                              unwanted_pattern in t.get('aria-label', '').lower()):
                        sub_tag.decompose()
                main_content_html = tag
                break

        article_text = ""
        if main_content_html:
            text_parts = []
            for element in main_content_html.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'div', 'span', 'td', 'th']):
                text = element.get_text(separator=' ', strip=True)
                if text:
                    if element.name == 'div' and element.find(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li']):
                        continue
                    text_parts.append(text)
            article_text = '\n'.join(filter(None, text_parts))
        elif soup.body:
            logger.info(f"Main content selectors failed for {url}, trying body text. This might be noisy.")
            article_text = soup.body.get_text(separator='\n', strip=True)
        else:
            logger.warning(f"Could not extract main content or body text from {url}."); return None

        article_text = re.sub(r'[ \t]+', ' ', article_text)
        article_text = re.sub(r'\n\s*\n', '\n\n', article_text)
        article_text = re.sub(r'\n{3,}', '\n\n', article_text).strip()

        if len(article_text) < 200:
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
    try:
        soup = BeautifulSoup(filing_text, 'lxml')
    except Exception:
        try:
            logger.warning("lxml parsing failed for SEC filing, trying html.parser.")
            soup = BeautifulSoup(filing_text, 'html.parser')
        except Exception as e_bs_parse:
            logger.error(f"BeautifulSoup failed to parse filing text with lxml and html.parser: {e_bs_parse}. Using raw text and regex matching might be less accurate.")
            normalized_text = re.sub(r'\s*\n\s*', '\n', filing_text.strip())
            normalized_text = ''.join(filter(lambda x: x.isprintable() or x.isspace(), normalized_text))
            soup = None

    if soup:
        for invisible_element_name in ['style', 'script', 'head', 'title', 'meta', 'link', 'noscript']:
            for element in soup.find_all(invisible_element_name):
                element.decompose()
        page_text = []
        for element in soup.find_all(['p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'td', 'tr', 'table', 'body']):
            text = element.get_text(separator='\n', strip=True)
            if text:
                page_text.append(text)
        normalized_text = '\n\n'.join(page_text)
        normalized_text = re.sub(r'\s*\n\s*', '\n', normalized_text)
        normalized_text = re.sub(r'\n{3,}', '\n\n', normalized_text)
        normalized_text = ''.join(filter(lambda x: x.isprintable() or x.isspace(), normalized_text))
    else: # soup is None, normalized_text was already prepared
        pass

    section_patterns = []
    for key, patterns_list in sections_map.items():
        item_num_pattern_str = patterns_list[0].replace('.', r'\.?')
        base_item_regex = r"(?:ITEM|Item)\s*" + item_num_pattern_str.split()[-1] + r"\.?\s*:?\s*"
        if len(patterns_list) > 1:
            descriptive_name_regex = re.escape(patterns_list[1])
            start_regex_str_item_desc = base_item_regex + descriptive_name_regex
            section_patterns.append({"key": key, "start_regex": re.compile(start_regex_str_item_desc, re.IGNORECASE)})
            start_regex_str_desc_only = r"^\s*" + descriptive_name_regex + r"\s*$"
            section_patterns.append({"key": key, "start_regex": re.compile(start_regex_str_desc_only, re.IGNORECASE | re.MULTILINE)})
        else:
            section_patterns.append({"key": key, "start_regex": re.compile(base_item_regex, re.IGNORECASE)})

    found_sections_matches = []
    for pattern_info in section_patterns:
        for match in pattern_info["start_regex"].finditer(normalized_text):
            found_sections_matches.append({
                "key": pattern_info["key"],
                "start": match.start(),
                "end_of_header": match.end(),
                "header_text": match.group(0).strip()
            })

    if not found_sections_matches:
        logger.warning("No sections extracted from SEC filing based on ITEM X or descriptive name patterns."); return {}

    found_sections_matches.sort(key=lambda x: x["start"])

    for i, current_sec_info in enumerate(found_sections_matches):
        start_index = current_sec_info["end_of_header"]
        end_index = len(normalized_text)
        for j in range(i + 1, len(found_sections_matches)):
            next_sec_info = found_sections_matches[j]
            if next_sec_info["key"] != current_sec_info["key"]:
                end_index = next_sec_info["start"]
                break
        section_text = normalized_text[start_index:end_index].strip()
        section_text = re.sub(r'(?i)\btable\s+of\s+contents\b.*?\n', '', section_text, flags=re.MULTILINE)
        section_text = re.sub(r'^\s*(?:Page\s+\d+|\d+|PART\s+[IVXLCDM]+)\s*$', '', section_text, flags=re.MULTILINE)
        section_text = re.sub(r'\n{3,}', '\n\n', section_text).strip()

        if section_text:
            if current_sec_info["key"] not in extracted_sections or len(section_text) > len(extracted_sections.get(current_sec_info["key"], "")):
                extracted_sections[current_sec_info["key"]] = section_text
                logger.debug(f"Extracted section '{current_sec_info['key']}' (header: '{current_sec_info['header_text']}') len {len(section_text)}")

    if not extracted_sections:
        logger.warning("No text content could be extracted for any identified section headers after processing.")
    return extracted_sections