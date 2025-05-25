import requests
import json
from datetime import datetime

from .base_client import APIClient
from core.config import EDGAR_USER_AGENT, API_REQUEST_TIMEOUT
from core.logging_setup import logger


class SECEDGARClient(APIClient):
    def __init__(self):
        self.company_tickers_url = "https://www.sec.gov/files/company_tickers.json"
        super().__init__("https://data.sec.gov/submissions/")
        self.headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
        self._cik_map = None
        self._archives_base = "https://www.sec.gov/Archives/edgar/data/"

    def _load_cik_map(self):
        if self._cik_map is None:
            logger.info("Fetching CIK map from SEC...")
            cache_key_str = f"GET:{self.company_tickers_url}"
            cached_map = self._get_cached_response(cache_key_str)
            if cached_map:
                self._cik_map = cached_map
                logger.info(f"CIK map loaded from cache with {len(self._cik_map)} entries.")
                return self._cik_map

            try:
                response = requests.get(self.company_tickers_url, headers=self.headers, timeout=API_REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()
                self._cik_map = {item['ticker']: str(item['cik_str']).zfill(10)
                                 for item in data.values() if 'ticker' in item and 'cik_str' in item}
                self._cache_response(cache_key_str, self._cik_map, "sec_cik_map")
                logger.info(f"CIK map fetched and cached with {len(self._cik_map)} entries.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching CIK map from SEC: {e}", exc_info=True)
                self._cik_map = {}
            except json.JSONDecodeError as e_json:
                logger.error(f"Error decoding CIK map JSON from SEC: {e_json}", exc_info=True)
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
        return self.request("GET", f"CIK{formatted_cik_for_api}.json", api_source_name="edgar_filings_summary")

    def get_filing_document_url(self, cik, form_type="10-K", priordate_str=None, count=1):
        if not cik: return None if count == 1 else []
        company_summary = self.get_company_filings_summary(cik)

        if not company_summary or "filings" not in company_summary or "recent" not in company_summary["filings"]:
            logger.warning(f"No recent filings data for CIK {cik} in company summary.")
            return None if count == 1 else []

        recent_filings = company_summary["filings"]["recent"]
        target_filings_info = []

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
                    continue

                acc_num_no_hyphens = accession_numbers[i].replace('-', '')
                try:
                    cik_int_for_url = int(cik)
                except ValueError:
                    logger.error(f"CIK '{cik}' for URL construction is not a valid integer. Skipping filing.")
                    continue

                doc_url = f"{self._archives_base}{cik_int_for_url}/{acc_num_no_hyphens}/{primary_documents[i]}"
                target_filings_info.append({"url": doc_url, "date": current_filing_date, "form": form_val})

        if not target_filings_info:
            logger.info(f"No '{form_type}' filings found for CIK {cik} matching criteria.")
            return None if count == 1 else []

        target_filings_info.sort(key=lambda x: x["date"], reverse=True)

        if count == 1:
            return target_filings_info[0]["url"]
        else:
            return [f_info["url"] for f_info in target_filings_info[:count]]

    def get_filing_text(self, filing_url):
        if not filing_url: return None
        logger.info(f"Fetching filing text from: {filing_url}")
        try:
            text_content = self.request("GET", filing_url, use_cache=True,
                                        api_source_name="edgar_filing_text_content",
                                        is_json_response=False)
            if text_content:
                if isinstance(text_content, bytes):
                    try:
                        text_content = text_content.decode('utf-8')
                    except UnicodeDecodeError:
                        logger.warning(f"UTF-8 decode failed for {filing_url}, trying latin-1.")
                        text_content = text_content.decode('latin-1', errors='replace')
            return text_content
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching SEC filing text from {filing_url}: {e}")
            return None