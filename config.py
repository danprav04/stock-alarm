# config.py

# API Keys
# IMPORTANT: In a production environment, load sensitive keys from environment variables or a secure vault.
# Example for local development (using python-dotenv, ensure .env is in .gitignore):
# import os
# from dotenv import load_dotenv
# load_dotenv()
# GOOGLE_API_KEY_1 = os.getenv("GOOGLE_API_KEY_1")
# etc.

GOOGLE_API_KEYS = [
    "AIzaSyDLkwkVYBTUjabShS7VfdLkQTe7vZkxcjY",
    "AIzaSyAjECAJZVZz6PzDaUVaAkgfcOeLXCPFA6Y",
    "AIzaSyBRDIgN7ffBvoqAgaizQfuWRQExKc_oVig",
    "AIzaSyC4XLSmSX4U2iuAqW_pvQ87eNyPaJwQpDo",
]

FINNHUB_API_KEY = "d0o7hphr01qqr9alj38gd0o7hphr01qqr9alj390"  # Replace with your actual key
FINANCIAL_MODELING_PREP_API_KEY = "62ERGmJoqQgGD0nSGxRZS91TVzfz61uB"  # Replace with your actual key
EODHD_API_KEY = "683079df749c42.21476005"  # Replace with your actual key or "demo"
RAPIDAPI_UPCOMING_IPO_KEY = "0bd9b5144cmsh50c0e6d95c0b662p1cbdefjsn2d1cb0104cde"  # Replace with your actual key
ALPHA_VANTAGE_API_KEY = "HB6N4X55UTFGN2FP" # User provided Alpha Vantage Key

# SEC EDGAR Configuration
EDGAR_USER_AGENT = "YourAppName YourContactEmail@example.com"  # SEC requests a user agent

# Database Configuration
DATABASE_URL = "postgresql://avnadmin:AVNS_IeMYS-rv46Au9xqkza2@pg-4d810ff-daxiake-7258.d.aivencloud.com:26922/stock-alarm?sslmode=require"

# Email Configuration
EMAIL_HOST = "smtp-relay.brevo.com"
EMAIL_PORT = 587
EMAIL_USE_TLS = True
EMAIL_HOST_USER = "8dca1d001@smtp-brevo.com" # Replace with your actual email user
EMAIL_HOST_PASSWORD = "VrNUkDdcR5G9AL8P"    # Replace with your actual email password
EMAIL_SENDER = "testypesty54@gmail.com" # Replace with your sender email
EMAIL_RECIPIENT = "daniprav@gmail.com"      # The user who receives the summary

# Logging Configuration
LOG_FILE_PATH = "app_analysis.log"
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# API Client Settings
API_REQUEST_TIMEOUT = 45  # seconds, increased for potentially larger data
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 10  # seconds, increased

# Gemini API Configuration
GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE = 500000  # Absolute max characters for any prompt string sent to Gemini API (final safety net in GeminiAPIClient.generate_text)
SUMMARIZATION_CHUNK_SIZE_CHARS = 500000       # Target character size for text chunks when summarizing long documents (e.g., 10-K sections).
SUMMARIZATION_CHUNK_OVERLAP_CHARS = 500000     # Character overlap between chunks for better context flow.
SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS = 500000 # Max char length of concatenated chunk summaries before a final "summary of summaries" pass is done.

# Analysis Settings
MAX_NEWS_ARTICLES_PER_QUERY = 10
MAX_NEWS_TO_ANALYZE_PER_RUN = 5
MIN_MARKET_CAP = 1000000000
STOCK_FINANCIAL_YEARS = 7
IPO_ANALYSIS_REANALYZE_DAYS = 7

# Path to store cached API responses
CACHE_EXPIRY_SECONDS = 3600 * 6  # 6 hours for general data

# DCF Analysis Defaults (Stock Analyzer)
DEFAULT_DISCOUNT_RATE = 0.09
DEFAULT_PERPETUAL_GROWTH_RATE = 0.025
DEFAULT_FCF_PROJECTION_YEARS = 5

# News Analysis
NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION = GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE - 5000 # Reserve space for prompt instructions

# IPO Analysis S-1/F-1 sections
S1_KEY_SECTIONS = {
    "business": ["Item 1.", "Business"],
    "risk_factors": ["Item 1A.", "Risk Factors"],
    "mda": ["Item 7.", "Management's Discussion and Analysis"],
    "financial_statements": ["Item 8.", "Financial Statements and Supplementary Data"]
}
# Note: MAX_S1_SECTION_LENGTH_FOR_GEMINI is removed; chunking will handle long S-1 sections if implemented there.
# For now, IPO Analyzer will rely on GeminiAPIClient's hard truncation.

# Stock Analysis 10-K sections
TEN_K_KEY_SECTIONS = S1_KEY_SECTIONS
# Note: MAX_10K_SECTION_LENGTH_FOR_GEMINI is removed; chunking logic in StockAnalyzer will handle long sections.