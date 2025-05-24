# config.py

# API Keys
# It's recommended to load sensitive keys from environment variables or a secure vault in production.

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
# Alpha Vantage Free Tier Limit: 25 requests per day.
# Using a longer delay for AV specifically in its client or being mindful of call frequency.
# For now, the general API_RETRY_DELAY will apply if AV hits HTTP errors like 429.
# Consider a specific rate limiter or call counter for Alpha Vantage if usage becomes high.
MAX_GEMINI_TEXT_LENGTH = 150000 # Max characters to send to Gemini for summaries to avoid hitting limits


# Analysis Settings
MAX_NEWS_ARTICLES_PER_QUERY = 10
MAX_NEWS_TO_ANALYZE_PER_RUN = 5 # Control how many new news items are analyzed in one script run
MIN_MARKET_CAP = 1000000000  # 1 Billion (example, not currently used but good for future filters)
STOCK_FINANCIAL_YEARS = 7 # Number of years for financial statement analysis
IPO_ANALYSIS_REANALYZE_DAYS = 7 # Re-analyze IPO if last analysis is older than this

# Path to store cached API responses
CACHE_DIR = "api_cache"  # Currently using DB cache, this is for potential file cache fallback
CACHE_EXPIRY_SECONDS = 3600 * 6  # 6 hours for general data

# DCF Analysis Defaults (Stock Analyzer)
DEFAULT_DISCOUNT_RATE = 0.09  # WACC estimate
DEFAULT_PERPETUAL_GROWTH_RATE = 0.025
DEFAULT_FCF_PROJECTION_YEARS = 5

# News Analysis
NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI = 250000  # Max characters of full news article to send to Gemini

# IPO Analysis
# Define keywords to identify sections in S-1/F-1 filings (very basic)
S1_KEY_SECTIONS = {
    "business": ["Item 1.", "Business"],
    "risk_factors": ["Item 1A.", "Risk Factors"],
    "mda": ["Item 7.", "Management's Discussion and Analysis"],
    "financial_statements": ["Item 8.", "Financial Statements and Supplementary Data"]
}
MAX_S1_SECTION_LENGTH_FOR_GEMINI = 200000  # Max characters per S-1 section to send to Gemini

# Stock Analysis (10-K sections, similar to S-1)
TEN_K_KEY_SECTIONS = S1_KEY_SECTIONS
MAX_10K_SECTION_LENGTH_FOR_GEMINI = MAX_S1_SECTION_LENGTH_FOR_GEMINI
