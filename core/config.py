# core/config.py

GOOGLE_API_KEYS = [
    "AIzaSyDLkwkVYBTUjabShS7VfdLkQTe7vZkxcjY", # Replace with your actual key
    "AIzaSyAjECAJZVZz6PzDaUVaAkgfcOeLXCPFA6Y", # Replace with your actual key
    "AIzaSyBRDIgN7ffBvoqAgaizQfuWRQExKc_oVig", # Replace with your actual key
    "AIzaSyC4XLSmSX4U2iuAqW_pvQ87eNyPaJwQpDo", # Replace with your actual key
]

FINNHUB_API_KEY = "d0o7hphr01qqr9alj38gd0o7hphr01qqr9alj390"  # Replace with your actual key
FINANCIAL_MODELING_PREP_API_KEY = "62ERGmJoqQgGD0nSGxRZS91TVzfz61uB"  # Replace with your actual key
EODHD_API_KEY = "683079df749c42.21476005"  # Replace with your actual key or "demo"
RAPIDAPI_UPCOMING_IPO_KEY = "0bd9b5144cmsh50c0e6d95c0b662p1cbdefjsn2d1cb0104cde"  # Replace with your actual key
ALPHA_VANTAGE_API_KEY = "HB6N4X55UTFGN2FP" # Replace with your actual Alpha Vantage Key

# SEC EDGAR Configuration
EDGAR_USER_AGENT = "FinancialAnalysisBot/1.0 YourCompanyName YourContactEmail@example.com"  # Be specific and polite

# Database Configuration
DATABASE_URL = "postgresql://avnadmin:AVNS_IeMYS-rv46Au9xqkza2@pg-4d810ff-daxiake-7258.d.aivencloud.com:26922/stock-alarm?sslmode=require"

# Email Configuration
EMAIL_HOST = "smtp-relay.brevo.com"
EMAIL_PORT = 587
EMAIL_USE_TLS = True
EMAIL_HOST_USER = "8dca1d001@smtp-brevo.com"
EMAIL_HOST_PASSWORD = "VrNUkDdcR5G9AL8P"
EMAIL_SENDER = "testypesty54@gmail.com"
EMAIL_RECIPIENT = "daniprav@gmail.com"

# Logging Configuration
LOG_FILE_PATH = "app_analysis.log"
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# API Client Settings
API_REQUEST_TIMEOUT = 45  # seconds
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 10  # seconds

# Gemini API Configuration
GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE = 400000

# Chunking for Summarization
SUMMARIZATION_CHUNK_SIZE_CHARS = 80000
SUMMARIZATION_CHUNK_OVERLAP_CHARS = 5000
SUMMARIZATION_MAX_CONCAT_SUMMARIES_CHARS = 100000

# Analysis Settings
MAX_NEWS_ARTICLES_PER_QUERY = 10
MAX_NEWS_TO_ANALYZE_PER_RUN = 5
MIN_MARKET_CAP = 1000000000
STOCK_FINANCIAL_YEARS = 7
IPO_ANALYSIS_REANALYZE_DAYS = 7

# Cache Settings
CACHE_EXPIRY_SECONDS = 3600 * 6

# DCF Analysis Defaults
DEFAULT_DISCOUNT_RATE = 0.09
DEFAULT_PERPETUAL_GROWTH_RATE = 0.025
DEFAULT_FCF_PROJECTION_YEARS = 5

# News Analysis
NEWS_ARTICLE_MAX_LENGTH_FOR_GEMINI_SUMMARIZATION = GEMINI_PROMPT_MAX_CHARS_HARD_TRUNCATE - 10000

# IPO/10-K Sections
S1_KEY_SECTIONS = {
    "business": ["Item 1.", "Business"],
    "risk_factors": ["Item 1A.", "Risk Factors"],
    "mda": ["Item 7.", "Management's Discussion and Analysis of Financial Condition and Results of Operations"],
    "financial_statements": ["Item 8.", "Financial Statements and Supplementary Data"]
}
TEN_K_KEY_SECTIONS = S1_KEY_SECTIONS

# Stock Analyzer specific settings
MAX_COMPETITORS_TO_ANALYZE = 5
Q_REVENUE_SANITY_CHECK_DEVIATION_THRESHOLD = 0.75
PRIORITY_REVENUE_SOURCES = ["fmp_quarterly", "finnhub_quarterly", "alphavantage_quarterly"]