# config.py

# API Keys
# It's recommended to load sensitive keys from environment variables or a secure vault in production.
# For this script, we'll list them here as per the prompt.

GOOGLE_API_KEYS = [
    "AIzaSyDLkwkVYBTUjabShS7VfdLkQTe7vZkxcjY",
    "AIzaSyAjECAJZVZz6PzDaUVaAkgfcOeLXCPFA6Y",
    "AIzaSyBRDIgN7ffBvoqAgaizQfuWRQExKc_oVig",
    "AIzaSyC4XLSmSX4U2iuAqW_pvQ87eNyPaJwQpDo",
]

FINNHUB_API_KEY = "d0o7hphr01qqr9alj38gd0o7hphr01qqr9alj390"
FINANCIAL_MODELING_PREP_API_KEY = "62ERGmJoqQgGD0nSGxRZS91TVzfz61uB"
EODHD_API_KEY = "683079df749c42.21476005" # Note: EODHD often requires 'demo' or your actual key for API calls.
RAPIDAPI_UPCOMING_IPO_KEY = "0bd9b5144cmsh50c0e6d95c0b662p1cbdefjsn2d1cb0104cde"

# Database Configuration
# Changed "postgres://" to "postgresql://"
DATABASE_URL = "postgresql://avnadmin:AVNS_IeMYS-rv46Au9xqkza2@pg-4d810ff-daxiake-7258.d.aivencloud.com:26922/stock-alarm?sslmode=require"

# Email Configuration (Placeholder - replace with actual SMTP details or email service API)
EMAIL_HOST = "smtp-relay.brevo.com"
EMAIL_PORT = 587
EMAIL_USE_TLS = True
EMAIL_HOST_USER = "8dca1d001@smtp-brevo.com"
EMAIL_HOST_PASSWORD = "VrNUkDdcR5G9AL8P"
EMAIL_SENDER = "testypesty54@gmail.com"
EMAIL_RECIPIENT = "daniprav@gmail.com"  # The user who receives the summary

# Logging Configuration
LOG_FILE_PATH = "app_analysis.log"
LOG_LEVEL = "INFO" # DEBUG, INFO, WARNING, ERROR, CRITICAL

# API Client Settings
API_REQUEST_TIMEOUT = 30  # seconds
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 5  # seconds

# Analysis Settings
# Example: Define how many news articles to fetch
MAX_NEWS_ARTICLES_PER_QUERY = 10
# Example: Minimum market cap for considering a stock (if applicable)
MIN_MARKET_CAP = 1000000000 # 1 Billion

# Path to store cached API responses if we implement file-based caching as a fallback
CACHE_DIR = "api_cache"
CACHE_EXPIRY_SECONDS = 3600 # 1 hour for general data, financial statements might be longer

# Current Google API Key Index (for rotation)
# This will be managed by the API client, but initialized here or in a state file if persistence between runs is needed without a DB
# For simplicity, let's assume the api_client module handles this in memory for a single run.
# If the script runs frequently and independently, this might need to be stored in the DB or a file.
CURRENT_GOOGLE_API_KEY_INDEX = 0
