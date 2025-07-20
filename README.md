# AI-Powered Financial Analysis Engine

![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)![License](https://img.shields.io/badge/license-MIT-green.svg)![Status](https://img.shields.io/badge/status-active-brightgreen.svg)

An advanced, AI-driven backend engine designed to perform comprehensive financial analysis on stocks, upcoming IPOs, and market news. It automatically gathers vast amounts of data from multiple professional APIs, synthesizes it using Google's Gemini AI, and delivers actionable insights through a detailed daily email report.

## Core Features

-   **Deep Stock Analysis**: Performs multi-faceted analysis of individual stocks, covering:
    -   **Quantitative Metrics**: Calculates over 30 financial ratios and growth metrics.
    -   **DCF Valuation**: Generates a Discounted Cash Flow model with sensitivity analysis.
    -   **Qualitative Insights**: Fetches, parses, and summarizes 10-K filings from the SEC to analyze business models, risks, and management discussions.
    -   **Competitive Landscape**: Identifies and analyzes key competitors.
    -   **AI-Synthesized Thesis**: Generates a final investment thesis, decision, and confidence score.

-   **Intelligent IPO Analysis**: Tracks the IPO calendar, fetches S-1 filings, and uses AI to:
    -   Summarize the business model, risk factors, and financial health.
    -   Provide a preliminary investment stance and identify critical verification points.

-   **Real-time News Analysis**: Monitors market news, scrapes article content, and leverages AI to:
    -   Determine market sentiment with detailed reasoning.
    -   Identify potentially affected stocks and sectors.
    -   Estimate the impact, magnitude, and duration of the news event.

-   **Automated Reporting**: Creates and sends a beautifully formatted HTML email summary of all analyses performed each day.

-   **Robust & Resilient Architecture**:
    -   **API Aggregation**: Integrates with Finnhub, FMP, Alpha Vantage, and SEC EDGAR.
    -   **Intelligent Caching**: Caches API responses in a PostgreSQL database to minimize costs and avoid rate limits.
    -   **Error Handling**: Features built-in retries with exponential backoff for all API calls.

## How It Works: Architecture Overview

The engine is built on a modular, service-oriented architecture that ensures a clean separation of concerns. The data flows through a pipeline from initial trigger to final report.

```mermaid
flowchart TD
    A[CLI Trigger via main.py] -->|e.g., --analyze-stocks AAPL| B{Analysis Service};

    subgraph "Services Layer"
        B(StockAnalyzer);
        C(IPOAnalyzer);
        D(NewsAnalyzer);
    end

    A --> B;
    A --> C;
    A --> D;

    B --> E[Data Fetcher];
    C --> E;
    D --> E;

    subgraph "Data Acquisition & Processing"
        E(Data Fetcher) -->|Fetches Raw Data| F[API Clients];
        F[api_clients] -->|Finnhub, FMP, AlphaVantage, SEC| G{External APIs};
        E --> H[Metrics & DCF Calculators];
        E --> I[10-K & S-1 Parser];
    end

    subgraph "AI Synthesis Layer"
        J[AI Analyzer] -->|Sends Prompts| K[Gemini API];
        H --> J;
        I --> J;
    end

    subgraph "Data Persistence"
        L[DB Handler] -->|Reads/Writes| M[PostgreSQL Database];
        J --> L;
    end

    subgraph "Reporting"
        N[Email Service] -->|Sends Report| O[User Email];
        A --> |e.g., --send-email| N;
        N --> |Reads recent analyses| M;
    end
```

## Tech Stack

-   **Backend**: Python
-   **AI Model**: Google Gemini Pro
-   **Database**: PostgreSQL
-   **Core Libraries**:
    -   SQLAlchemy (ORM)
    -   Requests (HTTP Client)
    -   Psycopg2 (PostgreSQL Adapter)
    -   BeautifulSoup4 & lxml (HTML/XML Parsing)
    -   Markdown2 (HTML Conversion)
-   **Primary Data APIs**:
    -   Financial Modeling Prep (FMP)
    -   Finnhub
    -   Alpha Vantage
    -   SEC EDGAR Data

---

## Setup and Installation

### 1. Prerequisites

-   Python 3.9 or higher
-   Access to a PostgreSQL database
-   API keys for the services listed in the configuration section.

### 2. Clone the Repository

```bash
git clone <your-repository-url>
cd project_structure_backend
```

### 3. Set Up a Virtual Environment

It is highly recommended to use a virtual environment.

```bash
# For Unix/macOS
python3 -m venv venv
source venv/bin/activate

# For Windows
python -m venv venv
.\venv\Scripts\activate
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

### 5. Configuration

All configurations are managed in `core/config.py`. You **must** edit this file to add your credentials.

-   **API Keys**:
    -   `GOOGLE_API_KEYS`: A list of one or more Gemini API keys.
    -   `FINNHUB_API_KEY`: Your Finnhub API key.
    -   `FINANCIAL_MODELING_PREP_API_KEY`: Your FMP API key.
    -   `EODHD_API_KEY`: Your EODHD API key.
    -   `ALPHA_VANTAGE_API_KEY`: Your Alpha Vantage API key.
-   **Database**:
    -   `DATABASE_URL`: The connection string for your PostgreSQL database (e.g., `postgresql://user:pass@host:port/dbname`).
-   **Email**:
    -   Update `EMAIL_HOST`, `EMAIL_PORT`, `EMAIL_HOST_USER`, `EMAIL_HOST_PASSWORD`, and recipient/sender addresses to set up email reporting. The current setup is configured for Brevo (formerly Sendinblue).
-   **SEC EDGAR**:
    -   `EDGAR_USER_AGENT`: It is courteous and required by the SEC to set a descriptive User-Agent. Follow the format in the config file.

### 6. Initialize the Database

Run the following command once to create all the necessary tables in your database:

```bash
python main.py --init-db
```

## Usage (Command-Line Interface)

The application is controlled via `main.py`.

### **Analyze Stocks**

Analyze one or more stocks. The analysis includes quantitative metrics, DCF, qualitative 10-K summary, and an AI-generated investment thesis.

```bash
python main.py --analyze-stocks AAPL MSFT GOOGL
```

### **Analyze IPOs**

Run the IPO analysis pipeline. By default, it looks at a wide range of IPOs.

```bash
# Run on all relevant IPOs (limited by MAX_IPOS_TO_ANALYZE)
python main.py --analyze-ipos --max-ipos-to-analyze 5

# Focus only on upcoming IPOs
python main.py --analyze-ipos --upcoming-ipos-only --max-ipos-to-analyze 3
```

### **Analyze News**

Run the news analysis pipeline.

```bash
# Analyze the latest 5 general news items
python main.py --analyze-news --news-count-analyze 5

# Analyze merger news
python main.py --analyze-news --news-category merger
```

### **Send Email Summary**

Generate and send an email with all analyses performed since midnight UTC.

```bash
python main.py --send-email
```

### **Run All Tasks**

A convenience command to run a default set of analyses and send the summary email.

```bash
# Runs analysis on a default stock list, upcoming IPOs, and general news, then sends the email.
python main.py --all
```

### **Command-Line Help**

For a full list of commands and options, run:

```bash
python main.py --help
```

---

## Project Structure

The project is organized into logical modules to ensure scalability and maintainability.

```
├── api_clients/         # Clients for external APIs (Finnhub, FMP, etc.)
├── core/                # Core components: configuration and logging.
├── database/            # Database connection, setup, and SQLAlchemy models.
├── services/            # Main business logic for each analysis type.
│   ├── stock_analyzer/  # Logic for stock analysis pipeline.
│   ├── ipo_analyzer/    # Logic for IPO analysis pipeline.
│   └── news_analyzer/   # Logic for news analysis pipeline.
├── main.py              # Main entry point and CLI handler.
├── requirements.txt     # Python package dependencies.
└── app_analysis.log     # Log file for application output.
```
