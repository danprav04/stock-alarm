# main.py
import argparse
from datetime import datetime
from database import init_db, get_db_session
from error_handler import logger  # , setup_logging, handle_exception # uncomment if sys.excepthook is used here
# import sys # uncomment if sys.excepthook is used here

from stock_analyzer import StockAnalyzer
from ipo_analyzer import IPOAnalyzer
from news_analyzer import NewsAnalyzer
from email_generator import EmailGenerator
from models import StockAnalysis, IPOAnalysis, NewsEventAnalysis  # For querying results


# sys.excepthook = handle_exception # Optional: for top-level unhandled exception logging

def run_stock_analysis(tickers):
    logger.info(f"--- Starting Individual Stock Analysis for: {tickers} ---")
    results = []
    for ticker in tickers:
        try:
            analyzer = StockAnalyzer(ticker=ticker)
            analysis_result = analyzer.analyze()
            if analysis_result:
                results.append(analysis_result)
        except Exception as e:
            logger.error(f"Error analyzing stock {ticker}: {e}", exc_info=True)
    return results


def run_ipo_analysis():
    logger.info("--- Starting IPO Analysis Pipeline ---")
    try:
        analyzer = IPOAnalyzer()
        results = analyzer.run_ipo_analysis_pipeline()
        return results
    except Exception as e:
        logger.error(f"Error during IPO analysis pipeline: {e}", exc_info=True)
        return []


def run_news_analysis(category="general", count=5):
    logger.info(f"--- Starting News Analysis Pipeline (Category: {category}, Count: {count}) ---")
    try:
        analyzer = NewsAnalyzer()
        results = analyzer.run_news_analysis_pipeline(category=category, count=count)
        return results
    except Exception as e:
        logger.error(f"Error during news analysis pipeline: {e}", exc_info=True)
        return []


def generate_and_send_todays_email_summary():
    logger.info("--- Generating Today's Email Summary ---")
    db_session = next(get_db_session())
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        # Fetch analyses performed today (or recent ones)
        # This assumes analyses are being run and stored before this function is called.
        # If analyses are run *by* this function, then pass results directly.

        # For this example, let's assume we want to send an email for analyses
        # that were just run in the current script execution.
        # If we want to fetch from DB based on date:
        recent_stock_analyses = db_session.query(StockAnalysis).filter(StockAnalysis.analysis_date >= today_start).all()
        recent_ipo_analyses = db_session.query(IPOAnalysis).filter(IPOAnalysis.analysis_date >= today_start).all()
        recent_news_analyses = db_session.query(NewsEventAnalysis).filter(
            NewsEventAnalysis.analysis_date >= today_start).all()

        logger.info(
            f"Found {len(recent_stock_analyses)} stock analyses, {len(recent_ipo_analyses)} IPO analyses, {len(recent_news_analyses)} news analyses for today's email.")

        if not any([recent_stock_analyses, recent_ipo_analyses, recent_news_analyses]):
            logger.info("No new analyses performed today to include in the email summary.")
            return

        email_gen = EmailGenerator()
        email_message = email_gen.create_summary_email(
            stock_analyses=recent_stock_analyses,
            ipo_analyses=recent_ipo_analyses,
            news_analyses=recent_news_analyses
        )

        if email_message:
            # In a real scenario, uncomment to send. Ensure SMTP settings in config.py are correct.
            # email_gen.send_email(email_message)
            logger.info("Email summary created. Sending is commented out in main.py for safety.")
            # For testing, save to file:
            with open(f"daily_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                f.write(email_message.get_payload(0).get_payload(decode=True).decode())  # type: ignore
            logger.info("Email HTML saved to a file.")
        else:
            logger.error("Failed to create the email message.")

    except Exception as e:
        logger.error(f"Error generating or sending email summary: {e}", exc_info=True)
    finally:
        db_session.close()


def main():
    parser = argparse.ArgumentParser(description="Financial Analysis and Reporting Tool")
    parser.add_argument("--analyze-stocks", nargs="+", metavar="TICKER",
                        help="List of stock tickers to analyze (e.g., AAPL MSFT)")
    parser.add_argument("--analyze-ipos", action="store_true", help="Run IPO analysis pipeline.")
    parser.add_argument("--analyze-news", action="store_true", help="Run news analysis pipeline.")
    parser.add_argument("--news-category", default="general",
                        help="Category for news analysis (e.g., general, forex, crypto, merger).")
    parser.add_argument("--news-count", type=int, default=3, help="Number of new news items to analyze.")
    parser.add_argument("--send-email", action="store_true",
                        help="Generate and send email summary of today's analyses.")
    parser.add_argument("--init-db", action="store_true", help="Initialize the database (create tables).")
    parser.add_argument("--all", action="store_true",
                        help="Run all analyses (stocks from a predefined list, IPOs, News) and send email. Define stock list below.")

    args = parser.parse_args()

    if args.init_db:
        logger.info("Initializing database as per command line argument...")
        try:
            init_db()
            logger.info("Database initialization complete.")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}", exc_info=True)
            return  # Stop if DB init fails

    # --- Execution Logic ---
    stock_analysis_results = []
    ipo_analysis_results = []
    news_analysis_results = []

    if args.all:
        # Define a default list of stocks for '--all' flag
        default_stocks_for_all = ["AAPL", "MSFT", "GOOGL"]  # Example list
        logger.info(f"Running all analyses for default stocks: {default_stocks_for_all}, IPOs, and News.")
        stock_analysis_results = run_stock_analysis(default_stocks_for_all)
        ipo_analysis_results = run_ipo_analysis()
        news_analysis_results = run_news_analysis(category=args.news_category, count=args.news_count)
        # The generate_and_send_todays_email_summary will pick these up if run on the same day
        # Or, you can pass these results directly if you modify generate_and_send_todays_email_summary
        generate_and_send_todays_email_summary()  # This will fetch from DB based on today's date
        return

    if args.analyze_stocks:
        stock_analysis_results = run_stock_analysis(args.analyze_stocks)
        # If only analyzing stocks and not sending a full summary, perhaps print to console or save differently.
        # For now, results are stored in DB.

    if args.analyze_ipos:
        ipo_analysis_results = run_ipo_analysis()

    if args.analyze_news:
        news_analysis_results = run_news_analysis(category=args.news_category, count=args.news_count)

    if args.send_email:
        # This function currently fetches from DB based on today's date.
        # So, it will include any analyses run prior in the same script execution if they were committed.
        generate_and_send_todays_email_summary()

    if not (
            args.analyze_stocks or args.analyze_ipos or args.analyze_news or args.send_email or args.init_db or args.all):
        logger.info("No action specified. Use --help for options.")
        parser.print_help()

    logger.info("--- Script execution finished. ---")


if __name__ == "__main__":
    # Setup logging (error_handler.py already does this when imported, but explicit call is fine too)
    # logger = setup_logging() # Ensures logger is configured if not already

    logger.info("===================================================================")
    logger.info(f"Starting Financial Analysis Script at {datetime.now()}")
    logger.info("===================================================================")
    main()