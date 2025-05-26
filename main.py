# main.py
import argparse
from datetime import datetime, timezone
from sqlalchemy.orm import joinedload
# import time # No longer needed for arbitrary sleeps
import sys

from database.connection import init_db, SessionLocal
from core.logging_setup import logger, handle_global_exception
from services import StockAnalyzer, IPOAnalyzer, NewsAnalyzer, EmailService
from database.models import StockAnalysis, IPOAnalysis, NewsEventAnalysis
from core.config import MAX_NEWS_TO_ANALYZE_PER_RUN, DEFAULT_STOCKS_FOR_ALL_MODE


def run_stock_analysis(tickers):
    logger.info(f"--- Starting Individual Stock Analysis for: {tickers} ---")
    results = []
    for ticker in tickers:
        try:
            analyzer = StockAnalyzer(ticker=ticker)
            analysis_result = analyzer.analyze()
            if analysis_result:
                results.append(analysis_result)
            else:
                logger.warning(f"Stock analysis for {ticker} did not return a result object.")
        except RuntimeError as rt_err:  # Catch specific init error
            logger.error(f"Could not run stock analysis for {ticker} due to critical init error: {rt_err}")
        except Exception as e:
            logger.error(f"Error analyzing stock {ticker}: {e}", exc_info=True)
        # Removed time.sleep(5) - API client's base_client handles delays/retries
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


def run_news_analysis(category="general", count_to_analyze=MAX_NEWS_TO_ANALYZE_PER_RUN):
    logger.info(f"--- Starting News Analysis Pipeline (Category: {category}, Max to Analyze: {count_to_analyze}) ---")
    try:
        analyzer = NewsAnalyzer()
        results = analyzer.run_news_analysis_pipeline(category=category, count_to_analyze_this_run=count_to_analyze)
        return results
    except Exception as e:
        logger.error(f"Error during news analysis pipeline: {e}", exc_info=True)
        return []


def generate_and_send_todays_email_summary():
    logger.info("--- Generating Today's Email Summary ---")
    db_session = SessionLocal()
    today_start_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        # Eager load related objects to prevent N+1 queries in email formatting
        recent_stock_analyses = db_session.query(StockAnalysis).options(joinedload(StockAnalysis.stock)).filter(
            StockAnalysis.analysis_date >= today_start_utc).all()
        recent_ipo_analyses = db_session.query(IPOAnalysis).options(joinedload(IPOAnalysis.ipo)).filter(
            IPOAnalysis.analysis_date >= today_start_utc).all()
        recent_news_analyses = db_session.query(NewsEventAnalysis).options(
            joinedload(NewsEventAnalysis.news_event)).filter(NewsEventAnalysis.analysis_date >= today_start_utc).all()

        logger.info(
            f"Found {len(recent_stock_analyses)} stock analyses, {len(recent_ipo_analyses)} IPO analyses, {len(recent_news_analyses)} news analyses since {today_start_utc.strftime('%Y-%m-%d %H:%M:%S %Z')} for email.")

        if not any([recent_stock_analyses, recent_ipo_analyses, recent_news_analyses]):
            logger.info("No new analyses performed recently to include in the email summary.")
            return

        email_svc = EmailService()
        email_message = email_svc.create_summary_email(
            stock_analyses=recent_stock_analyses,
            ipo_analyses=recent_ipo_analyses,
            news_analyses=recent_news_analyses
        )
        if email_message:
            email_svc.send_email(email_message)
        else:
            logger.error("Failed to create the email message (returned None).")
    except Exception as e:
        logger.error(f"Error generating or sending email summary: {e}", exc_info=True)
    finally:
        SessionLocal.remove()


def main():
    parser = argparse.ArgumentParser(description="Financial Analysis and Reporting Tool")
    parser.add_argument("--analyze-stocks", nargs="+", metavar="TICKER",
                        help="List of stock tickers to analyze (e.g., AAPL MSFT)")
    parser.add_argument("--analyze-ipos", action="store_true", help="Run IPO analysis pipeline.")
    parser.add_argument("--analyze-news", action="store_true", help="Run news analysis pipeline.")
    parser.add_argument("--news-category", default="general",
                        help="Category for news analysis (e.g., general, forex, crypto, merger).")
    parser.add_argument("--news-count-analyze", type=int, default=MAX_NEWS_TO_ANALYZE_PER_RUN,
                        help=f"Max number of new news items to analyze in this run (default from config: {MAX_NEWS_TO_ANALYZE_PER_RUN}).")
    parser.add_argument("--send-email", action="store_true",
                        help="Generate and send email summary of today's/recent analyses.")
    parser.add_argument("--init-db", action="store_true", help="Initialize the database (create tables).")
    parser.add_argument("--all", action="store_true",
                        help="Run all analyses (stocks from a predefined list, IPOs, News) and send email.")

    args = parser.parse_args()

    if args.init_db:
        logger.info("Initializing database as per command line argument...")
        try:
            init_db()
            logger.info("Database initialization complete.")
        except Exception as e:
            logger.critical(f"Database initialization failed: {e}", exc_info=True)
            return  # Exit if DB init fails

    if args.all:
        logger.info(
            f"Running all analyses for default stocks: {DEFAULT_STOCKS_FOR_ALL_MODE}, IPOs, and News (max {args.news_count_analyze} items).")
        if DEFAULT_STOCKS_FOR_ALL_MODE:
            run_stock_analysis(DEFAULT_STOCKS_FOR_ALL_MODE)
        run_ipo_analysis()
        run_news_analysis(category=args.news_category, count_to_analyze=args.news_count_analyze)
        generate_and_send_todays_email_summary()
        logger.info("--- '--all' tasks finished. ---")
        return

    if args.analyze_stocks:
        run_stock_analysis(args.analyze_stocks)
    if args.analyze_ipos:
        run_ipo_analysis()
    if args.analyze_news:
        run_news_analysis(category=args.news_category, count_to_analyze=args.news_count_analyze)
    if args.send_email:
        generate_and_send_todays_email_summary()

    if not any([args.analyze_stocks, args.analyze_ipos, args.analyze_news, args.send_email, args.init_db, args.all]):
        logger.info("No action specified. Use --help for options.")
        parser.print_help()

    logger.info("--- Main script execution finished. ---")


if __name__ == "__main__":
    # sys.excepthook = handle_global_exception # Uncomment to enable global exception logging
    script_start_time = datetime.now(timezone.utc)
    logger.info("===================================================================")
    logger.info(f"Starting Financial Analysis Script at {script_start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("===================================================================")

    main()

    script_end_time = datetime.now(timezone.utc)
    logger.info(f"Financial Analysis Script finished at {script_end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Total execution time: {script_end_time - script_start_time}")
    logger.info("===================================================================")