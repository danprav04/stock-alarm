# main.py
import argparse
from datetime import datetime, timezone
from sqlalchemy.orm import joinedload  # <--- ADDED THIS IMPORT
from database import init_db, get_db_session
from error_handler import logger
import time

from stock_analyzer import StockAnalyzer
from ipo_analyzer import IPOAnalyzer
from news_analyzer import NewsAnalyzer
from email_generator import EmailGenerator
from models import StockAnalysis, IPOAnalysis, NewsEventAnalysis
from config import MAX_NEWS_TO_ANALYZE_PER_RUN


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
        except RuntimeError as rt_err:
            logger.error(f"Could not run stock analysis for {ticker} due to critical init error: {rt_err}")
        except Exception as e:
            logger.error(f"Error analyzing stock {ticker}: {e}", exc_info=True)
        time.sleep(2)
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
    db_session = next(get_db_session())
    today_start_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        recent_stock_analyses = db_session.query(StockAnalysis).filter(StockAnalysis.analysis_date >= today_start_utc) \
            .options(joinedload(StockAnalysis.stock)).all()
        recent_ipo_analyses = db_session.query(IPOAnalysis).filter(IPOAnalysis.analysis_date >= today_start_utc) \
            .options(joinedload(IPOAnalysis.ipo)).all()
        recent_news_analyses = db_session.query(NewsEventAnalysis).filter(
            NewsEventAnalysis.analysis_date >= today_start_utc) \
            .options(joinedload(NewsEventAnalysis.news_event)).all()

        logger.info(
            f"Found {len(recent_stock_analyses)} stock analyses, {len(recent_ipo_analyses)} IPO analyses, "
            f"{len(recent_news_analyses)} news analyses since {today_start_utc.strftime('%Y-%m-%d %H:%M:%S %Z')} for email."
        )

        if not any([recent_stock_analyses, recent_ipo_analyses, recent_news_analyses]):
            logger.info("No new analyses performed recently to include in the email summary.")
            return

        email_gen = EmailGenerator()
        email_message = email_gen.create_summary_email(
            stock_analyses=recent_stock_analyses,
            ipo_analyses=recent_ipo_analyses,
            news_analyses=recent_news_analyses
        )

        if email_message:
            email_gen.send_email(email_message)
        else:
            logger.error("Failed to create the email message (returned None).")

    except Exception as e:
        logger.error(f"Error generating or sending email summary: {e}", exc_info=True)
    finally:
        if db_session.is_active:  # Ensure session is closed
            db_session.close()


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
                        help="Run all analyses (stocks from a predefined list, IPOs, News) and send email. Define stock list below.")

    args = parser.parse_args()

    if args.init_db:
        logger.info("Initializing database as per command line argument...")
        try:
            init_db()
            logger.info("Database initialization complete.")
        except Exception as e:
            logger.critical(f"Database initialization failed: {e}", exc_info=True)
            return

    if args.all:
        default_stocks_for_all = ["AAPL", "MSFT", "GOOGL", "NVDA", "JPM"]
        logger.info(
            f"Running all analyses for default stocks: {default_stocks_for_all}, IPOs, and News (max {args.news_count_analyze} items).")
        if default_stocks_for_all: run_stock_analysis(default_stocks_for_all)
        time.sleep(5)
        run_ipo_analysis()
        time.sleep(5)
        run_news_analysis(category=args.news_category, count_to_analyze=args.news_count_analyze)
        time.sleep(5)
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

    if not (
            args.analyze_stocks or args.analyze_ipos or args.analyze_news or args.send_email or args.init_db or args.all):
        logger.info("No action specified. Use --help for options.")
        parser.print_help()

    logger.info("--- Main script execution finished. ---")


if __name__ == "__main__":
    script_start_time = datetime.now(timezone.utc)
    logger.info("===================================================================")
    logger.info(f"Starting Financial Analysis Script at {script_start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("===================================================================")

    main()

    script_end_time = datetime.now(timezone.utc)
    logger.info(f"Financial Analysis Script finished at {script_end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Total execution time: {script_end_time - script_start_time}")
    logger.info("===================================================================")