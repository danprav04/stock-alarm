# database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session # scoped_session helps manage sessions in web apps or threads
from config import DATABASE_URL
from error_handler import logger

try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True) # pool_pre_ping can help with stale connections
    # Use scoped_session for thread safety if different parts of app might access DB concurrently.
    # For a sequential script, simple sessionmaker might be enough, but scoped_session is more robust.
    SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    SessionLocal = scoped_session(SessionFactory) # Creates a thread-local session

    Base = declarative_base()
    Base.query = SessionLocal.query_property() # For convenience if using Flask-SQLAlchemy style queries, less common now

    def init_db():
        """Initializes the database and creates tables if they don't exist."""
        try:
            logger.info("Initializing database and creating tables...")
            # Import all modules here that define models so that
            # they are registered with the Base meta-data when Base.metadata.create_all is called.
            import models # noqa F401 (flake8 ignore 'imported but unused')
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables created successfully (if they didn't exist).")
        except Exception as e:
            logger.critical(f"CRITICAL Error initializing database: {e}", exc_info=True)
            raise # Re-raise to halt execution if DB init fails

    def get_db_session():
        """Provides a database session. Caller is responsible for closing."""
        db = SessionLocal()
        try:
            yield db
        finally:
            # The session is closed by the context manager (StockAnalyzer, IPOAnalyzer, etc.)
            # or here if used in a `with get_db_session() as session:` block in main.
            # However, analyzers now manage their own sessions.
            # For main.py's email summary, it will close its own session.
            # If this function is directly used elsewhere, ensure closure.
            # Scoped session handles removal, but explicit close is good practice for non-scoped use.
            # SessionLocal.remove() is called automatically when the scope (e.g. thread) ends.
            # For a script, direct SessionLocal() then .close() is fine.
            # This generator pattern is more common for web request scopes.
            # For the analyzers, they call SessionLocal() directly.
            # For main.py's direct usage:
            if db.is_active:
                db.close()


except Exception as e:
    logger.critical(f"CRITICAL Failed to connect to database or setup SQLAlchemy: {e}", exc_info=True)
    raise # Re-raise as DB is critical