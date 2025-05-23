# database.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from config import DATABASE_URL
from error_handler import logger

try:
    engine = create_engine(DATABASE_URL)
    SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base = declarative_base()
    Base.query = SessionLocal.query_property() # For easier querying

    def init_db():
        """Initializes the database and creates tables if they don't exist."""
        try:
            logger.info("Initializing database and creating tables...")
            # Import all modules here that define models so that
            # they are registered with the Base meta-data
            import models # noqa
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables created successfully (if they didn't exist).")
        except Exception as e:
            logger.error(f"Error initializing database: {e}", exc_info=True)
            raise

    def get_db_session():
        """Provides a database session."""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

except Exception as e:
    logger.error(f"Failed to connect to database or setup SQLAlchemy: {e}", exc_info=True)
    # Optionally, re-raise or exit if DB is critical
    raise