from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from core.config import DATABASE_URL
from core.logging_setup import logger

Base = declarative_base() # Moved from models.py

try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    SessionLocal = scoped_session(SessionFactory)

    # Base.query = SessionLocal.query_property() # This is optional

    def init_db():
        """Initializes the database and creates tables if they don't exist."""
        try:
            logger.info("Initializing database and creating tables...")
            # Import models here to ensure they are registered with Base metadata
            from . import models # noqa F401
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables created successfully (if they didn't exist).")
        except Exception as e:
            logger.critical(f"CRITICAL Error initializing database: {e}", exc_info=True)
            raise

    def get_db_session():
        """Provides a database session. Caller is responsible for closing."""
        db = SessionLocal()
        try:
            yield db
        finally:
            # SessionLocal.remove() is called automatically by analyzers or when scope ends
            # For direct usage in main.py, ensure SessionLocal.remove() or db.close() is called.
             if db.is_active: # Check if session is still active before closing
                db.close()


except Exception as e:
    logger.critical(f"CRITICAL Failed to connect to database or setup SQLAlchemy: {e}", exc_info=True)
    raise