# error_handler.py
import logging
import sys
from config import LOG_FILE_PATH, LOG_LEVEL

def setup_logging():
    """Configures logging for the application."""
    numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {LOG_LEVEL}")

    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(LOG_FILE_PATH)

    # Create formatters and add it to handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    # Avoid adding handlers multiple times if setup_logging is called more than once.
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    elif not any(isinstance(h, logging.FileHandler) and h.baseFilename == file_handler.baseFilename for h in logger.handlers):
        # Add file handler if not already present (e.g., for subsequent calls in an interactive session)
        logger.addHandler(file_handler)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(console_handler)


    return logger

# Initialize logger when this module is imported
logger = setup_logging()

def handle_exception(exc_type, exc_value, exc_traceback):
    """Custom exception handler to log unhandled exceptions."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))

# Set the custom exception hook
# sys.excepthook = handle_exception # You might enable this in main.py if desired for top-level unhandled exceptions