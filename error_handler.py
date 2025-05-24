# error_handler.py
import logging
import sys
from config import LOG_FILE_PATH, LOG_LEVEL # Ensure these are imported

# Global flag to prevent multiple handler additions if setup_logging is called more than once
# though with import-time setup, this is less of an issue.
_logging_configured = False

def setup_logging():
    """Configures logging for the application."""
    global _logging_configured
    if _logging_configured:
        return logging.getLogger() # Return existing root logger if already configured

    numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
    if not isinstance(numeric_level, int):
        # Fallback to INFO if invalid level is provided in config
        logging.warning(f"Invalid log level: {LOG_LEVEL} in config. Defaulting to INFO.")
        numeric_level = logging.INFO

    # Get the root logger
    logger_obj = logging.getLogger() # Get the root logger
    logger_obj.setLevel(numeric_level) # Set its level

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')

    # Console Handler
    if not any(isinstance(h, logging.StreamHandler) for h in logger_obj.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger_obj.addHandler(console_handler)

    # File Handler
    # Check if a file handler for the specific LOG_FILE_PATH is already attached
    if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == LOG_FILE_PATH for h in logger_obj.handlers):
        try:
            file_handler = logging.FileHandler(LOG_FILE_PATH, mode='a') # Append mode
            file_handler.setFormatter(formatter)
            logger_obj.addHandler(file_handler)
        except Exception as e:
            # If file handler fails, log to console and continue
            logging.error(f"Failed to set up file handler for {LOG_FILE_PATH}: {e}", exc_info=True)


    _logging_configured = True
    return logger_obj # Return the configured root logger

# Initialize logger when this module is imported
logger = setup_logging()

def handle_global_exception(exc_type, exc_value, exc_traceback):
    """Custom global exception handler to log unhandled exceptions via the root logger."""
    if issubclass(exc_type, KeyboardInterrupt):
        # Call the default hook for KeyboardInterrupt so it behaves as expected (exit).
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # Log other unhandled exceptions.
    logger.critical("Unhandled global exception:", exc_info=(exc_type, exc_value, exc_traceback))

# Set the custom global exception hook.
# This will catch any exceptions not caught elsewhere in the application.
# sys.excepthook = handle_global_exception # Uncomment this in main.py or at the top of your script if desired.