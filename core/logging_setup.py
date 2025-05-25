import logging
import sys
from .config import LOG_FILE_PATH, LOG_LEVEL

_logging_configured = False

def setup_logging():
    """Configures logging for the application."""
    global _logging_configured
    if _logging_configured:
        return logging.getLogger()

    numeric_level = getattr(logging, LOG_LEVEL.upper(), None)
    if not isinstance(numeric_level, int):
        logging.warning(f"Invalid log level: {LOG_LEVEL} in config. Defaulting to INFO.")
        numeric_level = logging.INFO

    logger_obj = logging.getLogger()
    logger_obj.setLevel(numeric_level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')

    if not any(isinstance(h, logging.StreamHandler) for h in logger_obj.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger_obj.addHandler(console_handler)

    if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == LOG_FILE_PATH for h in logger_obj.handlers):
        try:
            file_handler = logging.FileHandler(LOG_FILE_PATH, mode='a')
            file_handler.setFormatter(formatter)
            logger_obj.addHandler(file_handler)
        except Exception as e:
            logging.error(f"Failed to set up file handler for {LOG_FILE_PATH}: {e}", exc_info=True)

    _logging_configured = True
    return logger_obj

logger = setup_logging()

def handle_global_exception(exc_type, exc_value, exc_traceback):
    """Custom global exception handler to log unhandled exceptions."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled global exception:", exc_info=(exc_type, exc_value, exc_traceback))

# To use the global exception handler, uncomment the following line in your main script (e.g., main.py)
# sys.excepthook = handle_global_exception