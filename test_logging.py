import logging

# Proper logging setup without conflicting parameters
logging.basicConfig(
    level=logging.DEBUG,  # Set log level
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("test.log"),  # Logs to file
        logging.StreamHandler()  # Logs to terminal
    ]
)

logger = logging.getLogger(__name__)  # Get logger instance

logger.debug("Debug: Test message")
logger.info("Info: Test message")
logger.warning("Warning: Test message")
logger.error("Error: Test message")
logger.critical("Critical: Test message")