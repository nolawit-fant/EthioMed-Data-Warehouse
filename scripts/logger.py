import logging
from logging.handlers import RotatingFileHandler

class Logger:
    def __init__(self, log_file='../data/scraper.log', max_bytes=10*1024*1024, backup_count=5):
        """
        Initializes the ScraperLogger with rotating file handler.

        Parameters:
        ----------
        log_file : str
            The name of the log file.
        max_bytes : int
            The maximum size of the log file in bytes before rotation.
        backup_count : int
            The number of backup log files to keep.
        """
        # Configure the logger
        self.logger = logging.getLogger("TelegramScraper")
        self.logger.setLevel(logging.INFO)

        # Create a rotating file handler
        handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        handler.setLevel(logging.INFO)

        # Define logging format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(handler)

    def info(self, message):
        """Log an info message."""
        self.logger.info(message)

    def error(self, message):
        """Log an error message."""
        self.logger.error(message)

    def warning(self, message):
        """Log a warning message."""
        self.logger.warning(message)

    def debug(self, message):
        """Log a debug message."""
        self.logger.debug(message)

# Usage Example
if __name__ == "__main__":
    # Specify the log file path dynamically
    log_file_path = '../data/scraper.log'  # You can change this path as needed

    # Initialize the logger with the specified log file path
    logger = Logger(log_file=log_file_path)

    # Log some messages
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.debug("This is a debug message.")