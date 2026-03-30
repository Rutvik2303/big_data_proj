import logging
import os
from logging.handlers import RotatingFileHandler

# Log path
log_dir = "/tmp/rutvik/project/logs"
log_file = os.path.join(log_dir, "pipeline.log")

# Create directory if not exists
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not logger.handlers:
        # File handler (with rotation)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=3
        )

        # Console handler
        stream_handler = logging.StreamHandler()

        # Format
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        # Prevent duplicate logs from root
        logger.propagate = False

    return logger