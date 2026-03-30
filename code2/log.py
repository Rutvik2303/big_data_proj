# logger_config.py
import logging
import os

log_file = "/tmp/rutvik_proj/logs/pyspark_etl.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def get_logger(name):
    return logging.getLogger(name)