import logging
import os
from datetime import datetime

LOG_FILE_PATH = None  # Global variable to ensure a single log file

def setup_logger():
    global LOG_FILE_PATH

    if LOG_FILE_PATH is not None:
        return  # Logging is already configured, no need to set it up again

    log_folder = "./logs"
    os.makedirs(log_folder, exist_ok=True)

    LOG_FILE_PATH = os.path.join(
        log_folder, datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
    )

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s (%(lineno)d): %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE_PATH),
            # logging.StreamHandler()  # Uncomment to log to console
        ]
    )

    # Set specific loggers to only log errors
    for logger_name in ["snowflake.connector", "snowflake.core.rest", "urllib3.connectionpool"]:
        logging.getLogger(logger_name).setLevel(logging.ERROR)
