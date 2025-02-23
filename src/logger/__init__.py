# import logging
# import os
# from datetime import datetime

# LOG_FILE_PATH = None  # Global variable to ensure a single log file

# def setup_logger():
#     global LOG_FILE_PATH

#     if LOG_FILE_PATH is not None:
#         return  # Logging is already configured, no need to set it up again

#     log_folder = "./logs"
#     os.makedirs(log_folder, exist_ok=True)

#     LOG_FILE_PATH = os.path.join(
#         log_folder, datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
#     )

#     logging.basicConfig(
#         level=logging.DEBUG,
#         format="%(asctime)s [%(levelname)s] %(name)s (%(lineno)d): %(message)s",
#         handlers=[
#             logging.FileHandler(LOG_FILE_PATH),
#             # logging.StreamHandler()  # Uncomment to log to console
#         ]
#     )

#     # Set specific loggers to only log errors
#     for logger_name in ["snowflake.connector", "snowflake.core.rest", "urllib3.connectionpool"]:
#         logging.getLogger(logger_name).setLevel(logging.ERROR)

############################################################################################################################################

# Following is the code of logging without initializing any setup logger function 
# due to following code we can directly use the logging .


import logging
import os
from datetime import datetime

# Ensure logs directory exists
log_folder = os.path.join(os.getcwd(), "logs")
os.makedirs(log_folder, exist_ok=True)

# Single log file for all modules
log_file_path = os.path.join(log_folder, datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log")

# Configure logging globally
logging.basicConfig(
    level=logging.DEBUG,  # Set log level
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),  # Log to file
        # logging.StreamHandler()  # Log to console
    ]
)

# Reduce verbosity for noisy loggers
for logger_name in ["snowflake.connector", "snowflake.core.rest", "urllib3.connectionpool"]:
    logging.getLogger(logger_name).setLevel(logging.ERROR)
