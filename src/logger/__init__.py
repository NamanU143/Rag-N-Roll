# # Import necessary libraries
# import os
# import logging
# from datetime import datetime
# LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

# logs_path = os.path.join(os.getcwd(), "\tmp", LOG_FILE)

# os.makedirs(logs_path, exist_ok=True)

# LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)

# # Configure the logging module
# logging.basicConfig(
#     filename=LOG_FILE_PATH,
#     level=logging.DEBUG,
#     format='[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s: %(message)s'
#     # Define the log message format. Here:
#     # - %(asctime)s: The timestamp of the log message
#     # - %(lineno)d: The line number where the log message was issued
#     # - %(name)s: The name of the logger (usually the name of the Python module)
#     # - %(levelname)s: The log level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
#     # - %(message)s: The actual log message content
# )

import logging
import os
from datetime import datetime

def setup_logger():
    import logging
    # Create a 'logs' folder if it doesn't exist
    log_folder = ".\logs"
    os.makedirs(log_folder, exist_ok=True)

    # Generate a unique log filename with a timestamp
    log_filename = os.path.join(
        log_folder,
        datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
    )

    # Set up the logging configuration
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s (%(lineno)d): %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            # logging.StreamHandler()  # this line logs to the console
        ]
    )
