import os
import sys
import traceback


def error_message_detail(error: Exception) -> str:
    """
    Extracts detailed error information including script name and line number.
    """
    exc_type, exc_value, exc_tb = sys.exc_info()
    if exc_tb is not None:
        tb_frame = traceback.extract_tb(exc_tb)[-1]  # Get last traceback frame
        file_name = os.path.basename(tb_frame.filename)
        line_number = tb_frame.lineno
    else:
        file_name = "Unknown"
        line_number = "Unknown"
    
    return f"Error occurred in script [{file_name}] at line [{line_number}]: {str(error)}"


class CustomException(Exception):
    """
    Custom exception class that provides detailed error messages.
    """
    def __init__(self, error: Exception):
        super().__init__(str(error))
        self.error_message = error_message_detail(error)

    def __str__(self):
        return self.error_message
