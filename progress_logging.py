"""
Logging function with a decorator to log the ETL progress of chosen functions.
"""

import logging
from functools import wraps

def log_progress(message: str) -> None:
    """Logs a message with a timestamp to both the console and a log file.

    Args:
        message (str): The message to be logged.

    Returns:
        None
    """

    print(message)
    logging.info(message)

def log(func: callable) -> callable:
    """Decorator that logs the start and end of a function call,
    along with exception handling and re-raising.

    Args:
        func (callable): The function to be decorated.

    Returns:
        function: The decorated wrapper function.

    Raises:
        Exception: Any exception raised within the decorated function.
    """
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Log start with message and function name
            log_progress(f"Calling {func.__name__} ...")
            result = func(*args, **kwargs)
            # Log end with message and function name
            log_progress(f"Finished {func.__name__}.")
            
            return result
        
        except Exception as e:
            logging.exception(f"Exception raised in {func.__name__}. exception: {str(e)}.")
            raise e

    return wrapper
