import logging
import os
from datetime import datetime

def setup_logger(log_directory: str = "logs") -> logging.Logger:
    """
    Set up and return a logger that logs to both console and a file in `log_directory`.
    """
    os.makedirs(log_directory, exist_ok=True)
    logger = logging.getLogger("youtube_archiver")
    logger.setLevel(logging.DEBUG)

    # Create file handler
    log_filename = os.path.join(
        log_directory,
        f"youtube_archiver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Avoid adding multiple handlers if logger already has them
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
