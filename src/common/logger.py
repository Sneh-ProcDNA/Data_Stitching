import logging
import sys
from logging.handlers import RotatingFileHandler

def get_logger(name: str = "pipeline", log_file: str = "pipeline.log", level=logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger  # Avoid duplicate handlers on re-import

    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler (rotating: max 10MB, keep 5 backups)
    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger