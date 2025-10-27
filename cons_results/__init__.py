import logging
import os
from logging.handlers import MemoryHandler
from pathlib import Path

from cons_results.utilities.utils import S3LoggingHandler, configure_s3_client

PROJECT_NAME = "cons_results"
logger = logging.getLogger(PROJECT_NAME)

# Get the mbs logger.
# Ensure that logger name matches MBS configuration. E.g., "MBS" is used here.
mbs_logger = logging.getLogger("MBS")

BUFFER_CAPACITY = 1000

# Using MemoryHandler to buffer log records util FileHandler is ready
memory_handler = MemoryHandler(BUFFER_CAPACITY)

# Add MemoryHandler to both loggers to capture early logs
logger.addHandler(memory_handler)
mbs_logger.addHandler(memory_handler)

# Set levels and propagation for both loggers
logger.setLevel(logging.DEBUG)
mbs_logger.setLevel(logging.DEBUG)
logger.propagate = False
mbs_logger.propagate = False

# Remove any pre-existing handlers from MBS (e.g. its own FileHandler)
for handler in list(mbs_logger.handlers):
    if isinstance(handler, logging.FileHandler):
        mbs_logger.removeHandler(handler)
        handler.close()


# Global variable for run_id (start as None; updated later)
run_id = None

logging_str = (
    "[%(asctime)s: %(name)s: %(levelname)s: %(module)s: "
    "%(funcName)s: %(lineno)d] [run_id=%(run_id)s] %(message)s"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(logging_str)
console_handler.setFormatter(formatter)

# Add filter to both loggers
logger.addHandler(console_handler)
mbs_logger.addHandler(console_handler)


# Custom filter to add run_id to the log record
class RunIDFilter(logging.Filter):
    def filter(self, record):
        record.run_id = str(run_id) if run_id else "_"
        return True


logger.addFilter(RunIDFilter())
mbs_logger.addFilter(RunIDFilter())


def get_file_handler_for_run_id(config):
    """Function to create a FileHandler for a specific run_id."""
    platform = config.get("platform", "network")
    run_id = config["run_id"]

    if platform.lower() == "s3":
        s3_client = configure_s3_client(config)
        s3_bucket = config.get("bucket")
        s3_key = (
            f"bat/cons_results_files/{PROJECT_NAME}_logs/{PROJECT_NAME}_{run_id}.log"
        )
        # s3_key = f"ons/construction/{PROJECT_NAME}_logs/{PROJECT_NAME}_{run_id}.log"

        file_handler = S3LoggingHandler(s3_bucket, s3_key, s3_client)
    else:
        # Create FileHandler when run_id is known (shared for both loggers)
        project_root = Path(__file__).resolve().parent.parent
        project_name = PROJECT_NAME  # project_root.name
        log_dir = project_root / f"{project_name}_logs"
        os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(
            os.path.join(log_dir, f"{PROJECT_NAME}_{run_id}.log")
        )

    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(logging_str)
    file_handler.setFormatter(formatter)

    return file_handler


def configure_logger_with_run_id(config):
    """Function to configure logger with a specific run_id and set up FileHandler."""
    global run_id
    run_id = config["run_id"]

    file_handler = get_file_handler_for_run_id(config)

    loggers = [logger, mbs_logger]

    for log in loggers:
        # Remove existing FileHandlers to avoid duplicates
        seen_types = set()
        for handler in list(log.handlers):
            if isinstance(handler, logging.FileHandler):
                handler_type = type(handler)
                if handler_type not in seen_types:
                    seen_types.add(handler_type)
                    log.removeHandler(handler)
                    handler.close()
                else:
                    seen_types.add(handler_type)

    # Flush MemoryHandler to FileHandler
    for handler in list(logger.handlers):
        if isinstance(handler, MemoryHandler):
            handler.setTarget(file_handler)
            handler.flush()

            for log in loggers:
                log.removeHandler(handler)
            handler.close()

    # Add the new FileHandler to the logger
    for log in loggers:
        log.addHandler(file_handler)

    logger.info(
        f"Logger configured with run_id: {run_id}. Buffered logs flushed to file."
    )
    mbs_logger.info(
        f"MBS Logger configured with run_id: {run_id}. Buffered logs flushed to file."
    )
