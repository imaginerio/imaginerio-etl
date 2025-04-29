import datetime
import logging.config
import os


class CustomFormatter(logging.Formatter):
    # format = "%(asctime)s - %(levelname)s - %(message)s"

    """ANSI color codes"""
    BLACK = "\033[0;30m"
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BROWN = "\033[0;33m"
    BLUE = "\033[0;34m"
    PURPLE = "\033[0;35m"
    CYAN = "\033[0;36m"
    LIGHT_GRAY = "\033[0;37m"
    DARK_GRAY = "\033[1;30m"
    LIGHT_RED = "\033[1;31m"
    LIGHT_GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    LIGHT_BLUE = "\033[1;34m"
    LIGHT_PURPLE = "\033[1;35m"
    LIGHT_CYAN = "\033[1;36m"
    LIGHT_WHITE = "\033[1;37m"
    BOLD = "\033[1m"
    FAINT = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    NEGATIVE = "\033[7m"
    CROSSED = "\033[9m"
    RESET = "\033[0m"

    COLORS = {
        "WARNING": YELLOW,
        "INFO": BLUE,
        "DEBUG": PURPLE,
        "ERROR": RED,
    }

    def format(self, record):
        # Save original format
        original_format = self._style._fmt

        # Colorize levelname and asctime directly in the format string
        self._style._fmt = original_format.replace(
            "%(levelname)s",
            self.COLORS.get(record.levelname, "") + "%(levelname)s" + self.RESET,
        )

        # Apply color to asctime by temporarily adjusting the format string
        formatted_asctime = (
            self.CYAN + self.formatTime(record, self.datefmt) + self.RESET
        )
        self._style._fmt = self._style._fmt.replace("%(asctime)s", formatted_asctime)

        # Call the super class format, which will now use the modified format string
        formatted_message = super().format(record)

        # Include the actual message content
        # The super().format already includes record.message formatted according to the format string,
        # so if "%(message)s" is part of self._fmt, it will be handled by super().format(record)

        # Reset format to original to avoid affecting subsequent messages
        self._style._fmt = original_format

        return formatted_message


# os.makedirs("logs", exist_ok=True)
# log_filename = f"logs/log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
        "handlers": {
            # "fileHandler": {
            #     "class": logging.FileHandler,
            #     "formatter": "custom",
            #     "filename": log_filename,
            # },
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "custom",
                "level": "DEBUG",
                "stream": "ext://sys.stdout",  # Use sys.stdout
            },
        },
        "formatters": {
            "custom": {  # Name your formatter; referred in handlers
                "()": CustomFormatter,  # Callable that returns an instance of CustomFormatter
                "fmt": "%(asctime)s - %(levelname)s - %(message)s",
                # "datefmt": "%Y-%m-%d %H:%M:%S",  # Optional: Specify date format
            },
        },
        "root": {
            "handlers": ["console"],  # "fileHandler",
            "level": "DEBUG",
        },
    }
)

logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("nose").setLevel(logging.CRITICAL)
logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)
