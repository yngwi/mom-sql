import logging
from datetime import datetime


class Logger:
    _logger = logging.getLogger("MOM-Check Logger")

    def __init__(self):
        if not self._logger.handlers:
            self._logger.setLevel(logging.DEBUG)
            console_handler = logging.StreamHandler()
            file_handler = logging.FileHandler(
                f"logs/log_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log"
            )
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)
            self._logger.addHandler(file_handler)

    def debug(self, message):
        self._logger.debug(message)

    def info(self, message):
        self._logger.info(message)

    def warn(self, message):
        self._logger.warning(message)

    def error(self, message):
        self._logger.error(message)
