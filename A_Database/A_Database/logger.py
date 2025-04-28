import logging
import os
from datetime import datetime
from typing import List

class Logger:
    def __init__(self, name: str, log_file: str = "app.log"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.log_file = log_file

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)

    def debug(self, message: str):
        self.logger.debug(message)

    def info(self, message: str):
        self.logger.info(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def error(self, message: str):
        self.logger.error(message)

    def critical(self, message: str):
        self.logger.critical(message)

    def get_recent_logs(self, limit: int = 10) -> List[str]:
        logs = []
        try:
            with open(self.log_file, 'r') as f:
                logs = f.readlines()
            return logs[-limit:]
        except FileNotFoundError:
            return []