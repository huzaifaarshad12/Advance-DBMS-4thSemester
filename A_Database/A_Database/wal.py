from typing import Dict, List
from storage import Storage
from logger import Logger

class WAL:
    def __init__(self, log_file: str = "mydb.log"):
        self.log_file = log_file
        self.logger = Logger("WAL", log_file="wal.log")

    def log(self, operation: str, key: str = None, data: Dict = None, conditions: Dict = None):
        Storage.log_operation(operation, key, data, conditions)
        self.logger.info(f"WAL logged: {operation}, key={key}")

    def recover(self) -> List[Dict]:
        logs = Storage.read_log()
        self.logger.info(f"WAL recovered {len(logs)} log entries")
        return logs

    def clear(self):
        Storage.clear_log()
        self.logger.info("WAL cleared")