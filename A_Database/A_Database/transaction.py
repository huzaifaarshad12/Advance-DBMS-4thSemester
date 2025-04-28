from typing import Dict, List
from database import Collection
from logger import Logger
from storage import Storage

class Transaction:
    def __init__(self, collection: Collection):
        self.collection = collection
        self.logger = Logger("Transaction", log_file="transaction.log")
        self.original_data = collection.data.copy()
        self.original_indexes = collection.indexes.copy()
        self.operations = []

    def insert(self, record: Dict, user_role: str):
        self.operations.append(("insert", record, user_role))
        self.logger.info(f"Queued insert: {record}")

    def update(self, condition: Dict, update_data: Dict, user_role: str):
        self.operations.append(("update", condition, update_data, user_role))
        self.logger.info(f"Queued update: condition={condition}, data={update_data}")

    def delete(self, condition: Dict, user_role: str):
        self.operations.append(("delete", condition, user_role))
        self.logger.info(f"Queued delete: condition={condition}")

    def commit(self):
        try:
            for op in self.operations:
                op_type = op[0]
                if op_type == "insert":
                    self.collection.insert(op[1], op[2])
                elif op_type == "update":
                    self.collection.update(op[1], op[2], op[3])
                elif op_type == "delete":
                    self.collection.delete(op[1], op[2])
            self.logger.info("Transaction committed")
        except Exception as e:
            self.rollback()
            self.logger.error(f"Commit failed: {e}")
            raise

    def rollback(self):
        self.collection.data = self.original_data.copy()
        self.collection.indexes = self.original_indexes.copy()
        self.collection.save_data()
        self.logger.info("Transaction rolled back")