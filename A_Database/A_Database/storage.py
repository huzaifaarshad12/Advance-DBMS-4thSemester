import json
import os
from mydb_types import Records, Indexes
from typing import Dict, List
from mydb_types import Data, Conditions

class Storage:
    log_file = "mydb.log"

    @staticmethod
    def to_json(data: Dict) -> str:
        def custom_serializer(obj):
            if isinstance(obj, (dict, list, str, int, float, bool, type(None))):
                return obj
            return str(obj)
        return json.dumps(data, default=custom_serializer, indent=None, separators=(",", ":"))

    @staticmethod
    def parse_json(s: str) -> Dict:
        if not s or s == "{}":
            return {}
        try:
            return json.loads(s)
        except json.JSONDecodeError as e:
            print(f"Warning: Invalid JSON format: {s}")
            return {}

    @staticmethod
    def load_db(db_file: str) -> Records:
        try:
            with open(db_file, "r") as f:
                content = f.read()
            return Storage.parse_json(content)
        except FileNotFoundError:
            return {}

    @staticmethod
    def save_db(db_file: str, data: Records):
        with open(db_file, "w") as f:
            f.write(Storage.to_json(data))

    @staticmethod
    def load_indexes(index_file: str) -> Indexes:
        try:
            with open(index_file, "r") as f:
                return Storage.parse_json(f.read())
        except FileNotFoundError:
            return {}

    @staticmethod
    def save_indexes(index_file: str, indexes: Indexes):
        with open(index_file, "w") as f:
            f.write(Storage.to_json(indexes))

    @staticmethod
    def log_operation(op_type: str, key: str = None, data: Data = None, conditions: Conditions = None):
        log_entry = {"op_type": op_type, "key": key, "data": data, "conditions": conditions}
        with open(Storage.log_file, "a") as f:
            f.write(Storage.to_json(log_entry) + "\n")
        print(f"Logged operation: {op_type}, key={key}, data={data}, conditions={conditions}")

    @staticmethod
    def read_log() -> List[Dict]:
        logs = []
        try:
            with open(Storage.log_file, "r") as f:
                for line in f:
                    logs.append(Storage.parse_json(line.strip()))
        except FileNotFoundError:
            return []
        return logs

    @staticmethod
    def clear_log():
        if os.path.exists(Storage.log_file):
            os.remove(Storage.log_file)