import json
from typing import Any, Dict

class MyDBUtilsError(Exception):
    pass

class MyDBUtils:
    @staticmethod
    def read_json(file_path: str) -> Dict:
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise MyDBUtilsError(f"Failed to read JSON from {file_path}: {e}")

    @staticmethod
    def write_json(file_path: str, data: Any):
        try:
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            raise MyDBUtilsError(f"Failed to write JSON to {file_path}: {e}")