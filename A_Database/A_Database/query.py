from enum import Enum
from typing import List, Tuple
from mydb_types import Conditions, Data, BulkData

class QueryAction(Enum):
    CREATE = "CREATE"
    INSERT = "INSERT"
    SELECT = "SELECT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INDEX = "INDEX"
    TRANSACT = "TRANSACT"
    BULK_INSERT = "BULK_INSERT"
    EXPLAIN = "EXPLAIN"

class Query:
    def __init__(self):
        self.action: QueryAction = None
        self.conditions: Conditions = {}
        self.data: Data = {}
        self.bulk_data: BulkData = []
        self.index_field: str = ""
        self.transact_ops: List[Tuple[str, Conditions, Data]] = []