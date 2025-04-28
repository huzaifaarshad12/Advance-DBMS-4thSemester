import os
import time
from datetime import datetime, timedelta
from threading import Lock
from mydb_types import Data, Records, Conditions, Indexes, Record, BulkData, ExplainPlan
from storage import Storage
from index import IndexManager
from wal import WAL
from security import Security
from performance import Performance
from logger import Logger
from query import Query, QueryAction
from queryParser import parse_my_query
from utils import MyDBUtils, MyDBUtilsError
from typing import Dict, List, Optional

class MyDB:
    def __init__(self):
        self.db_file = "mydb_data.json"
        self.collections: Dict[str, 'Collection'] = {}
        self.lock = Lock()
        self.logger = Logger("MyDB", log_file="mydb.log")
        self.wal = WAL()
        if not os.path.exists(self.db_file):
            Storage.save_db(self.db_file, {})
        self.load_db()

    def load_db(self):
        data = Storage.load_db(self.db_file)
        for collection_name, collection_data in data.items():
            if isinstance(collection_data, dict):
                self.collections[collection_name] = Collection(
                    collection_name,
                    collection_data.get("schema", []),
                    collection_data.get("sensitive_fields", []),
                    self,
                    collection_data.get("data", {}),
                    collection_data.get("indexes", {})
                )

    def save_db(self):
        data = {}
        for collection_name, collection in self.collections.items():
            data[collection_name] = {
                "schema": collection.schema,
                "sensitive_fields": collection.sensitive_fields,
                "data": collection.data,
                "indexes": collection.indexes
            }
        Storage.save_db(self.db_file, data)

class Collection:
    def __init__(self, name: str, schema: List[str], sensitive_fields: List[str], db: MyDB, data: Records = None, indexes: Indexes = None):
        self.name = name
        self.schema = schema  # Optional schema hints
        self.sensitive_fields = sensitive_fields or []
        self.db = db
        self.data = data or {}
        self.indexes = indexes or {}
        self.lock = db.lock
        self.logger = db.logger
        self.security = Security(self.logger)
        self.performance = Performance()
        self.performance.start_monitoring()
        self.wal = db.wal
        self.explain_plan: ExplainPlan = {"method": "full_scan", "field": None}
        self.recover_from_log()

    def recover_from_log(self):
        logs = self.wal.recover()
        if not logs:
            return
        self.logger.info(f"Recovering {self.name} from log...")
        for log in logs:
            op_type = log["op_type"]
            key = log["key"]
            log_data = log["data"] or {}
            conditions = log["conditions"] or {}
            if op_type == "INSERT" and key:
                self.data[key] = self.security.encrypt_sensitive_fields(log_data, self.sensitive_fields)
                self.data[key]["_id"] = key
                self.data[key]["created_at"] = self.current_time()
            elif op_type == "UPDATE":
                for k, record in self.data.items():
                    if self.match_query(record, conditions, check_ttl=False):
                        record.update(log_data)
                        record["updated_at"] = self.current_time()
            elif op_type == "DELETE":
                to_delete = [k for k, r in self.data.items() if self.match_query(r, conditions, check_ttl=False)]
                for k in to_delete:
                    del self.data[k]
        self.save_data()
        for field in self.indexes:
            IndexManager.build_index(field, self.data, self.indexes)
        self.wal.clear()
        self.logger.info(f"Recovery complete for {self.name}")

    def current_time(self) -> str:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def parse_time(self, time_str: str) -> datetime:
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")

    def validate_record(self, record: Data) -> bool:
        if "_id" in record or "created_at" in record:
            raise ValueError("Cannot set reserved fields: _id, created_at")
        # Optional schema validation
        if self.schema:
            for field in record:
                if field not in self.schema and field not in ["ttl", "updated_at"]:
                    self.logger.warning(f"Field {field} not in schema: {self.schema}")
        return True

    def is_expired(self, record: Record) -> bool:
        if "ttl" not in record or "created_at" not in record:
            return False
        ttl = float(record["ttl"])
        created = self.parse_time(record["created_at"])
        return datetime.now() >= created + timedelta(seconds=ttl)

    def match_query(self, record: Record, query: Dict, check_ttl: bool = True) -> bool:
        if check_ttl and self.is_expired(record):
            return False
        for key, condition in query.items():
            record_value = record.get(key)
            if isinstance(condition, str):
                if record_value != condition:
                    return False
            elif isinstance(condition, dict):
                ops = condition
                if record_value is None:
                    return False
                try:
                    record_num = float(record_value)
                except (ValueError, TypeError):
                    return False
                for op, value in ops.items():
                    if op == "$gt" and not (record_num > value):
                        return False
                    elif op == "$gte" and not (record_num >= value):
                        return False
                    elif op == "$lt" and not (record_num < value):
                        return False
                    elif op == "$lte" and not (record_num <= value):
                        return False
                    elif op == "$in" and record_value not in value:
                        return False
        return True

    def insert(self, record: Data, user_role: str) -> str:
        start_time = time.time()
        with self.lock:
            self.security.restrict_access("insert", user_role, self.name)
            self.validate_record(record)
            key = str(len(self.data) + 1)
            record = record.copy()
            record["_id"] = key
            record["created_at"] = self.current_time()
            record = self.security.encrypt_sensitive_fields(record, self.sensitive_fields)
            self.wal.log("INSERT", key, record)
            self.data[key] = record
            for field in self.indexes:
                IndexManager.build_index(field, self.data, self.indexes)
            self.save_data()
            self.performance.track_operation("INSERT", self.name, start_time)
            self.logger.info(f"Inserted record with ID: {key} by {user_role}")
            return key

    def bulk_insert(self, records: BulkData, user_role: str) -> List[str]:
        start_time = time.time()
        with self.lock:
            self.security.restrict_access("insert", user_role, self.name)
            keys = []
            for record in records:
                self.validate_record(record)
                key = str(len(self.data) + 1)
                record = record.copy()
                record["_id"] = key
                record["created_at"] = self.current_time()
                record = self.security.encrypt_sensitive_fields(record, self.sensitive_fields)
                self.wal.log("INSERT", key, record)
                self.data[key] = record
                keys.append(key)
            for field in self.indexes:
                IndexManager.build_index(field, self.data, self.indexes)
            self.save_data()
            self.performance.track_operation("BULK_INSERT", self.name, start_time)
            self.logger.info(f"Bulk inserted {len(keys)} records by {user_role}")
            return keys

    def parse_query(self, query_str: str, user_role: str) -> List[Record]:
        start_time = time.time()
        with self.lock:
            self.security.restrict_access("select", user_role, self.name)
            cached = self.performance.get_cached_query(query_str)
            if cached is not None:
                self.performance.track_operation("SELECT_CACHED", self.name, start_time)
                return cached
            query = parse_my_query(query_str)
            results = []
            indexed = False
            self.explain_plan = {"method": "full_scan", "field": None}

            if query.filter:
                field = query.filter.get("field")
                self.performance.suggest_index(self.name, field)
                if field in self.indexes and query.filter["type"] == "compare" and query.filter["operator"] == "=":
                    self.explain_plan = {"method": "index", "field": field}
                    value = query.filter["value"]
                    if value in self.indexes[field]:
                        for key in self.indexes[field][value]:
                            record = self.data.get(key)
                            if record and self.match_query(record, query.conditions):
                                results.append(self.security.decrypt_sensitive_fields(record, self.sensitive_fields))
                        indexed = True

            if not indexed:
                for key, record in self.data.items():
                    if self.match_query(record, query.conditions):
                        results.append(self.security.decrypt_sensitive_fields(record, self.sensitive_fields))

            self.performance.cache_query(query_str, results)
            self.performance.track_operation("SELECT", self.name, start_time)
            return results

    def explain(self, query_str: str, user_role: str) -> ExplainPlan:
        start_time = time.time()
        with self.lock:
            self.security.restrict_access("select", user_role, self.name)
            self.parse_query(query_str, user_role)  # Sets explain_plan
            self.performance.track_operation("EXPLAIN", self.name, start_time)
            self.logger.info(f"Explained query: {query_str}")
            return self.explain_plan

    def update(self, operations: Dict, update_data: Data, user_role: str) -> int:
        start_time = time.time()
        with self.lock:
            self.security.restrict_access("update", user_role, self.name)
            count = 0
            for key, record in self.data.items():
                if self.match_query(record, operations):
                    self.wal.log("UPDATE", key, update_data, operations)
                    record.update(update_data)
                    record["updated_at"] = self.current_time()
                    record = self.security.encrypt_sensitive_fields(record, self.sensitive_fields)
                    self.data[key] = record
                    count += 1
            if count > 0:
                for field in self.indexes:
                    IndexManager.build_index(field, self.data, self.indexes)
                self.save_data()
            self.performance.track_operation("UPDATE", self.name, start_time)
            self.logger.info(f"Updated {count} records by {user_role}")
            return count

    def delete(self, query: Dict, user_role: str) -> int:
        start_time = time.time()
        with self.lock:
            self.security.restrict_access("delete", user_role, self.name)
            to_delete = [key for key, record in self.data.items() if self.match_query(record, query)]
            for key in to_delete:
                self.wal.log("DELETE", key, conditions=query)
                del self.data[key]
            if to_delete:
                for field in self.indexes:
                    IndexManager.build_index(field, self.data, self.indexes)
                self.save_data()
            self.performance.track_operation("DELETE", self.name, start_time)
            self.logger.info(f"Deleted {len(to_delete)} records by {user_role}")
            return len(to_delete)

    def transaction(self, operations: List[Dict], user_role: str) -> bool:
        from transaction import Transaction
        start_time = time.time()
        with self.lock:
            self.security.restrict_access("transaction", user_role, self.name)
            tx = Transaction(self)
            try:
                for op in operations:
                    op_type = op.get("type")
                    if op_type == "insert":
                        tx.insert(op.get("data", {}), user_role)
                    elif op_type == "update":
                        tx.update(op.get("conditions", {}), op.get("data", {}), user_role)
                    elif op_type == "delete":
                        tx.delete(op.get("conditions", {}), user_role)
                tx.commit()
                self.performance.track_operation("TRANSACTION", self.name, start_time)
                self.logger.info(f"Transaction committed by {user_role}")
                return True
            except Exception as e:
                tx.rollback()
                self.performance.track_operation("TRANSACTION_FAILED", self.name, start_time)
                self.logger.error(f"Transaction failed: {e}")
                return False

    def create_index(self, field: str):
        start_time = time.time()
        with self.lock:
            IndexManager.build_index(field, self.data, self.indexes)
            self.save_data()
            self.performance.track_operation("INDEX", self.name, start_time)
            self.logger.info(f"Created index on {field}")

    def save_data(self):
        self.db.save_db()