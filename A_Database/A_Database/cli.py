import argparse
import json
import os
import time
from typing import List
from database import MyDB, Collection
from security import Security
from performance import Performance
from logger import Logger
from queryParser import parse_my_query
from mydb_types import ExplainPlan
from utils import MyDBUtils, MyDBUtilsError

class CLI:
    def __init__(self):
        self.db = MyDB()
        self.collection: Collection = None
        self.user_role: str = "guest"
        self.logger = Logger("CLI", log_file="cli.log")
        self.security = Security(logger=self.logger)
        self.performance = Performance()

    def create_collection(self, collection_name: str, schema: str = "", sensitive_fields: List[str] = None, schema_file: str = None):
        if schema_file:
            try:
                schema = MyDBUtils.read_json(os.path.join(os.getcwd(), schema_file))
            except MyDBUtilsError as e:
                print(f"Error reading schema file: {e}")
                return
        else:
            try:
                schema = json.loads(schema) if schema else []
            except json.JSONDecodeError as e:
                print(f"Error parsing schema: {e}")
                return
        self.collection = Collection(collection_name, schema, sensitive_fields, self.db)
        self.db.collections[collection_name] = self.collection
        self.db.save_db()
        self.logger.info(f"Created collection: {collection_name}")
        print(f"Collection '{collection_name}' created successfully")
        if sensitive_fields:
            print(f"Sensitive fields: {sensitive_fields}")

    def set_role(self, role: str):
        if role in ["admin", "user", "guest"]:
            self.user_role = role
            self.logger.info(f"Set role to: {role}")
            print(f"User role set to: {role}")
        else:
            print(f"Error: Invalid role '{role}'. Choose from: admin, user, guest")

    def insert(self, data: str, data_file: str = None):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        if data_file:
            try:
                data = MyDBUtils.read_json(os.path.join(os.getcwd(), data_file))
            except MyDBUtilsError as e:
                print(f"Error reading data file: {e}")
                return
        else:
            try:
                data = json.loads(data) if data else {}
            except json.JSONDecodeError as e:
                print(f"Error parsing data: {e}")
                return
        start_time = time.time()
        try:
            record_id = self.collection.insert(data, self.user_role)
            self.performance.track_operation("insert", self.collection.name, start_time)
            self.logger.info(f"Inserted record with _id: {record_id}")
            print(f"Inserted record with _id: {record_id}")
            print(f"Record: {data}")
        except Exception as e:
            self.logger.error(f"Insert failed: {e}")
            print(f"Error: {e}")

    def bulk_insert(self, data_file: str):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        try:
            records = MyDBUtils.read_json(os.path.join(os.getcwd(), data_file))
            start_time = time.time()
            keys = self.collection.bulk_insert(records, self.user_role)
            self.performance.track_operation("bulk_insert", self.collection.name, start_time)
            self.logger.info(f"Bulk inserted {len(keys)} records")
            print(f"Bulk inserted {len(keys)} records with IDs: {keys[:5]}...")
        except MyDBUtilsError as e:
            print(f"Error reading data file: {e}")
        except Exception as e:
            self.logger.error(f"Bulk insert failed: {e}")
            print(f"Error: {e}")

    def query(self, query_str: str):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        start_time = time.time()
        try:
            results = self.collection.parse_query(query_str, self.user_role)
            self.performance.track_operation("query", self.collection.name, start_time)
            self.performance.cache_query(query_str, results)
            self.logger.info(f"Query executed: {query_str}")
            print(json.dumps(results, indent=2))
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            print(f"Error: {e}")

    def explain(self, query_str: str):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        try:
            plan = self.collection.explain(query_str, self.user_role)
            self.logger.info(f"Explained query: {query_str}")
            print(f"Query Plan: method={plan['method']}, field={plan['field']}")
        except Exception as e:
            self.logger.error(f"Explain failed: {e}")
            print(f"Error: {e}")

    def update(self, operations: str, data: str):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        try:
            operations = json.loads(operations) if operations else {}
            data = json.loads(data) if data else {}
            start_time = time.time()
            count = self.collection.update(operations, data, self.user_role)
            self.performance.track_operation("update", self.collection.name, start_time)
            self.logger.info(f"Updated {count} record(s)")
            print(f"Updated {count} record(s)")
        except json.JSONDecodeError as e:
            print(f"Error parsing input: {e}")
        except Exception as e:
            self.logger.error(f"Update failed: {e}")
            print(f"Error: {e}")

    def delete(self, data: str):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        try:
            data = json.loads(data) if data else {}
            start_time = time.time()
            count = self.collection.delete(data, self.user_role)
            self.performance.track_operation("delete", self.collection.name, start_time)
            self.logger.info(f"Deleted {count} record(s)")
            print(f"Deleted {count} record(s)")
        except json.JSONDecodeError as e:
            print(f"Error parsing data: {e}")
        except Exception as e:
            self.logger.error(f"Delete failed: {e}")
            print(f"Error: {e}")

    def transaction(self, operations: str, operations_file: str = None):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        if operations_file:
            try:
                operations = MyDBUtils.read_json(os.path.join(os.getcwd(), operations_file))
            except MyDBUtilsError as e:
                print(f"Error reading operations file: {e}")
                return
        else:
            try:
                operations = json.loads(operations) if operations else []
            except json.JSONDecodeError as e:
                print(f"Error parsing operations: {e}")
                return
        start_time = time.time()
        try:
            success = self.collection.transaction(operations, self.user_role)
            self.performance.track_operation("transaction", self.collection.name, start_time)
            self.logger.info("Transaction completed successfully" if success else "Transaction failed")
            print("Transaction completed successfully" if success else "Transaction failed")
        except Exception as e:
            self.logger.error(f"Transaction failed: {e}")
            print(f"Error: {e}")

    def create_index(self, field: str):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        try:
            self.collection.create_index(field)
            self.logger.info(f"Created index on field: {field}")
            print(f"Index created on field: {field}")
        except MyDBUtilsError as e:
            self.logger.error(f"Failed to create index: {e}")
            print(f"Error: {e}")

    def list_collections(self):
        collections = list(self.db.collections.keys())
        if collections:
            print("Collections:")
            for collection in collections:
                print(f" - {collection}")
        else:
            print("No collections found.")

    def show_encryption(self):
        if not self.collection:
            print("Error: No collection selected. Use 'create_collection' first.")
            return
        sensitive_fields = self.collection.sensitive_fields
        if sensitive_fields:
            print(f"Encryption enabled for fields: {', '.join(sensitive_fields)}")
        else:
            print("No fields are encrypted.")

    def list_roles(self):
        roles = self.security.get_roles()
        print("Available roles and permissions:")
        for role, perms in roles.items():
            print(f" - {role}: {', '.join(perms)}")

    def show_audit_log(self, limit: int = 10):
        logs = self.logger.get_recent_logs(limit)
        if logs:
            print(f"Recent audit logs (last {limit}):")
            for log in logs:
                print(f" - {log}")
        else:
            print("No audit logs found.")

    def enable_monitoring(self):
        self.performance.start_monitoring()
        self.logger.info("Performance monitoring enabled")
        print("Performance monitoring enabled")

    def generate_report(self):
        report = self.performance.get_metrics()
        self.logger.info("Generated performance report")
        print(json.dumps(report, indent=2))

    def interactive_mode(self):
        print("Welcome to the Generic NoSQL JSON Database CLI!")
        print("Features:")
        print(" - Schema-less: Store any JSON document")
        print(" - Role-Based Access Control (RBAC)")
        print(" - Encryption for sensitive fields")
        print(" - Advanced queries ($gt, $in, etc.)")
        print(" - Transactions with Write-Ahead Logging")
        print(" - Bulk inserts, TTL records, query explain plans")
        print("\nType 'help' for commands or 'exit' to quit.")
        while True:
            try:
                command = input("db> ").strip()
                if command.lower() == "exit":
                    break
                elif command.lower() == "help":
                    print("Commands:")
                    print("  create_collection <name> [schema] [--sensitive-fields <fields>] [--schema-file <file>]")
                    print("  set_role <role>")
                    print("  insert <data> [--data-file <file>]")
                    print("  bulk_insert <data-file>")
                    print("  query <query>")
                    print("  explain <query>")
                    print("  update <operations> <data>")
                    print("  delete <data>")
                    print("  transaction <operations> [--operations-file <file>]")
                    print("  create_index <field>")
                    print("  list_collections")
                    print("  show_encryption")
                    print("  list_roles")
                    print("  show_audit_log [--limit <n>]")
                    print("  enable_monitoring")
                    print("  generate_report")
                    print("  exit")
                else:
                    self.parse_command(command)
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                self.logger.error(f"Command failed: {e}")
                print(f"Error: {e}")

    def parse_command(self, command: str):
        parts = command.split()
        if not parts:
            return
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd == "create_collection":
            schema = []
            sensitive_fields = []
            schema_file = None
            i = 1
            collection_name = args[0] if args else ""
            while i < len(args):
                if args[i] == "--sensitive-fields":
                    i += 1
                    sensitive_fields = args[i].split(",") if i < len(args) else []
                elif args[i] == "--schema-file":
                    i += 1
                    schema_file = args[i] if i < len(args) else None
                else:
                    schema.append(args[i])
                i += 1
            self.create_collection(collection_name, " ".join(schema), sensitive_fields, schema_file)
        elif cmd == "set_role":
            self.set_role(args[0] if args else "")
        elif cmd == "insert":
            data = []
            data_file = None
            i = 0
            while i < len(args):
                if args[i] == "--data-file":
                    i += 1
                    data_file = args[i] if i < len(args) else None
                else:
                    data.append(args[i])
                i += 1
            self.insert(" ".join(data), data_file)
        elif cmd == "bulk_insert":
            self.bulk_insert(args[0] if args else "")
        elif cmd == "query":
            self.query(" ".join(args))
        elif cmd == "explain":
            self.explain(" ".join(args))
        elif cmd == "update":
            operations = []
            data = []
            i = 0
            while i < len(args):
                if args[i].startswith("{"):
                    if operations:
                        data.append(args[i])
                    else:
                        operations.append(args[i])
                else:
                    if operations:
                        data.append(args[i])
                    else:
                        operations.append(args[i])
                i += 1
            self.update(" ".join(operations), " ".join(data))
        elif cmd == "delete":
            self.delete(" ".join(args))
        elif cmd == "transaction":
            operations = []
            operations_file = None
            i = 0
            while i < len(args):
                if args[i] == "--operations-file":
                    i += 1
                    operations_file = args[i] if i < len(args) else None
                else:
                    operations.append(args[i])
                i += 1
            self.transaction(" ".join(operations), operations_file)
        elif cmd == "create_index":
            self.create_index(args[0] if args else "")
        elif cmd == "list_collections":
            self.list_collections()
        elif cmd == "show_encryption":
            self.show_encryption()
        elif cmd == "list_roles":
            self.list_roles()
        elif cmd == "show_audit_log":
            limit = 10
            if len(args) > 1 and args[0] == "--limit":
                try:
                    limit = int(args[1])
                except ValueError:
                    print("Error: Limit must be an integer")
                    return
            self.show_audit_log(limit)
        elif cmd == "enable_monitoring":
            self.enable_monitoring()
        elif cmd == "generate_report":
            self.generate_report()
        else:
            print(f"Error: Unknown command '{cmd}'. Type 'help' for commands.")

def main():
    parser = argparse.ArgumentParser(description="Generic NoSQL JSON Database CLI")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--command", choices=["create_collection", "set_role", "insert", "bulk_insert", "query", "explain", "update", "delete", "transaction", "create_index", "list_collections", "show_encryption", "list_roles", "show_audit_log", "enable_monitoring", "generate_report"], help="Command to execute")
    parser.add_argument("--collection", help="Collection name")
    parser.add_argument("--schema", help="Collection schema (JSON string, optional)")
    parser.add_argument("--sensitive-fields", help="Comma-separated sensitive fields")
    parser.add_argument("--schema-file", help="Path to schema JSON file")
    parser.add_argument("--role", help="User role (admin, user, guest)")
    parser.add_argument("--data", help="Data (JSON string)")
    parser.add_argument("--data-file", help="Path to data JSON file")
    parser.add_argument("--query", help="Query string")
    parser.add_argument("--operations", help="Operations (JSON string)")
    parser.add_argument("--operations-file", help="Path to operations JSON file")
    parser.add_argument("--field", help="Field name for index")
    parser.add_argument("--limit", type=int, default=10, help="Limit for audit log")

    args = parser.parse_args()
    cli = CLI()

    if args.interactive:
        cli.interactive_mode()
    elif args.command:
        if args.command == "create_collection":
            cli.create_collection(args.collection or "", args.schema or "", args.sensitive_fields.split(",") if args.sensitive_fields else [], args.schema_file)
        elif args.command == "set_role":
            cli.set_role(args.role or "")
        elif args.command == "insert":
            cli.insert(args.data or "", args.data_file)
        elif args.command == "bulk_insert":
            cli.bulk_insert(args.data_file or "")
        elif args.command == "query":
            cli.query(args.query or "")
        elif args.command == "explain":
            cli.explain(args.query or "")
        elif args.command == "update":
            cli.update(args.operations or "", args.data or "")
        elif args.command == "delete":
            cli.delete(args.data or "")
        elif args.command == "transaction":
            cli.transaction(args.operations or "", args.operations_file)
        elif args.command == "create_index":
            cli.create_index(args.field or "")
        elif args.command == "list_collections":
            cli.list_collections()
        elif args.command == "show_encryption":
            cli.show_encryption()
        elif args.command == "list_roles":
            cli.list_roles()
        elif args.command == "show_audit_log":
            cli.show_audit_log(args.limit)
        elif args.command == "enable_monitoring":
            cli.enable_monitoring()
        elif args.command == "generate_report":
            cli.generate_report()
        else:
            print("Error: Specify --interactive or --command")
            return 1
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())