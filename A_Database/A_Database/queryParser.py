import re
from query import Query, QueryAction
from mydb_types import Conditions, Data, BulkData

def parse_my_query(query: str) -> Query:
    q = Query()

    def parse_filter(text: str) -> dict:
        result = {"type": "compare", "field": "", "operator": "", "value": ""}
        match = re.match(r"(\w+)\s*([=><!]+)\s*('[^']*'|[0-9.]+)", text)
        if match:
            field, op, value = match.groups()
            result["field"] = field
            result["operator"] = op
            result["value"] = value[1:-1] if value.startswith("'") else float(value)
        elif "AND" in text or "OR" in text:
            parts = re.split(r"\s+(AND|OR)\s+", text)
            if len(parts) == 3:
                left, op, right = parts
                result = {
                    "type": "logical",
                    "operator": op,
                    "left": parse_filter(left.strip()),
                    "right": parse_filter(right.strip())
                }
        return result

    def parse_bulk_data(text: str) -> list:
        result = []
        for record_match in re.finditer(r"\((.+?)\)(?:,|$)", text):
            record_text = record_match.group(1)
            record = {}
            for pair in re.finditer(r"(\w+)=('[^']*'|[0-9.]+)", record_text):
                key, value = pair.groups()
                record[key] = value[1:-1] if value.startswith("'") else value
            result.append(record)
        return result

    if re.match(r"INIT", query, re.I):
        q.action = QueryAction.CREATE
    elif m := re.match(r"ADD DATA \((.+)\)", query, re.I):
        q.action = QueryAction.INSERT
        q.data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", m.group(1))}
    elif m := re.match(r"ADD BULK DATA \[(.+)\]", query, re.I):
        q.action = QueryAction.BULK_INSERT
        q.bulk_data = parse_bulk_data(m.group(1))
    elif m := re.match(r"FETCH(?: FILTER \((.+)\))?", query, re.I):
        q.action = QueryAction.SELECT
        if m.group(1):
            q.filter = parse_filter(m.group(1))
            q.conditions = parse_conditions(m.group(1))
    elif m := re.match(r"EXPLAIN FETCH(?: FILTER \((.+)\))?", query, re.I):
        q.action = QueryAction.EXPLAIN
        if m.group(1):
            q.filter = parse_filter(m.group(1))
            q.conditions = parse_conditions(m.group(1))
            print(f"Parsed EXPLAIN: filter={q.filter}, conditions={q.conditions}")
    elif m := re.match(r"MODIFY FILTER \((.+)\) WITH \((.+)\)", query, re.I):
        q.action = QueryAction.UPDATE
        q.conditions = parse_conditions(m.group(1))
        q.data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", m.group(2))}
    elif m := re.match(r"REMOVE FILTER \((.+)\)", query, re.I):
        q.action = QueryAction.DELETE
        q.conditions = parse_conditions(m.group(1))
    elif m := re.match(r"INDEX FIELD (\w+)", query, re.I):
        q.action = QueryAction.INDEX
        q.index_field = m.group(1)
    elif m := re.match(r"TRANSACT OPS \((.+)\)", query, re.I):
        q.action = QueryAction.TRANSACT
        ops_str = m.group(1)
        for op in re.finditer(r"(?:ADD DATA \((.+?)\)|MODIFY FILTER \((.+?)\) WITH \((.+?)\)|REMOVE FILTER \((.+?)\))(?:;|$)", ops_str):
            if op.group(1):
                q.transact_ops.append(("INSERT", {}, {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", op.group(1))}))
            elif op.group(2) and op.group(3):
                q.transact_ops.append(("UPDATE", parse_conditions(op.group(2)), {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", op.group(3))}))
            elif op.group(4):
                q.transact_ops.append(("DELETE", parse_conditions(op.group(4)), {}))
    else:
        raise ValueError("Invalid query")
    return q

def parse_conditions(text: str) -> dict:
    result = {}
    if not text:
        return result
    for pair in re.finditer(r"(\w+)\s*([=><!]+|[$]\w+)\s*('[^']*'|[0-9.]+|\{[^{}]*\})", text):
        key, op, value = pair.groups()
        if value.startswith("'") and value.endswith("'"):
            result[key] = value[1:-1]
        elif value.startswith("{"):
            ops = {}
            inner = value[1:-1]
            for op_match in re.finditer(r"(\w+)\s*:\s*([0-9.]+|\[[^\]]*\])", inner):
                op_key, op_value = op_match.groups()
                if op_value.startswith("["):
                    values = re.findall(r'"([^"]+)"', op_value[1:-1])
                    ops[op_key] = values
                else:
                    ops[op_key] = float(op_value)
            result[key] = ops
        else:
            result[key] = value
    return result