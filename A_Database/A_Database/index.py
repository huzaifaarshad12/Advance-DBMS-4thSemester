from typing import Dict, List
from mydb_types import Records, Indexes

class IndexManager:
    @staticmethod
    def build_index(field: str, data: Records, indexes: Indexes):
        index = {}
        for id_, record in data.items():
            if field in record:
                value = record[field]
                if value not in index:
                    index[value] = []
                index[value].append(id_)
        indexes[field] = index