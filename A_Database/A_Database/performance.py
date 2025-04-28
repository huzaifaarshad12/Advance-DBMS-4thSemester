from typing import Dict, Any
import time
from logger import Logger

class Performance:
    def __init__(self):
        self.logger = Logger("Performance", log_file="performance.log")
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        self.metrics = {}
        self.auto_index_fields = set()
        self.is_monitoring = False

    def start_monitoring(self):
        self.is_monitoring = True
        self.logger.info("Performance monitoring enabled")

    def track_operation(self, operation: str, collection_name: str, start_time: float):
        if not self.is_monitoring:
            return
        elapsed = time.time() - start_time
        if collection_name not in self.metrics:
            self.metrics[collection_name] = {}
        if operation not in self.metrics[collection_name]:
            self.metrics[collection_name][operation] = {"count": 0, "total_time": 0.0}
        self.metrics[collection_name][operation]["count"] += 1
        self.metrics[collection_name][operation]["total_time"] += elapsed
        self.logger.info(f"Tracked {operation} on {collection_name}: {elapsed:.3f}s")

    def cache_query(self, query_str: str, results: Any):
        if not self.is_monitoring:
            return
        self.cache[query_str] = results
        self.logger.debug(f"Cached query: {query_str}")

    def get_cached_query(self, query_str: str) -> Any:
        if not self.is_monitoring:
            return None
        if query_str in self.cache:
            self.cache_hits += 1
            self.logger.debug(f"Cache hit for query: {query_str}")
            return self.cache[query_str]
        self.cache_misses += 1
        self.logger.debug(f"Cache miss for query: {query_str}")
        return None

    def suggest_index(self, collection_name: str, field: str):
        if not self.is_monitoring:
            return
        self.auto_index_fields.add((collection_name, field))
        self.logger.info(f"Suggested index on {collection_name}.{field}")

    def get_metrics(self) -> Dict:
        return {
            "cache_stats": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_ratio": self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0
            },
            "index_hints": list(self.auto_index_fields),
            "metrics_summary": self.metrics
        }