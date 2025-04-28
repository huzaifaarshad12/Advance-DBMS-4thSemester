from typing import Dict
from performance import Performance
from logger import Logger

class PerformanceMonitor:
    def __init__(self, performance: Performance):
        self.performance = performance
        self.logger = Logger("PerformanceMonitor", log_file="monitor.log")
        self.is_monitoring = False

    def start_monitoring(self):
        self.is_monitoring = True
        self.performance.start_monitoring()
        self.logger.info("Performance monitoring started")

    def stop_monitoring(self):
        self.is_monitoring = False
        self.performance.is_monitoring = False
        self.logger.info("Performance monitoring stopped")

    def get_performance_metrics(self) -> Dict:
        if not self.is_monitoring:
            self.logger.warning("Monitoring is not enabled")
            return {}
        return self.performance.get_metrics()