import hashlib
from typing import Dict, List
from logger import Logger

class Security:
    def __init__(self, logger: Logger):
        self.logger = logger
        self.roles = {
            "admin": ["insert", "select", "update", "delete"],
            "user": ["insert", "select", "update"],
            "guest": ["select"]
        }

    def restrict_access(self, operation: str, role: str, collection_name: str):
        if role not in self.roles:
            self.logger.error(f"Invalid role: {role}")
            raise ValueError(f"Invalid role: {role}")
        if operation not in self.roles[role]:
            self.logger.error(f"Operation {operation} not allowed for role {role} on collection {collection_name}")
            raise PermissionError(f"Operation {operation} not allowed for role {role}")

    def encrypt_sensitive_fields(self, record: Dict, sensitive_fields: List[str]) -> Dict:
        encrypted_record = record.copy()
        for field in sensitive_fields:
            if field in encrypted_record:
                encrypted_record[field] = self._encrypt(encrypted_record[field])
                self.logger.debug(f"Encrypted field: {field}")
        return encrypted_record

    def decrypt_sensitive_fields(self, record: Dict, sensitive_fields: List[str]) -> Dict:
        decrypted_record = record.copy()
        for field in sensitive_fields:
            if field in decrypted_record:
                decrypted_record[field] = self._decrypt(decrypted_record[field])
                self.logger.debug(f"Decrypted field: {field}")
        return decrypted_record

    def _encrypt(self, data: str) -> str:
        return hashlib.sha256(data.encode()).hexdigest()

    def _decrypt(self, data: str) -> str:
        return data  # Note: SHA-256 is one-way; this is a placeholder

    def get_roles(self) -> Dict[str, List[str]]:
        return self.roles