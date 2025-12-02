"""Exception definitions used by the simplified botocore stubs."""

from __future__ import annotations


class ClientError(Exception):
    """Exception raised to mirror :class:`botocore.exceptions.ClientError`."""

    def __init__(self, error_response: dict | None = None, operation_name: str | None = None):
        self.response = error_response or {}
        self.operation_name = operation_name or ""
        message = self.response.get("Error", {}).get("Message")
        if not message:
            message = f"An error occurred ({self.response.get('Error', {}).get('Code', 'Unknown')})"
        super().__init__(message)


class WaiterError(Exception):
    """Lightweight equivalent of :class:`botocore.exceptions.WaiterError`."""

    def __init__(self, name: str, reason: str, last_response: dict | None = None):
        self.name = name
        self.reason = reason
        self.last_response = last_response or {}
        super().__init__(reason)


__all__ = ["ClientError", "WaiterError"]
