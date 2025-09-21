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


__all__ = ["ClientError"]
