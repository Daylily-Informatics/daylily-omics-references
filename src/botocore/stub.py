"""A greatly simplified implementation of :mod:`botocore.stub`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class _QueuedResponse:
    operation: str
    expected_params: Dict[str, Any] | None
    response: Any


class Stubber:
    """Queue stubbed responses for a client."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self._queue: List[_QueuedResponse] = []
        self._active = False

    def add_response(
        self,
        operation_name: str,
        service_response: Any | None,
        expected_params: Dict[str, Any] | None = None,
    ) -> None:
        """Queue a response for *operation_name*."""

        self._queue.append(
            _QueuedResponse(
                operation=operation_name,
                expected_params=dict(expected_params) if expected_params is not None else None,
                response=service_response,
            )
        )

    def __enter__(self) -> "Stubber":
        if getattr(self.client, "_active_stubber", None) is not None:
            raise RuntimeError("A stubber is already active on this client")
        self.client._active_stubber = self
        self._active = True
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.client._active_stubber = None
        self._active = False
        if exc_type is None and self._queue:
            raise AssertionError("Not all stubbed responses were consumed")

    # ------------------------------------------------------------------
    def consume(self, operation_name: str, params: Dict[str, Any]) -> Any:
        if not self._queue:
            raise AssertionError(f"Unexpected call to {operation_name}; no responses queued")

        queued = self._queue.pop(0)
        if queued.operation != operation_name:
            raise AssertionError(
                f"Expected call to {queued.operation!r} but got {operation_name!r}"
            )

        if queued.expected_params is not None and queued.expected_params != params:
            raise AssertionError(
                "Parameters did not match expected values for "
                f"{operation_name!r}: expected {queued.expected_params!r}, got {params!r}"
            )

        response = queued.response
        if isinstance(response, Exception):
            raise response
        return response


__all__ = ["Stubber"]
