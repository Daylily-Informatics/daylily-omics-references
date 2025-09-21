"""Minimal subset of the :mod:`botocore` package for the test suite."""

from . import exceptions, response, stub

__all__ = ["exceptions", "response", "stub"]
