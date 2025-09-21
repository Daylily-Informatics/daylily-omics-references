"""A lightweight subset of the :mod:`boto3` package used for testing."""

from . import session
from .session import Session

__all__ = ["Session", "session"]
