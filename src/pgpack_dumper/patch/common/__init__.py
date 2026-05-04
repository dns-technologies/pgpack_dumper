"""Common classes, constants and modules."""

from . import errors as Error
from .defines import TIMESTAMP
from .structs import ResClass


__all__ = (
    "Error",
    "ResClass",
    "TIMESTAMP",
)
