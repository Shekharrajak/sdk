"""Configuration for Spark Client."""

from enum import Enum


class AuthMethod(Enum):
    """Authentication methods supported by Batch Processing Gateway."""

    BASIC = "basic"
    HEADER = "header"
    NONE = "none"
