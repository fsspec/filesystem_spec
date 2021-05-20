"""
fsspec user-defined exception classes
"""


class FSBaseException(Exception):
    """
    Base exception for fsspec user-defined exceptions
    """

    ...


class FSTimeoutError(FSBaseException):
    """
    Raised when a fsspec function timed out occurs
    """

    ...
