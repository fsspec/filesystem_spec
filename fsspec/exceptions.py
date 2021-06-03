"""
fsspec user-defined exception classes
"""
import asyncio


class FSTimeoutError(asyncio.TimeoutError):
    """
    Raised when a fsspec function timed out occurs
    """

    ...
