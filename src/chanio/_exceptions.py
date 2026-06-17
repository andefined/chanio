"""Exception vocabulary, reused from AnyIO for familiarity."""

from anyio import BrokenResourceError, ClosedResourceError, EndOfStream, WouldBlock

__all__ = ["ClosedResourceError", "BrokenResourceError", "EndOfStream", "WouldBlock"]
