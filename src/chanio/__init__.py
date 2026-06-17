"""Select and CSP combinators for AnyIO."""

from chanio._channel import Channel
from chanio._exceptions import BrokenResourceError, ClosedResourceError, EndOfStream, WouldBlock
from chanio._select import after, select

__all__ = [
    "Channel",
    "select",
    "after",
    "ClosedResourceError",
    "BrokenResourceError",
    "EndOfStream",
    "WouldBlock",
]

try:
    from chanio._version import __version__
except ImportError:
    __version__ = "0.0.0+unknown"
