"""Channel[T]: a go-style channel built on top of AnyIO memory object streams."""

from __future__ import annotations

from typing import Generic, Self, TypeVar

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from chanio._exceptions import EndOfStream

T = TypeVar("T")


class Channel(Generic[T]):
    """A bidirectional channel. ``capacity=0`` (the default) is an unbuffered rendezvous."""

    def __init__(self, capacity: int = 0) -> None:
        send: MemoryObjectSendStream[T]
        recv: MemoryObjectReceiveStream[T]
        send, recv = anyio.create_memory_object_stream(max_buffer_size=capacity)
        self._send = send
        self._recv = recv

    async def send(self, value: T) -> None:
        await self._send.send(value)

    async def recv(self) -> T:
        return await self._recv.receive()

    async def recv_ok(self) -> tuple[T | None, bool]:
        try:
            return await self._recv.receive(), True
        except EndOfStream:
            return None, False

    def close(self) -> None:
        """Close the channel. Buffered values still drain to receivers first."""
        self._send.close()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        try:
            return await self._recv.receive()
        except EndOfStream:
            raise StopAsyncIteration from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.close()
