"""select(): race recv/send cases across channels, go-style."""

from __future__ import annotations

import random
from typing import Any

import anyio

from chanio._channel import Channel
from chanio._exceptions import WouldBlock

_MISSING = object()

_RecvOp = Channel[Any]
_SendOp = tuple[Channel[Any], Any]
_Op = _RecvOp | _SendOp | None


async def after(delay: float) -> Channel[None]:
    """A channel that closes after ``delay`` seconds  go's ``time.After``."""
    ch: Channel[None] = Channel()

    async def _fire() -> None:
        await anyio.sleep(delay)
        ch.close()

    async with anyio.create_task_group() as tg:
        tg.start_soon(_fire)
    return ch


async def select(
    *ops: _Op,
    default: Any = _MISSING,
    priority: bool = False,
) -> tuple[Any, int | None]:
    """Race over recv/send cases. Returns ``(value, index)``.

    ``index`` is the position of the winning op in ``ops`` (``None`` cases are 
    skipped but keep their original index), or ``None`` if ``default`` fired.
    """
    indexed: list[tuple[int, _RecvOp | _SendOp]] = [
        (i, op) for i, op in enumerate(ops) if op is not None
    ]
    if not priority:
        random.shuffle(indexed)

    for i, op in indexed:
        try:
            if isinstance(op, tuple):
                send_ch, value = op
                send_ch._send.send_nowait(value)
                return value, i
            recv_value = op._recv.receive_nowait()
            return recv_value, i
        except WouldBlock:
            continue

    if default is not _MISSING:
        return default, None

    result: dict[str, Any] = {}

    async def _wait_recv(
        i: int, ch: Channel[Any], cancel_scope: anyio.CancelScope
    ) -> None:
        value = await ch.recv()
        if "value" not in result:
            result["value"] = value
            result["index"] = i
            cancel_scope.cancel()

    async def _wait_send(
        i: int, ch: Channel[Any], value: Any, cancel_scope: anyio.CancelScope
    ) -> None:
        await ch.send(value)
        if "value" not in result:
            result["value"] = value
            result["index"] = i
            cancel_scope.cancel()

    async with anyio.create_task_group() as tg:
        for i, op in indexed:
            if isinstance(op, tuple):
                send_ch, value = op
                tg.start_soon(_wait_send, i, send_ch, value, tg.cancel_scope)
            else:
                tg.start_soon(_wait_recv, i, op, tg.cancel_scope)

    return result["value"], result["index"]
