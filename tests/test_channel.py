import pytest

from chanio import Channel, EndOfStream


@pytest.mark.anyio
async def test_buffered_send_recv() -> None:
    ch = Channel[int](capacity=1)
    await ch.send(1)
    assert await ch.recv() == 1


@pytest.mark.anyio
async def test_close_then_recv_raises() -> None:
    ch = Channel[int](capacity=1)
    ch.close()
    with pytest.raises(EndOfStream):
        await ch.recv()


@pytest.mark.anyio
async def test_async_for_drains_then_stops() -> None:
    ch = Channel[int](capacity=2)
    await ch.send(1)
    await ch.send(2)
    ch.close()
    assert [v async for v in ch] == [1, 2]
