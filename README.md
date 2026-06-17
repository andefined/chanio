# chanio

Select and CSP combinators for [AnyIO](https://anyio.readthedocs.io/).

`chanio` adds the `select` statement and go-style fan-in/fan-out combinators
(`merge`, `tee`, `broadcast`, `worker_pool`, ...) on top of AnyIO memory
object streams, so it runs on both asyncio and Trio.

```python
import anyio
from chanio import Channel, select

async def main() -> None:
    ch = Channel[int](capacity=1)
    await ch.send(42)
    value, _ = await select(ch)
    print(value)

anyio.run(main)
```

## Status

Early alpha. API is not yet stable.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy src
```
