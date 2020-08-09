"""Aioextensions module."""

# Standard library
import asyncio
from itertools import (
    tee,
)
from typing import (
    Any,
    Awaitable,
    cast,
    Dict,
    Iterable,
    Optional,
    TypeVar,
)

# Constants
_T = TypeVar('_T')


def schedule(
    awaitable: Awaitable[_T],
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> Awaitable[_T]:
    """Schedule an awaitable in the event loop and return a wrapper for it."""
    # pylint: disable=unnecessary-lambda
    wrapper = (loop or asyncio.get_event_loop()).create_future()
    asyncio.create_task(awaitable).add_done_callback(
        lambda future: wrapper.set_result(future),
    )

    return wrapper


def resolve(
    awaitables: Iterable[Awaitable[_T]],
    *,
    workers: int = 1,
    greediness: int = 0,
) -> Iterable[Awaitable[_T]]:
    """Resolve concurrently the iterable of awaitables using many workers."""
    if workers < 1:
        raise ValueError('workers must be >= 1')
    if greediness < 0:
        raise ValueError('greediness must be >= 0')

    loop = asyncio.get_event_loop()
    readable = asyncio.Event()
    store = {}
    stream, stream_copy = tee(enumerate(awaitables))
    workers_tasks: Dict[int, asyncio.Task[Any]] = {}

    async def worker(loop: asyncio.AbstractEventLoop) -> None:
        done = asyncio.Queue(greediness)
        for index, awaitable in stream:
            store[index] = done
            future = loop.create_future()
            future.set_result(await schedule(awaitable, loop=loop))
            await done.put(future)
            readable.set()

    async def create_team() -> None:
        workers_tasks.update({
            n: asyncio.create_task(worker(loop)) for n in range(workers)
        })
        workers_task = asyncio.gather(*workers_tasks.values())
        workers_task.add_done_callback(lambda _: readable.set())

        await readable.wait()

    async def get_one(index: int) -> Awaitable[_T]:
        if not workers_tasks:
            await create_team()

        awaitable = await store.pop(index).get()
        result: Awaitable[_T] = (await awaitable).result()
        return result

    for index, _ in stream_copy:
        yield cast(Awaitable[_T], get_one(index))
