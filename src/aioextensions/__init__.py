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
    greediness: int = 1,
) -> Iterable[Awaitable[_T]]:
    """Resolve concurrently the iterable of awaitables using many workers."""
    if workers < 1:
        raise ValueError('workers must be >= 1')
    if greediness < 1:
        raise ValueError('greediness must be >= 1')

    loop = asyncio.get_event_loop()
    readable = asyncio.Event()
    store = {}
    stream, stream_copy = tee(enumerate(awaitables))
    workers_greediness: Dict[int, asyncio.BoundedSemaphore] = {}
    workers_tasks: Dict[int, asyncio.Task[Any]] = {}

    async def worker(identifier: int, loop: asyncio.AbstractEventLoop) -> None:
        for index, awaitable in stream:
            future = loop.create_future()
            store[index] = (identifier, future)
            await workers_greediness[identifier].acquire()
            future.set_result(await schedule(awaitable, loop=loop))
            readable.set()

    async def create_team() -> None:
        workers_tasks.update({
            n: asyncio.create_task(worker(n, loop)) for n in range(workers)
        })
        workers_greediness.update({
            n: asyncio.BoundedSemaphore(greediness) for n in range(workers)
        })
        workers_task = asyncio.gather(*workers_tasks.values())
        workers_task.add_done_callback(lambda _: readable.set())

        await readable.wait()

    async def get_one(index: int) -> Awaitable[_T]:
        if not workers_tasks:
            await create_team()

        identifier, awaitable = store.pop(index)
        workers_greediness[identifier].release()
        result: Awaitable[_T] = (await awaitable).result()
        return result

    for index, _ in stream_copy:
        yield cast(Awaitable[_T], get_one(index))
