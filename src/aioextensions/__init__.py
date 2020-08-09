"""Aioextensions module."""

# Standard library
import asyncio
from itertools import (
    tee,
)
from typing import (
    Awaitable,
    cast,
    Dict,
    Iterable,
    Optional,
    Tuple,
    TypeVar,
)

# Constants
_T = TypeVar('_T')

# Linters
# pylint: disable=unsubscriptable-object


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


async def force_loop_cycle() -> None:
    """Force the event loop to perform once cycle."""
    await asyncio.sleep(0)


async def _resolve_worker(
    *,
    awaitables: Iterable[Tuple[int, Awaitable[_T]]],
    greediness: int,
    loop: asyncio.AbstractEventLoop,
    store: Dict[int, asyncio.Queue],
    stream_finished: asyncio.Event,
    workers_up: asyncio.Event,
) -> None:
    done: asyncio.Queue = asyncio.Queue(greediness)
    for index, awaitable in awaitables:
        store[index] = done
        future = loop.create_future()
        future.set_result(await schedule(awaitable, loop=loop))
        await done.put(future)
        workers_up.set()
    workers_up.set()
    stream_finished.set()


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
    store: Dict[int, asyncio.Queue] = {}
    stream, stream_copy = tee(enumerate(awaitables))
    stream_finished = asyncio.Event()
    workers_up = asyncio.Event()
    workers_tasks: Dict[int, asyncio.Task] = {}

    async def start_workers() -> None:
        for index in range(workers):
            if stream_finished.is_set():
                break
            workers_tasks[index] = asyncio.create_task(_resolve_worker(
                awaitables=stream,
                greediness=greediness,
                loop=loop,
                store=store,
                stream_finished=stream_finished,
                workers_up=workers_up,
            ))
            await force_loop_cycle()
        await workers_up.wait()

    async def get_one(index: int) -> Awaitable[_T]:
        if not workers_tasks:
            await start_workers()

        awaitable = await store.pop(index).get()
        result: Awaitable[_T] = (await awaitable).result()
        return result

    for index, _ in stream_copy:
        yield cast(Awaitable[_T], get_one(index))
