"""Python Asyncio Extensions

[![PyPI](https://img.shields.io/pypi/v/aioextensions)](https://pypi.org/project/aioextensions)
[![Status](https://img.shields.io/pypi/status/aioextensions)](https://pypi.org/project/aioextensions)
[![License](https://img.shields.io/pypi/l/aioextensions)](https://github.com/kamadorueda/aioextensions/blob/latest/LICENSE.md)
[![Downloads](https://img.shields.io/pypi/dm/aioextensions)](https://pypi.org/project/aioextensions)

Install:
    ```
    $ pip install aioextensions
    ```

Import:

    >>> from aioextensions import *  # to import everything
    >>> from aioextensions import (  # recommended way
            unblock,
            unblock_cpu,
            resolve,
            schedule,
            # ...
        )
"""

# Standard library
import asyncio
from concurrent.futures import (
    Executor,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from functools import (
    partial,
    wraps,
)
from itertools import (
    tee,
)
from os import (
    cpu_count,
)
from typing import (
    Any,
    Awaitable,
    Callable,
    cast,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

# Third party libraries
import uvloop

# Constants
CPU_COUNT: int = cpu_count() or 1
_F = TypeVar('_F', bound=Callable[..., Any])
_T = TypeVar('_T')

# Linters
# pylint: disable=unsubscriptable-object


def block(
    function: Callable[..., Awaitable[_T]],
    *args: Any,
    **kwargs: Any,
) -> _T:
    """Execute an asynchronous function synchronously and return its result.

    Example:
        >>> async def do(a, b=0):
                await something
                return a + b

        >>> block(do, 1, b=2)

        >>> 3

    This function acts as a drop-in replacement of asyncio.run and
    installs `uvloop` (the fastest event-loop implementation out there) first.

    .. tip::
        Use this as the entrypoint for your program.
    """
    uvloop.install()
    return asyncio.run(function(*args, **kwargs))


def block_decorator(function: _F) -> _F:
    """Decorator to turn an asynchronous function into a synchronous one.

    Example:
        >>> @block_decorator
            async def do(a, b=0):
                return a + b

        >>> do(1, b=2) == 3

    This can be used as a bridge between synchronous and asynchronous code.
    """

    @wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return block(function, *args, **kwargs)

    return cast(_F, wrapper)


class ExecutorPool:

    def __init__(
        self,
        cls: Union[
            Type[ProcessPoolExecutor],
            Type[ThreadPoolExecutor],
        ],
    ) -> None:
        self._cls = cls
        self._pool: Optional[Executor] = None

    def initialize(self, *, max_workers: Optional[int] = None) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False)

        self._pool = self._cls(max_workers=max_workers)

    def shutdown(self, *, wait: bool) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=wait)
            self._pool = None

    @property
    def pool(self) -> Executor:
        if self._pool is None:
            raise RuntimeError('Must call initialize first')

        return self._pool

    @property
    def initialized(self) -> bool:
        return self._pool is not None


async def force_loop_cycle() -> None:
    """Force the event loop to perform one cycle.

    This can be used to suspend the execution of the current coroutine and
    yield control back to the event-loop.
    """
    await asyncio.sleep(0)


def resolve(  # noqa: mccabe
    awaitables: Iterable[Awaitable[_T]],
    *,
    workers: int = 1024,
    worker_greediness: int = 0,
) -> Iterable[Awaitable[_T]]:
    """Resolve concurrently the input stream and yield back in the same order.

    The algorithm makes sure that at any point in time every worker is busy.
    Also, at any point in time there will be at most _number of `workers`_
    tasks being resolved concurrently.

    The `worker_greediness` parameter controlls how much each worker can
    process before waiting for you to retrieve results. This is important
    when the input stream is big as it allows you to control the memory usage.

    .. tip::
        This is similar to asyncio.as_completed. However it allows you
        to control how much resources are consumed throughout the execution,
        for instance:

        - How many open files will be opened at the same time
        - How many HTTP requests will be performed to a service (rate limit)
        - How many sockets will be opened concurrently
        - Etc

        This is useful for finite resources, for instance: the number
        of sockets provided by the operative system is limited; going beyond it
        would make the kernel to kill the program abruptly.

    Usage:
        >>> async def do(n):
                print('running:', n)
                await asyncio.sleep(1)
                print('returning:', n)
                return n

        >>> iterable = map(do, range(5))

        >>> for next in resolve(iterable, workers=2):
                try:
                    print('got resolved result:', await next)
                except:
                    pass  # Handle possible exceptions

    Output:
        ```
        running: 0
        running: 1
        returning: 0
        returning: 1
        got resolved result: 0
        got resolved result: 1
        running: 2
        running: 3
        returning: 2
        returning: 3
        got resolved result: 2
        got resolved result: 3
        running: 4
        returning: 4
        got resolved result: 4
        ```

    Args:
        awaitables: An iterable (generator, list, tuple, set, etc) of
            awaitables (coroutine, asyncio.Task, or asyncio.Future).
        workers: The number of independent workers that will be processing
            the input stream.
        worker_greediness: How much tasks can a worker process before waiting
            for you to retrieve its results. 0 means unlimited.

    Yields:
        A future with the result of the next ready task. Futures are yielded in
        the same order of the input stream (opposite to asyncio.as_completed)

    .. tip::
        This approach may be many times faster than batching because
        workers are independent of each other and they are constantly fetching
        the next task as soon as they get free (as long as the greediness
        allows them).

    .. tip::
        If awaitables is an instance of Sized (has `__len__` prototype).
        This function will launch at most `len(awaitables)` workers.
    """
    if workers < 1:
        raise ValueError('workers must be >= 1')
    if worker_greediness < 0:
        raise ValueError('worker_greediness must be >= 0')

    if hasattr(awaitables, '__len__'):
        workers = min(workers, len(awaitables))  # type: ignore

    loop = asyncio.get_event_loop()
    store: Dict[int, asyncio.Queue] = {}
    stream, stream_copy = tee(enumerate(awaitables))
    stream_finished = asyncio.Event()
    workers_up = asyncio.Event()
    workers_tasks: Dict[int, asyncio.Task] = {}

    async def worker() -> None:
        done: asyncio.Queue = asyncio.Queue(worker_greediness)
        for index, awaitable in stream:
            store[index] = done
            future = loop.create_future()
            future.set_result(await schedule(awaitable, loop=loop))
            await done.put(future)
            workers_up.set()
        workers_up.set()
        stream_finished.set()

    async def start_workers() -> None:
        for index in range(workers):
            if stream_finished.is_set():
                break
            workers_tasks[index] = asyncio.create_task(worker())
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


async def collect(
    awaitables: Iterable[Awaitable[_T]],
    *,
    workers: int = 1024,
    worker_greediness: int = 0,
) -> Tuple[_T, ...]:
    """Resolve concurrently the input stream and return back in the same order.

    See `resolve` for more information on the algorithm used and parameters.

    Usage:
        >>> async def do(n):
                print('running:', n)
                await asyncio.sleep(1)
                print('returning:', n)
                return n

        >>> iterable = map(do, range(5))

        >>> results = await collect(iterable, workers=2)

        >>> print(results)

    Output:
        ```
        running: 0
        running: 1
        returning: 0
        returning: 1
        running: 2
        running: 3
        returning: 2
        returning: 3
        running: 4
        returning: 4
        (0, 1, 2, 3, 4)
        ```
    """
    return tuple([
        await elem
        for elem in resolve(
            awaitables,
            workers=workers,
            worker_greediness=worker_greediness,
        )
    ])


def schedule(
    awaitable: Awaitable[_T],
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> Awaitable[_T]:
    """Schedule an awaitable in the event loop and return a wrapper for it.

    """
    wrapper = (loop or asyncio.get_event_loop()).create_future()

    def _done_callback(future: asyncio.Future) -> None:
        if not wrapper.done():
            wrapper.set_result(future)

    asyncio.create_task(awaitable).add_done_callback(_done_callback)

    return wrapper


async def unblock(
    function: Callable[..., _T],
    *args: Any,
    **kwargs: Any,
) -> _T:
    """Execute function(*args, **kwargs) in the specified thread executor."""
    if not THREAD_POOL.initialized:
        THREAD_POOL.initialize(max_workers=10 * CPU_COUNT)

    return await asyncio.get_running_loop().run_in_executor(
        THREAD_POOL.pool, partial(function, *args, **kwargs),
    )


async def unblock_cpu(
    function: Callable[..., _T],
    *args: Any,
    **kwargs: Any,
) -> _T:
    """Execute function(*args, **kwargs) in the specified process executor."""
    if not PROCESS_POOL.initialized:
        PROCESS_POOL.initialize(max_workers=CPU_COUNT)

    return await asyncio.get_running_loop().run_in_executor(
        PROCESS_POOL.pool, partial(function, *args, **kwargs),
    )


# Constants
PROCESS_POOL: ExecutorPool = ExecutorPool(ProcessPoolExecutor)
THREAD_POOL: ExecutorPool = ExecutorPool(ThreadPoolExecutor)
