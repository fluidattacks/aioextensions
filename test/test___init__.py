# Standard library
import asyncio
from contextlib import (
    suppress,
)
from typing import (
    Any,
    List,
)

# Local libraries
from aioextensions import (
    run_decorator,
    collect,
    PROCESS_POOL,
    resolve,
    THREAD_POOL,
    unblock,
    unblock_cpu,
)


class Error(Exception):
    pass


def sync() -> None:
    pass


async def do(n: int) -> int:
    print('running', n)

    if n == 0:
        await asyncio.sleep(0.4)
    else:
        await asyncio.sleep(0.1)

    if n == 3:
        print('raising', n)
        raise Error('n == 3')

    print('returning', n)
    return n


def test_executor_pool() -> None:
    for pool in [PROCESS_POOL, THREAD_POOL]:
        pool.shutdown(wait=False)
        with suppress(RuntimeError):
            pool.pool
        pool.initialize()
        pool.pool
        pool.initialize()
        pool.shutdown(wait=False)


@run_decorator
async def test_unblock() -> None:
    await unblock(sync)
    await unblock(sync)
    await unblock_cpu(sync)
    await unblock_cpu(sync)


@run_decorator
async def test_collect_and_resolve() -> None:
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    with suppress(ValueError):
        tuple(resolve([], workers=0))

    with suppress(ValueError):
        tuple(resolve([], worker_greediness=-1))

    results: List[Any] = []
    start = loop.time()
    for x in resolve(map(do, range(5)), workers=2, worker_greediness=4):
        try:
            results.append(await x)
        except Error:
            results.append('catched')

    # One worker is filled while the other one is processing small tasks
    # t   a b
    # 0.0 0 1
    # 0.1 0 2
    # 0.2 0 3
    # 0.3 0 4
    # 0.4 - -
    assert round(loop.time() - start, 1) == 0.4
    assert results == [0, 1, 2, 'catched', 4]

    start = loop.time()
    assert (0,) == await collect([do(0)])
    assert round(loop.time() - start, 1) == 0.4

    start = loop.time()
    assert () == await collect([])
    assert round(loop.time() - start, 1) == 0.0

    results = []
    start = loop.time()
    for x in resolve(map(do, range(5)), workers=100000, worker_greediness=1):
        try:
            results.append(await x)
        except Error:
            results.append('catched')
    assert round(loop.time() - start, 1) == 0.4
    assert results == [0, 1, 2, 'catched', 4]
