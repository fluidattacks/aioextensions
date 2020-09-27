# Standard library
import asyncio
from contextlib import (
    suppress,
)
from os import (
    scandir,
)
from typing import (
    Any,
    List,
)

# Third party libraries
import pytest

# Local libraries
from aioextensions import (
    BoundedSemaphore,
    collect,
    in_thread,
    in_process,
    generate_in_thread,
    PROCESS_POOL,
    rate_limited,
    resolve,
    run_decorator,
    Semaphore,
    THREAD_POOL,
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
async def test_in() -> None:
    await in_thread(sync)
    await in_thread(sync)
    await in_process(sync)
    await in_process(sync)
    [_ async for _ in generate_in_thread(scandir)]  # type: ignore


@run_decorator
async def test_rate_limited() -> None:

    async def get_loop_time() -> float:
        return asyncio.get_event_loop().time()

    # Test wrong values
    for max_calls, max_calls_period, min_seconds_between_calls in [
        (0, 1, 0.2),
        (2, 0, 0.2),
        (2, 1, -0.2),
    ]:
        with pytest.raises(ValueError):
            rate_limited(
                max_calls=max_calls,
                max_calls_period=max_calls_period,
                min_seconds_between_calls=min_seconds_between_calls,
            )(get_loop_time)

    # Test times
    do = rate_limited(
        max_calls=2,
        max_calls_period=1,
        min_seconds_between_calls=0.2,
    )(get_loop_time)
    result = await collect([do() for _ in range(10)])

    assert [round(x - result[0], 1) for x in result] == [
        # at most 2 calls per second
        # separated minimum 0.2 seconds between each one
        0.0, 0.2,
        1.0, 1.2,
        2.0, 2.2,
        3.0, 3.2,
        4.0, 4.2,
    ]


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


@run_decorator
async def test_acquire() -> None:
    for semaphore_cls in (BoundedSemaphore, Semaphore):
        semaphore = semaphore_cls(10)

        assert semaphore._value == 10

        async with semaphore.acquire_many(5):
            assert semaphore._value == 5

            async with semaphore.acquire_many(5):
                assert semaphore._value == 0

            assert semaphore._value == 5

            async with semaphore.acquire_many(5):
                assert semaphore._value == 0

            assert semaphore._value == 5

        assert semaphore._value == 10

        with pytest.raises(ValueError):
            async with semaphore.acquire_many(0):
                pass
