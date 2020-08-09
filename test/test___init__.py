# Standard library
import asyncio

# Local libraries
from aioextensions import (
    resolve,
)


class Error(Exception):
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


async def _test_resolve(loop: asyncio.AbstractEventLoop) -> None:
    results = []
    start = loop.time()
    for x in resolve(map(do, range(5)), workers=2, greediness=4):
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
    assert [0] == [await x for x in resolve([do(0)])]
    assert round(loop.time() - start, 1) == 0.4

    start = loop.time()
    assert [] == [x for x in resolve([])]
    assert round(loop.time() - start, 1) == 0.0

    results = []
    start = loop.time()
    for x in resolve(map(do, range(5)), workers=100000, greediness=1):
        try:
            results.append(await x)
        except Error:
            results.append('catched')
    assert round(loop.time() - start, 1) == 0.4
    assert results == [0, 1, 2, 'catched', 4]


def test_resolve() -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_test_resolve(loop))
