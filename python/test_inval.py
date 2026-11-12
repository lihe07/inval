"""Needs a Redis at REDIS_URL (default redis://127.0.0.1:6379).
The reconnect test kills every pub/sub connection on that server."""

import asyncio
import itertools
import os

import pytest
from redis.asyncio import Redis

import inval
from inval import Cache, mutation, query

URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
_counter = itertools.count()


class Db:
    """A stand-in for Postgres: a value plus a count of how often it was read."""

    def __init__(self):
        self.value = 0
        self.reads = 0

    def load(self):
        self.reads += 1
        return self.value

    def store(self, value):
        self.value = value


@pytest.fixture
def db():
    return Db()


@pytest.fixture
async def connect():
    """Opens caches that share one namespace, fresh for each test."""
    namespace = f"pytest:{os.getpid()}:{next(_counter)}"
    caches = []

    async def connect():
        caches.append(await Cache.connect(URL, namespace))
        return caches[-1]

    yield connect
    for cache in caches:
        await cache.close()


async def eventually(cache, db, expected):
    """Invalidation reaches other processes asynchronously."""
    for _ in range(200):
        if await cache.query("k", db.load) == expected:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"never saw {expected}")


async def test_loads_once_then_serves_from_memory(connect, db):
    cache = await connect()
    for _ in range(10):
        assert await cache.query("k", db.load) == 0
    assert db.reads == 1


async def test_second_process_reads_from_redis(connect, db):
    assert await (await connect()).query("k", db.load) == 0
    assert await (await connect()).query("k", db.load) == 0
    assert db.reads == 1


async def test_mutation_invalidates_every_process(connect, db):
    a, b = await connect(), await connect()
    assert await a.query("k", db.load) == 0
    assert await b.query("k", db.load) == 0

    await a.mutation("k", lambda: db.store(7))

    assert await a.query("k", db.load) == 7  # the writer reads its own write
    await eventually(b, db, 7)


async def test_failed_mutation_does_not_invalidate(connect, db):
    cache = await connect()
    await cache.query("k", db.load)
    with pytest.raises(ZeroDivisionError):
        await cache.mutation("k", lambda: 1 / 0)
    await cache.query("k", db.load)
    assert db.reads == 1


async def test_slow_reader_cannot_restore_a_stale_value(connect, db):
    a, b = await connect(), await connect()

    async def load_then_get_overtaken():
        old = db.load()
        await b.mutation("k", lambda: db.store(7))
        return old

    assert await a.query("k", load_then_get_overtaken) == 0  # this call may be stale
    assert await a.query("k", db.load) == 7
    assert await b.query("k", db.load) == 7
    assert await (await connect()).query("k", db.load) == 7


async def test_survives_a_lost_subscription(connect, db):
    a, b = await connect(), await connect()
    assert await a.query("k", db.load) == 0

    async with Redis.from_url(URL) as redis:
        await redis.execute_command("CLIENT", "KILL", "TYPE", "pubsub")
    await asyncio.sleep(0.1)

    # `a` cannot hear this, but has already stopped trusting its memory.
    await b.mutation("k", lambda: db.store(7))
    assert await a.query("k", db.load) == 7

    # Once resubscribed, `a` serves from memory and hears invalidations again.
    await asyncio.sleep(1.5)
    reads = db.reads
    for _ in range(10):
        assert await a.query("k", db.load) == 7
    assert db.reads == reads
    await b.mutation("k", lambda: db.store(8))
    await eventually(a, db, 8)


@pytest.fixture
async def global_cache():
    inval.set_global(await Cache.connect(URL, f"pytest:{os.getpid()}:{next(_counter)}"))
    yield inval.get_global()
    inval._global = None


# Decorated at import time, before `set_global`, as in an application.
@query("settings")
def settings():
    DB.reads += 1
    return DB.value


@mutation("settings")
async def save_settings(value):
    DB.value = value
    return "saved"


DB = Db()


async def test_global_decorators(global_cache):
    assert await settings() == 0
    assert await save_settings(7) == "saved"
    assert await settings() == 7
    for _ in range(200):  # served from memory once the subscription is up
        reads = DB.reads
        if await settings() == 7 and DB.reads == reads:
            break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("never served from memory")
    await global_cache.close()


async def test_using_the_cache_before_set_global_fails():
    with pytest.raises(RuntimeError, match="call inval.set_global"):
        await settings()


async def test_own_cache_and_keys_from_arguments(connect):
    cache = await connect()
    loads = itertools.count()

    @query("tenant:{tenant}:{kind}", cache=cache)
    async def setting(tenant, kind="theme"):
        return f"{kind} of {tenant}, load {next(loads)}"

    @mutation("tenant:{tenant}:theme", cache=cache)
    def save_theme(tenant):
        pass

    assert await setting(1) == "theme of 1, load 0"
    assert await setting(2, kind="theme") == "theme of 2, load 1"
    assert await setting(tenant=1) == "theme of 1, load 0"
    assert setting.__name__ == "setting"

    await save_theme(1)
    assert await setting(1) == "theme of 1, load 2"
    assert await setting(2) == "theme of 2, load 1"


async def test_max_entries_limits_memory(db):
    namespace = f"pytest:{os.getpid()}:{next(_counter)}"
    cache = await Cache.connect(URL, namespace, max_entries=10)
    keys = [f"k{i}" for i in range(1000)]
    for key in keys:
        await cache.query(key, db.load)

    # Delete the values from Redis behind the cache's back: now only keys
    # still in memory can be served without calling the loader.
    async with Redis.from_url(URL) as redis:
        await redis.delete(*(f"{namespace}:val:{key}" for key in keys))
    for key in reversed(keys):  # newest first, before re-reads evict them
        await cache.query(key, db.load)
    assert 1 <= 2000 - db.reads <= 10
    await cache.close()
