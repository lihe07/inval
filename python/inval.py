"""Cache a function's result in memory and in Redis, and invalidate it in
every process when the data changes.

    import inval
    from inval import Cache, query, mutation

    @query("tenant:{tenant}:settings")
    async def settings(tenant): ...

    @mutation("tenant:{tenant}:settings")
    async def save_settings(tenant, new): ...

    inval.set_global(await Cache.connect("redis://127.0.0.1", "myapp"))

Without decorators:

    cache = await Cache.connect("redis://127.0.0.1", "myapp")
    value = await cache.query("settings", load_settings)
    await cache.mutation("settings", lambda: save_settings(new))
    await cache.invalidate("settings")

A query returns the same object to every caller, so treat it as read-only.
"""

import asyncio
import contextlib
import functools
import inspect
import json

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

_RESUBSCRIBE_DELAY = 1.0

# Stores a loaded value only if no mutation happened while it was loading.
_SET_IF_VERSION = """
if (redis.call('GET', KEYS[2]) or '0') == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2])
end"""

_MISSING = object()
_global = None


def set_global(cache):
    global _global
    if _global is not None:
        raise RuntimeError("inval.set_global called twice")
    _global = cache


def get_global():
    if _global is None:
        raise RuntimeError("call inval.set_global before using the cache")
    return _global


def query(key, cache=None):
    return _decorator(key, cache, Cache.query)


def mutation(key, cache=None):
    return _decorator(key, cache, Cache.mutation)


def _decorator(key, cache, method):
    def decorate(fn):
        signature = inspect.signature(fn) if "{" in key else None

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            full_key = key
            if signature is not None:
                bound = signature.bind(*args, **kwargs)
                bound.apply_defaults()
                full_key = key.format(**bound.arguments)
            # Looked up per call, so decorating can happen before `set_global`.
            return await method(cache or get_global(), full_key, lambda: fn(*args, **kwargs))

        return wrapper

    return decorate


class Cache:
    @classmethod
    async def connect(cls, url, namespace, max_entries=None):
        """Processes with the same `namespace` share a cache. When
        `max_entries` is reached, an arbitrary key is evicted."""
        self = cls()
        self._namespace = namespace
        self._max_entries = None if max_entries is None else max(1, max_entries)
        self._redis = Redis.from_url(url)
        # No retries: redis-py would silently resubscribe, hiding missed
        # invalidations.
        self._pubsub_redis = Redis.from_url(url, retry=Retry(NoBackoff(), 0))
        self._set_if_version = self._redis.register_script(_SET_IF_VERSION)
        self._values = {}
        self._listening = False
        # Bumped on every invalidation; a load is only kept if it didn't change.
        self._epoch = 0
        self._listener = asyncio.create_task(self._listen(await self._subscribe()))
        return self

    async def close(self):
        self._listener.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._listener
        await self._redis.aclose()
        await self._pubsub_redis.aclose()

    async def query(self, key, loader):
        value = self._values.get(key, _MISSING)
        if value is not _MISSING:
            return value

        epoch = self._epoch
        value_key = f"{self._namespace}:val:{key}"
        version_key = f"{self._namespace}:ver:{key}"
        stored, version = await self._redis.mget(value_key, version_key)
        if stored is not None:
            value = json.loads(stored)
        else:
            value = await _call(loader)
            await self._set_if_version(
                keys=[value_key, version_key],
                args=[version or 0, json.dumps(value)],
            )

        self._insert(key, value, epoch)
        return value

    async def mutation(self, key, fn):
        result = await _call(fn)
        await self.invalidate(key)
        return result

    async def invalidate(self, key):
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.incr(f"{self._namespace}:ver:{key}")
            pipe.delete(f"{self._namespace}:val:{key}")
            pipe.publish(f"{self._namespace}:invalidate", key)
            await pipe.execute()
        # Not waiting for our own broadcast, so the caller reads its own write.
        self._remove(key)

    def _insert(self, key, value, epoch):
        if self._listening and self._epoch == epoch:
            if self._max_entries is not None and len(self._values) >= self._max_entries:
                del self._values[next(iter(self._values))]
            self._values[key] = value

    def _remove(self, key):
        self._epoch += 1
        self._values.pop(key, None)

    def _reset(self, listening):
        self._epoch += 1
        self._listening = listening
        self._values.clear()

    async def _subscribe(self):
        pubsub = self._pubsub_redis.pubsub()
        await pubsub.subscribe(f"{self._namespace}:invalidate")
        return pubsub

    async def _listen(self, pubsub):
        while True:
            self._reset(True)
            with contextlib.suppress(RedisError, OSError):
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        self._remove(message["data"].decode())
            await pubsub.aclose()

            # Invalidations may be missed until we resubscribe.
            self._reset(False)
            pubsub = None
            while pubsub is None:
                await asyncio.sleep(_RESUBSCRIBE_DELAY)
                with contextlib.suppress(RedisError, OSError):
                    pubsub = await self._subscribe()


async def _call(fn):
    result = fn()
    return await result if inspect.isawaitable(result) else result
