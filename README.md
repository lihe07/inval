# inval

inval caches a function's result in memory and in Redis. When the data
changes, one call drops it from every process. It's built for things that are
read on every request and written a few times a day, like settings in
Postgres.

The names come from React Query. A **query** is cached. A **mutation**
changes the data and invalidates a query.

## Rust

Mark the functions, then set the global cache once at startup:

```rust
use inval::{Cache, query, mutation};

#[query("tenant:{tenant}:settings")]
async fn settings(pg: &PgPool, tenant: u32) -> Result<Settings, AppError> {
    Ok(sqlx::query_as("SELECT ...").bind(tenant).fetch_one(pg).await?)
}   // callers get Result<Arc<Settings>, AppError>

#[mutation("tenant:{tenant}:settings")]
async fn save_settings(pg: &PgPool, tenant: u32, new: &Settings) -> Result<(), AppError> {
    sqlx::query("UPDATE ...").bind(tenant).execute(pg).await?;
    Ok(())
}

type AppError = sqlx::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    inval::set_global(Cache::connect("redis://127.0.0.1", "myapp")?);
    // ...
}
```

The key is a `format!` string and can name the function's arguments. The
function has to be `async` and return a `Result`, and its body only runs on a
cache miss.

Your error type stays yours, and the only errors you get back are the ones
your function returned. `Cache::connect` fails only on a URL that isn't a
Redis URL. Redis doesn't have to be up, then or later.

Calling a marked function before `set_global` panics and tells you why. To
use some other cache, pass it in, as in
`#[query("settings", cache = self.cache)]`.

Without the macros:

```rust
let cache = Cache::connect("redis://127.0.0.1", "myapp")?;   // or inval::global()

let settings = cache.query("settings", || load_settings(&pg)).await?;   // Arc<Settings>
cache.mutation("settings", || save_settings(&pg, new)).await?;
cache.invalidate("settings").await;
```

To cap memory, build the cache with
`Cache::builder(url, ns).max_entries(10_000).connect()?`.

## Python

The Python package lives in `python/` and uses asyncio. Decorators can be
applied at import time. The cache is set later, inside a running event loop,
which in FastAPI means the lifespan hook:

```python
from contextlib import asynccontextmanager

import inval
from inval import Cache, query, mutation

@query("tenant:{tenant}:settings")
async def settings(tenant): ...

@mutation("tenant:{tenant}:settings")
async def save_settings(tenant, new): ...

@asynccontextmanager
async def lifespan(app):
    inval.set_global(await Cache.connect("redis://127.0.0.1", "myapp"))
    yield
```

The decorated function can be sync or async, but you always `await` the
result. To use some other cache, pass `cache=cache`. To cap memory, pass
`max_entries=10_000` to `Cache.connect`. Without decorators, call
`await cache.query(key, loader)`, `await cache.mutation(key, fn)` and
`await cache.invalidate(key)`.

Both languages store values as JSON, so Rust and Python processes can share a
namespace.

## How it behaves

`query(key, loader)` checks memory, then Redis, then calls `loader`. If two
requests miss at the same moment, both run the loader. That keeps the code
simple, and for settings it costs a few duplicate queries right after a
change.

`mutation(key, fn)` runs `fn`. If it succeeds, the key is deleted from Redis
and a broadcast tells every process in the namespace to drop it from memory.
`invalidate(key)` does the same without running anything.

The process that made the change reads the new value right away. Other
processes catch up when the broadcast arrives.

A slow read can't bring an old value back. Each key has a version in Redis
that every mutation bumps, and a loaded value is only stored if the version
didn't move in the meantime.

## When Redis is unreachable

Nothing fails. A process that has lost Redis keeps serving what it holds in
memory and calls the loader for the rest, so the database sees one read per
key per process, not one per request.

Redis pub/sub loses messages while a connection is down, so a disconnected
process can serve a value another process has changed. That lasts as long as
the outage: on resubscribing, a process empties its memory and reads
everything again.

An invalidation that can't reach Redis is kept and sent once Redis is back.
Until then only the process that made the change sees it.

## Limits

- A Rust cache belongs to the Tokio runtime that created it. Each
  `#[tokio::test]` gets its own runtime, so tests should create a cache per
  test with `Cache::connect` instead of setting the global one.
- An unsent invalidation lives in memory. If the process exits before Redis
  is back, the others keep the old value until the key is invalidated again.
- A value that can't be stored as JSON is kept in memory only.
- There's no TTL. Without `max_entries`, memory keeps every key it has read.
  With it, an arbitrary key is evicted when memory is full and fetched from
  Redis next time.
- Give Redis room for every key. If Redis evicts a version key, the guard
  against slow reads writing back old values gets weaker.
- Redis Cluster isn't supported, since a key's value and version could land
  on different nodes.

If your data changes every few seconds, this is the wrong tool. Each change
empties the key in every process, so frequent changes leave the cache cold
most of the time.

## Development

The tests need a Redis on `redis://127.0.0.1:6379` (or set `REDIS_URL`).
They kill the server's pub/sub connections, so don't point them at a shared
server.

```sh
docker run -d -p 6379:6379 redis:7-alpine
cargo test
cargo run --release --example bench
cd python && pip install -e '.[test]' && pytest
```

## License

MIT or Apache-2.0, at your option.
