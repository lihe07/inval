# inval

inval caches a function's result in memory and in Redis. When the data
changes, one call drops it from every process. It's built for things that are
read on every request and written a few times a day, like settings in
Postgres.

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

Values are stored as JSON, so Python processes can share a cache with the
Rust crate of the same name. The full documentation, including what happens
when Redis goes away, is at https://github.com/lihe07/inval.

MIT or Apache-2.0, at your option.
