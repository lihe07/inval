//! Cache a function's result in memory and in Redis, and invalidate it in
//! every process when the data changes.
//!
//! ```no_run
//! use inval::{Cache, query, mutation};
//!
//! #[query("tenant:{tenant}:settings")]
//! async fn settings(tenant: u32) -> Result<String, AppError> {
//!     Ok(format!("loaded from Postgres for {tenant}"))
//! }
//!
//! #[mutation("tenant:{tenant}:settings")]
//! async fn save_settings(tenant: u32, new: &str) -> Result<(), AppError> {
//!     Ok(())
//! }
//!
//! #[derive(Debug, thiserror::Error)]
//! enum AppError {
//!     #[error(transparent)]
//!     Db(#[from] std::io::Error),
//!     #[error(transparent)]
//!     Cache(#[from] inval::Error),
//! }
//!
//! # async fn demo() -> Result<(), AppError> {
//! inval::set_global(Cache::connect("redis://127.0.0.1", "myapp").await?);
//!
//! let current = settings(1).await?;
//! save_settings(1, "dark").await?;
//! # Ok(()) }
//! ```
//!
//! Without macros:
//!
//! ```no_run
//! # type BoxError = Box<dyn std::error::Error + Send + Sync>;
//! # async fn load() -> Result<String, BoxError> { Ok(String::new()) }
//! # async fn save() -> Result<(), BoxError> { Ok(()) }
//! # async fn demo() -> Result<(), BoxError> {
//! let cache = inval::Cache::connect("redis://127.0.0.1", "myapp").await?;
//! let settings = cache.query("settings", || load()).await?;
//! cache.mutation("settings", || save()).await?;
//! cache.invalidate("settings").await?;
//! # Ok(()) }
//! ```

use std::any::Any;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures_util::StreamExt;
use redis::aio::{ConnectionManager, PubSub};
use serde::{Serialize, de::DeserializeOwned};

pub use inval_macros::{mutation, query};

const RESUBSCRIBE_DELAY: Duration = Duration::from_secs(1);

/// Stores a loaded value only if no mutation happened while it was loading.
const SET_IF_VERSION: &str = r"
if (redis.call('GET', KEYS[2]) or '0') == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2])
end";

/// Converted into your functions' error type with `From`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("could not serialize value: {0}")]
    Serialize(#[from] serde_json::Error),
}

type Value = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
pub struct Cache(Arc<Inner>);

struct Inner {
    namespace: String,
    redis: ConnectionManager,
    set_if_version: redis::Script,
    values: papaya::HashMap<String, Value>,
    max_entries: usize,
    state: Mutex<State>,
    listener: OnceLock<tokio::task::JoinHandle<Option<()>>>,
}

/// One lock, so checking the epoch and inserting can't interleave with an
/// invalidation.
#[derive(Default)]
struct State {
    listening: bool,
    /// Bumped on every invalidation; a load is only kept if it didn't change.
    epoch: u64,
}

impl Inner {
    fn get<T: Send + Sync + 'static>(&self, key: &str) -> Option<Arc<T>> {
        let value = self.values.pin().get(key).cloned()?;
        Some(
            value
                .downcast()
                .unwrap_or_else(|_| panic!("cache key {key:?} read as two different types")),
        )
    }

    fn epoch(&self) -> u64 {
        self.state.lock().unwrap().epoch
    }

    fn insert(&self, key: &str, value: Value, epoch: u64) {
        let state = self.state.lock().unwrap();
        if state.listening && state.epoch == epoch {
            let values = self.values.pin();
            // Hash order makes this a random eviction.
            if values.len() >= self.max_entries
                && let Some(evict) = values.keys().find(|k| *k != key)
            {
                values.remove(evict);
            }
            values.insert(key.to_owned(), value);
        }
    }

    fn remove(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        state.epoch += 1;
        self.values.pin().remove(key);
    }

    fn reset(&self, listening: bool) {
        let mut state = self.state.lock().unwrap();
        state.epoch += 1;
        state.listening = listening;
        self.values.pin().clear();
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.get() {
            listener.abort();
        }
    }
}

static GLOBAL: OnceLock<Cache> = OnceLock::new();

/// Call once, inside the Tokio runtime that will use the cache. Panics if
/// called twice.
pub fn set_global(cache: Cache) {
    if GLOBAL.set(cache).is_err() {
        panic!("inval::set_global called twice");
    }
}

/// Panics if [`set_global`] has not been called.
pub fn global() -> &'static Cache {
    GLOBAL
        .get()
        .expect("call inval::set_global before using the cache")
}

pub struct Builder {
    url: String,
    namespace: String,
    max_entries: usize,
}

impl Builder {
    /// Unlimited by default. When full, an arbitrary key is evicted.
    pub fn max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries.max(1);
        self
    }

    pub async fn connect(self) -> Result<Cache, Error> {
        let client = redis::Client::open(self.url)?;
        let redis = ConnectionManager::new(client.clone()).await?;
        let channel = format!("{}:invalidate", self.namespace);
        let pubsub = subscribe(&client, &channel).await?;
        let inner = Arc::new(Inner {
            namespace: self.namespace,
            redis,
            set_if_version: redis::Script::new(SET_IF_VERSION),
            values: papaya::HashMap::new(),
            max_entries: self.max_entries,
            state: Mutex::default(),
            listener: OnceLock::new(),
        });
        // Weak, so the listener doesn't keep the cache alive.
        let listener = tokio::spawn(listen(Arc::downgrade(&inner), client, channel, pubsub));
        let _ = inner.listener.set(listener);
        Ok(Cache(inner))
    }
}

impl Cache {
    /// Processes with the same `namespace` share a cache.
    pub fn builder(url: &str, namespace: &str) -> Builder {
        Builder {
            url: url.to_owned(),
            namespace: namespace.to_owned(),
            max_entries: usize::MAX,
        }
    }

    pub async fn connect(url: &str, namespace: &str) -> Result<Self, Error> {
        Self::builder(url, namespace).connect().await
    }

    /// Panics if `key` is read as two different types.
    pub async fn query<T, E, F, Fut>(&self, key: &str, loader: F) -> Result<Arc<T>, E>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
        E: From<Error>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.0.get(key) {
            return Ok(value);
        }

        let epoch = self.0.epoch();
        let mut redis = self.0.redis.clone();
        let value_key = format!("{}:val:{key}", self.0.namespace);
        let version_key = format!("{}:ver:{key}", self.0.namespace);
        let (stored, version): (Option<Vec<u8>>, Option<u64>) = redis::cmd("MGET")
            .arg(&value_key)
            .arg(&version_key)
            .query_async(&mut redis)
            .await
            .map_err(Error::Redis)?;

        // A value that no longer deserializes (the type changed) counts as a miss.
        let value = match stored.and_then(|bytes| serde_json::from_slice::<T>(&bytes).ok()) {
            Some(value) => Arc::new(value),
            None => {
                let value = loader().await?;
                let _: () = self
                    .0
                    .set_if_version
                    .key(&value_key)
                    .key(&version_key)
                    .arg(version.unwrap_or(0))
                    .arg(serde_json::to_vec(&value).map_err(Error::Serialize)?)
                    .invoke_async(&mut redis)
                    .await
                    .map_err(Error::Redis)?;
                Arc::new(value)
            }
        };

        self.0.insert(key, value.clone(), epoch);
        Ok(value)
    }

    pub async fn mutation<R, E, F, Fut>(&self, key: &str, f: F) -> Result<R, E>
    where
        E: From<Error>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<R, E>>,
    {
        let result = f().await?;
        self.invalidate(key).await?;
        Ok(result)
    }

    pub async fn invalidate(&self, key: &str) -> Result<(), Error> {
        let namespace = &self.0.namespace;
        let _: () = redis::pipe()
            .atomic()
            .incr(format!("{namespace}:ver:{key}"), 1)
            .del(format!("{namespace}:val:{key}"))
            .publish(format!("{namespace}:invalidate"), key)
            .query_async(&mut self.0.redis.clone())
            .await?;
        // Not waiting for our own broadcast, so the caller reads its own write.
        self.0.remove(key);
        Ok(())
    }
}

async fn subscribe(client: &redis::Client, channel: &str) -> redis::RedisResult<PubSub> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    Ok(pubsub)
}

/// Returns once the cache is dropped.
async fn listen(
    cache: Weak<Inner>,
    client: redis::Client,
    channel: String,
    mut pubsub: PubSub,
) -> Option<()> {
    loop {
        cache.upgrade()?.reset(true);
        let mut messages = pubsub.on_message();
        while let Some(message) = messages.next().await {
            if let Ok(key) = message.get_payload::<String>() {
                cache.upgrade()?.remove(&key);
            }
        }
        drop(messages);

        // Invalidations may be missed until we resubscribe.
        cache.upgrade()?.reset(false);
        pubsub = loop {
            tokio::time::sleep(RESUBSCRIBE_DELAY).await;
            if let Ok(pubsub) = subscribe(&client, &channel).await {
                break pubsub;
            }
        };
    }
}

#[doc(hidden)]
pub mod __macro {
    use super::{Arc, Future};

    pub trait Wrapped {
        type Query;
        type Mutation;
    }

    impl<T, E> Wrapped for Result<T, E> {
        type Query = Result<Arc<T>, E>;
        type Mutation = Result<T, E>;
    }

    /// Pins the body's output type, which closure inference can't see.
    pub fn returning<R, F: Future<Output = R>>(body: F) -> F {
        body
    }
}
