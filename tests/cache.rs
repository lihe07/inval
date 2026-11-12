//! Needs a Redis at REDIS_URL (default redis://127.0.0.1:6379).

use std::sync::atomic::{AtomicU32, Ordering::SeqCst};
use std::time::Duration;

use inval::Cache;

fn url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

/// A fresh namespace per test, so tests don't see each other's keys.
fn namespace() -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!("test:{}:{}", std::process::id(), N.fetch_add(1, SeqCst))
}

async fn connect(namespace: &str) -> Cache {
    Cache::connect(&url(), namespace).await.unwrap()
}

/// A stand-in for Postgres: a value plus a count of how often it was read.
#[derive(Default)]
struct Db {
    value: AtomicU32,
    reads: AtomicU32,
}

impl Db {
    async fn load(&self) -> Result<u32, inval::Error> {
        self.reads.fetch_add(1, SeqCst);
        Ok(self.value.load(SeqCst))
    }

    async fn store(&self, value: u32) -> Result<(), inval::Error> {
        self.value.store(value, SeqCst);
        Ok(())
    }
}

async fn read(cache: &Cache, db: &Db) -> u32 {
    *cache.query("k", || db.load()).await.unwrap()
}

/// Invalidation reaches other processes asynchronously.
async fn eventually(cache: &Cache, db: &Db, expected: u32) {
    for _ in 0..200 {
        if read(cache, db).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never saw {expected}");
}

#[tokio::test]
async fn loads_once_then_serves_from_memory() {
    let (cache, db) = (connect(&namespace()).await, Db::default());
    for _ in 0..10 {
        assert_eq!(read(&cache, &db).await, 0);
    }
    assert_eq!(db.reads.load(SeqCst), 1);
}

#[tokio::test]
async fn second_process_reads_from_redis() {
    let (ns, db) = (namespace(), Db::default());
    assert_eq!(read(&connect(&ns).await, &db).await, 0);
    assert_eq!(read(&connect(&ns).await, &db).await, 0);
    assert_eq!(db.reads.load(SeqCst), 1);
}

#[tokio::test]
async fn mutation_invalidates_every_process() {
    let (ns, db) = (namespace(), Db::default());
    let (a, b) = (connect(&ns).await, connect(&ns).await);
    assert_eq!(read(&a, &db).await, 0);
    assert_eq!(read(&b, &db).await, 0);

    a.mutation("k", || db.store(7)).await.unwrap();

    assert_eq!(
        read(&a, &db).await,
        7,
        "the writer reads its own write immediately"
    );
    eventually(&b, &db, 7).await;
}

#[tokio::test]
async fn failed_mutation_does_not_invalidate() {
    let (cache, db) = (connect(&namespace()).await, Db::default());
    read(&cache, &db).await;
    type BoxError = Box<dyn std::error::Error + Send + Sync>;
    let result = cache
        .mutation("k", || async { Err::<(), BoxError>("nope".into()) })
        .await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "nope",
        "your error comes back unchanged"
    );
    read(&cache, &db).await;
    assert_eq!(db.reads.load(SeqCst), 1);
}

/// A reader that loaded the old value before a mutation, and finishes after
/// it, must not put the old value back.
#[tokio::test]
async fn slow_reader_cannot_restore_a_stale_value() {
    let (ns, db) = (namespace(), Db::default());
    let (a, b) = (connect(&ns).await, connect(&ns).await);

    let stale = a
        .query("k", || async {
            let old = db.load().await;
            b.mutation("k", || db.store(7)).await.unwrap();
            old
        })
        .await
        .unwrap();
    assert_eq!(*stale, 0, "this one call may return what it loaded");

    assert_eq!(read(&a, &db).await, 7);
    assert_eq!(read(&b, &db).await, 7);
    assert_eq!(read(&connect(&ns).await, &db).await, 7);
}

#[tokio::test]
async fn max_entries_limits_memory() {
    let ns = namespace();
    let cache = Cache::builder(&url(), &ns)
        .max_entries(10)
        .connect()
        .await
        .unwrap();
    let db = Db::default();
    let keys: Vec<String> = (0..1000).map(|i| format!("k{i}")).collect();
    for key in &keys {
        cache.query(key, || db.load()).await.unwrap();
    }

    // Delete the values from Redis behind the cache's back: now only keys
    // still in memory can be served without calling the loader.
    let mut redis = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let stored: Vec<String> = keys.iter().map(|k| format!("{ns}:val:{k}")).collect();
    let _: () = redis::cmd("DEL")
        .arg(&stored)
        .query_async(&mut redis)
        .await
        .unwrap();

    for key in &keys {
        cache.query(key, || db.load()).await.unwrap();
    }
    let from_memory = 2000 - db.reads.load(SeqCst);
    assert!(
        (1..=10).contains(&from_memory),
        "{from_memory} keys were in memory"
    );
}
