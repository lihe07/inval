//! Needs a Redis at REDIS_URL (default redis://127.0.0.1:6379).
//! Kills every pub/sub connection on that server, hence its own test binary.

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

/// Pub/sub drops messages while disconnected, so memory must not be trusted
/// across a lost subscription.
#[tokio::test]
async fn survives_a_lost_subscription() {
    let (ns, db) = (namespace(), Db::default());
    let (a, b) = (connect(&ns).await, connect(&ns).await);
    assert_eq!(read(&a, &db).await, 0);

    let mut redis = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("pubsub")
        .query_async(&mut redis)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // `a` cannot hear this, but has already stopped trusting its memory.
    b.mutation("k", || db.store(7)).await.unwrap();
    assert_eq!(read(&a, &db).await, 7);

    // Once resubscribed, `a` serves from memory and hears invalidations again.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let reads = db.reads.load(SeqCst);
    for _ in 0..10 {
        assert_eq!(read(&a, &db).await, 7);
    }
    assert_eq!(db.reads.load(SeqCst), reads);
    b.mutation("k", || db.store(8)).await.unwrap();
    eventually(&a, &db, 8).await;
}
