//! Needs a Redis at REDIS_URL (default redis://127.0.0.1:6379).
//!
//! Each `#[tokio::test]` has its own runtime, and a cache only works inside
//! the runtime that created it, so only one test here uses the global cache.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering::SeqCst};

use inval::{Cache, mutation, query};

fn url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

fn namespace() -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "test:macros:{}:{}",
        std::process::id(),
        N.fetch_add(1, SeqCst)
    )
}

/// An application's own error type. It only needs to accept inval's.
#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("rejected")]
    Rejected,
    #[error(transparent)]
    Parse(#[from] std::num::ParseIntError),
    #[error(transparent)]
    Cache(#[from] inval::Error),
}

/// A stand-in for Postgres: a value plus a count of how often it was read.
static VALUE: AtomicU32 = AtomicU32::new(0);
static READS: AtomicU32 = AtomicU32::new(0);

#[query("settings")]
async fn settings() -> Result<u32, AppError> {
    READS.fetch_add(1, SeqCst);
    Ok(VALUE.load(SeqCst))
}

#[mutation("settings")]
async fn save_settings(value: u32) -> Result<u32, AppError> {
    if value == 0 {
        return Err(AppError::Rejected);
    }
    Ok(VALUE.swap(value, SeqCst))
}

#[tokio::test]
async fn global_cache() {
    inval::set_global(Cache::connect(&url(), &namespace()).await.unwrap());

    let first: Arc<u32> = settings().await.unwrap();
    assert_eq!((*first, *settings().await.unwrap()), (0, 0));
    assert_eq!(READS.load(SeqCst), 1);

    assert_eq!(save_settings(7).await.unwrap(), 0);
    assert_eq!(*settings().await.unwrap(), 7);
    assert_eq!(READS.load(SeqCst), 2);

    let failed = save_settings(0).await;
    assert!(
        matches!(failed, Err(AppError::Rejected)),
        "your error comes back unchanged"
    );
    assert_eq!(*settings().await.unwrap(), 7);
    assert_eq!(READS.load(SeqCst), 2, "a failed write invalidates nothing");

    inval::global().invalidate("settings").await.unwrap();
    settings().await.unwrap();
    assert_eq!(READS.load(SeqCst), 3);
}

/// Keys can use the arguments, and methods can use a cache of their own.
struct Tenants {
    cache: Cache,
    loads: AtomicU32,
}

impl Tenants {
    #[query("tenant:{tenant}:{kind}", cache = self.cache)]
    async fn setting(&self, tenant: u32, kind: &str) -> Result<String, AppError> {
        let generation = self.loads.fetch_add(1, SeqCst);
        "1".parse::<u32>()?;
        Ok(format!("{kind} of {tenant}, load {generation}"))
    }

    #[mutation("tenant:{tenant}:theme", cache = self.cache)]
    async fn save_theme(&self, tenant: u32) -> Result<(), AppError> {
        Ok(())
    }
}

#[tokio::test]
async fn own_cache_and_keys_from_arguments() {
    let cache = Cache::connect(&url(), &namespace()).await.unwrap();
    let tenants = Tenants {
        cache,
        loads: AtomicU32::new(0),
    };
    assert_eq!(
        *tenants.setting(1, "theme").await.unwrap(),
        "theme of 1, load 0"
    );
    assert_eq!(
        *tenants.setting(2, "theme").await.unwrap(),
        "theme of 2, load 1"
    );
    assert_eq!(
        *tenants.setting(1, "theme").await.unwrap(),
        "theme of 1, load 0"
    );

    tenants.save_theme(1).await.unwrap();
    assert_eq!(
        *tenants.setting(1, "theme").await.unwrap(),
        "theme of 1, load 2"
    );
    assert_eq!(
        *tenants.setting(2, "theme").await.unwrap(),
        "theme of 2, load 1"
    );
}
