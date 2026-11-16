//! Needs a Redis at REDIS_URL (default redis://127.0.0.1:6379), which the
//! tests reach through a proxy they can cut.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::SeqCst};
use std::time::Duration;

use inval::Cache;
use tokio::net::{TcpListener, TcpStream};

fn redis_address() -> String {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    url.trim_start_matches("redis://").to_owned()
}

fn namespace() -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "test:outage:{}:{}",
        std::process::id(),
        N.fetch_add(1, SeqCst)
    )
}

/// A Redis that can be made unreachable.
struct Proxy {
    url: String,
    is_up: Arc<AtomicBool>,
}

impl Proxy {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("redis://{}", listener.local_addr().unwrap());
        let is_up = Arc::new(AtomicBool::new(true));
        let up = is_up.clone();
        tokio::spawn(async move {
            loop {
                let (mut client, _) = listener.accept().await.unwrap();
                let up = up.clone();
                tokio::spawn(async move {
                    if !up.load(SeqCst) {
                        return;
                    }
                    let mut redis = TcpStream::connect(redis_address()).await.unwrap();
                    let copy = tokio::io::copy_bidirectional(&mut client, &mut redis);
                    let cut = async {
                        while up.load(SeqCst) {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    };
                    tokio::select! { _ = copy => {}, _ = cut => {} }
                });
            }
        });
        Self { url, is_up }
    }

    async fn set_up(&self, is_up: bool) {
        self.is_up.store(is_up, SeqCst);
        // Long enough for the caches to notice and, when up, resubscribe.
        tokio::time::sleep(Duration::from_millis(if is_up { 1500 } else { 200 })).await;
    }
}

/// A stand-in for Postgres: a value plus a count of how often it was read.
#[derive(Default)]
struct Db {
    value: AtomicU32,
    reads: AtomicU32,
}

impl Db {
    async fn load(&self) -> Result<u32, std::convert::Infallible> {
        self.reads.fetch_add(1, SeqCst);
        Ok(self.value.load(SeqCst))
    }

    async fn store(&self, value: u32) -> Result<(), std::convert::Infallible> {
        self.value.store(value, SeqCst);
        Ok(())
    }
}

async fn read(cache: &Cache, db: &Db) -> u32 {
    *cache.query("k", || db.load()).await.unwrap()
}

#[tokio::test]
async fn works_without_redis() {
    let (cache, db) = (
        Cache::connect("redis://127.0.0.1:1", "none").unwrap(),
        Db::default(),
    );
    for _ in 0..10 {
        assert_eq!(read(&cache, &db).await, 0);
    }
    assert_eq!(db.reads.load(SeqCst), 1, "held in memory");

    cache.mutation("k", || db.store(7)).await.unwrap();
    assert_eq!(read(&cache, &db).await, 7);
}

#[tokio::test]
async fn an_outage_delays_invalidations_but_loses_none() {
    let (proxy, ns, db) = (Proxy::start().await, namespace(), Db::default());
    let a = Cache::connect(&proxy.url, &ns).unwrap();
    let b = Cache::connect(&proxy.url, &ns).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!((read(&a, &db).await, read(&b, &db).await), (0, 0));

    proxy.set_up(false).await;
    a.mutation("k", || db.store(7)).await.unwrap();
    assert_eq!(read(&a, &db).await, 7, "the writer reads its own write");
    assert_eq!(read(&b, &db).await, 0, "the other serves what it holds");

    proxy.set_up(true).await;
    let fresh = Cache::connect(&proxy.url, &ns).unwrap();
    for cache in [&b, &fresh] {
        for _ in 0..500 {
            if read(cache, &db).await == 7 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(read(cache, &db).await, 7);
    }
}
