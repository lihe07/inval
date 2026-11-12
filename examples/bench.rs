//! cargo run --release --example bench

use std::time::Instant;

use inval::Cache;

const READS: u32 = 2_000_000;

async fn reads(cache: &Cache) {
    for _ in 0..READS {
        let value = cache
            .query("k", || async { Ok::<_, inval::Error>(1u64) })
            .await
            .unwrap();
        std::hint::black_box(value);
    }
}

#[tokio::main]
async fn main() {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let cache = Cache::connect(&url, "bench").await.unwrap();
    reads(&cache).await;

    for tasks in [1, 4, 16] {
        let start = Instant::now();
        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let cache = cache.clone();
                tokio::spawn(async move { reads(&cache).await })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap();
        }
        let elapsed = start.elapsed();
        println!(
            "{tasks:>2} tasks: {:>4.0} ns/read, {:>5.1} M reads/s total",
            elapsed.as_nanos() as f64 / READS as f64,
            (tasks * READS) as f64 / elapsed.as_secs_f64() / 1e6,
        );
    }

    let start = Instant::now();
    for _ in 0..2000 {
        cache.invalidate("k").await.unwrap();
        reads_once(&cache).await;
    }
    println!(
        "invalidate + reload: {:.0} µs",
        start.elapsed().as_micros() as f64 / 2000.0
    );
}

async fn reads_once(cache: &Cache) {
    cache
        .query("k", || async { Ok::<_, inval::Error>(1u64) })
        .await
        .unwrap();
}
