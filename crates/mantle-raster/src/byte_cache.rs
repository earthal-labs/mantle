//! In-process cache of raw byte ranges read from object storage.
//!
//! oxigdal's `CogReader` has no cache of its own: opening a COG re-reads the
//! TIFF header/IFD/GeoKeys, and — critically — `tile_byte_range` re-fetches
//! the *entire* `TileOffsets`/`TileByteCounts` arrays from the source on
//! every single `read_tile_raw` call (they're stored out-of-line in the IFD
//! for any COG with more than a couple of tiles, so there's no way around a
//! `read_range` call to get them). Left alone, a single tile request that
//! spans a few source tiles ends up making a dozen-plus blocking network
//! round trips, and every one of those round trips repeats on every request
//! for the same service.
//!
//! Caching at this level (below oxigdal, keyed by the exact byte range
//! requested) fixes all of that in one place without having to reimplement
//! or track oxigdal's internal parsing logic: COG files are immutable once
//! uploaded, so a cached range is valid indefinitely (bounded here by a
//! size-based eviction policy and TTL, not correctness).
use moka::sync::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct ByteRangeCache {
    inner: Cache<String, Arc<Vec<u8>>>,
}

impl ByteRangeCache {
    pub fn new(max_capacity_bytes: u64, ttl: Duration) -> Self {
        let inner = Cache::builder()
            .weigher(|_key: &String, value: &Arc<Vec<u8>>| -> u32 {
                value.len().try_into().unwrap_or(u32::MAX)
            })
            .max_capacity(max_capacity_bytes)
            .time_to_live(ttl)
            .build();
        Self { inner }
    }

    pub fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.inner.get(key)
    }

    pub fn insert(&self, key: String, value: Arc<Vec<u8>>) {
        self.inner.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_after_insert() {
        let cache = ByteRangeCache::new(1024, Duration::from_secs(60));
        assert!(cache.get("k").is_none());
        cache.insert("k".into(), Arc::new(vec![1, 2, 3]));
        assert_eq!(cache.get("k").as_deref(), Some(&vec![1u8, 2, 3]));
    }

    #[test]
    fn distinct_keys_dont_collide() {
        let cache = ByteRangeCache::new(1024, Duration::from_secs(60));
        cache.insert("a:0:10".into(), Arc::new(vec![1]));
        cache.insert("b:0:10".into(), Arc::new(vec![2]));
        assert_eq!(cache.get("a:0:10").as_deref(), Some(&vec![1u8]));
        assert_eq!(cache.get("b:0:10").as_deref(), Some(&vec![2u8]));
    }
}
