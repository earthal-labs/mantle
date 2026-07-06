//! Bridges our async `object_store` S3/MinIO client to oxigdal's synchronous
//! `DataSource` trait. oxigdal's GeoTIFF/COG reader has no async variant
//! (`GeoTiffReader<S: DataSource>` requires the sync trait regardless of
//! feature flags), so callers must open/read it inside `spawn_blocking` and
//! this adapter bridges back to the async object store from there via
//! `Handle::block_on` — safe because we're already off the async runtime's
//! worker threads at that point.

use crate::byte_cache::ByteRangeCache;
use object_store::path::Path;
use object_store::ObjectStore;
use oxigdal::core_types::error::{IoError, OxiGdalError, Result as OxiGdalResult};
use oxigdal::core_types::io::{ByteRange, DataSource};
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Clone)]
pub struct ObjectStoreDataSource {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    handle: Handle,
    cache: ByteRangeCache,
}

impl ObjectStoreDataSource {
    /// Issues one HEAD request to learn the object's size upfront —
    /// `DataSource::size` must return synchronously. `cache` is shared across
    /// requests (owned by the raster engine) so repeated `read_range` calls
    /// for the same byte range — which oxigdal makes constantly, see
    /// `byte_cache` module docs — become in-memory hits instead of S3/MinIO
    /// round trips.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        path: Path,
        cache: ByteRangeCache,
    ) -> Result<Self, object_store::Error> {
        let meta = store.head(&path).await?;
        Ok(Self {
            store,
            path,
            size: meta.size as u64,
            handle: Handle::current(),
            cache,
        })
    }
}

impl DataSource for ObjectStoreDataSource {
    fn size(&self) -> OxiGdalResult<u64> {
        Ok(self.size)
    }

    fn read_range(&self, range: ByteRange) -> OxiGdalResult<Vec<u8>> {
        let key = format!("{}:{}:{}", self.path.as_ref(), range.start, range.end);
        if let Some(cached) = self.cache.get(&key) {
            return Ok((*cached).clone());
        }

        let store = self.store.clone();
        let path = self.path.clone();
        let start = range.start as usize;
        let end = range.end as usize;
        let bytes = self
            .handle
            .block_on(async move { store.get_range(&path, start..end).await })
            .map(|bytes| bytes.to_vec())
            .map_err(|e| {
                OxiGdalError::Io(IoError::Read {
                    message: e.to_string(),
                })
            })?;

        self.cache.insert(key, Arc::new(bytes.clone()));
        Ok(bytes)
    }
}
