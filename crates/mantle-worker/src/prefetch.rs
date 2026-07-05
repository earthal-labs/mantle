//! Pre-fetch Icechunk `.zmetadata` into cache-ready blobs.
//!
//! COG IFD pre-fetching was removed here: tile rendering
//! (`mantle-raster::cog`) now reads COGs via oxigdal, which needs random
//! byte-range access to the whole file rather than a cached IFD-bytes
//! prefix, so nothing consumes that cache anymore.

use anyhow::{Context, Result};
use object_store::ObjectStore;
use std::sync::Arc;

/// Fetch Icechunk consolidated `.zmetadata` bytes for Redis (`mantle:zmeta:{repo_id}`).
pub async fn fetch_zmetadata_blob(
    store: Arc<dyn ObjectStore>,
    storage_uri: &str,
    default_bucket: &str,
) -> Result<Vec<u8>> {
    let (_bucket, prefix) = crate::storage::parse_storage_uri(storage_uri, default_bucket)?;
    let path = crate::storage::join_object_key(&prefix, ".zmetadata");
    let meta = store
        .get(&path)
        .await
        .with_context(|| format!("fetch icechunk .zmetadata at {}", path))?;
    Ok(meta.bytes().await?.to_vec())
}
