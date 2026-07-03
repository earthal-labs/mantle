//! Pre-fetch COG IFD headers and Icechunk `.zmetadata` into cache-ready blobs.

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use tracing::debug;

const INITIAL_HEADER_BYTES: u64 = 16_384;

/// Serialized COG IFD cache entry: byte segments covering the TIFF header + IFD chain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CogIfdCacheBlob {
    pub segments: Vec<ByteSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteSegment {
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Fetch and serialize COG IFD byte ranges for Redis (`mantle:ifd:{s3_key}`).
pub async fn fetch_cog_ifd_blob(store: Arc<dyn ObjectStore>, s3_key: &str) -> Result<Vec<u8>> {
    let path = Path::from(s3_key.trim_start_matches('/'));
    let header = get_range(&store, &path, 0..INITIAL_HEADER_BYTES).await?;
    let ranges = plan_ifd_ranges(&header)?;
    debug!(s3_key, segments = ranges.len(), "planned COG IFD byte ranges");

    let mut segments = Vec::with_capacity(ranges.len());
    for range in ranges {
        let data = if range.start == 0 && range.end <= header.len() as u64 {
            header[range.start as usize..range.end as usize].to_vec()
        } else {
            get_range(&store, &path, range.clone()).await?.to_vec()
        };
        segments.push(ByteSegment {
            offset: range.start,
            data,
        });
    }

    let blob = CogIfdCacheBlob { segments };
    Ok(bincode::serialize(&blob).context("serialize COG IFD cache blob")?)
}

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

async fn get_range(store: &dyn ObjectStore, path: &Path, range: Range<u64>) -> Result<Bytes> {
    let start = usize::try_from(range.start).context("range start exceeds usize")?;
    let end = usize::try_from(range.end).context("range end exceeds usize")?;
    let opts = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };
    Ok(store
        .get_opts(path, opts)
        .await
        .with_context(|| {
            format!(
                "range read {} bytes at offset {}",
                range.end - range.start,
                range.start
            )
        })?
        .bytes()
        .await?)
}

fn plan_ifd_ranges(header: &[u8]) -> Result<Vec<Range<u64>>> {
    if header.len() < 8 {
        return Err(anyhow!("TIFF header too short"));
    }

    let le = match &header[0..2] {
        b"II" => true,
        b"MM" => false,
        _ => return Err(anyhow!("invalid TIFF byte order marker")),
    };

    let magic = read_u16(header, 2, le)?;
    if magic != 42 {
        return Err(anyhow!("invalid TIFF magic {magic}"));
    }

    let mut ranges = vec![0..INITIAL_HEADER_BYTES.min(header.len() as u64)];
    let mut next_ifd = read_u32(header, 4, le)? as u64;

    while next_ifd != 0 {
        if next_ifd + 2 > header.len() as u64 {
            // IFD directory starts beyond the initial read — extend range for entry count.
            ranges.push(next_ifd..next_ifd + 2);
        }

        let entry_count = if next_ifd + 2 <= header.len() as u64 {
            read_u16(header, next_ifd, le)? as u64
        } else {
            0
        };

        let ifd_len = 2 + entry_count * 12 + 4;
        ranges.push(next_ifd..next_ifd + ifd_len);

        let next_ptr_offset = next_ifd + 2 + entry_count * 12;
        if next_ptr_offset + 4 <= header.len() as u64 {
            next_ifd = read_u32(header, next_ptr_offset, le)? as u64;
        } else {
            ranges.push(next_ptr_offset..next_ptr_offset + 4);
            break;
        }
    }

    Ok(merge_ranges(ranges))
}

fn merge_ranges(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|r| r.start);
    let mut merged = Vec::new();
    let mut current = ranges[0].clone();
    for range in ranges.into_iter().skip(1) {
        if range.start <= current.end {
            current.end = current.end.max(range.end);
        } else {
            merged.push(current);
            current = range;
        }
    }
    merged.push(current);
    merged
}

fn read_u16(buf: &[u8], offset: u64, le: bool) -> Result<u16> {
    let start = offset as usize;
    let end = start + 2;
    if end > buf.len() {
        return Err(anyhow!("read u16 out of bounds at {offset}"));
    }
    Ok(if le {
        u16::from_le_bytes([buf[start], buf[start + 1]])
    } else {
        u16::from_be_bytes([buf[start], buf[start + 1]])
    })
}

fn read_u32(buf: &[u8], offset: u64, le: bool) -> Result<u32> {
    let start = offset as usize;
    let end = start + 4;
    if end > buf.len() {
        return Err(anyhow!("read u32 out of bounds at {offset}"));
    }
    Ok(if le {
        u32::from_le_bytes([buf[start], buf[start + 1], buf[start + 2], buf[start + 3]])
    } else {
        u32::from_be_bytes([buf[start], buf[start + 1], buf[start + 2], buf[start + 3]])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_ifd_ranges_for_minimal_little_endian_tiff() {
        let mut header = vec![0u8; 64];
        header[0..2].copy_from_slice(b"II");
        header[2..4].copy_from_slice(&42u16.to_le_bytes());
        header[4..8].copy_from_slice(&8u32.to_le_bytes());
        header[8..10].copy_from_slice(&0u16.to_le_bytes());
        header[10..14].copy_from_slice(&0u32.to_le_bytes());

        let ranges = plan_ifd_ranges(&header).expect("plan ranges");
        assert!(ranges.iter().any(|r| r.start <= 8 && r.end > 8));
    }

    #[test]
    fn merge_ranges_coalesces_overlapping() {
        let merged = merge_ranges(vec![0..10, 8..20, 30..40]);
        assert_eq!(merged, vec![0..20, 30..40]);
    }
}
