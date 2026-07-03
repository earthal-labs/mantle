//! COG byte-range reads with Redis IFD cache read-through.
//!
//! IFD blobs use the same bincode layout as `mantle-worker::prefetch::CogIfdCacheBlob`
//! (`mantle:ifd:{s3_key}` per AGENTS.md).

use crate::storage::object_path;
use crate::tile_math::{DatasetCrs, GeoTransform};
use crate::RasterError;
use bytes::Bytes;
use mantle_cache::CacheClient;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use tracing::debug;

const INITIAL_HEADER_BYTES: u64 = 16_384;

/// Serialized COG IFD cache entry (contract shared with mantle-worker).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CogIfdCacheBlob {
    pub segments: Vec<ByteSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteSegment {
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Parsed GeoTIFF metadata from cached IFD bytes.
#[derive(Debug, Clone)]
pub struct CogMetadata {
    pub width: u32,
    pub height: u32,
    pub bits_per_sample: u16,
    pub samples_per_pixel: u16,
    pub geotransform: GeoTransform,
    pub crs: DatasetCrs,
    pub strip_offsets: Vec<u64>,
    pub strip_byte_counts: Vec<u64>,
    pub tile_width: Option<u32>,
    pub tile_height: Option<u32>,
    pub tile_offsets: Vec<u64>,
    pub tile_byte_counts: Vec<u64>,
    pub little_endian: bool,
}

/// Read-through IFD fetch: Redis → S3 header planning → cache populate.
pub async fn get_ifd_blob_read_through<C: CacheClient + ?Sized>(
    cache: &C,
    store: Arc<dyn ObjectStore>,
    s3_key: &str,
    ttl_seconds: u64,
) -> Result<Vec<u8>, RasterError> {
    if let Some(cached) = cache.get_ifd(s3_key).await? {
        return Ok(cached);
    }
    let blob = fetch_cog_ifd_blob(store, s3_key).await?;
    cache.set_ifd(s3_key, &blob, ttl_seconds).await?;
    Ok(blob)
}

pub async fn fetch_cog_ifd_blob(
    store: Arc<dyn ObjectStore>,
    s3_key: &str,
) -> Result<Vec<u8>, RasterError> {
    let path = object_path(s3_key);
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

    bincode::serialize(&CogIfdCacheBlob { segments })
        .map_err(|e| RasterError::Cog(format!("serialize IFD blob: {e}")))
}

pub fn parse_cog_metadata(blob_bytes: &[u8]) -> Result<CogMetadata, RasterError> {
    let blob: CogIfdCacheBlob = bincode::deserialize(blob_bytes)
        .map_err(|e| RasterError::Cog(format!("deserialize IFD blob: {e}")))?;
    let map = SegmentMap::new(blob.segments);
    parse_tiff_metadata(&map)
}

/// Read a raster window as `f32` samples (band 0 / first sample).
pub async fn read_cog_window_f32(
    store: Arc<dyn ObjectStore>,
    s3_key: &str,
    meta: &CogMetadata,
    col0: u32,
    row0: u32,
    col1: u32,
    row1: u32,
) -> Result<Vec<f32>, RasterError> {
    let width = col1.saturating_sub(col0);
    let height = row1.saturating_sub(row0);
    if width == 0 || height == 0 {
        return Ok(Vec::new());
    }

    let path = object_path(s3_key);
    let mut out = vec![f32::NAN; (width * height) as usize];

    if !meta.tile_offsets.is_empty() {
        let tw = meta.tile_width.unwrap_or(256);
        let th = meta.tile_height.unwrap_or(256);
        let tiles_across = meta.width.div_ceil(tw);

        for row in row0..row1 {
            for col in col0..col1 {
                let tile_col = col / tw;
                let tile_row = row / th;
                let tile_idx = tile_row * tiles_across + tile_col;
                let tile_offset = *meta
                    .tile_offsets
                    .get(tile_idx as usize)
                    .ok_or_else(|| RasterError::Cog("tile offset out of range".into()))?;
                let tile_bytes = *meta
                    .tile_byte_counts
                    .get(tile_idx as usize)
                    .unwrap_or(&0);
                if tile_bytes == 0 {
                    continue;
                }

                let data = get_range(&store, &path, tile_offset..tile_offset + tile_bytes).await?;
                let local_col = col % tw;
                let local_row = row % th;
                if let Some(v) = decode_sample(
                    &data,
                    meta,
                    local_col,
                    local_row,
                    tw,
                    th,
                ) {
                    out[((row - row0) * width + (col - col0)) as usize] = v;
                }
            }
        }
    } else {
        for (strip_idx, (&offset, &byte_count)) in meta
            .strip_offsets
            .iter()
            .zip(meta.strip_byte_counts.iter())
            .enumerate()
        {
            let strip_row = strip_idx as u32;
            if strip_row < row0 || strip_row >= row1 || byte_count == 0 {
                continue;
            }
            let data = get_range(&store, &path, offset..offset + byte_count).await?;
            for col in col0..col1 {
                if let Some(v) = decode_sample(&data, meta, col - col0, 0, width, 1) {
                    out[((strip_row - row0) * width + (col - col0)) as usize] = v;
                }
            }
        }
    }

    Ok(out)
}

fn decode_sample(
    data: &[u8],
    meta: &CogMetadata,
    col: u32,
    row: u32,
    width: u32,
    _height: u32,
) -> Option<f32> {
    let spp = meta.samples_per_pixel.max(1) as usize;
    let bps = meta.bits_per_sample.max(8) as usize;
    let pixel_stride = (bps / 8).max(1) * spp;
    let idx = ((row * width + col) as usize) * pixel_stride;
    if idx + pixel_stride > data.len() {
        return None;
    }
    match bps {
        8 => Some(data[idx] as f32),
        16 => {
            let raw = if meta.little_endian {
                u16::from_le_bytes([data[idx], data[idx + 1]])
            } else {
                u16::from_be_bytes([data[idx], data[idx + 1]])
            };
            Some(raw as f32)
        }
        32 => {
            let raw = if meta.little_endian {
                f32::from_le_bytes([data[idx], data[idx + 1], data[idx + 2], data[idx + 3]])
            } else {
                f32::from_be_bytes([data[idx], data[idx + 1], data[idx + 2], data[idx + 3]])
            };
            Some(raw)
        }
        _ => None,
    }
}

struct SegmentMap {
    segments: Vec<ByteSegment>,
}

impl SegmentMap {
    fn new(segments: Vec<ByteSegment>) -> Self {
        Self { segments }
    }

    fn read(&self, offset: u64, len: usize) -> Option<Vec<u8>> {
        let mut out = vec![0u8; len];
        let mut remaining = len;
        let mut cursor = offset;
        let mut written = 0usize;

        while remaining > 0 {
            let seg = self.segments.iter().find(|s| {
                cursor >= s.offset && cursor < s.offset + s.data.len() as u64
            })?;
            let local = (cursor - seg.offset) as usize;
            let take = remaining.min(seg.data.len() - local);
            out[written..written + take].copy_from_slice(&seg.data[local..local + take]);
            written += take;
            remaining -= take;
            cursor += take as u64;
        }
        Some(out)
    }
}

fn parse_tiff_metadata(map: &SegmentMap) -> Result<CogMetadata, RasterError> {
    let header = map.read(0, 8).ok_or_else(|| RasterError::Cog("missing TIFF header".into()))?;
    let le = match &header[0..2] {
        b"II" => true,
        b"MM" => false,
        _ => return Err(RasterError::Cog("invalid TIFF byte order".into())),
    };
    if read_u16(&header, 2, le)? != 42 {
        return Err(RasterError::Cog("invalid TIFF magic".into()));
    }

    let ifd_offset = read_u32(&header, 4, le)? as u64;
    let mut tags = read_ifd_tags(map, ifd_offset, le)?;

    let width = tags.remove(&256).and_then(tag_u32).unwrap_or(0);
    let height = tags.remove(&257).and_then(tag_u32).unwrap_or(0);
    let bits_per_sample = tags
        .remove(&258)
        .and_then(tag_u16)
        .unwrap_or(8);
    let samples_per_pixel = tags.remove(&277).and_then(tag_u16).unwrap_or(1);

    let strip_offsets = tags
        .remove(&273)
        .map(|t| tag_u64_list(map, &t, le))
        .transpose()?
        .unwrap_or_default();
    let strip_byte_counts = tags
        .remove(&279)
        .map(|t| tag_u64_list(map, &t, le))
        .transpose()?
        .unwrap_or_default();
    let tile_width = tags.remove(&322).and_then(tag_u32);
    let tile_height = tags.remove(&323).and_then(tag_u32);
    let tile_offsets = tags
        .remove(&324)
        .map(|t| tag_u64_list(map, &t, le))
        .transpose()?
        .unwrap_or_default();
    let tile_byte_counts = tags
        .remove(&325)
        .map(|t| tag_u64_list(map, &t, le))
        .transpose()?
        .unwrap_or_default();

    let geotransform = geotransform_from_tags(map, &mut tags, le, width, height)?;
    let crs = crs_from_tags(&tags);

    Ok(CogMetadata {
        width,
        height,
        bits_per_sample,
        samples_per_pixel,
        geotransform,
        crs,
        strip_offsets,
        strip_byte_counts,
        tile_width,
        tile_height,
        tile_offsets,
        tile_byte_counts,
        little_endian: le,
    })
}

#[derive(Debug, Clone)]
struct IfdTag {
    tag_type: u16,
    count: u32,
    value_offset: u64,
}

fn read_ifd_tags(
    map: &SegmentMap,
    ifd_offset: u64,
    le: bool,
) -> Result<std::collections::HashMap<u16, IfdTag>, RasterError> {
    let entry_count = read_u16_at(map, ifd_offset, le)? as usize;
    let mut tags = std::collections::HashMap::new();
    for i in 0..entry_count {
        let base = ifd_offset + 2 + (i as u64 * 12);
        let tag_id = read_u16_at(map, base, le)?;
        let tag_type = read_u16_at(map, base + 2, le)?;
        let count = read_u32_at(map, base + 4, le)?;
        let value_offset = read_u32_at(map, base + 8, le)? as u64;
        tags.insert(
            tag_id,
            IfdTag {
                tag_type,
                count,
                value_offset,
            },
        );
    }
    Ok(tags)
}

fn geotransform_from_tags(
    map: &SegmentMap,
    tags: &mut std::collections::HashMap<u16, IfdTag>,
    le: bool,
    width: u32,
    height: u32,
) -> Result<GeoTransform, RasterError> {
    if let Some(scale_tag) = tags.remove(&33550) {
        let tie_tag = tags.remove(&33922);
        let scales = read_rational_list(map, &scale_tag, le)?;
        let tie = tie_tag
            .as_ref()
            .map(|t| read_f64_list(map, t, le))
            .transpose()?
            .unwrap_or_else(|| vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);

        let pixel_w = scales.first().copied().unwrap_or(1.0);
        let pixel_h = scales.get(1).copied().unwrap_or(1.0);
        let origin_x = tie.get(3).copied().unwrap_or(0.0);
        let origin_y = tie.get(4).copied().unwrap_or(0.0);
        return Ok(GeoTransform::north_up(origin_x, origin_y, pixel_w, pixel_h));
    }

    // Fallback: unit pixel grid when geo tags are absent.
    let _ = (map, le, tags, width);
    Ok(GeoTransform::north_up(0.0, height as f64, 1.0, 1.0))
}

fn crs_from_tags(tags: &std::collections::HashMap<u16, IfdTag>) -> DatasetCrs {
    if let Some(tag) = tags.get(&34737) {
        if tag.count > 0 {
            // ASCII params — best-effort scan deferred; use GeoKey directory when present.
        }
    }
    if let Some(tag) = tags.get(&34735) {
        if tag.count >= 4 {
            return DatasetCrs::Wgs84;
        }
    }
    DatasetCrs::Unknown
}

fn tag_u16(tag: IfdTag) -> Option<u16> {
    if tag.count >= 1 && tag.tag_type == 3 {
        Some(tag.value_offset as u16)
    } else {
        None
    }
}

fn tag_u32(tag: IfdTag) -> Option<u32> {
    if tag.count >= 1 && tag.tag_type == 4 {
        Some(tag.value_offset as u32)
    } else {
        None
    }
}

fn tag_u64_list(map: &SegmentMap, tag: &IfdTag, le: bool) -> Result<Vec<u64>, RasterError> {
    match tag.tag_type {
        3 if tag.count == 1 => Ok(vec![tag.value_offset]),
        4 if tag.count == 1 => Ok(vec![tag.value_offset]),
        4 => {
            let bytes = map
                .read(tag.value_offset, (tag.count * 4) as usize)
                .ok_or_else(|| RasterError::Cog("cannot read tag values".into()))?;
            let mut out = Vec::with_capacity(tag.count as usize);
            for i in 0..tag.count as usize {
                out.push(read_u32(&bytes, (i * 4) as u64, le)? as u64);
            }
            Ok(out)
        }
        _ => Ok(vec![tag.value_offset]),
    }
}

fn read_f64_list(map: &SegmentMap, tag: &IfdTag, le: bool) -> Result<Vec<f64>, RasterError> {
    let bytes = map
        .read(tag.value_offset, (tag.count * 8) as usize)
        .ok_or_else(|| RasterError::Cog("cannot read f64 tag".into()))?;
    let mut out = Vec::with_capacity(tag.count as usize);
    for i in 0..tag.count as usize {
        let start = i * 8;
        let chunk: [u8; 8] = bytes[start..start + 8].try_into().unwrap();
        out.push(if le {
            f64::from_le_bytes(chunk)
        } else {
            f64::from_be_bytes(chunk)
        });
    }
    Ok(out)
}

fn read_rational_list(map: &SegmentMap, tag: &IfdTag, le: bool) -> Result<Vec<f64>, RasterError> {
    let bytes = map
        .read(tag.value_offset, (tag.count * 8) as usize)
        .ok_or_else(|| RasterError::Cog("cannot read rational tag".into()))?;
    let mut out = Vec::with_capacity(tag.count as usize);
    for i in 0..tag.count as usize {
        let start = i * 8;
        let num = if le {
            u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap())
        } else {
            u32::from_be_bytes(bytes[start..start + 4].try_into().unwrap())
        };
        let den = if le {
            u32::from_le_bytes(bytes[start + 4..start + 8].try_into().unwrap())
        } else {
            u32::from_be_bytes(bytes[start + 4..start + 8].try_into().unwrap())
        };
        out.push(if den == 0 {
            0.0
        } else {
            num as f64 / den as f64
        });
    }
    Ok(out)
}

async fn get_range(
    store: &dyn ObjectStore,
    path: &Path,
    range: Range<u64>,
) -> Result<Bytes, RasterError> {
    let start = usize::try_from(range.start)
        .map_err(|_| RasterError::Cog("range start overflow".into()))?;
    let end = usize::try_from(range.end)
        .map_err(|_| RasterError::Cog("range end overflow".into()))?;
    let opts = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };
    let bytes = store
        .get_opts(path, opts)
        .await
        .map_err(|e| RasterError::Cog(format!("range read: {e}")))?
        .bytes()
        .await
        .map_err(|e| RasterError::Cog(format!("range bytes: {e}")))?;
    Ok(bytes)
}

fn plan_ifd_ranges(header: &[u8]) -> Result<Vec<Range<u64>>, RasterError> {
    if header.len() < 8 {
        return Err(RasterError::Cog("TIFF header too short".into()));
    }
    let le = match &header[0..2] {
        b"II" => true,
        b"MM" => false,
        _ => return Err(RasterError::Cog("invalid TIFF byte order marker".into())),
    };
    if read_u16(header, 2, le)? != 42 {
        return Err(RasterError::Cog("invalid TIFF magic".into()));
    }

    let mut ranges = vec![0..INITIAL_HEADER_BYTES.min(header.len() as u64)];
    let mut next_ifd = read_u32(header, 4, le)? as u64;

    while next_ifd != 0 {
        if next_ifd + 2 > header.len() as u64 {
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

fn read_u16_at(map: &SegmentMap, offset: u64, le: bool) -> Result<u16, RasterError> {
    let buf = map
        .read(offset, 2)
        .ok_or_else(|| RasterError::Cog(format!("read u16 at {offset}")))?;
    read_u16(&buf, 0, le)
}

fn read_u32_at(map: &SegmentMap, offset: u64, le: bool) -> Result<u32, RasterError> {
    let buf = map
        .read(offset, 4)
        .ok_or_else(|| RasterError::Cog(format!("read u32 at {offset}")))?;
    read_u32(&buf, 0, le)
}

fn read_u16(buf: &[u8], offset: u64, le: bool) -> Result<u16, RasterError> {
    let start = offset as usize;
    if start + 2 > buf.len() {
        return Err(RasterError::Cog(format!("read u16 out of bounds at {offset}")));
    }
    Ok(if le {
        u16::from_le_bytes([buf[start], buf[start + 1]])
    } else {
        u16::from_be_bytes([buf[start], buf[start + 1]])
    })
}

fn read_u32(buf: &[u8], offset: u64, le: bool) -> Result<u32, RasterError> {
    let start = offset as usize;
    if start + 4 > buf.len() {
        return Err(RasterError::Cog(format!("read u32 out of bounds at {offset}")));
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
