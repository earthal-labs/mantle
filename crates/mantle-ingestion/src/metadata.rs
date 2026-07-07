//! Spatial metadata harvest from GeoTIFF/COG byte headers.

use crate::IngestionError;

/// Harvested spatial metadata for catalog footprint rows.
#[derive(Debug, Clone, PartialEq)]
pub struct SpatialMetadata {
    pub crs: Option<String>,
    pub geometry_wkt: String,
    pub band_count: u32,
    pub nodata: Option<f64>,
}

impl Default for SpatialMetadata {
    fn default() -> Self {
        Self {
            // We never parse the file's real GeoKeyDirectory tag, so we
            // genuinely don't know the CRS — claiming EPSG:4326 here was
            // actively wrong for projected (e.g. UTM) sources and overrode
            // the raster engine's own, correctly-detected CRS at render time
            // (see OxigdalRasterEngine::read_service_layer's fallback).
            crs: None,
            geometry_wkt: "POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))".into(),
            band_count: 1,
            nodata: None,
        }
    }
}

/// Harvest metadata from uploaded bytes (header + IFD tags).
///
/// Pathway A (multipart upload) only accepts Cloud-Optimized GeoTIFFs: the IFD
/// must be located within the leading [`crate::storage::HEADER_PEEK_BYTES`] we
/// keep from the stream, which is the defining property of a COG (it's what
/// makes metadata readable from a single small range request). A plain/legacy
/// GeoTIFF that writes its IFD at the end of the file will fail this check —
/// convert it first, e.g. `gdal_translate -of COG in.tif out.tif`.
pub fn harvest_from_bytes(data: &[u8], content_type: &str) -> Result<SpatialMetadata, IngestionError> {
    let looks_like_tiff =
        is_geotiff(data) || content_type.contains("tiff") || content_type.contains("geotiff");
    if !looks_like_tiff {
        return Err(IngestionError::NotCog(format!(
            "unsupported content type '{content_type}': only Cloud-Optimized GeoTIFF uploads are supported"
        )));
    }

    if !ifd_within_peek(data) {
        return Err(IngestionError::NotCog(format!(
            "primary IFD not found within the first {} bytes of the file — this is not a \
             valid Cloud-Optimized GeoTIFF (the IFD must be located near the start); \
             convert it with `gdal_translate -of COG in.tif out.tif`",
            data.len()
        )));
    }

    read_geotiff_bounds(data)
        .map(|(width, height, origin_x, origin_y, pixel_w, pixel_h)| {
            geotiff_bounds_to_metadata(data, width, height, origin_x, origin_y, pixel_w, pixel_h)
        })
        .ok_or_else(|| {
            IngestionError::NotCog(
                "could not read ModelTiepoint/ModelPixelScale georeferencing tags from file"
                    .into(),
            )
        })
}

/// Whether the TIFF header's IFD offset points within the already-peeked bytes.
/// This is the same property that makes a GeoTIFF "cloud-optimized" in the
/// first place: metadata must be reachable from the leading bytes alone.
fn ifd_within_peek(data: &[u8]) -> bool {
    if data.len() < 8 {
        return false;
    }
    let little_endian = data.starts_with(b"II");
    match read_u32(data, 4, little_endian) {
        Some(offset) => (offset as usize) < data.len(),
        None => false,
    }
}

/// Harvest metadata from a remote reference header sample.
pub fn harvest_from_header_sample(data: &[u8], format: crate::uri::ReferenceFormat) -> SpatialMetadata {
    match format {
        crate::uri::ReferenceFormat::Cog if is_geotiff(data) => parse_geotiff_metadata(data),
        crate::uri::ReferenceFormat::NetCdf | crate::uri::ReferenceFormat::Hdf5 => SpatialMetadata {
            crs: Some("EPSG:4326".into()),
            geometry_wkt: "POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))".into(),
            band_count: 0,
            nodata: None,
        },
        _ => SpatialMetadata::default(),
    }
}

fn is_geotiff(data: &[u8]) -> bool {
    data.len() >= 4 && (data.starts_with(b"II\x2A\x00") || data.starts_with(b"MM\x00\x2A"))
}

fn parse_geotiff_metadata(data: &[u8]) -> SpatialMetadata {
    match read_geotiff_bounds(data) {
        Some((width, height, origin_x, origin_y, pixel_w, pixel_h)) => {
            geotiff_bounds_to_metadata(data, width, height, origin_x, origin_y, pixel_w, pixel_h)
        }
        None => {
            let mut meta = SpatialMetadata::default();
            if let Some(bands) = read_sample_count(data) {
                meta.band_count = bands;
            }
            meta
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn geotiff_bounds_to_metadata(
    data: &[u8],
    width: u32,
    height: u32,
    origin_x: f64,
    origin_y: f64,
    pixel_w: f64,
    pixel_h: f64,
) -> SpatialMetadata {
    let mut meta = SpatialMetadata::default();

    let min_x = origin_x;
    let max_x = origin_x + pixel_w * width as f64;
    let min_y = origin_y + pixel_h * height as f64;
    let max_y = origin_y;
    let (west, east) = if min_x <= max_x {
        (min_x, max_x)
    } else {
        (max_x, min_x)
    };
    let (south, north) = if min_y <= max_y {
        (min_y, max_y)
    } else {
        (max_y, min_y)
    };
    meta.geometry_wkt = format!(
        "POLYGON(({west} {south}, {west} {north}, {east} {north}, {east} {south}, {west} {south}))"
    );

    if let Some(bands) = read_sample_count(data) {
        meta.band_count = bands;
    }

    meta
}

/// Best-effort GeoTIFF bounds from ModelTiepoint + ModelPixelScale tags.
fn read_geotiff_bounds(data: &[u8]) -> Option<(u32, u32, f64, f64, f64, f64)> {
    let little_endian = data.starts_with(b"II");
    let ifd_offset = read_u32(data, 4, little_endian)? as usize;
    let (width, height, tags) = parse_ifd(data, ifd_offset, little_endian)?;

    let tiepoints = tags.get(&33922)?.clone();
    let scales = tags.get(&33550)?.clone();
    if tiepoints.len() < 6 || scales.len() < 3 {
        return None;
    }

    let origin_x = f64_from_bytes(&tiepoints[3..11], little_endian);
    let origin_y = f64_from_bytes(&tiepoints[11..19], little_endian);
    let pixel_w = f64_from_bytes(&scales[0..8], little_endian);
    let pixel_h = f64_from_bytes(&scales[8..16], little_endian);

    Some((width, height, origin_x, origin_y, pixel_w, pixel_h))
}

fn read_sample_count(data: &[u8]) -> Option<u32> {
    let little_endian = data.starts_with(b"II");
    let ifd_offset = read_u32(data, 4, little_endian)? as usize;
    let (_, _, tags) = parse_ifd(data, ifd_offset, little_endian)?;
    tags.get(&277).map(|v| u16_from_bytes(v, little_endian) as u32)
}

fn parse_ifd(
    data: &[u8],
    offset: usize,
    little_endian: bool,
) -> Option<(u32, u32, std::collections::HashMap<u16, Vec<u8>>)> {
    if offset + 2 > data.len() {
        return None;
    }
    let count = read_u16(data, offset, little_endian)? as usize;
    let mut tags = std::collections::HashMap::new();
    let mut width = 0u32;
    let mut height = 0u32;
    let mut entry_offset = offset + 2;

    for _ in 0..count {
        if entry_offset + 12 > data.len() {
            break;
        }
        let tag = read_u16(data, entry_offset, little_endian)?;
        let field_type = read_u16(data, entry_offset + 2, little_endian)?;
        let value_count = read_u32(data, entry_offset + 4, little_endian)? as usize;
        let value_offset = entry_offset + 8;

        let value_bytes = read_tag_value(data, field_type, value_count, value_offset, little_endian)?;
        match tag {
            256 => width = u32_from_tag(&value_bytes, field_type, little_endian),
            257 => height = u32_from_tag(&value_bytes, field_type, little_endian),
            _ => {
                tags.insert(tag, value_bytes);
            }
        }
        entry_offset += 12;
    }

    Some((width, height, tags))
}

fn read_tag_value(
    data: &[u8],
    field_type: u16,
    count: usize,
    value_offset: usize,
    little_endian: bool,
) -> Option<Vec<u8>> {
    let type_size = match field_type {
        1 | 2 | 6 | 7 => 1,
        3 | 8 => 2,
        4 | 9 | 11 => 4,
        5 | 10 | 12 => 8,
        _ => 1,
    };
    let byte_len = type_size * count;
    if byte_len <= 4 {
        Some(data.get(value_offset..value_offset + byte_len)?.to_vec())
    } else {
        let ptr = read_u32(data, value_offset, little_endian)? as usize;
        Some(data.get(ptr..ptr + byte_len)?.to_vec())
    }
}

fn u32_from_tag(bytes: &[u8], field_type: u16, little_endian: bool) -> u32 {
    match field_type {
        3 => u16_from_bytes(bytes, little_endian) as u32,
        _ => read_u32(bytes, 0, little_endian).unwrap_or(0),
    }
}

fn read_u16(data: &[u8], offset: usize, little_endian: bool) -> Option<u16> {
    let bytes = data.get(offset..offset + 2)?;
    Some(if little_endian {
        u16::from_le_bytes([bytes[0], bytes[1]])
    } else {
        u16::from_be_bytes([bytes[0], bytes[1]])
    })
}

fn read_u32(data: &[u8], offset: usize, little_endian: bool) -> Option<u32> {
    let bytes = data.get(offset..offset + 4)?;
    Some(if little_endian {
        u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    } else {
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    })
}

fn u16_from_bytes(bytes: &[u8], little_endian: bool) -> u16 {
    read_u16(bytes, 0, little_endian).unwrap_or(1)
}

fn f64_from_bytes(bytes: &[u8], little_endian: bool) -> f64 {
    let slice = bytes.get(0..8).unwrap_or(&[0; 8]);
    let arr: [u8; 8] = slice.try_into().unwrap_or([0; 8]);
    if little_endian {
        f64::from_le_bytes(arr)
    } else {
        f64::from_be_bytes(arr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_metadata_has_valid_wkt() {
        let meta = SpatialMetadata::default();
        assert!(meta.geometry_wkt.starts_with("POLYGON"));
        assert!(!meta.geometry_wkt.trim().is_empty());
    }

    #[test]
    fn harvest_rejects_non_tiff_content() {
        let err = harvest_from_bytes(&[], "application/octet-stream").expect_err("not a COG");
        assert!(matches!(err, IngestionError::NotCog(_)));
    }

    #[test]
    fn harvest_rejects_ifd_beyond_peek_window() {
        // Valid little-endian TIFF header whose IFD offset (0xFFFF_FF00) is
        // nowhere near the tiny buffer we actually have — simulates a
        // plain/legacy GeoTIFF with the IFD written at the end of the file.
        let mut data = vec![0u8; 16];
        data[0] = b'I';
        data[1] = b'I';
        data[2] = 0x2A;
        data[3] = 0x00;
        data[4..8].copy_from_slice(&0xFFFF_FF00u32.to_le_bytes());

        let err = harvest_from_bytes(&data, "image/tiff").expect_err("IFD not in peek window");
        assert!(matches!(err, IngestionError::NotCog(_)));
    }
}
