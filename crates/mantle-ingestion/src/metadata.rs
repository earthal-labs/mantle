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
            crs: Some("EPSG:4326".into()),
            geometry_wkt: "POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))".into(),
            band_count: 1,
            nodata: None,
        }
    }
}

/// Harvest metadata from uploaded GeoTIFF/COG bytes (header + IFD tags).
pub fn harvest_from_bytes(data: &[u8], content_type: &str) -> Result<SpatialMetadata, IngestionError> {
    if data.len() < 8 {
        return Ok(SpatialMetadata::default());
    }

    if is_geotiff(data) || content_type.contains("tiff") || content_type.contains("geotiff") {
        return Ok(parse_geotiff_metadata(data));
    }

    Ok(SpatialMetadata::default())
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
    let mut meta = SpatialMetadata::default();

    if let Some((width, height, origin_x, origin_y, pixel_w, pixel_h)) = read_geotiff_bounds(data) {
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
    }

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
    fn harvest_empty_bytes_returns_default() {
        let meta = harvest_from_bytes(&[], "application/octet-stream").expect("ok");
        assert_eq!(meta.band_count, 1);
    }
}
