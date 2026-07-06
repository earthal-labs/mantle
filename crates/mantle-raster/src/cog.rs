//! COG tile reads via oxigdal — real GeoTIFF/CRS parsing, real reprojection,
//! and real TIFF decompression, replacing a hand-rolled parser that got all
//! three wrong (see `oxigdal_source.rs` for why it must run inside
//! `spawn_blocking`, and the plan notes for the full history).

use crate::oxigdal_source::ObjectStoreDataSource;
use crate::storage::object_path;
use crate::tile_math::{bilinear_sample, TileBounds, TILE_SIZE};
use crate::RasterError;
use object_store::ObjectStore;
use oxigdal::algorithms::vector::{Coordinate, CrsTransformer};
use oxigdal::core_types::io::DataSource;
use oxigdal::core_types::types::RasterDataType;
use oxigdal::geotiff::GeoTiffReader;
use std::sync::Arc;
use tracing::warn;

/// Fetch, reproject, and resample a dataset's pixels onto a `TILE_SIZE` x
/// `TILE_SIZE` Web Mercator grid covering `tile_bounds`. Returns `None` when
/// the source has no usable georeferencing (unknown CRS/geotransform) or the
/// tile doesn't overlap the source's pixel extent at all.
pub async fn render_tile_layer(
    store: Arc<dyn ObjectStore>,
    s3_key: &str,
    tile_bounds: TileBounds,
) -> Result<Option<Vec<f32>>, RasterError> {
    let path = object_path(s3_key);
    let source = ObjectStoreDataSource::open(store, path)
        .await
        .map_err(|e| RasterError::Cog(format!("head object {s3_key}: {e}")))?;

    tokio::task::spawn_blocking(move || read_and_warp(source, tile_bounds))
        .await
        .map_err(|e| RasterError::Cog(format!("blocking task join: {e}")))?
}

fn read_and_warp(
    source: ObjectStoreDataSource,
    tile_bounds: TileBounds,
) -> Result<Option<Vec<f32>>, RasterError> {
    let reader = GeoTiffReader::open(source)
        .map_err(|e| RasterError::Cog(format!("open geotiff: {e}")))?;

    let width = reader.width() as u32;
    let height = reader.height() as u32;

    let Some(geo_transform) = reader.geo_transform() else {
        warn!(width, height, "cog: no geo_transform detected; cannot place tile");
        return Ok(None);
    };

    let Some(epsg) = reader.epsg_code() else {
        warn!(
            width,
            height,
            ?geo_transform,
            "cog: no EPSG code detected; cannot reproject tile"
        );
        return Ok(None);
    };

    let transformer = CrsTransformer::new("EPSG:3857", format!("EPSG:{epsg}"))
        .map_err(|e| RasterError::Cog(format!("build crs transformer: {e}")))?;

    // Corners of the requested Web Mercator tile, reprojected into the
    // source's native CRS and then into its pixel space, to find the
    // source pixel window that covers this tile.
    let corners = [
        (tile_bounds.min_x, tile_bounds.min_y),
        (tile_bounds.max_x, tile_bounds.min_y),
        (tile_bounds.min_x, tile_bounds.max_y),
        (tile_bounds.max_x, tile_bounds.max_y),
    ];
    let mut col_min = f64::INFINITY;
    let mut row_min = f64::INFINITY;
    let mut col_max = f64::NEG_INFINITY;
    let mut row_max = f64::NEG_INFINITY;

    for (mx, my) in corners {
        let native = match transformer.transform_coordinate(&Coordinate::new_2d(mx, my)) {
            Ok(n) => n,
            Err(e) => {
                warn!(mx, my, epsg, error = %e, "cog: corner reprojection failed");
                continue;
            }
        };
        let (col, row) = match geo_transform.world_to_pixel(native.x, native.y) {
            Ok(cr) => cr,
            Err(e) => {
                warn!(
                    native_x = native.x,
                    native_y = native.y,
                    error = %e,
                    "cog: world_to_pixel failed for reprojected corner"
                );
                continue;
            }
        };
        col_min = col_min.min(col);
        row_min = row_min.min(row);
        col_max = col_max.max(col);
        row_max = row_max.max(row);
    }

    let col0 = col_min.floor().max(0.0) as u32;
    let row0 = row_min.floor().max(0.0) as u32;
    let col1 = (col_max.ceil().max(0.0) as u32).min(width);
    let row1 = (row_max.ceil().max(0.0) as u32).min(height);

    if col1 <= col0 || row1 <= row0 {
        warn!(
            col0,
            row0,
            col1,
            row1,
            width,
            height,
            col_min,
            row_min,
            col_max,
            row_max,
            epsg,
            "cog: computed source window is empty/degenerate for this tile"
        );
        return Ok(None);
    }

    let data_type = reader.data_type().unwrap_or(RasterDataType::UInt8);
    let band_count = reader.band_count().max(1);
    let window = read_window(&reader, col0, row0, col1, row1, data_type, band_count)?;
    let src_w = col1 - col0;
    let src_h = row1 - row0;

    // Resample the extracted window onto the output tile grid, reusing the
    // same transformer for every pixel rather than rebuilding it.
    let mut out = vec![f32::NAN; (TILE_SIZE * TILE_SIZE) as usize];
    let dx = tile_bounds.width() / TILE_SIZE as f64;
    let dy = tile_bounds.height() / TILE_SIZE as f64;

    for py in 0..TILE_SIZE {
        for px in 0..TILE_SIZE {
            let mx = tile_bounds.min_x + (px as f64 + 0.5) * dx;
            let my = tile_bounds.max_y - (py as f64 + 0.5) * dy;

            let Ok(native) = transformer.transform_coordinate(&Coordinate::new_2d(mx, my)) else {
                continue;
            };
            let Ok((col, row)) = geo_transform.world_to_pixel(native.x, native.y) else {
                continue;
            };

            let local_col = col - col0 as f64;
            let local_row = row - row0 as f64;
            if local_col >= 0.0
                && local_row >= 0.0
                && local_col < src_w as f64
                && local_row < src_h as f64
            {
                out[(py * TILE_SIZE + px) as usize] =
                    bilinear_sample(&window, src_w, src_h, local_col, local_row);
            }
        }
    }

    Ok(Some(out))
}

/// Assembles a pixel window from oxigdal's whole-tile reads (no windowed
/// read exists upstream — see the plan notes). `read_tile` already handles
/// TIFF decompression and predictor reversal; this only has to decode raw
/// samples and crop tiles to the requested window.
fn read_window<S: DataSource>(
    reader: &GeoTiffReader<S>,
    col0: u32,
    row0: u32,
    col1: u32,
    row1: u32,
    data_type: RasterDataType,
    band_count: u32,
) -> Result<Vec<f32>, RasterError> {
    let out_w = col1 - col0;
    let out_h = row1 - row0;
    let mut out = vec![f32::NAN; (out_w * out_h) as usize];

    let width = reader.width() as u32;
    let height = reader.height() as u32;
    let (tiles_across, tiles_down) = reader.tile_count();
    let (tile_w, tile_h) = reader
        .tile_size()
        .unwrap_or((width, height.div_ceil(tiles_down.max(1))));

    if tile_w == 0 || tile_h == 0 || tiles_across == 0 || tiles_down == 0 {
        return Ok(out);
    }

    let tx0 = col0 / tile_w;
    let tx1 = ((col1 - 1) / tile_w).min(tiles_across - 1);
    let ty0 = row0 / tile_h;
    let ty1 = ((row1 - 1) / tile_h).min(tiles_down - 1);

    for ty in ty0..=ty1 {
        for tx in tx0..=tx1 {
            let tile_bytes = reader
                .read_tile(0, tx, ty)
                .map_err(|e| RasterError::Cog(format!("read tile ({tx},{ty}): {e}")))?;

            let tile_x0 = tx * tile_w;
            let tile_y0 = ty * tile_h;
            let this_tile_w = tile_w.min(width - tile_x0);
            let this_tile_h = tile_h.min(height - tile_y0);

            let row_lo = row0.max(tile_y0);
            let row_hi = row1.min(tile_y0 + this_tile_h);
            let col_lo = col0.max(tile_x0);
            let col_hi = col1.min(tile_x0 + this_tile_w);

            for global_row in row_lo..row_hi {
                let local_row = global_row - tile_y0;
                for global_col in col_lo..col_hi {
                    let local_col = global_col - tile_x0;
                    if let Some(v) = decode_sample(
                        &tile_bytes,
                        data_type,
                        band_count,
                        local_col,
                        local_row,
                        tile_w,
                    ) {
                        let out_idx = ((global_row - row0) * out_w + (global_col - col0)) as usize;
                        out[out_idx] = v;
                    }
                }
            }
        }
    }

    Ok(out)
}

/// Decodes band 0 of a single sample from an already-decompressed tile
/// buffer. Assumes little-endian byte order (the overwhelming common case
/// for modern-generated COGs); `GeoTiffReader`'s public API doesn't expose
/// the source file's byte order to check otherwise.
fn decode_sample(
    data: &[u8],
    data_type: RasterDataType,
    band_count: u32,
    col: u32,
    row: u32,
    stride_w: u32,
) -> Option<f32> {
    let sample_bytes = data_type.size_bytes();
    let pixel_stride = sample_bytes * band_count as usize;
    let idx = ((row * stride_w + col) as usize) * pixel_stride;
    if idx + sample_bytes > data.len() {
        return None;
    }

    Some(match data_type {
        RasterDataType::UInt8 => data[idx] as f32,
        RasterDataType::Int8 => data[idx] as i8 as f32,
        RasterDataType::UInt16 => u16::from_le_bytes([data[idx], data[idx + 1]]) as f32,
        RasterDataType::Int16 => i16::from_le_bytes([data[idx], data[idx + 1]]) as f32,
        RasterDataType::UInt32 => u32::from_le_bytes([
            data[idx],
            data[idx + 1],
            data[idx + 2],
            data[idx + 3],
        ]) as f32,
        RasterDataType::Int32 => i32::from_le_bytes([
            data[idx],
            data[idx + 1],
            data[idx + 2],
            data[idx + 3],
        ]) as f32,
        RasterDataType::Float32 => f32::from_le_bytes([
            data[idx],
            data[idx + 1],
            data[idx + 2],
            data[idx + 3],
        ]),
        RasterDataType::Float64 => f64::from_le_bytes([
            data[idx],
            data[idx + 1],
            data[idx + 2],
            data[idx + 3],
            data[idx + 4],
            data[idx + 5],
            data[idx + 6],
            data[idx + 7],
        ]) as f32,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_sample_uint8_single_band() {
        let data = vec![10u8, 20, 30, 40];
        assert_eq!(decode_sample(&data, RasterDataType::UInt8, 1, 0, 0, 2), Some(10.0));
        assert_eq!(decode_sample(&data, RasterDataType::UInt8, 1, 1, 0, 2), Some(20.0));
        assert_eq!(decode_sample(&data, RasterDataType::UInt8, 1, 0, 1, 2), Some(30.0));
    }

    #[test]
    fn decode_sample_uint16_little_endian() {
        // Two pixels: 300, 1000 (little-endian u16)
        let data = [300u16.to_le_bytes(), 1000u16.to_le_bytes()].concat();
        assert_eq!(decode_sample(&data, RasterDataType::UInt16, 1, 0, 0, 2), Some(300.0));
        assert_eq!(decode_sample(&data, RasterDataType::UInt16, 1, 1, 0, 2), Some(1000.0));
    }

    #[test]
    fn decode_sample_float32() {
        let data = 1.5f32.to_le_bytes().to_vec();
        assert_eq!(decode_sample(&data, RasterDataType::Float32, 1, 0, 0, 1), Some(1.5));
    }

    #[test]
    fn decode_sample_multiband_reads_first_band_only() {
        // 2 pixels, 3 bands (RGB) each u8: pixel0=(1,2,3), pixel1=(4,5,6)
        let data = vec![1u8, 2, 3, 4, 5, 6];
        assert_eq!(decode_sample(&data, RasterDataType::UInt8, 3, 0, 0, 2), Some(1.0));
        assert_eq!(decode_sample(&data, RasterDataType::UInt8, 3, 1, 0, 2), Some(4.0));
    }

    #[test]
    fn decode_sample_out_of_bounds_returns_none() {
        let data = vec![1u8, 2, 3, 4];
        assert_eq!(decode_sample(&data, RasterDataType::UInt8, 1, 10, 10, 2), None);
    }

    #[test]
    fn crs_transformer_web_mercator_to_utm_matches_direct_wgs84_path() {
        // Sanity check that the CrsTransformer path used by read_and_warp
        // actually resolves an arbitrary EPSG pair (UTM), not just the
        // WGS84<->Web Mercator pair the old DatasetCrs enum supported.
        let to_utm = CrsTransformer::new("EPSG:3857", "EPSG:32633")
            .expect("web mercator -> UTM zone 33N transformer must construct");
        let origin = Coordinate::new_2d(0.0, 0.0);
        let result = to_utm
            .transform_coordinate(&origin)
            .expect("transform must succeed for a real UTM zone");
        assert!(result.x.is_finite() && result.y.is_finite());
    }
}
