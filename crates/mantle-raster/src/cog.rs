//! COG tile reads via oxigdal — real GeoTIFF/CRS parsing, real reprojection,
//! and real TIFF decompression, replacing a hand-rolled parser that got all
//! three wrong (see `oxigdal_source.rs` for why it must run inside
//! `spawn_blocking`, and the plan notes for the full history).
//!
//! Uses `CogReader` (not the higher-level `GeoTiffReader` wrapper) because
//! JPEG-compressed COGs commonly store their quantization/Huffman tables
//! once in the shared `JPEGTables` tag (347) rather than in every tile —
//! oxigdal's generic `read_tile()` doesn't know about that tag, so we read
//! raw tile bytes ourselves and merge the tables in before decoding.

use crate::byte_cache::ByteRangeCache;
use crate::oxigdal_source::ObjectStoreDataSource;
use crate::storage::object_path;
use crate::tile_math::{bilinear_sample, TileBounds, TILE_SIZE};
use crate::{CogDebugInfo, RasterError};
use object_store::ObjectStore;
use oxigdal::algorithms::vector::{Coordinate, CrsTransformer};
use oxigdal::core_types::io::{ByteRange, DataSource};
use oxigdal::core_types::types::RasterDataType;
use oxigdal::geotiff::tiff::{Compression, TiffTag};
use oxigdal::geotiff::{compression, CogReader, GeoKey, GeoKeyDirectory};
use rayon::prelude::*;
use std::sync::Arc;
use tracing::warn;

/// Reports what oxigdal actually detects for a service (CRS, geotransform,
/// dimensions, tiling) without rendering a tile — the direct answer to "why
/// is this tile blank" without log-grepping or guessing tile coordinates.
pub async fn debug_metadata(
    store: Arc<dyn ObjectStore>,
    cache: ByteRangeCache,
    s3_key: &str,
) -> Result<CogDebugInfo, RasterError> {
    let path = object_path(s3_key);
    let source = ObjectStoreDataSource::open(store, path, cache)
        .await
        .map_err(|e| RasterError::Cog(format!("head object {s3_key}: {e}")))?;

    tokio::task::spawn_blocking(move || {
        let reader = CogReader::open(source)
            .map_err(|e| RasterError::Cog(format!("open geotiff: {e}")))?;
        let info = reader.primary_info();

        let geo_transform = reader
            .geo_transform()
            .map_err(|e| RasterError::Cog(format!("read geo_transform: {e}")))?;
        let epsg_code = reader.epsg_code();
        let resolved_crs = reader.geo_keys().and_then(resolve_source_crs);

        let footprint_wgs84 = match (&geo_transform, &resolved_crs) {
            (Some(gt), Some(crs)) => {
                compute_wgs84_footprint(gt, reader.width(), reader.height(), crs)
            }
            _ => None,
        };

        Ok(CogDebugInfo {
            width: reader.width(),
            height: reader.height(),
            band_count: info.samples_per_pixel as u32,
            data_type: info.data_type().map(|d| format!("{d:?}")),
            tile_size: reader.tile_size(),
            epsg_code,
            resolved_crs,
            geo_transform,
            footprint_wgs84,
        })
    })
    .await
    .map_err(|e| RasterError::Cog(format!("blocking task join: {e}")))?
}

/// Resolves the CRS to use for reprojection from a file's raw GeoKeys,
/// handling the case `GeoKeyDirectory::epsg_code()` can't: a "user-defined"
/// (GeoTIFF code 32767) projected CRS. Some producers — notably USGS Landsat
/// Collection 2 ARD/CONUS products — encode their projection directly via
/// GeoTIFF `Proj*` GeoKeys instead of referencing a registered EPSG code.
/// `GeoKeyDirectory::epsg_code()` (oxigdal-geotiff's own convenience method)
/// silently falls back to the *geographic* datum key whenever
/// `ProjectedCsType` isn't a real registered code — including when the key
/// is simply absent, not just when it's explicitly the GeoTIFF
/// "user-defined" sentinel (32767). That fallback reports e.g. 4326 (the
/// datum, not the actual projected CRS), which produces an identity no-op
/// transform and garbage output. So this does **not** delegate to
/// `epsg_code()` — it inspects `ProjectedCsType`/`ProjCoordTrans` itself and
/// only trusts a plain EPSG reference when there's no raw projection
/// definition to prefer instead. Rather than special-case one known
/// dataset, the reconstruction covers the GeoTIFF `ProjCoordTrans` methods
/// common to satellite imagery products, so it works for any file using
/// this pattern, not just Landsat.
fn resolve_source_crs(geo_keys: &GeoKeyDirectory) -> Option<String> {
    let projected_cs = geo_keys.get_short(GeoKey::ProjectedCsType);

    // A real, registered projected CRS reference — the common, fast case.
    if let Some(code) = projected_cs.filter(|&v| v != 0 && v != 32767) {
        return Some(format!("EPSG:{code}"));
    }

    // Either explicitly "user-defined" (32767) or `ProjectedCsType` is
    // simply absent but `ProjCoordTrans` is present anyway (some encoders
    // write raw Proj* parameters without ever setting `ProjectedCsType`) —
    // in both cases, reconstruct a PROJ4 definition from the raw parameters
    // rather than falling through to the geographic datum code, which is
    // not the same CRS as the actual projected raster data.
    if geo_keys.get_short(GeoKey::ProjCoordTrans).is_some() {
        if let Some(crs) = reconstruct_user_defined_crs(geo_keys) {
            return Some(crs);
        }
    }

    // Genuinely geographic (lat/lon) data — no projection at all.
    geo_keys
        .get_short(GeoKey::GeographicType)
        .filter(|&v| v != 32767)
        .map(|code| format!("EPSG:{code}"))
}

/// Builds a PROJ4 string directly from a file's raw `Proj*` GeoKeys. Only
/// called once `resolve_source_crs` has confirmed a `ProjCoordTrans` key is
/// present (i.e. the file defines its own projection method + parameters).
fn reconstruct_user_defined_crs(geo_keys: &GeoKeyDirectory) -> Option<String> {
    let coord_trans = geo_keys.get_short(GeoKey::ProjCoordTrans)?;

    let lat1 = geo_keys.get_double(GeoKey::ProjStdParallel1);
    let lat2 = geo_keys.get_double(GeoKey::ProjStdParallel2);
    let nat_origin_lat = geo_keys.get_double(GeoKey::ProjNatOriginLat);
    let nat_origin_lon = geo_keys.get_double(GeoKey::ProjNatOriginLong);
    let false_origin_lat = geo_keys.get_double(GeoKey::ProjFalseOriginLat);
    let false_origin_lon = geo_keys.get_double(GeoKey::ProjFalseOriginLong);
    let center_lat = geo_keys.get_double(GeoKey::ProjCenterLat);
    let center_lon = geo_keys.get_double(GeoKey::ProjCenterLong);
    let false_easting = geo_keys
        .get_double(GeoKey::ProjFalseEasting)
        .or_else(|| geo_keys.get_double(GeoKey::ProjFalseOriginEasting))
        .unwrap_or(0.0);
    let false_northing = geo_keys
        .get_double(GeoKey::ProjFalseNorthing)
        .or_else(|| geo_keys.get_double(GeoKey::ProjFalseOriginNorthing))
        .unwrap_or(0.0);
    let scale = geo_keys.get_double(GeoKey::ProjScaleAtNatOrigin).unwrap_or(1.0);

    // GeoTIFF `CT_*` projection method codes (a subset of the spec covering
    // the methods satellite imagery products actually use in practice).
    let proj = match coord_trans {
        11 => {
            // CT_AlbersEqualArea — e.g. Landsat Collection 2 ARD/CONUS (EPSG:5070-like).
            let lat0 = false_origin_lat.or(nat_origin_lat).unwrap_or(0.0);
            let lon0 = false_origin_lon.or(nat_origin_lon).unwrap_or(0.0);
            format!(
                "+proj=aea +lat_1={} +lat_2={} +lat_0={lat0} +lon_0={lon0} +x_0={false_easting} +y_0={false_northing}",
                lat1?, lat2?
            )
        }
        8 => {
            // CT_LambertConfConic_2SP
            let lat0 = false_origin_lat.or(nat_origin_lat).unwrap_or(0.0);
            let lon0 = false_origin_lon.or(nat_origin_lon).unwrap_or(0.0);
            format!(
                "+proj=lcc +lat_1={} +lat_2={} +lat_0={lat0} +lon_0={lon0} +x_0={false_easting} +y_0={false_northing}",
                lat1?, lat2?
            )
        }
        9 => {
            // CT_LambertConfConic_Helmert (1SP)
            let lat0 = nat_origin_lat?;
            let lon0 = nat_origin_lon.unwrap_or(0.0);
            format!(
                "+proj=lcc +lat_1={lat0} +lat_0={lat0} +lon_0={lon0} +k_0={scale} +x_0={false_easting} +y_0={false_northing}"
            )
        }
        1 => {
            // CT_TransverseMercator
            let lat0 = nat_origin_lat.unwrap_or(0.0);
            let lon0 = nat_origin_lon?;
            format!(
                "+proj=tmerc +lat_0={lat0} +lon_0={lon0} +k={scale} +x_0={false_easting} +y_0={false_northing}"
            )
        }
        7 => {
            // CT_Mercator
            let lat0 = nat_origin_lat.unwrap_or(0.0);
            let lon0 = nat_origin_lon?;
            format!("+proj=merc +lat_ts={lat0} +lon_0={lon0} +x_0={false_easting} +y_0={false_northing}")
        }
        10 => {
            // CT_LambertAzimEqualArea
            let lat0 = center_lat.or(nat_origin_lat)?;
            let lon0 = center_lon.or(nat_origin_lon).unwrap_or(0.0);
            format!("+proj=laea +lat_0={lat0} +lon_0={lon0} +x_0={false_easting} +y_0={false_northing}")
        }
        13 => {
            // CT_EquidistantConic
            let lat0 = false_origin_lat.or(nat_origin_lat).unwrap_or(0.0);
            let lon0 = false_origin_lon.or(nat_origin_lon).unwrap_or(0.0);
            let l1 = lat1?;
            let l2 = lat2.unwrap_or(l1);
            format!(
                "+proj=eqdc +lat_1={l1} +lat_2={l2} +lat_0={lat0} +lon_0={lon0} +x_0={false_easting} +y_0={false_northing}"
            )
        }
        15 => {
            // CT_PolarStereographic
            let lat0 = nat_origin_lat.unwrap_or(90.0);
            let lon0 = nat_origin_lon.unwrap_or(0.0);
            format!(
                "+proj=stere +lat_0={lat0} +lat_ts={lat0} +lon_0={lon0} +k={scale} +x_0={false_easting} +y_0={false_northing}"
            )
        }
        24 => {
            // CT_Sinusoidal
            let lon0 = nat_origin_lon.or(center_lon).unwrap_or(0.0);
            format!("+proj=sinu +lon_0={lon0} +x_0={false_easting} +y_0={false_northing}")
        }
        other => {
            warn!(
                coord_trans = other,
                "cog: user-defined projected CRS uses an unhandled projection method; cannot reproject"
            );
            return None;
        }
    };

    // Ellipsoid: prefer the file's own semi-major axis / inverse flattening
    // when present (keeps the derived CRS exact for non-WGS84/GRS80 datums);
    // otherwise WGS84 is close enough for tile placement (GRS80 — the
    // ellipsoid behind NAD83, by far the most common case here — differs
    // from WGS84 by a fraction of a millimeter in semi-major axis).
    let ellipsoid = match (
        geo_keys.get_double(GeoKey::GeogSemiMajorAxis),
        geo_keys.get_double(GeoKey::GeogInvFlattening),
    ) {
        (Some(a), Some(rf)) => format!("+a={a} +rf={rf}"),
        _ => "+ellps=WGS84".to_string(),
    };

    Some(format!("{proj} {ellipsoid} +units=m +no_defs"))
}

/// Reprojects the service's four pixel-extent corners (upper-left,
/// upper-right, lower-right, lower-left, then upper-left again to close the
/// ring) from its native CRS to EPSG:4326, so callers get a ready-to-draw
/// footprint without needing their own CRS transform. Returns `None` if the
/// native CRS can't be resolved or any corner fails to reproject.
fn compute_wgs84_footprint(
    geo_transform: &oxigdal::core_types::types::GeoTransform,
    width: u64,
    height: u64,
    source_crs: &str,
) -> Option<Vec<[f64; 2]>> {
    let transformer = CrsTransformer::new(source_crs, "EPSG:4326").ok()?;
    let corners = [
        (0.0, 0.0),
        (width as f64, 0.0),
        (width as f64, height as f64),
        (0.0, height as f64),
    ];

    let mut ring = Vec::with_capacity(5);
    for (px, py) in corners {
        let (wx, wy) = geo_transform.pixel_to_world(px, py);
        let lonlat = transformer
            .transform_coordinate(&Coordinate::new_2d(wx, wy))
            .ok()?;
        // Sanity check: a genuine WGS84 corner must fall within valid lon/lat
        // bounds. Some sources (notably USGS Albers-projected products like
        // Landsat Collection 2 ARD/CU tiles) encode their projection as
        // GeoTIFF "user-defined" rather than a plain EPSG reference; oxigdal's
        // `epsg_code()` falls back to the geographic datum key in that case
        // (e.g. 4326), which isn't the actual projected CRS. Transforming
        // "EPSG:4326 -> EPSG:4326" is an identity pass-through, so the raw
        // projected-meter coordinates come out looking like nonsense degrees
        // (e.g. lon=-1215570). Reject rather than hand back a garbage
        // footprint that draws as a huge, wrong rectangle on the map.
        if !(-180.0..=180.0).contains(&lonlat.x) || !(-90.0..=90.0).contains(&lonlat.y) {
            return None;
        }
        ring.push([lonlat.x, lonlat.y]);
    }
    ring.push(ring[0]);
    Some(ring)
}

/// Fetch, reproject, and resample a service's pixels onto a `TILE_SIZE` x
/// `TILE_SIZE` Web Mercator grid covering `tile_bounds`. Returns `None` when
/// the source has no usable georeferencing (unknown CRS/geotransform) or the
/// tile doesn't overlap the source's pixel extent at all.
pub async fn render_tile_layer(
    store: Arc<dyn ObjectStore>,
    cache: ByteRangeCache,
    s3_key: &str,
    tile_bounds: TileBounds,
) -> Result<Option<Vec<f32>>, RasterError> {
    let path = object_path(s3_key);
    let source = ObjectStoreDataSource::open(store, path, cache)
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
    // Keep an independent handle to fetch the JPEGTables tag value (if any)
    // ourselves — `CogReader::open` takes ownership of `source` and doesn't
    // expose its internal DataSource.
    let tables_source = source.clone();

    let reader = CogReader::open(source)
        .map_err(|e| RasterError::Cog(format!("open geotiff: {e}")))?;

    let width = reader.width() as u32;
    let height = reader.height() as u32;

    let geo_transform = reader
        .geo_transform()
        .map_err(|e| RasterError::Cog(format!("read geo_transform: {e}")))?;
    let Some(geo_transform) = geo_transform else {
        warn!(width, height, "cog: no geo_transform detected; cannot place tile");
        return Ok(None);
    };

    let Some(source_crs) = reader.geo_keys().and_then(resolve_source_crs) else {
        warn!(
            width,
            height,
            ?geo_transform,
            "cog: no usable CRS detected; cannot reproject tile"
        );
        return Ok(None);
    };

    let transformer = CrsTransformer::new("EPSG:3857", &source_crs)
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
                warn!(mx, my, source_crs = %source_crs, error = %e, "cog: corner reprojection failed");
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
            source_crs = %source_crs,
            "cog: computed source window is empty/degenerate for this tile"
        );
        return Ok(None);
    }

    let info = reader.primary_info();
    let data_type = info.data_type().unwrap_or(RasterDataType::UInt8);
    let band_count = (info.samples_per_pixel as u32).max(1);
    let window = read_window(&reader, &tables_source, col0, row0, col1, row1, data_type, band_count)?;
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
/// read exists upstream — see the plan notes). Decoding is done ourselves
/// (not via `CogReader::read_tile`) so JPEG-with-shared-tables data can be
/// merged and decoded correctly; see `read_tile_decoded`.
#[allow(clippy::too_many_arguments)]
fn read_window(
    reader: &CogReader<ObjectStoreDataSource>,
    tables_source: &ObjectStoreDataSource,
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

    // Fetch+decode covering source tiles concurrently (via rayon) rather
    // than one at a time: each one is a blocking S3/MinIO round trip (on a
    // cache miss), and a tile request commonly spans 2-4 source tiles, so
    // serial reads pay that latency 2-4x over instead of ~once.
    let tile_coords: Vec<(u32, u32)> = (ty0..=ty1)
        .flat_map(|ty| (tx0..=tx1).map(move |tx| (tx, ty)))
        .collect();

    let decoded_tiles: Vec<(u32, u32, Vec<u8>)> = tile_coords
        .into_par_iter()
        .map(|(tx, ty)| {
            let bytes = read_tile_decoded(reader, tables_source, tx, ty)?;
            Ok::<_, RasterError>((tx, ty, bytes))
        })
        .collect::<Result<Vec<_>, RasterError>>()?;

    for (tx, ty, tile_bytes) in decoded_tiles {
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

    Ok(out)
}

/// Reads and decompresses one tile (level 0), mirroring `CogReader::read_tile`
/// exactly except for the JPEG path: if the IFD has a `JPEGTables` tag (347),
/// its bytes are fetched and merged with the tile's raw JPEG stream before
/// decoding — the generic `read_tile` doesn't do this, which is what caused
/// "use of unset quantization table" for JPEG-compressed COGs that share
/// their tables (a common GDAL default).
fn read_tile_decoded(
    reader: &CogReader<ObjectStoreDataSource>,
    tables_source: &ObjectStoreDataSource,
    tile_x: u32,
    tile_y: u32,
) -> Result<Vec<u8>, RasterError> {
    let compressed = reader
        .read_tile_raw(0, tile_x, tile_y)
        .map_err(|e| RasterError::Cog(format!("read tile raw ({tile_x},{tile_y}): {e}")))?;

    let info = reader.primary_info();
    let is_tiled = info.tile_width.is_some() && info.tile_height.is_some();
    let (tile_width, tile_height) = if is_tiled {
        (
            info.tile_width.unwrap_or(info.width as u32),
            info.tile_height.unwrap_or(info.height as u32),
        )
    } else {
        let strip_height = info.rows_per_strip.unwrap_or(info.height as u32);
        let actual_height = if tile_y == reader.tile_count().1.saturating_sub(1) {
            let remaining = info.height as u32 - (tile_y * strip_height);
            remaining.min(strip_height)
        } else {
            strip_height
        };
        (info.width as u32, actual_height)
    };

    let bytes_per_sample = info.bits_per_sample.first().map_or(1, |&b| (b / 8) as usize);
    let samples_per_pixel = info.samples_per_pixel as usize;
    let expected_size =
        tile_width as usize * tile_height as usize * bytes_per_sample * samples_per_pixel;

    let mut decompressed = if info.compression == Compression::Jpeg {
        match reader.tiff().ifds[0].get_entry(TiffTag::JpegTables) {
            Some(entry) => {
                let tables = if let Some(inline) = &entry.inline_value {
                    inline[..entry.value_size() as usize].to_vec()
                } else {
                    tables_source
                        .read_range(ByteRange::from_offset_length(
                            entry.value_offset,
                            entry.value_size(),
                        ))
                        .map_err(|e| RasterError::Cog(format!("read JPEGTables: {e}")))?
                };
                compression::decompress_jpeg_with_tables(&tables, &compressed)
                    .map_err(|e| RasterError::Cog(format!("jpeg decompress with tables: {e}")))?
            }
            None => compression::decompress(&compressed, info.compression, expected_size)
                .map_err(|e| RasterError::Cog(format!("decompress: {e}")))?,
        }
    } else {
        compression::decompress(&compressed, info.compression, expected_size)
            .map_err(|e| RasterError::Cog(format!("decompress: {e}")))?
    };

    compression::apply_predictor_reverse(
        &mut decompressed,
        info.predictor,
        bytes_per_sample,
        samples_per_pixel,
        tile_width as usize,
    );

    Ok(decompressed)
}

/// Decodes band 0 of a single sample from an already-decompressed tile
/// buffer. Assumes little-endian byte order (the overwhelming common case
/// for modern-generated COGs); oxigdal's public API doesn't expose the
/// source file's byte order to check otherwise.
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

    #[test]
    fn compute_wgs84_footprint_returns_closed_ring_near_utm_zone() {
        // 100x100px, 10m resolution, origin near UTM zone 33N's central
        // meridian (easting 500000) at ~45N (northing 5,000,000).
        let gt = oxigdal::core_types::types::GeoTransform {
            origin_x: 500_000.0,
            pixel_width: 10.0,
            row_rotation: 0.0,
            origin_y: 5_000_000.0,
            col_rotation: 0.0,
            pixel_height: -10.0,
        };

        let ring = compute_wgs84_footprint(&gt, 100, 100, "EPSG:32633")
            .expect("footprint must reproject for a real UTM zone");

        assert_eq!(ring.len(), 5, "ring must close (first corner repeated last)");
        assert_eq!(ring[0], ring[4], "ring must be closed");
        for [lon, lat] in &ring {
            assert!(lon.is_finite() && lat.is_finite());
            assert!((13.0..17.0).contains(lon), "lon {lon} not near zone 33 central meridian");
            assert!((44.0..46.0).contains(lat), "lat {lat} not near expected northing");
        }
    }

    #[test]
    fn compute_wgs84_footprint_none_for_unresolvable_epsg() {
        let gt = oxigdal::core_types::types::GeoTransform {
            origin_x: 0.0,
            pixel_width: 1.0,
            row_rotation: 0.0,
            origin_y: 0.0,
            col_rotation: 0.0,
            pixel_height: -1.0,
        };
        assert!(compute_wgs84_footprint(&gt, 10, 10, "EPSG:0").is_none());
    }

    #[test]
    fn compute_wgs84_footprint_none_when_reported_epsg_is_actually_geographic_identity() {
        // Mirrors a real Landsat Collection 2 CONUS/ARD tile: geo_transform
        // is in Albers meters (pixel_width=30, origin in the millions), but
        // oxigdal's epsg_code() fell back to reporting 4326 (the underlying
        // geographic datum key) because the file's actual projection is
        // GeoTIFF "user-defined" rather than a plain EPSG reference.
        // "EPSG:4326 -> EPSG:4326" is an identity transform, so naively
        // trusting it would hand back the raw meter values as bogus degrees.
        let gt = oxigdal::core_types::types::GeoTransform {
            origin_x: -1_215_570.0,
            pixel_width: 30.0,
            row_rotation: 0.0,
            origin_y: 2_564_790.0,
            col_rotation: 0.0,
            pixel_height: -30.0,
        };
        assert!(compute_wgs84_footprint(&gt, 5000, 5000, "EPSG:4326").is_none());
    }

    /// Builds a minimal `GeoKeyDirectory` for a "user-defined" projected CRS
    /// using the raw Albers Equal Area parameters USGS Landsat Collection 2
    /// ARD/CONUS products actually encode (NAD83 / Conus Albers, ~EPSG:5070),
    /// mirroring what `oxigdal-geotiff` would parse from such a file's IFD.
    fn albers_user_defined_geo_keys() -> GeoKeyDirectory {
        use oxigdal::geotiff::geokeys::GeoKeyEntry;

        let double_params = vec![29.5, 45.5, 23.0, -96.0, 0.0, 0.0];
        GeoKeyDirectory {
            version: 1,
            key_revision_major: 1,
            key_revision_minor: 0,
            entries: vec![
                GeoKeyEntry {
                    key_id: GeoKey::ProjectedCsType as u16,
                    tiff_tag_location: 0,
                    count: 1,
                    value_offset: 32767, // user-defined
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjCoordTrans as u16,
                    tiff_tag_location: 0,
                    count: 1,
                    value_offset: 11, // CT_AlbersEqualArea
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjStdParallel1 as u16,
                    tiff_tag_location: TiffTag::GeoDoubleParams as u16,
                    count: 1,
                    value_offset: 0,
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjStdParallel2 as u16,
                    tiff_tag_location: TiffTag::GeoDoubleParams as u16,
                    count: 1,
                    value_offset: 1,
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjNatOriginLat as u16,
                    tiff_tag_location: TiffTag::GeoDoubleParams as u16,
                    count: 1,
                    value_offset: 2,
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjNatOriginLong as u16,
                    tiff_tag_location: TiffTag::GeoDoubleParams as u16,
                    count: 1,
                    value_offset: 3,
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjFalseEasting as u16,
                    tiff_tag_location: TiffTag::GeoDoubleParams as u16,
                    count: 1,
                    value_offset: 4,
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjFalseNorthing as u16,
                    tiff_tag_location: TiffTag::GeoDoubleParams as u16,
                    count: 1,
                    value_offset: 5,
                },
            ],
            double_params,
            ascii_params: String::new(),
        }
    }

    #[test]
    fn resolve_source_crs_builds_proj4_for_user_defined_albers() {
        let geo_keys = albers_user_defined_geo_keys();
        let crs = resolve_source_crs(&geo_keys).expect("must derive a PROJ4 string");
        assert!(crs.starts_with("+proj=aea"), "unexpected CRS: {crs}");
        assert!(crs.contains("+lat_1=29.5"));
        assert!(crs.contains("+lat_2=45.5"));
        assert!(crs.contains("+lon_0=-96"));
    }

    #[test]
    fn resolve_source_crs_recovers_correct_landsat_footprint() {
        // Same geo_transform as the "identity" rejection test above, but now
        // paired with the real user-defined GeoKeys instead of a bare EPSG
        // code — this must produce a real, plausible CONUS-area footprint
        // instead of `None`.
        let gt = oxigdal::core_types::types::GeoTransform {
            origin_x: -1_215_570.0,
            pixel_width: 30.0,
            row_rotation: 0.0,
            origin_y: 2_564_790.0,
            col_rotation: 0.0,
            pixel_height: -30.0,
        };
        let geo_keys = albers_user_defined_geo_keys();
        let crs = resolve_source_crs(&geo_keys).expect("must derive a PROJ4 string");
        let ring = compute_wgs84_footprint(&gt, 5000, 5000, &crs)
            .expect("footprint must reproject using the derived Albers CRS");
        for [lon, lat] in &ring {
            assert!(
                (-125.0..=-65.0).contains(lon),
                "lon {lon} not within CONUS bounds"
            );
            assert!(
                (24.0..=50.0).contains(lat),
                "lat {lat} not within CONUS bounds"
            );
        }
    }

    #[test]
    fn resolve_source_crs_none_for_unhandled_projection_method() {
        use oxigdal::geotiff::geokeys::GeoKeyEntry;

        let geo_keys = GeoKeyDirectory {
            version: 1,
            key_revision_major: 1,
            key_revision_minor: 0,
            entries: vec![
                GeoKeyEntry {
                    key_id: GeoKey::ProjectedCsType as u16,
                    tiff_tag_location: 0,
                    count: 1,
                    value_offset: 32767,
                },
                GeoKeyEntry {
                    key_id: GeoKey::ProjCoordTrans as u16,
                    tiff_tag_location: 0,
                    count: 1,
                    value_offset: 99, // not a method we handle
                },
            ],
            double_params: Vec::new(),
            ascii_params: String::new(),
        };
        assert!(resolve_source_crs(&geo_keys).is_none());
    }

    /// Reproduces the real-world failure this whole mechanism exists for:
    /// a file with no `ProjectedCsType` key at all (not even the explicit
    /// 32767 "user-defined" sentinel), only `GeographicType=4326` plus a
    /// full raw Albers `ProjCoordTrans` definition. `epsg_code()`'s own
    /// fallback logic would happily report 4326 here since `ProjectedCsType`
    /// is simply missing, not "user-defined" — `resolve_source_crs` must not
    /// delegate to it, or it silently wins over the real projection data.
    fn albers_geo_keys_with_geographic_type_but_no_projected_cs_type() -> GeoKeyDirectory {
        use oxigdal::geotiff::geokeys::GeoKeyEntry;

        let mut geo_keys = albers_user_defined_geo_keys();
        geo_keys.entries.retain(|e| e.key_id != GeoKey::ProjectedCsType as u16);
        geo_keys.entries.push(GeoKeyEntry {
            key_id: GeoKey::GeographicType as u16,
            tiff_tag_location: 0,
            count: 1,
            value_offset: 4326,
        });
        geo_keys
    }

    #[test]
    fn resolve_source_crs_prefers_raw_albers_params_over_geographic_type_fallback() {
        let geo_keys = albers_geo_keys_with_geographic_type_but_no_projected_cs_type();
        let crs = resolve_source_crs(&geo_keys).expect("must derive a PROJ4 string");
        assert!(
            crs.starts_with("+proj=aea"),
            "must reconstruct the raw Albers definition, not fall back to EPSG:4326: {crs}"
        );
    }

    #[test]
    fn resolve_source_crs_falls_back_to_geographic_type_when_no_proj_coord_trans() {
        use oxigdal::geotiff::geokeys::GeoKeyEntry;

        // A genuinely geographic (lat/lon) file: no ProjectedCsType, no
        // ProjCoordTrans, just a GeographicType datum reference. This must
        // still resolve — it's the one case where trusting GeographicType
        // is actually correct.
        let geo_keys = GeoKeyDirectory {
            version: 1,
            key_revision_major: 1,
            key_revision_minor: 0,
            entries: vec![GeoKeyEntry {
                key_id: GeoKey::GeographicType as u16,
                tiff_tag_location: 0,
                count: 1,
                value_offset: 4326,
            }],
            double_params: Vec::new(),
            ascii_params: String::new(),
        };
        assert_eq!(resolve_source_crs(&geo_keys), Some("EPSG:4326".to_string()));
    }
}
