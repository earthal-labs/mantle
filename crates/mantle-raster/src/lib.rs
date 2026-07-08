//! COG/Icechunk raster read + tile encode.

mod byte_cache;
mod cog;
mod colormap;
mod encode;
mod engine;
mod mosaic;
mod oxigdal_source;
mod storage;
mod tile_math;

pub use encode::{encode_empty_tile, encode_tile};
pub use colormap::{colormap_from_lut_id, apply_colormap, compose_rgb, normalize_band, parse_colormap, Colormap, PseudocolorRamp};
pub use mosaic::{mosaic_by_reducer, mosaic_first_valid, mosaic_mean, TileLayer};
pub use engine::{OxigdalRasterEngine, StubRasterEngine};
pub use tile_math::{tile_bounds_web_mercator, TileBounds, TILE_SIZE};

use async_trait::async_trait;
use mantle_arrow::{ServiceRef, TileRequest};
use oxigdal::core_types::types::GeoTransform;
use serde::Serialize;
use thiserror::Error;

/// What oxigdal actually detected for a service — surfaced directly via
/// `GET /admin/services/{id}/debug` so a CRS/geotransform mismatch can be
/// diagnosed from one curl instead of log-grepping or guessing tile
/// coordinates.
#[derive(Debug, Clone, Serialize)]
pub struct CogDebugInfo {
    pub width: u64,
    pub height: u64,
    pub band_count: u32,
    pub data_type: Option<String>,
    pub tile_size: Option<(u32, u32)>,
    pub epsg_code: Option<u32>,
    pub geo_transform: Option<GeoTransform>,
    /// The service's pixel extent reprojected to EPSG:4326, as a closed
    /// ring of `[lon, lat]` corners (NW, NE, SE, SW, NW) — lets clients
    /// draw the real footprint / zoom to it without doing CRS math
    /// themselves. `None` if CRS/geotransform detection failed.
    pub footprint_wgs84: Option<Vec<[f64; 2]>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TileFormat {
    WebP,
    Png,
}

impl TileFormat {
    pub fn from_query(value: Option<&str>) -> Self {
        match value.map(str::to_ascii_lowercase).as_deref() {
            Some("png") => Self::Png,
            _ => Self::WebP,
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::WebP => "image/webp",
            Self::Png => "image/png",
        }
    }
}

#[derive(Debug, Error)]
pub enum RasterError {
    #[error("cache error: {0}")]
    Cache(#[from] mantle_cache::CacheError),
    #[error("catalog error: {0}")]
    Catalog(#[from] mantle_catalog::CatalogError),
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("COG read error: {0}")]
    Cog(String),
    #[error("encode error: {0}")]
    Encode(#[from] crate::encode::EncodeError),
    #[error("raster not implemented: {0}")]
    NotImplemented(String),
}

#[async_trait]
pub trait RasterEngine: Send + Sync {
    async fn render_tile(
        &self,
        services: &[ServiceRef],
        request: &TileRequest,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError>;

    /// Read per-band float32 tile layers from a service (for on-the-fly plugins).
    async fn read_tile_bands(
        &self,
        service: &ServiceRef,
        request: &TileRequest,
        band_indices: &[u32],
    ) -> Result<Vec<TileLayer>, RasterError>;

    /// Composite named single-band assets (e.g. `[("r", B4), ("g", B3), ("b",
    /// B2)]`) into one RGBA tile — no vRPM round-trip, pure band-stacking.
    /// Backs the multi-asset scene "View in Map" composite viewer.
    async fn render_composite_tile(
        &self,
        assets: &[(String, ServiceRef)],
        request: &TileRequest,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError>;

    /// Report what the raster engine actually detects for a service
    /// (CRS, geotransform, dimensions, tiling) without rendering a tile.
    async fn debug_metadata(&self, service: &ServiceRef) -> Result<CogDebugInfo, RasterError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::colormap::{apply_colormap, parse_colormap, Colormap};
    use crate::tile_math::tile_bounds_web_mercator;

    #[test]
    fn tile_format_defaults_to_webp() {
        assert_eq!(TileFormat::from_query(None), TileFormat::WebP);
        assert_eq!(TileFormat::from_query(Some("png")), TileFormat::Png);
    }

    #[test]
    fn tile_bounds_integration_with_colormap() {
        let bounds = tile_bounds_web_mercator(10, 512, 384);
        assert!(bounds.max_x > bounds.min_x);
        let cm = parse_colormap(Some(r#"{"colormap":"grayscale"}"#));
        let rgba = apply_colormap(&[0.5], &cm);
        assert_eq!(rgba.len(), 4);
        assert_eq!(cm, Colormap::Grayscale);
    }
}
