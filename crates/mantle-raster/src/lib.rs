//! COG/Icechunk raster read + tile encode.

mod cog;
mod colormap;
mod encode;
mod engine;
mod mosaic;
mod oxigdal_source;
mod storage;
mod tile_math;

pub use encode::{encode_empty_tile, encode_tile};
pub use colormap::{colormap_from_lut_id, apply_colormap, normalize_band, parse_colormap, Colormap, PseudocolorRamp};
pub use mosaic::{mosaic_by_reducer, mosaic_first_valid, mosaic_mean, TileLayer};
pub use engine::{OxigdalRasterEngine, StubRasterEngine};
pub use tile_math::{tile_bounds_web_mercator, TileBounds, TILE_SIZE};

use async_trait::async_trait;
use mantle_arrow::{DatasetRef, TileRequest};
use thiserror::Error;

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
        datasets: &[DatasetRef],
        request: &TileRequest,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError>;

    /// Read per-band float32 tile layers from a dataset (for on-the-fly plugins).
    async fn read_tile_bands(
        &self,
        dataset: &DatasetRef,
        request: &TileRequest,
        band_indices: &[u32],
    ) -> Result<Vec<TileLayer>, RasterError>;
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
