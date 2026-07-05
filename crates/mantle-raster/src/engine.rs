//! `OxigdalRasterEngine` — COG tile rendering with cache + catalog integration.

use crate::cog::render_tile_layer;
use crate::colormap::{apply_colormap, colormap_from_lut_id, normalize_band, parse_colormap};
use crate::encode::{encode_empty_tile, encode_tile};
use crate::mosaic::{mosaic_by_reducer, mosaic_first_valid, TileLayer};
use crate::storage::{build_object_store, parse_storage_uri};
use crate::tile_math::{tile_bounds_web_mercator, TILE_SIZE};
use crate::{RasterEngine, RasterError, TileFormat};
use async_trait::async_trait;
use mantle_arrow::{DatasetFormat, DatasetRef, TileRequest};
use mantle_cache::CacheClient;
use mantle_catalog::{CatalogClient, SpatialQuery};
use mantle_config::{CacheConfig, StorageConfig};
use mantle_render_ast::{
    collect_band_refs, execution_target, execute_expr, parse_render_rule, peel_colormap,
    BandContext, ExecutionTarget, Expr,
};
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Production raster engine: COG byte-range reads, Web Mercator warp, mosaic, colormap.
///
/// Takes `cache`/`cache_config` for API-compatibility with callers that still
/// wire up Redis (used elsewhere, e.g. cache-warming) — the oxigdal-based COG
/// reader in `cog.rs` doesn't consult it today. A metadata-level cache (not
/// raw IFD bytes) is a possible follow-up; see the plan notes.
pub struct OxigdalRasterEngine {
    storage: Arc<StorageConfig>,
    catalog: Arc<dyn CatalogClient>,
    store: Arc<dyn ObjectStore>,
}

impl OxigdalRasterEngine {
    pub fn new(
        storage: Arc<StorageConfig>,
        _cache: Arc<dyn CacheClient>,
        catalog: Arc<dyn CatalogClient>,
        _cache_config: &CacheConfig,
    ) -> Result<Self, RasterError> {
        let store = build_object_store(&storage)?;
        Ok(Self {
            storage,
            catalog,
            store,
        })
    }

    pub fn with_store(
        storage: Arc<StorageConfig>,
        _cache: Arc<dyn CacheClient>,
        catalog: Arc<dyn CatalogClient>,
        store: Arc<dyn ObjectStore>,
        _cache_ttl: u64,
    ) -> Self {
        Self {
            storage,
            catalog,
            store,
        }
    }

    async fn resolve_datasets(
        &self,
        datasets: &[DatasetRef],
        request: &TileRequest,
    ) -> Result<Vec<DatasetRef>, RasterError> {
        if !datasets.is_empty() {
            return Ok(datasets.to_vec());
        }

        let bounds = tile_bounds_web_mercator(request.z, request.x, request.y);
        self.catalog
            .spatial_query(SpatialQuery {
                bbox: Some(bounds.to_rect()),
                ..Default::default()
            })
            .await
            .map_err(RasterError::Catalog)
    }

    async fn read_dataset_layer(
        &self,
        dataset: &DatasetRef,
        tile_bounds: &crate::tile_math::TileBounds,
        band: u32,
    ) -> Result<Option<TileLayer>, RasterError> {
        if dataset.format != DatasetFormat::Cog {
            debug!(dataset_id = %dataset.id, "skipping non-COG dataset");
            return Ok(None);
        }

        let _ = band; // band index reserved for multi-band COG reads
        let (_bucket, s3_key) = parse_storage_uri(&dataset.storage_uri, &self.storage.bucket)?;

        let values = render_tile_layer(self.store.clone(), &s3_key, *tile_bounds).await?;
        let Some(values) = values else {
            return Ok(None);
        };

        Ok(Some(TileLayer {
            values,
            width: TILE_SIZE,
            height: TILE_SIZE,
        }))
    }

    async fn load_band_context(
        &self,
        refs: &[DatasetRef],
        band_refs: &[mantle_render_ast::BandRefKey],
        tile_bounds: &crate::tile_math::TileBounds,
    ) -> Result<BandContext, RasterError> {
        let mut bands = HashMap::new();
        let mut pixel_len = 0usize;

        for key in band_refs {
            let dataset = refs
                .iter()
                .find(|d| d.id == key.dataset_id)
                .ok_or_else(|| {
                    RasterError::NotImplemented(format!(
                        "dataset {} not in tile context",
                        key.dataset_id
                    ))
                })?;

            if let Some(layer) = self
                .read_dataset_layer(dataset, tile_bounds, key.band)
                .await?
            {
                pixel_len = layer.values.len();
                bands.insert((key.dataset_id, key.band), layer.values);
            }
        }

        Ok(BandContext::new(pixel_len, bands))
    }

    async fn render_ast_tile(
        &self,
        refs: &[DatasetRef],
        request: &TileRequest,
        format: TileFormat,
        expr: &Expr,
        tile_bounds: &crate::tile_math::TileBounds,
    ) -> Result<Vec<u8>, RasterError> {
        match execution_target(expr) {
            ExecutionTarget::RayAsync => {
                return Err(RasterError::NotImplemented(
                    "render rule requires async Ray execution".into(),
                ));
            }
            ExecutionTarget::MosaicParallel => {
                self.render_mosaic_ast(refs, request, format, expr, tile_bounds)
                    .await
            }
            ExecutionTarget::SimdLocal => {
                self.render_simd_ast(refs, format, expr, tile_bounds).await
            }
        }
    }

    async fn render_simd_ast(
        &self,
        refs: &[DatasetRef],
        format: TileFormat,
        expr: &Expr,
        tile_bounds: &crate::tile_math::TileBounds,
    ) -> Result<Vec<u8>, RasterError> {
        let band_refs = collect_band_refs(expr);
        let ctx = self.load_band_context(refs, &band_refs, tile_bounds).await?;

        if ctx.pixel_len == 0 {
            return encode_empty_tile(TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode);
        }

        let values = execute_expr(expr, &ctx).map_err(|e| RasterError::Cog(e.to_string()))?;
        self.encode_scalar_tile(&values, expr, format)
    }

    async fn render_mosaic_ast(
        &self,
        refs: &[DatasetRef],
        request: &TileRequest,
        format: TileFormat,
        expr: &Expr,
        tile_bounds: &crate::tile_math::TileBounds,
    ) -> Result<Vec<u8>, RasterError> {
        let mosaic_expr = peel_colormap(expr);
        let Expr::Mosaic { reducer, .. } = mosaic_expr else {
            return Err(RasterError::NotImplemented(
                "expected mosaic node in MosaicParallel expression".into(),
            ));
        };

        let mosaic_refs = self.resolve_mosaic_datasets(refs, request).await?;

        let band = request.band.unwrap_or(1);
        let mut layers = Vec::new();
        for dataset in &mosaic_refs {
            if let Some(layer) = self.read_dataset_layer(dataset, tile_bounds, band).await? {
                layers.push(layer);
            }
        }

        if layers.is_empty() {
            return encode_empty_tile(TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode);
        }

        let merged = mosaic_by_reducer(&layers, *reducer);
        self.encode_scalar_tile(&merged.values, expr, format)
    }

    async fn resolve_mosaic_datasets(
        &self,
        refs: &[DatasetRef],
        request: &TileRequest,
    ) -> Result<Vec<DatasetRef>, RasterError> {
        if !refs.is_empty() {
            return Ok(refs.to_vec());
        }
        let bounds = tile_bounds_web_mercator(request.z, request.x, request.y);
        self.catalog
            .spatial_query(SpatialQuery {
                bbox: Some(bounds.to_rect()),
                ..Default::default()
            })
            .await
            .map_err(RasterError::Catalog)
    }

    fn encode_scalar_tile(
        &self,
        values: &[f32],
        expr: &Expr,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError> {
        let colormap = match mantle_render_ast::colormap_lut_id(expr) {
            Some(lut) => colormap_from_lut_id(lut),
            None => parse_colormap(None),
        };
        let normalized = normalize_band(values);
        let rgba = apply_colormap(&normalized, &colormap);
        encode_tile(&rgba, TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode)
    }
}

#[async_trait]
impl RasterEngine for OxigdalRasterEngine {
    async fn render_tile(
        &self,
        datasets: &[DatasetRef],
        request: &TileRequest,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError> {
        let tile_bounds = tile_bounds_web_mercator(request.z, request.x, request.y);
        let refs = self.resolve_datasets(datasets, request).await?;

        if let Some(ref rule) = request.render_rule {
            if let Ok(expr) = parse_render_rule(rule) {
                return self
                    .render_ast_tile(&refs, request, format, &expr, &tile_bounds)
                    .await;
            }
        }

        let band = request.band.unwrap_or(1);
        let colormap = parse_colormap(request.render_rule.as_deref());

        let mut layers = Vec::new();
        for dataset in &refs {
            if let Some(layer) = self.read_dataset_layer(dataset, &tile_bounds, band).await? {
                layers.push(layer);
            }
        }

        if layers.is_empty() {
            return encode_empty_tile(TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode);
        }

        let merged = mosaic_first_valid(&layers);
        let normalized = normalize_band(&merged.values);
        let rgba = apply_colormap(&normalized, &colormap);
        encode_tile(&rgba, TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode)
    }

    async fn read_tile_bands(
        &self,
        dataset: &DatasetRef,
        request: &TileRequest,
        band_indices: &[u32],
    ) -> Result<Vec<TileLayer>, RasterError> {
        let tile_bounds = tile_bounds_web_mercator(request.z, request.x, request.y);
        let mut layers = Vec::with_capacity(band_indices.len());
        for &band in band_indices {
            if let Some(layer) = self.read_dataset_layer(dataset, &tile_bounds, band).await? {
                layers.push(layer);
            } else {
                layers.push(TileLayer::transparent(TILE_SIZE, TILE_SIZE));
            }
        }
        Ok(layers)
    }
}

/// Stub raster engine — returns empty tile bytes (no S3/catalog).
pub struct StubRasterEngine {
    _storage: Arc<StorageConfig>,
    _cache: Arc<dyn CacheClient>,
}

impl StubRasterEngine {
    pub fn new(storage: Arc<StorageConfig>, cache: Arc<dyn CacheClient>) -> Self {
        Self {
            _storage: storage,
            _cache: cache,
        }
    }
}

#[async_trait]
impl RasterEngine for StubRasterEngine {
    async fn render_tile(
        &self,
        _datasets: &[DatasetRef],
        _request: &TileRequest,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError> {
        encode_empty_tile(TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode)
    }

    async fn read_tile_bands(
        &self,
        _dataset: &DatasetRef,
        _request: &TileRequest,
        band_indices: &[u32],
    ) -> Result<Vec<TileLayer>, RasterError> {
        Ok(band_indices
            .iter()
            .map(|_| TileLayer::transparent(TILE_SIZE, TILE_SIZE))
            .collect())
    }
}
