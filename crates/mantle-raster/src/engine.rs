//! `OxigdalRasterEngine` — COG tile rendering with cache + catalog integration.

use crate::byte_cache::ByteRangeCache;
use crate::cog::{debug_metadata as cog_debug_metadata, render_tile_layer};
use crate::colormap::{apply_colormap, colormap_from_lut_id, normalize_band, parse_colormap};
use crate::encode::{encode_empty_tile, encode_tile};
use crate::mosaic::{mosaic_by_reducer, mosaic_first_valid, TileLayer};
use crate::storage::{build_object_store, parse_storage_uri};
use crate::tile_math::{tile_bounds_web_mercator, TILE_SIZE};
use crate::{CogDebugInfo, RasterEngine, RasterError, TileFormat};
use async_trait::async_trait;
use mantle_arrow::{ServiceFormat, ServiceRef, TileRequest};
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
use std::time::Duration;
use tracing::{debug, warn};

/// Production raster engine: COG byte-range reads, Web Mercator warp, mosaic, colormap.
///
/// Two layers of caching sit in front of actual rendering work:
/// - `byte_cache`: in-process cache of raw object-storage byte ranges (COG
///   header/IFD/tile-offset arrays/tile data) — see `byte_cache` module docs.
///   Eliminates the repeated-parsing cost of opening the same COG on every
///   tile request.
/// - `cache` (Redis, via `CacheClient::get_tile`/`set_tile`): the fully
///   encoded output tile, keyed by service(s)/z/x/y/band/render_rule/format.
///   A hit here skips rendering entirely — this is what gets a warm request
///   down to a single Redis round trip.
pub struct OxigdalRasterEngine {
    storage: Arc<StorageConfig>,
    catalog: Arc<dyn CatalogClient>,
    store: Arc<dyn ObjectStore>,
    cache: Arc<dyn CacheClient>,
    tile_ttl_seconds: u64,
    byte_cache: ByteRangeCache,
}

impl OxigdalRasterEngine {
    pub fn new(
        storage: Arc<StorageConfig>,
        cache: Arc<dyn CacheClient>,
        catalog: Arc<dyn CatalogClient>,
        cache_config: &CacheConfig,
    ) -> Result<Self, RasterError> {
        let store = build_object_store(&storage)?;
        let byte_cache = ByteRangeCache::new(
            cache_config.byte_cache_capacity_bytes,
            Duration::from_secs(cache_config.ifd_ttl_seconds),
        );
        Ok(Self {
            storage,
            catalog,
            store,
            cache,
            tile_ttl_seconds: cache_config.tile_ttl_seconds,
            byte_cache,
        })
    }

    pub fn with_store(
        storage: Arc<StorageConfig>,
        cache: Arc<dyn CacheClient>,
        catalog: Arc<dyn CatalogClient>,
        store: Arc<dyn ObjectStore>,
        cache_ttl: u64,
    ) -> Self {
        let byte_cache = ByteRangeCache::new(256 * 1024 * 1024, Duration::from_secs(cache_ttl));
        Self {
            storage,
            catalog,
            store,
            cache,
            tile_ttl_seconds: cache_ttl,
            byte_cache,
        }
    }

    async fn resolve_services(
        &self,
        services: &[ServiceRef],
        request: &TileRequest,
    ) -> Result<Vec<ServiceRef>, RasterError> {
        if !services.is_empty() {
            return Ok(services.to_vec());
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

    async fn read_service_layer(
        &self,
        service: &ServiceRef,
        tile_bounds: &crate::tile_math::TileBounds,
        band: u32,
    ) -> Result<Option<TileLayer>, RasterError> {
        if service.format != ServiceFormat::Cog {
            debug!(service_id = %service.id, "skipping non-COG service");
            return Ok(None);
        }

        let _ = band; // band index reserved for multi-band COG reads
        let (_bucket, s3_key) = parse_storage_uri(&service.storage_uri, &self.storage.bucket)?;

        let values = render_tile_layer(
            self.store.clone(),
            self.byte_cache.clone(),
            &s3_key,
            *tile_bounds,
        )
        .await?;
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
        refs: &[ServiceRef],
        band_refs: &[mantle_render_ast::BandRefKey],
        tile_bounds: &crate::tile_math::TileBounds,
    ) -> Result<BandContext, RasterError> {
        let mut bands = HashMap::new();
        let mut pixel_len = 0usize;

        for key in band_refs {
            let service = refs
                .iter()
                .find(|d| d.id == key.service_id)
                .ok_or_else(|| {
                    RasterError::NotImplemented(format!(
                        "service {} not in tile context",
                        key.service_id
                    ))
                })?;

            if let Some(layer) = self
                .read_service_layer(service, tile_bounds, key.band)
                .await?
            {
                pixel_len = layer.values.len();
                bands.insert((key.service_id, key.band), layer.values);
            }
        }

        Ok(BandContext::new(pixel_len, bands))
    }

    async fn render_ast_tile(
        &self,
        refs: &[ServiceRef],
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
        refs: &[ServiceRef],
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
        refs: &[ServiceRef],
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

        let mosaic_refs = self.resolve_mosaic_services(refs, request).await?;

        let band = request.band.unwrap_or(1);
        let mut layers = Vec::new();
        for service in &mosaic_refs {
            if let Some(layer) = self.read_service_layer(service, tile_bounds, band).await? {
                layers.push(layer);
            }
        }

        if layers.is_empty() {
            return encode_empty_tile(TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode);
        }

        let merged = mosaic_by_reducer(&layers, *reducer);
        self.encode_scalar_tile(&merged.values, expr, format)
    }

    async fn resolve_mosaic_services(
        &self,
        refs: &[ServiceRef],
        request: &TileRequest,
    ) -> Result<Vec<ServiceRef>, RasterError> {
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

    async fn render_tile_uncached(
        &self,
        refs: &[ServiceRef],
        request: &TileRequest,
        format: TileFormat,
        tile_bounds: &crate::tile_math::TileBounds,
    ) -> Result<Vec<u8>, RasterError> {
        if let Some(ref rule) = request.render_rule {
            if let Ok(expr) = parse_render_rule(rule) {
                return self
                    .render_ast_tile(refs, request, format, &expr, tile_bounds)
                    .await;
            }
        }

        let band = request.band.unwrap_or(1);
        let colormap = parse_colormap(request.render_rule.as_deref());

        let mut layers = Vec::new();
        for service in refs {
            if let Some(layer) = self.read_service_layer(service, tile_bounds, band).await? {
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
}

/// Cache key for the encoded-output-tile cache: stable across the same
/// resolved service set + request params + format, regardless of whether the
/// caller passed `service_id` explicitly or services were resolved via
/// spatial query (hence sorting — resolution order isn't guaranteed stable).
fn tile_cache_key(refs: &[ServiceRef], request: &TileRequest, format: TileFormat) -> String {
    let mut ids: Vec<String> = refs.iter().map(|d| d.id.to_string()).collect();
    ids.sort_unstable();
    format!(
        "{}:{}:{}:{}:{}:{}:{:?}",
        ids.join(","),
        request.z,
        request.x,
        request.y,
        request.band.unwrap_or(1),
        request.render_rule.as_deref().unwrap_or(""),
        format,
    )
}

#[async_trait]
impl RasterEngine for OxigdalRasterEngine {
    async fn render_tile(
        &self,
        services: &[ServiceRef],
        request: &TileRequest,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError> {
        let tile_bounds = tile_bounds_web_mercator(request.z, request.x, request.y);
        let refs = self.resolve_services(services, request).await?;
        let cache_key = tile_cache_key(&refs, request, format);

        match self.cache.get_tile(&cache_key).await {
            Ok(Some(bytes)) => return Ok(bytes),
            Ok(None) => {}
            Err(e) => warn!(error = %e, cache_key, "tile cache read failed; rendering fresh"),
        }

        let bytes = self
            .render_tile_uncached(&refs, request, format, &tile_bounds)
            .await?;

        if let Err(e) = self
            .cache
            .set_tile(&cache_key, &bytes, self.tile_ttl_seconds)
            .await
        {
            warn!(error = %e, cache_key, "tile cache write failed");
        }

        Ok(bytes)
    }

    async fn read_tile_bands(
        &self,
        service: &ServiceRef,
        request: &TileRequest,
        band_indices: &[u32],
    ) -> Result<Vec<TileLayer>, RasterError> {
        let tile_bounds = tile_bounds_web_mercator(request.z, request.x, request.y);
        let mut layers = Vec::with_capacity(band_indices.len());
        for &band in band_indices {
            if let Some(layer) = self.read_service_layer(service, &tile_bounds, band).await? {
                layers.push(layer);
            } else {
                layers.push(TileLayer::transparent(TILE_SIZE, TILE_SIZE));
            }
        }
        Ok(layers)
    }

    async fn debug_metadata(&self, service: &ServiceRef) -> Result<CogDebugInfo, RasterError> {
        if service.format != ServiceFormat::Cog {
            return Err(RasterError::NotImplemented(
                "debug_metadata only supports COG services".into(),
            ));
        }
        let (_bucket, s3_key) = parse_storage_uri(&service.storage_uri, &self.storage.bucket)?;
        cog_debug_metadata(self.store.clone(), self.byte_cache.clone(), &s3_key).await
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
        _services: &[ServiceRef],
        _request: &TileRequest,
        format: TileFormat,
    ) -> Result<Vec<u8>, RasterError> {
        encode_empty_tile(TILE_SIZE, TILE_SIZE, format).map_err(RasterError::Encode)
    }

    async fn read_tile_bands(
        &self,
        _service: &ServiceRef,
        _request: &TileRequest,
        band_indices: &[u32],
    ) -> Result<Vec<TileLayer>, RasterError> {
        Ok(band_indices
            .iter()
            .map(|_| TileLayer::transparent(TILE_SIZE, TILE_SIZE))
            .collect())
    }

    async fn debug_metadata(&self, _service: &ServiceRef) -> Result<CogDebugInfo, RasterError> {
        Err(RasterError::NotImplemented(
            "debug_metadata not supported by stub engine".into(),
        ))
    }
}
