//! Slippy-map tile bounds and pixel resampling.
//!
//! GeoTransform/CRS handling now lives in `cog.rs` via oxigdal
//! (`oxigdal::core_types::types::GeoTransform`,
//! `oxigdal::algorithms::vector::transform::CrsTransformer`) — this module
//! keeps only the Web Mercator tile-grid math and pixel sampling that don't
//! depend on any particular source CRS.

use geo_types::{coord, Rect};

/// Standard XYZ tile dimension in pixels.
pub const TILE_SIZE: u32 = 256;

/// Web Mercator extent (meters).
const ORIGIN_SHIFT: f64 = 20037508.342789244;

/// Bounding box of a tile in Web Mercator (EPSG:3857), meters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TileBounds {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl TileBounds {
    pub fn width(&self) -> f64 {
        self.max_x - self.min_x
    }

    pub fn height(&self) -> f64 {
        self.max_y - self.min_y
    }

    pub fn to_rect(&self) -> Rect<f64> {
        Rect::new(
            coord! { x: self.min_x, y: self.min_y },
            coord! { x: self.max_x, y: self.max_y },
        )
    }
}

/// Compute Web Mercator bounds for an XYZ tile at zoom `z`.
pub fn tile_bounds_web_mercator(z: u32, x: u32, y: u32) -> TileBounds {
    let n = 2_f64.powi(z as i32);
    let tile_span = (2.0 * ORIGIN_SHIFT) / n;

    let min_x = -ORIGIN_SHIFT + x as f64 * tile_span;
    let max_x = min_x + tile_span;
    let max_y = ORIGIN_SHIFT - y as f64 * tile_span;
    let min_y = max_y - tile_span;

    TileBounds {
        min_x,
        min_y,
        max_x,
        max_y,
    }
}

/// Bilinear sample of `src` (row-major `width`×`height`) at fractional pixel coords.
pub fn bilinear_sample(src: &[f32], width: u32, height: u32, col: f64, row: f64) -> f32 {
    if width == 0 || height == 0 || src.is_empty() {
        return f32::NAN;
    }
    let max_col = (width - 1) as f64;
    let max_row = (height - 1) as f64;
    let col = col.clamp(0.0, max_col);
    let row = row.clamp(0.0, max_row);

    let c0 = col.floor() as u32;
    let r0 = row.floor() as u32;
    let c1 = (c0 + 1).min(width - 1);
    let r1 = (r0 + 1).min(height - 1);
    let fc = col - c0 as f64;
    let fr = row - r0 as f64;

    let idx = |c: u32, r: u32| (r * width + c) as usize;
    let v00 = src[idx(c0, r0)];
    let v10 = src[idx(c1, r0)];
    let v01 = src[idx(c0, r1)];
    let v11 = src[idx(c1, r1)];

    let top = v00 * (1.0 - fc) as f32 + v10 * fc as f32;
    let bot = v01 * (1.0 - fc) as f32 + v11 * fc as f32;
    top * (1.0 - fr) as f32 + bot * fr as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_bounds_at_zoom_zero_is_world_extent() {
        let b = tile_bounds_web_mercator(0, 0, 0);
        assert!((b.min_x + ORIGIN_SHIFT).abs() < 1.0);
        assert!((b.max_x - ORIGIN_SHIFT).abs() < 1.0);
        assert!((b.max_y - ORIGIN_SHIFT).abs() < 1.0);
        assert!((b.min_y + ORIGIN_SHIFT).abs() < 1.0);
    }

    #[test]
    fn adjacent_tiles_share_edges() {
        let a = tile_bounds_web_mercator(2, 1, 1);
        let b = tile_bounds_web_mercator(2, 2, 1);
        assert!((a.max_x - b.min_x).abs() < 1e-9);
    }

    #[test]
    fn bilinear_sample_at_integer_pixel_returns_exact_value() {
        let src = vec![1.0, 2.0, 3.0, 4.0];
        assert_eq!(bilinear_sample(&src, 2, 2, 0.0, 0.0), 1.0);
        assert_eq!(bilinear_sample(&src, 2, 2, 1.0, 1.0), 4.0);
    }
}
