//! Slippy-map tile bounds and Web Mercator math.

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

/// Convert Web Mercator coordinates to WGS84 lon/lat (degrees).
pub fn mercator_to_wgs84(x: f64, y: f64) -> (f64, f64) {
    let lon = (x / ORIGIN_SHIFT) * 180.0;
    let lat = (y * std::f64::consts::PI / ORIGIN_SHIFT).sinh().atan().to_degrees();
    (lon, lat)
}

/// Convert WGS84 lon/lat (degrees) to Web Mercator meters (test helper for round-trip checks).
#[cfg(test)]
pub fn wgs84_to_mercator(lon: f64, lat: f64) -> (f64, f64) {
    let lat = lat.clamp(-85.05112878, 85.05112878);
    let x = lon * ORIGIN_SHIFT / 180.0;
    let lat_rad = lat.to_radians();
    let y = lat_rad.tan().asinh() * ORIGIN_SHIFT / std::f64::consts::PI;
    (x, y)
}

/// Six-parameter affine geo-transform (GDAL convention).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoTransform {
    pub origin_x: f64,
    pub pixel_width: f64,
    pub rot_x: f64,
    pub origin_y: f64,
    pub rot_y: f64,
    pub pixel_height: f64,
}

impl GeoTransform {
    pub fn north_up(origin_x: f64, origin_y: f64, pixel_width: f64, pixel_height: f64) -> Self {
        Self {
            origin_x,
            pixel_width,
            rot_x: 0.0,
            origin_y,
            rot_y: 0.0,
            pixel_height: -pixel_height.abs(),
        }
    }

    pub fn pixel_to_world(&self, col: f64, row: f64) -> (f64, f64) {
        let x = self.origin_x + col * self.pixel_width + row * self.rot_x;
        let y = self.origin_y + col * self.rot_y + row * self.pixel_height;
        (x, y)
    }

    pub fn world_to_pixel(&self, x: f64, y: f64) -> (f64, f64) {
        let det = self.pixel_width * self.pixel_height - self.rot_x * self.rot_y;
        if det.abs() < f64::EPSILON {
            return (0.0, 0.0);
        }
        let dx = x - self.origin_x;
        let dy = y - self.origin_y;
        let col = (self.pixel_height * dx - self.rot_x * dy) / det;
        let row = (-self.rot_y * dx + self.pixel_width * dy) / det;
        (col, row)
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

/// Warp a single-band `f32` raster to a Web Mercator tile grid.
pub fn warp_to_tile(
    src: &[f32],
    src_width: u32,
    src_height: u32,
    gt: &GeoTransform,
    src_crs: DatasetCrs,
    tile: &TileBounds,
    out_size: u32,
) -> Vec<f32> {
    let mut out = vec![f32::NAN; (out_size * out_size) as usize];
    let dx = tile.width() / out_size as f64;
    let dy = tile.height() / out_size as f64;

    for py in 0..out_size {
        for px in 0..out_size {
            let mx = tile.min_x + (px as f64 + 0.5) * dx;
            let my = tile.max_y - (py as f64 + 0.5) * dy;

            let (wx, wy) = match src_crs {
                DatasetCrs::WebMercator => (mx, my),
                DatasetCrs::Wgs84 => {
                    let (lon, lat) = mercator_to_wgs84(mx, my);
                    (lon, lat)
                }
                DatasetCrs::Unknown => (mx, my),
            };

            let (col, row) = gt.world_to_pixel(wx, wy);
            if col >= 0.0 && row >= 0.0 && col < src_width as f64 && row < src_height as f64 {
                out[(py * out_size + px) as usize] =
                    bilinear_sample(src, src_width, src_height, col, row);
            }
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetCrs {
    WebMercator,
    Wgs84,
    Unknown,
}

/// Parse a CRS hint from catalog metadata or GeoTIFF tags.
pub fn parse_crs(crs: Option<&str>) -> DatasetCrs {
    match crs.map(str::trim) {
        Some(s) if s.contains("3857") => DatasetCrs::WebMercator,
        Some(s) if s.contains("4326") || s.eq_ignore_ascii_case("WGS84") => DatasetCrs::Wgs84,
        Some(s) if s.contains("EPSG:3857") => DatasetCrs::WebMercator,
        Some(s) if s.contains("EPSG:4326") => DatasetCrs::Wgs84,
        _ => DatasetCrs::Unknown,
    }
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
    fn mercator_wgs84_round_trip_is_stable() {
        let (mx, my) = wgs84_to_mercator(-73.9857, 40.7484);
        let (lon, lat) = mercator_to_wgs84(mx, my);
        assert!((lon + 73.9857).abs() < 0.01);
        assert!((lat - 40.7484).abs() < 0.01);
    }

    #[test]
    fn geo_transform_pixel_world_round_trip() {
        let gt = GeoTransform::north_up(0.0, 10.0, 1.0, 1.0);
        let (x, y) = gt.pixel_to_world(5.0, 3.0);
        let (c, r) = gt.world_to_pixel(x, y);
        assert!((c - 5.0).abs() < 1e-9);
        assert!((r - 3.0).abs() < 1e-9);
    }

    #[test]
    fn bilinear_sample_at_integer_pixel_returns_exact_value() {
        let src = vec![1.0, 2.0, 3.0, 4.0];
        assert_eq!(bilinear_sample(&src, 2, 2, 0.0, 0.0), 1.0);
        assert_eq!(bilinear_sample(&src, 2, 2, 1.0, 1.0), 4.0);
    }
}
