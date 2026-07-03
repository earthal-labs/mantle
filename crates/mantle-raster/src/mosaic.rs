//! On-the-fly mosaic: merge intersecting raster layers for a tile.

/// Single-band tile layer in tile pixel space.
#[derive(Debug, Clone)]
pub struct TileLayer {
    pub values: Vec<f32>,
    pub width: u32,
    pub height: u32,
}

impl TileLayer {
    pub fn transparent(width: u32, height: u32) -> Self {
        Self {
            values: vec![f32::NAN; (width * height) as usize],
            width,
            height,
        }
    }
}

/// Merge layers top-to-bottom: first valid (non-NaN) pixel wins.
pub fn mosaic_first_valid(layers: &[TileLayer]) -> TileLayer {
    if layers.is_empty() {
        return TileLayer::transparent(0, 0);
    }
    let width = layers[0].width;
    let height = layers[0].height;
    let mut out = vec![f32::NAN; layers[0].values.len()];

    for layer in layers {
        if layer.width != width || layer.height != height {
            continue;
        }
        for (dst, &src) in out.iter_mut().zip(layer.values.iter()) {
            if dst.is_nan() && src.is_finite() {
                *dst = src;
            }
        }
    }

    TileLayer {
        values: out,
        width,
        height,
    }
}

/// Merge layers by arithmetic mean of valid pixels.
pub fn mosaic_mean(layers: &[TileLayer]) -> TileLayer {
    if layers.is_empty() {
        return TileLayer::transparent(0, 0);
    }
    let width = layers[0].width;
    let height = layers[0].height;
    let len = layers[0].values.len();
    let mut sums = vec![0.0f64; len];
    let mut counts = vec![0u32; len];

    for layer in layers {
        if layer.width != width || layer.height != height {
            continue;
        }
        for i in 0..len {
            let v = layer.values[i];
            if v.is_finite() {
                sums[i] += v as f64;
                counts[i] += 1;
            }
        }
    }

    let values = sums
        .iter()
        .zip(counts.iter())
        .map(|(&s, &c)| {
            if c > 0 {
                (s / c as f64) as f32
            } else {
                f32::NAN
            }
        })
        .collect();

    TileLayer {
        values,
        width,
        height,
    }
}

/// Merge layers by per-pixel maximum of valid pixels.
pub fn mosaic_max(layers: &[TileLayer]) -> TileLayer {
    mosaic_reduce(layers, |a, b| a.max(b))
}

/// Merge layers by per-pixel minimum of valid pixels.
pub fn mosaic_min(layers: &[TileLayer]) -> TileLayer {
    mosaic_reduce(layers, |a, b| a.min(b))
}

/// Merge layers by per-pixel sum of valid pixels.
pub fn mosaic_sum(layers: &[TileLayer]) -> TileLayer {
    if layers.is_empty() {
        return TileLayer::transparent(0, 0);
    }
    let width = layers[0].width;
    let height = layers[0].height;
    let len = layers[0].values.len();
    let mut sums = vec![0.0f64; len];
    let mut any = vec![false; len];

    for layer in layers {
        if layer.width != width || layer.height != height {
            continue;
        }
        for i in 0..len {
            let v = layer.values[i];
            if v.is_finite() {
                sums[i] += v as f64;
                any[i] = true;
            }
        }
    }

    let values = sums
        .iter()
        .zip(any.iter())
        .map(|(&s, &has)| {
            if has {
                s as f32
            } else {
                f32::NAN
            }
        })
        .collect();

    TileLayer {
        values,
        width,
        height,
    }
}

fn mosaic_reduce<F>(layers: &[TileLayer], op: F) -> TileLayer
where
    F: Fn(f32, f32) -> f32,
{
    if layers.is_empty() {
        return TileLayer::transparent(0, 0);
    }
    let width = layers[0].width;
    let height = layers[0].height;
    let len = layers[0].values.len();
    let mut out = vec![f32::NAN; len];

    for layer in layers {
        if layer.width != width || layer.height != height {
            continue;
        }
        for (dst, &src) in out.iter_mut().zip(layer.values.iter()) {
            if src.is_finite() {
                *dst = if dst.is_finite() { op(*dst, src) } else { src };
            }
        }
    }

    TileLayer {
        values: out,
        width,
        height,
    }
}

/// Apply a mosaic reducer from the render AST.
pub fn mosaic_by_reducer(
    layers: &[TileLayer],
    reducer: mantle_render_ast::MosaicReducer,
) -> TileLayer {
    match reducer {
        mantle_render_ast::MosaicReducer::Mean => mosaic_mean(layers),
        mantle_render_ast::MosaicReducer::Max => mosaic_max(layers),
        mantle_render_ast::MosaicReducer::Min => mosaic_min(layers),
        mantle_render_ast::MosaicReducer::Sum => mosaic_sum(layers),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_valid_prefers_earlier_layer() {
        let a = TileLayer {
            values: vec![1.0, f32::NAN],
            width: 2,
            height: 1,
        };
        let b = TileLayer {
            values: vec![2.0, 2.0],
            width: 2,
            height: 1,
        };
        let merged = mosaic_first_valid(&[a, b]);
        assert_eq!(merged.values[0], 1.0);
        assert_eq!(merged.values[1], 2.0);
    }

    #[test]
    fn mean_averages_valid_pixels() {
        let a = TileLayer {
            values: vec![1.0, 2.0],
            width: 2,
            height: 1,
        };
        let b = TileLayer {
            values: vec![3.0, f32::NAN],
            width: 2,
            height: 1,
        };
        let merged = mosaic_mean(&[a, b]);
        assert_eq!(merged.values[0], 2.0);
        assert_eq!(merged.values[1], 2.0);
    }
}
