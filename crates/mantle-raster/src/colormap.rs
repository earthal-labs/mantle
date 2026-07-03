//! Grayscale, pseudocolor, and user LUT colormap application.

use serde::Deserialize;

/// Colormap applied to a single-band scalar tile before RGBA encode.
#[derive(Debug, Clone, PartialEq)]
pub enum Colormap {
    Grayscale,
    Pseudocolor(PseudocolorRamp),
    Lut(Vec<[u8; 4]>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PseudocolorRamp {
    Viridis,
    Plasma,
    Inferno,
    Turbo,
}

impl Default for Colormap {
    fn default() -> Self {
        Self::Grayscale
    }
}

#[derive(Debug, Deserialize)]
struct ColormapSpec {
    #[serde(default)]
    colormap: Option<String>,
    #[serde(default)]
    ramp: Option<PseudocolorRamp>,
    #[serde(default)]
    entries: Option<Vec<[u8; 4]>>,
}

/// Parse optional `render_rule` JSON into a [`Colormap`].
pub fn parse_colormap(render_rule: Option<&str>) -> Colormap {
    let Some(raw) = render_rule else {
        return Colormap::default();
    };
    let Ok(spec) = serde_json::from_str::<ColormapSpec>(raw) else {
        return Colormap::default();
    };

    match spec.colormap.as_deref() {
        Some("lut") => {
            if let Some(entries) = spec.entries {
                if entries.len() >= 2 {
                    return Colormap::Lut(normalize_lut(entries));
                }
            }
            Colormap::default()
        }
        Some("pseudocolor") => Colormap::Pseudocolor(
            spec.ramp.unwrap_or(PseudocolorRamp::Viridis),
        ),
        Some("grayscale") | None => Colormap::Grayscale,
        Some(_) => Colormap::default(),
    }
}

/// Map an AST colormap `lut_id` to a [`Colormap`].
pub fn colormap_from_lut_id(lut_id: &str) -> Colormap {
    match lut_id.to_ascii_lowercase().as_str() {
        "grayscale" => Colormap::Grayscale,
        "viridis" => Colormap::Pseudocolor(PseudocolorRamp::Viridis),
        "plasma" => Colormap::Pseudocolor(PseudocolorRamp::Plasma),
        "inferno" => Colormap::Pseudocolor(PseudocolorRamp::Inferno),
        "turbo" => Colormap::Pseudocolor(PseudocolorRamp::Turbo),
        "pseudocolor" => Colormap::Pseudocolor(PseudocolorRamp::Viridis),
        other => parse_colormap(Some(other)),
    }
}

fn normalize_lut(entries: Vec<[u8; 4]>) -> Vec<[u8; 4]> {
    if entries.len() == 256 {
        return entries;
    }
    let mut out = vec![[0, 0, 0, 255]; 256];
    let last = entries.len().saturating_sub(1).max(1);
    for i in 0..256 {
        let t = i as f64 / 255.0;
        let idx = (t * last as f64).round() as usize;
        out[i] = entries[idx.min(entries.len() - 1)];
    }
    out
}

/// Map normalized scalar values in `[0, 1]` to RGBA bytes.
pub fn apply_colormap(values: &[f32], colormap: &Colormap) -> Vec<u8> {
    let mut rgba = Vec::with_capacity(values.len() * 4);
    for &v in values {
        let t = normalize_scalar(v);
        let [r, g, b, a] = sample_colormap(t, colormap);
        rgba.extend_from_slice(&[r, g, b, a]);
    }
    rgba
}

fn normalize_scalar(v: f32) -> f32 {
    if v.is_nan() {
        return f32::NAN;
    }
    v.clamp(0.0, 1.0)
}

fn sample_colormap(t: f32, colormap: &Colormap) -> [u8; 4] {
    if t.is_nan() {
        return [0, 0, 0, 0];
    }
    let idx = (t * 255.0).round() as usize;
    match colormap {
        Colormap::Grayscale => {
            let g = (t * 255.0).round() as u8;
            [g, g, g, 255]
        }
        Colormap::Pseudocolor(ramp) => pseudocolor_sample(*ramp, t),
        Colormap::Lut(lut) => lut[idx.min(255)],
    }
}

fn pseudocolor_sample(ramp: PseudocolorRamp, t: f32) -> [u8; 4] {
    let stops: &[([f32; 3], f32)] = match ramp {
        PseudocolorRamp::Viridis => &[
            ([68.0, 1.0, 84.0], 0.0),
            ([59.0, 82.0, 139.0], 0.25),
            ([33.0, 145.0, 140.0], 0.5),
            ([94.0, 201.0, 98.0], 0.75),
            ([253.0, 231.0, 37.0], 1.0),
        ],
        PseudocolorRamp::Plasma => &[
            ([13.0, 8.0, 135.0], 0.0),
            ([126.0, 3.0, 168.0], 0.25),
            ([204.0, 71.0, 120.0], 0.5),
            ([248.0, 149.0, 64.0], 0.75),
            ([240.0, 249.0, 33.0], 1.0),
        ],
        PseudocolorRamp::Inferno => &[
            ([0.0, 0.0, 4.0], 0.0),
            ([85.0, 16.0, 109.0], 0.25),
            ([187.0, 55.0, 84.0], 0.5),
            ([249.0, 142.0, 10.0], 0.75),
            ([252.0, 255.0, 164.0], 1.0),
        ],
        PseudocolorRamp::Turbo => &[
            ([48.0, 18.0, 59.0], 0.0),
            ([67.0, 63.0, 167.0], 0.25),
            ([40.0, 175.0, 229.0], 0.5),
            ([220.0, 225.0, 57.0], 0.75),
            ([122.0, 4.0, 3.0], 1.0),
        ],
    };

    for window in stops.windows(2) {
        let (c0, t0) = (&window[0].0, window[0].1);
        let (c1, t1) = (&window[1].0, window[1].1);
        if t <= t1 {
            let u = if (t1 - t0).abs() < f32::EPSILON {
                0.0
            } else {
                (t - t0) / (t1 - t0)
            };
            return [
                lerp(c0[0], c1[0], u),
                lerp(c0[1], c1[1], u),
                lerp(c0[2], c1[2], u),
                255,
            ];
        }
    }
    let last = stops.last().map(|s| s.0).unwrap_or([0.0, 0.0, 0.0]);
    [
        last[0] as u8,
        last[1] as u8,
        last[2] as u8,
        255,
    ]
}

fn lerp(a: f32, b: f32, t: f32) -> u8 {
    (a + (b - a) * t).round().clamp(0.0, 255.0) as u8
}

/// Scale raw band values to `[0, 1]` using min/max (ignoring NaN).
pub fn normalize_band(values: &[f32]) -> Vec<f32> {
    let mut min_v = f32::INFINITY;
    let mut max_v = f32::NEG_INFINITY;
    for &v in values {
        if v.is_finite() {
            min_v = min_v.min(v);
            max_v = max_v.max(v);
        }
    }
    if !min_v.is_finite() || !max_v.is_finite() || (max_v - min_v).abs() < f32::EPSILON {
        return values.iter().map(|v| if v.is_finite() { 0.0 } else { f32::NAN }).collect();
    }
    values
        .iter()
        .map(|&v| {
            if v.is_finite() {
                (v - min_v) / (max_v - min_v)
            } else {
                f32::NAN
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_grayscale_render_rule() {
        let cm = parse_colormap(Some(r#"{"colormap":"grayscale"}"#));
        assert_eq!(cm, Colormap::Grayscale);
    }

    #[test]
    fn parse_pseudocolor_with_ramp() {
        let cm = parse_colormap(Some(r#"{"colormap":"pseudocolor","ramp":"viridis"}"#));
        assert_eq!(
            cm,
            Colormap::Pseudocolor(PseudocolorRamp::Viridis)
        );
    }

    #[test]
    fn parse_user_lut() {
        let cm = parse_colormap(Some(
            r#"{"colormap":"lut","entries":[[0,0,0,255],[255,255,255,255]]}"#,
        ));
        assert!(matches!(cm, Colormap::Lut(_)));
    }

    #[test]
    fn grayscale_maps_endpoints() {
        let rgba = apply_colormap(&[0.0, 1.0], &Colormap::Grayscale);
        assert_eq!(&rgba[0..4], &[0, 0, 0, 255]);
        assert_eq!(&rgba[4..8], &[255, 255, 255, 255]);
    }

    #[test]
    fn nan_values_are_transparent() {
        let rgba = apply_colormap(&[f32::NAN], &Colormap::Grayscale);
        assert_eq!(&rgba[0..4], &[0, 0, 0, 0]);
    }
}
