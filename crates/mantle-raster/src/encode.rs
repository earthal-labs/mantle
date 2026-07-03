//! WebP / PNG tile encoding.

use crate::TileFormat;
use image::{ImageBuffer, ImageEncoder, Rgba};

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("encode failed: {0}")]
    Failed(String),
}

/// Encode RGBA8 tile bytes to WebP or PNG.
pub fn encode_tile(rgba: &[u8], width: u32, height: u32, format: TileFormat) -> Result<Vec<u8>, EncodeError> {
    if rgba.len() != (width * height * 4) as usize {
        return Err(EncodeError::Failed(format!(
            "expected {} rgba bytes, got {}",
            width * height * 4,
            rgba.len()
        )));
    }

    let img: ImageBuffer<Rgba<u8>, Vec<u8>> =
        ImageBuffer::from_raw(width, height, rgba.to_vec()).ok_or_else(|| {
            EncodeError::Failed("invalid RGBA dimensions".into())
        })?;

    let mut out = Vec::new();
    let mut cursor = std::io::Cursor::new(&mut out);
    match format {
        TileFormat::Png => {
            image::codecs::png::PngEncoder::new(&mut cursor)
                .write_image(
                    img.as_raw(),
                    width,
                    height,
                    image::ExtendedColorType::Rgba8,
                )
                .map_err(|e| EncodeError::Failed(e.to_string()))?;
        }
        TileFormat::WebP => {
            image::codecs::webp::WebPEncoder::new_lossless(&mut cursor)
                .write_image(
                    img.as_raw(),
                    width,
                    height,
                    image::ExtendedColorType::Rgba8,
                )
                .map_err(|e| EncodeError::Failed(e.to_string()))?;
        }
    }
    Ok(out)
}

/// Fully transparent tile encoded in the requested format.
pub fn encode_empty_tile(width: u32, height: u32, format: TileFormat) -> Result<Vec<u8>, EncodeError> {
    let rgba = vec![0u8; (width * height * 4) as usize];
    encode_tile(&rgba, width, height, format)
}
