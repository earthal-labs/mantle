//! HTTP client for the Python vRPM compute sidecar.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use mantle_ogc::{PluginDescriptor, PluginListResponse};
use mantle_raster::{TileLayer, TILE_SIZE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct VrpmComputeRequest {
    pub function_id: String,
    pub params: Value,
    pub tile_meta: VrpmTileMeta,
    pub bands: HashMap<String, VrpmBandPayload>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VrpmTileMeta {
    pub z: u32,
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
    pub crs: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VrpmBandPayload {
    pub data: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VrpmComputeResponse {
    pub width: u32,
    pub height: u32,
    pub data: String,
}

#[derive(Debug, thiserror::Error)]
pub enum VrpmClientError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("sidecar error: {0}")]
    Sidecar(String),
    #[error("decode error: {0}")]
    Decode(String),
}

pub struct VrpmSidecarClient {
    base_url: String,
    http: reqwest::Client,
}

impl VrpmSidecarClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn encode_band(layer: &TileLayer) -> Result<String, VrpmClientError> {
        let bytes: Vec<u8> = layer
            .values
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        Ok(BASE64.encode(bytes))
    }

    pub async fn compute_tile(
        &self,
        function_id: &str,
        params: &Value,
        z: u32,
        x: u32,
        y: u32,
        band_map: &HashMap<String, TileLayer>,
    ) -> Result<Vec<f32>, VrpmClientError> {
        let mut bands = HashMap::new();
        for (name, layer) in band_map {
            bands.insert(
                name.clone(),
                VrpmBandPayload {
                    data: Self::encode_band(layer)?,
                },
            );
        }

        let body = VrpmComputeRequest {
            function_id: function_id.to_string(),
            params: params.clone(),
            tile_meta: VrpmTileMeta {
                z,
                x,
                y,
                width: TILE_SIZE,
                height: TILE_SIZE,
                crs: "EPSG:3857".into(),
            },
            bands,
        };

        let url = format!("{}/vrpm/compute-tile", self.base_url);
        let response = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| VrpmClientError::Http(e.to_string()))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| VrpmClientError::Http(e.to_string()))?;

        if !status.is_success() {
            if let Ok(err) = serde_json::from_str::<Value>(&text) {
                if let Some(msg) = err.get("error").and_then(|v| v.as_str()) {
                    return Err(VrpmClientError::Sidecar(msg.to_string()));
                }
            }
            return Err(VrpmClientError::Sidecar(format!(
                "status {status}: {text}"
            )));
        }

        let parsed: VrpmComputeResponse = serde_json::from_str(&text)
            .map_err(|e| VrpmClientError::Decode(e.to_string()))?;

        let raw = BASE64
            .decode(&parsed.data)
            .map_err(|e| VrpmClientError::Decode(e.to_string()))?;

        let expected = (parsed.width * parsed.height * 4) as usize;
        if raw.len() != expected {
            return Err(VrpmClientError::Decode(format!(
                "byte length {} != expected {}",
                raw.len(),
                expected
            )));
        }

        let mut values = Vec::with_capacity((parsed.width * parsed.height) as usize);
        for chunk in raw.chunks_exact(4) {
            values.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(values)
    }

    pub async fn list_plugins(&self) -> Result<Vec<PluginDescriptor>, VrpmClientError> {
        let url = format!("{}/plugins", self.base_url);
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| VrpmClientError::Http(e.to_string()))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| VrpmClientError::Http(e.to_string()))?;

        if !status.is_success() {
            return Err(VrpmClientError::Sidecar(format!("status {status}: {text}")));
        }

        let parsed: PluginListResponse = serde_json::from_str(&text)
            .map_err(|e| VrpmClientError::Decode(e.to_string()))?;
        Ok(parsed.plugins)
    }

    pub async fn get_plugin(&self, plugin_id: &str) -> Result<PluginDescriptor, VrpmClientError> {
        let url = format!("{}/plugins/{}", self.base_url, plugin_id);
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| VrpmClientError::Http(e.to_string()))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| VrpmClientError::Http(e.to_string()))?;

        if status.as_u16() == 404 {
            return Err(VrpmClientError::Sidecar(format!(
                "unknown plugin id: {plugin_id}"
            )));
        }
        if !status.is_success() {
            return Err(VrpmClientError::Sidecar(format!("status {status}: {text}")));
        }

        serde_json::from_str(&text).map_err(|e| VrpmClientError::Decode(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_band_round_trip_length() {
        let layer = TileLayer {
            values: vec![0.5, 1.0],
            width: 2,
            height: 1,
        };
        let encoded = VrpmSidecarClient::encode_band(&layer).unwrap();
        assert!(!encoded.is_empty());
    }
}
