//! VirtualiZarr → Icechunk bridge for multidimensional cloud references.

use crate::IngestionError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct VirtualizeRequest {
    pub name: String,
    pub source_uri: String,
    pub target_uri: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VirtualizeResponse {
    pub storage_uri: String,
    #[serde(default)]
    pub format: String,
}

/// Invoke analytics virtualize endpoint or fall back to Python module stub.
pub async fn virtualize_to_icechunk(request: VirtualizeRequest) -> Result<VirtualizeResponse, IngestionError> {
    if let Ok(url) = std::env::var("MANTLE_VIRTUALIZE_URL") {
        if !url.is_empty() {
            return virtualize_via_http(&url, &request).await;
        }
    }
    virtualize_via_python(&request).await
}

async fn virtualize_via_http(
    base_url: &str,
    request: &VirtualizeRequest,
) -> Result<VirtualizeResponse, IngestionError> {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/virtualize", base_url.trim_end_matches('/')))
        .json(request)
        .send()
        .await
        .map_err(|e| IngestionError::Virtualize(format!("HTTP virtualize request failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(IngestionError::Virtualize(format!(
            "virtualize HTTP {status}: {body}"
        )));
    }

    response
        .json()
        .await
        .map_err(|e| IngestionError::Virtualize(format!("virtualize response decode failed: {e}")))
}

async fn virtualize_via_python(request: &VirtualizeRequest) -> Result<VirtualizeResponse, IngestionError> {
    let payload = serde_json::to_string(request)
        .map_err(|e| IngestionError::Virtualize(format!("serialize virtualize request: {e}")))?;

    let output = tokio::process::Command::new("python")
        .args(["-m", "mantle_analytics.virtualize"])
        .env("VIRTUALIZE_JSON", &payload)
        .output()
        .await
        .map_err(|e| IngestionError::Virtualize(format!("spawn virtualize python: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(IngestionError::Virtualize(format!(
            "virtualize python exited {}: {stderr}",
            output.status
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim())
        .map_err(|e| IngestionError::Virtualize(format!("virtualize python output parse failed: {e}")))
}
