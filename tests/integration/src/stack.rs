//! Live-stack HTTP helpers (requires `docker compose up` or EKS endpoint).

use crate::env;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct IngestionResponse {
    pub dataset_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct ProcessExecutionResponse {
    pub job_id: Uuid,
    pub status_url: String,
}

#[derive(Debug, Deserialize)]
pub struct JobStatusResponse {
    pub state: String,
    #[serde(default)]
    pub progress: Option<f64>,
    #[serde(default)]
    pub result_url: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .expect("reqwest client")
}

pub fn admin_token() -> String {
    std::env::var(env::ADMIN_TOKEN).expect("set MANTLE_ADMIN_TOKEN for live integration tests")
}

/// Panics if the API `/health` endpoint is unreachable.
pub async fn require_api_healthy() {
    let url = format!("{}/health", env::api_base_url());
    let resp = http_client()
        .get(&url)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {url} failed: {e}"));
    assert!(
        resp.status().is_success(),
        "API unhealthy at {url}: {}",
        resp.status()
    );
}

pub async fn upload_cog_fixture(name: &str, cog_path: &Path) -> Uuid {
    let bytes = std::fs::read(cog_path)
        .unwrap_or_else(|e| panic!("read COG fixture {}: {e}", cog_path.display()));
    let part = Part::bytes(bytes)
        .file_name(
            cog_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("fixture.tif")
                .to_string(),
        )
        .mime_str("image/tiff")
        .expect("mime");
    let form = Form::new().text("name", name.to_string()).part("file", part);

    let url = format!("{}/admin/datasets/upload", env::api_base_url());
    let resp = http_client()
        .post(&url)
        .header(AUTHORIZATION, format!("Bearer {}", admin_token()))
        .multipart(form)
        .send()
        .await
        .expect("upload request");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "upload failed: {status} {body_text}"
    );
    let body: IngestionResponse =
        serde_json::from_str(&body_text).expect("upload json");
    body.dataset_id
}

pub async fn register_cloud_reference(name: &str, storage_uri: &str) -> Uuid {
    let url = format!("{}/admin/datasets/reference", env::api_base_url());
    let resp = http_client()
        .post(&url)
        .header(AUTHORIZATION, format!("Bearer {}", admin_token()))
        .header(CONTENT_TYPE, "application/json")
        .json(&serde_json::json!({
            "name": name,
            "storage_uri": storage_uri,
        }))
        .send()
        .await
        .expect("reference request");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "reference failed: {status} {body_text}"
    );
    let body: IngestionResponse =
        serde_json::from_str(&body_text).expect("reference json");
    body.dataset_id
}

pub async fn stac_search_bbox(bbox: &str) -> serde_json::Value {
    let url = format!("{}/stac/search", env::api_base_url());
    let resp = http_client()
        .get(&url)
        .query(&[("bbox", bbox), ("limit", "10")])
        .send()
        .await
        .expect("stac search");
    assert!(resp.status().is_success(), "stac search: {}", resp.status());
    resp.json().await.expect("stac json")
}

pub async fn fetch_tile(dataset_id: Uuid, z: u32, x: u32, y: u32) -> reqwest::Response {
    let url = format!(
        "{}/ogc/tiles/WebMercatorQuad/{z}/{y}/{x}",
        env::api_base_url()
    );
    http_client()
        .get(&url)
        .query(&[("dataset_id", dataset_id.to_string()), ("format", "webp".into())])
        .send()
        .await
        .expect("tile request")
}

pub async fn submit_process(process_id: &str, datasets: &[Uuid]) -> ProcessExecutionResponse {
    let url = format!(
        "{}/ogc/processes/{process_id}/execution",
        env::api_base_url()
    );
    let resp = http_client()
        .post(&url)
        .header(CONTENT_TYPE, "application/json")
        .json(&serde_json::json!({
            "inputs": {},
            "datasets": datasets,
        }))
        .send()
        .await
        .expect("process execution");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "process submit: {body_text}"
    );
    serde_json::from_str(&body_text).expect("process json")
}

pub async fn poll_job_until_terminal(job_id: Uuid, max_attempts: u32) -> JobStatusResponse {
    let url = format!("{}/status/{job_id}", env::api_base_url());
    for _ in 0..max_attempts {
        let resp = http_client().get(&url).send().await.expect("status poll");
        assert!(resp.status().is_success(), "status poll: {}", resp.status());
        let status: JobStatusResponse = resp.json().await.expect("status json");
        if matches!(status.state.as_str(), "succeeded" | "failed") {
            return status;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    panic!("job {job_id} did not reach terminal state within {max_attempts} polls");
}

pub async fn edr_position(collection_id: &str, coords: &str) -> reqwest::Response {
    let url = format!(
        "{}/ogc/edr/collections/{collection_id}/position",
        env::api_base_url()
    );
    http_client()
        .get(&url)
        .query(&[("coords", coords), ("variables", "temp")])
        .send()
        .await
        .expect("edr position")
}

pub fn redis_url() -> String {
    std::env::var(env::REDIS_URL).unwrap_or_else(|_| "redis://localhost:6379".into())
}

pub async fn redis_has_ifd_key(s3_key: &str) -> bool {
    let key = mantle_cache::ifd_key(s3_key);
    let client = redis::Client::open(redis_url()).expect("redis url");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connect");
    redis::cmd("EXISTS")
        .arg(&key)
        .query_async::<i32>(&mut conn)
        .await
        .map(|n| n > 0)
        .unwrap_or(false)
}
