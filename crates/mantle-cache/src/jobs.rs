//! Redis Streams job queue and job status keys for analytics workers.

use crate::CacheError;
use async_trait::async_trait;
use mantle_arrow::{encode_job_spec, JobSpec};
use mantle_config::{AnalyticsConfig, CacheConfig};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Redis key prefix for job status polling (`mantle:job:{id}`).
pub const JOB_KEY_PREFIX: &str = "mantle:job:";

/// Default TTL for job status keys (7 days).
pub const JOB_STATUS_TTL_SECONDS: u64 = 604_800;

pub fn job_key(job_id: Uuid) -> String {
    format!("{JOB_KEY_PREFIX}{job_id}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Pending,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobStatus {
    pub state: JobState,
    pub progress: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JobStatus {
    pub fn pending() -> Self {
        Self {
            state: JobState::Pending,
            progress: 0.0,
            result_url: None,
            error: None,
        }
    }
}

#[async_trait]
pub trait JobQueueClient: Send + Sync {
    async fn enqueue_job(&self, job: &JobSpec) -> Result<Uuid, CacheError>;
    async fn get_job_status(&self, job_id: Uuid) -> Result<Option<JobStatus>, CacheError>;
}

/// Redis-backed job queue (Streams) and status store.
pub struct RedisJobQueueClient {
    conn: ConnectionManager,
    stream_key: String,
}

impl RedisJobQueueClient {
    pub async fn connect(
        cache: &CacheConfig,
        analytics: &AnalyticsConfig,
    ) -> Result<Self, CacheError> {
        let client = redis::Client::open(cache.redis_url.as_str())?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            stream_key: analytics.stream_key.clone(),
        })
    }

    pub fn from_parts(conn: ConnectionManager, analytics: Arc<AnalyticsConfig>) -> Self {
        Self {
            conn,
            stream_key: analytics.stream_key.clone(),
        }
    }
}

#[async_trait]
impl JobQueueClient for RedisJobQueueClient {
    async fn enqueue_job(&self, job: &JobSpec) -> Result<Uuid, CacheError> {
        let mut job_for_stream = job.clone();
        if !job.service_refs.is_empty() {
            if let serde_json::Value::Object(ref mut map) = job_for_stream.params {
                map.insert(
                    "service_refs".into(),
                    serde_json::to_value(&job.service_refs)
                        .map_err(|err| CacheError::Redis(err.to_string()))?,
                );
            } else {
                job_for_stream.params = serde_json::json!({
                    "service_refs": job.service_refs,
                    "inputs": job.params,
                });
            }
        }

        let payload = encode_job_spec(&job_for_stream)
            .map_err(|err| CacheError::Redis(err.to_string()))?;
        let payload_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &payload,
        );

        let mut conn = self.conn.clone();
        redis::cmd("XADD")
            .arg(&self.stream_key)
            .arg("*")
            .arg("payload")
            .arg(payload_b64)
            .query_async::<()>(&mut conn)
            .await?;

        let status = JobStatus::pending();
        let status_json = serde_json::to_string(&status)
            .map_err(|err| CacheError::Redis(err.to_string()))?;
        conn.set_ex::<_, _, ()>(
            job_key(job.job_id),
            status_json,
            JOB_STATUS_TTL_SECONDS,
        )
        .await?;

        Ok(job.job_id)
    }

    async fn get_job_status(&self, job_id: Uuid) -> Result<Option<JobStatus>, CacheError> {
        let key = job_key(job_id);
        let value: Option<String> = self.conn.clone().get(key).await?;
        match value {
            Some(json) => {
                let status = serde_json::from_str(&json)
                    .map_err(|err| CacheError::Redis(err.to_string()))?;
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }
}

/// No-op job queue for offline stubs and tests.
pub struct StubJobQueueClient;

#[async_trait]
impl JobQueueClient for StubJobQueueClient {
    async fn enqueue_job(&self, job: &JobSpec) -> Result<Uuid, CacheError> {
        Ok(job.job_id)
    }

    async fn get_job_status(&self, _job_id: Uuid) -> Result<Option<JobStatus>, CacheError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn job_key_matches_agents_md_contract() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            job_key(id),
            "mantle:job:550e8400-e29b-41d4-a716-446655440000"
        );
        assert!(job_key(id).starts_with(JOB_KEY_PREFIX));
    }

    #[test]
    fn job_status_serializes_expected_fields() {
        let status = JobStatus {
            state: JobState::Running,
            progress: 0.5,
            result_url: None,
            error: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"state\":\"running\""));
        assert!(json.contains("\"progress\":0.5"));
    }

    #[test]
    fn pending_status_has_zero_progress() {
        let status = JobStatus::pending();
        assert_eq!(status.state, JobState::Pending);
        assert_eq!(status.progress, 0.0);
    }

    #[tokio::test]
    async fn stub_enqueue_returns_job_id() {
        let job = JobSpec {
            job_id: Uuid::new_v4(),
            process_id: "ndvi".into(),
            service_refs: vec![],
            params: serde_json::json!({}),
            submitted_at: Utc::now(),
        };
        let client = StubJobQueueClient;
        let id = client.enqueue_job(&job).await.unwrap();
        assert_eq!(id, job.job_id);
    }
}
