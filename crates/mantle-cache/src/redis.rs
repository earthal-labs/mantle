//! Redis-backed cache client with connection pooling.

use crate::{resolve_ttl, CacheClient, CacheError};
use async_trait::async_trait;
use mantle_config::CacheConfig;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::future::Future;
use std::sync::Arc;

/// Redis cache client using a tokio connection manager (auto-reconnect).
pub struct RedisCacheClient {
    conn: ConnectionManager,
    default_ttl: u64,
}

impl RedisCacheClient {
    pub async fn connect(config: &CacheConfig) -> Result<Self, CacheError> {
        let client = redis::Client::open(config.redis_url.as_str())?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            default_ttl: config.ifd_ttl_seconds,
        })
    }

    pub fn from_parts(conn: ConnectionManager, config: Arc<CacheConfig>) -> Self {
        Self {
            conn,
            default_ttl: config.ifd_ttl_seconds,
        }
    }

    pub fn default_ttl(&self) -> u64 {
        self.default_ttl
    }

    /// Read-through: return cached IFD bytes or fetch, store, and return.
    pub async fn get_ifd_read_through<F, Fut, E>(
        &self,
        s3_key: &str,
        ttl_seconds: u64,
        fetch: F,
    ) -> Result<Vec<u8>, CacheError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, E>>,
        E: Into<CacheError>,
    {
        if let Some(cached) = self.get_ifd(s3_key).await? {
            return Ok(cached);
        }
        let data = fetch().await.map_err(Into::into)?;
        self.set_ifd(s3_key, &data, ttl_seconds).await?;
        Ok(data)
    }

    /// Read-through: return cached zmetadata bytes or fetch, store, and return.
    pub async fn get_zmetadata_read_through<F, Fut, E>(
        &self,
        repo_id: &str,
        ttl_seconds: u64,
        fetch: F,
    ) -> Result<Vec<u8>, CacheError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, E>>,
        E: Into<CacheError>,
    {
        if let Some(cached) = self.get_zmetadata(repo_id).await? {
            return Ok(cached);
        }
        let data = fetch().await.map_err(Into::into)?;
        self.set_zmetadata(repo_id, &data, ttl_seconds).await?;
        Ok(data)
    }
}

#[async_trait]
impl CacheClient for RedisCacheClient {
    async fn get_ifd(&self, s3_key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let key = crate::ifd_key(s3_key);
        let value: Option<Vec<u8>> = self.conn.clone().get(key).await?;
        Ok(value)
    }

    async fn set_ifd(&self, s3_key: &str, data: &[u8], ttl_seconds: u64) -> Result<(), CacheError> {
        let key = crate::ifd_key(s3_key);
        let ttl = resolve_ttl(ttl_seconds, self.default_ttl);
        let mut conn = self.conn.clone();
        conn.set_ex::<_, _, ()>(key, data, ttl).await?;
        Ok(())
    }

    async fn get_zmetadata(&self, repo_id: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let key = crate::zmeta_key(repo_id);
        let value: Option<Vec<u8>> = self.conn.clone().get(key).await?;
        Ok(value)
    }

    async fn set_zmetadata(
        &self,
        repo_id: &str,
        data: &[u8],
        ttl_seconds: u64,
    ) -> Result<(), CacheError> {
        let key = crate::zmeta_key(repo_id);
        let ttl = resolve_ttl(ttl_seconds, self.default_ttl);
        let mut conn = self.conn.clone();
        conn.set_ex::<_, _, ()>(key, data, ttl).await?;
        Ok(())
    }

    async fn get_tile(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let key = crate::tile_key(key);
        let value: Option<Vec<u8>> = self.conn.clone().get(key).await?;
        Ok(value)
    }

    async fn set_tile(&self, key: &str, data: &[u8], ttl_seconds: u64) -> Result<(), CacheError> {
        let key = crate::tile_key(key);
        let ttl = resolve_ttl(ttl_seconds, self.default_ttl);
        let mut conn = self.conn.clone();
        conn.set_ex::<_, _, ()>(key, data, ttl).await?;
        Ok(())
    }
}
