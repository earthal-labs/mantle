//! Bearer token auth for `/admin/*` routes.

use crate::error::ApiError;
use crate::AppState;
use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::env;

/// Load admin bearer token from the env var named in config (`auth.admin_token_env`).
pub fn load_admin_token(env_var: &str) -> Option<String> {
    env::var(env_var).ok().filter(|value| !value.is_empty())
}

/// Tower middleware: require `Authorization: Bearer <token>` matching admin token.
pub async fn require_admin_auth(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let expected = state
        .admin_token
        .as_ref()
        .ok_or_else(|| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "admin auth not configured"))?;

    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    match auth_header {
        Some(value) if value.starts_with("Bearer ") => {
            let token = value.trim_start_matches("Bearer ").trim();
            if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
                Ok(next.run(request).await)
            } else {
                Err(unauthorized())
            }
        }
        Some(_) => Err(unauthorized()),
        None => Err(unauthorized()),
    }
}

fn unauthorized() -> ApiError {
    ApiError::new(StatusCode::UNAUTHORIZED, "invalid or missing bearer token")
}

/// Constant-time comparison to avoid timing leaks on bearer tokens.
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_admin_token_reads_env() {
        std::env::set_var("MANTLE_TEST_ADMIN_TOKEN", "secret");
        assert_eq!(
            load_admin_token("MANTLE_TEST_ADMIN_TOKEN").as_deref(),
            Some("secret")
        );
    }

    #[test]
    fn constant_time_eq_matches_identical_tokens() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }

    #[test]
    fn bearer_prefix_is_required() {
        let expected = "secret";
        let auth_header = Some("Token secret");
        let authorized = match auth_header {
            Some(value) if value.starts_with("Bearer ") => {
                let token = value.trim_start_matches("Bearer ").trim();
                constant_time_eq(token.as_bytes(), expected.as_bytes())
            }
            _ => false,
        };
        assert!(!authorized);
    }
}
