//! Virtual service records — attached on-the-fly functions and batch outputs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VirtualServiceKind {
    /// On-the-fly function attached to an existing dataset (no data copy).
    Attached,
    /// New output dataset produced by a pRPM job.
    Output,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualServiceRecord {
    pub id: Uuid,
    pub slug: String,
    pub service_kind: VirtualServiceKind,
    /// For attached: parent dataset. For output: the output dataset.
    pub dataset_id: Uuid,
    /// Parent dataset when service_kind is Attached.
    pub parent_dataset_id: Option<Uuid>,
    pub function_id: String,
    pub params_defaults: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// Sanitize a user-supplied slug to URL-safe lowercase alphanumeric + hyphens.
pub fn sanitize_slug(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == '_' || ch.is_whitespace() {
            if !out.is_empty() && !out.ends_with('-') {
                out.push('-');
            }
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "service".into()
    } else {
        trimmed.to_string()
    }
}

/// Generate a default slug from dataset id prefix and function id.
pub fn generate_service_slug(dataset_id: Uuid, function_id: &str, custom: Option<&str>) -> String {
    if let Some(slug) = custom {
        return sanitize_slug(slug);
    }
    let prefix = &dataset_id.to_string()[..8];
    let func = function_id.replace('_', "-");
    sanitize_slug(&format!("{prefix}-{func}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_slug_strips_invalid_chars() {
        assert_eq!(sanitize_slug("My NDVI_Service!"), "my-ndvi-service");
    }

    #[test]
    fn generate_service_slug_uses_prefix_and_function() {
        let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let slug = generate_service_slug(id, "ndvi", None);
        assert!(slug.starts_with("550e8400"));
        assert!(slug.contains("ndvi"));
    }

    #[test]
    fn generate_service_slug_respects_custom() {
        let id = Uuid::nil();
        assert_eq!(
            generate_service_slug(id, "ndvi", Some("custom-slug")),
            "custom-slug"
        );
    }
}
