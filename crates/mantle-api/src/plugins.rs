//! Plugin registry routes — proxy Python vRPM sidecar descriptors.

use crate::error::ApiError;
use crate::vrpm_client::VrpmSidecarClient;
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use mantle_ogc::PluginDescriptor;

pub fn plugins_router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_plugins))
        .route("/{plugin_id}", get(get_plugin))
}

async fn list_plugins(State(state): State<AppState>) -> Result<Json<Vec<PluginDescriptor>>, ApiError> {
    let client = VrpmSidecarClient::new(&state.config.analytics.vrpm_sidecar_url);
    let plugins = client.list_plugins().await.map_err(sidecar_err)?;
    Ok(Json(plugins))
}

async fn get_plugin(
    State(state): State<AppState>,
    Path(plugin_id): Path<String>,
) -> Result<Json<PluginDescriptor>, ApiError> {
    let client = VrpmSidecarClient::new(&state.config.analytics.vrpm_sidecar_url);
    let plugin = client.get_plugin(&plugin_id).await.map_err(sidecar_err)?;
    Ok(Json(plugin))
}

fn sidecar_err(err: crate::vrpm_client::VrpmClientError) -> ApiError {
    match err {
        crate::vrpm_client::VrpmClientError::Sidecar(msg) if msg.contains("unknown plugin") => {
            ApiError::new(StatusCode::NOT_FOUND, msg)
        }
        other => ApiError::new(StatusCode::BAD_GATEWAY, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_ogc::{ModelKind, ParamDirection, ParamType, ParameterSpec};

    #[test]
    fn plugin_descriptor_serializes_model_kind() {
        let descriptor = PluginDescriptor {
            id: "ndvi".into(),
            version: "1.0.0".into(),
            model_kind: ModelKind::Vrpm,
            inputs: vec![ParameterSpec {
                name: "red_band".into(),
                param_type: ParamType::Band,
                description: "Red band".into(),
                direction: ParamDirection::Input,
                required: false,
                default: Some(serde_json::json!(1)),
                minimum: None,
                maximum: None,
                role: Some("red".into()),
                filename_template: None,
                subpath: None,
            }],
            outputs: vec![],
            metadata: None,
        };
        let json = serde_json::to_value(&descriptor).expect("serialize");
        assert_eq!(json["model_kind"], "vrpm");
        assert_eq!(json["inputs"][0]["param_type"], "band");
    }
}
