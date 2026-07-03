//! Plugin parameter schema types shared with the analytics sidecar.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Sidecar URL newtype for `FromRef` wiring in the API shell.
#[derive(Clone, Debug)]
pub struct VrpmSidecarUrl(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ParamDirection {
    #[default]
    Input,
    Output,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    Band,
    BandName,
    Number,
    String,
    Boolean,
    Dataset,
    StringList,
    NumberList,
    OutputJson,
    OutputGeojson,
    OutputCog,
    OutputZarr,
    OutputText,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParameterSpec {
    pub name: String,
    pub param_type: ParamType,
    pub description: String,
    #[serde(default)]
    pub direction: ParamDirection,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename_template: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subpath: Option<String>,
}

impl ParameterSpec {
    pub fn is_output(&self) -> bool {
        self.direction == ParamDirection::Output
            || matches!(
                self.param_type,
                ParamType::OutputJson
                    | ParamType::OutputGeojson
                    | ParamType::OutputCog
                    | ParamType::OutputZarr
                    | ParamType::OutputText
            )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelKind {
    Vrpm,
    Prpm,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PluginDescriptor {
    pub id: String,
    pub version: String,
    pub model_kind: ModelKind,
    pub inputs: Vec<ParameterSpec>,
    pub outputs: Vec<ParameterSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginListResponse {
    pub plugins: Vec<PluginDescriptor>,
}

#[derive(Debug, thiserror::Error)]
pub enum PluginValidationError {
    #[error("unknown parameter: {0}")]
    UnknownParameter(String),
    #[error("missing required parameter: {0}")]
    MissingRequired(String),
    #[error("invalid parameter {name}: {reason}")]
    Invalid { name: String, reason: String },
}

pub fn normalize_process_id(process_id: &str) -> String {
    process_id.replace('-', "_")
}

pub fn validate_params_against_specs(
    specs: &[ParameterSpec],
    params: &Value,
) -> Result<(), PluginValidationError> {
    let input_specs: Vec<&ParameterSpec> = specs.iter().filter(|spec| !spec.is_output()).collect();
    let obj = params.as_object().ok_or_else(|| PluginValidationError::Invalid {
        name: "inputs".into(),
        reason: "must be a JSON object".into(),
    })?;

    let known: std::collections::HashSet<&str> = input_specs.iter().map(|s| s.name.as_str()).collect();
    for key in obj.keys() {
        if !known.contains(key.as_str()) {
            return Err(PluginValidationError::UnknownParameter(key.clone()));
        }
    }

    for spec in input_specs {
        let value = obj.get(&spec.name);
        if value.is_none() {
            if spec.required && spec.default.is_none() {
                return Err(PluginValidationError::MissingRequired(spec.name.clone()));
            }
            continue;
        }

        let value = value.expect("checked is_some");
        if value.is_null() {
            if spec.required {
                return Err(PluginValidationError::Invalid {
                    name: spec.name.clone(),
                    reason: "must not be null".into(),
                });
            }
            continue;
        }

        validate_param_value(spec, value)?;
    }

    Ok(())
}

fn validate_param_value(spec: &ParameterSpec, value: &Value) -> Result<(), PluginValidationError> {
    let invalid = |reason: &str| PluginValidationError::Invalid {
        name: spec.name.clone(),
        reason: reason.into(),
    };

    match spec.param_type {
        ParamType::Band => {
            let band = value.as_u64().ok_or_else(|| invalid("must be a positive integer"))?;
            if band < 1 {
                return Err(invalid("must be a positive integer band index"));
            }
        }
        ParamType::BandName => {
            if !value.is_string() || value.as_str().unwrap_or("").trim().is_empty() {
                return Err(invalid("must be a non-empty band name"));
            }
        }
        ParamType::Number => {
            let number = value.as_f64().ok_or_else(|| invalid("must be a number"))?;
            if let Some(min) = spec.minimum {
                if number < min {
                    return Err(invalid(&format!("must be >= {min}")));
                }
            }
            if let Some(max) = spec.maximum {
                if number > max {
                    return Err(invalid(&format!("must be <= {max}")));
                }
            }
        }
        ParamType::String => {
            if !value.is_string() {
                return Err(invalid("must be a string"));
            }
        }
        ParamType::Boolean => {
            if value.as_bool().is_none() {
                return Err(invalid("must be a boolean"));
            }
        }
        ParamType::Dataset => {
            if !value.is_string() || value.as_str().unwrap_or("").trim().is_empty() {
                return Err(invalid("must be a dataset UUID string"));
            }
        }
        ParamType::StringList => {
            let items = value.as_array().ok_or_else(|| invalid("must be a list of strings"))?;
            if !items.iter().all(|item| item.is_string()) {
                return Err(invalid("must be a list of strings"));
            }
        }
        ParamType::NumberList => {
            let items = value.as_array().ok_or_else(|| invalid("must be a non-empty number list"))?;
            if items.is_empty() || !items.iter().all(|item| item.is_number()) {
                return Err(invalid("must be a non-empty list of numbers"));
            }
        }
        ParamType::OutputJson
        | ParamType::OutputGeojson
        | ParamType::OutputCog
        | ParamType::OutputZarr
        | ParamType::OutputText => {}
    }

    Ok(())
}

pub fn resolve_parameters_with_defaults(specs: &[ParameterSpec], params: &Value) -> Value {
    let mut merged = serde_json::Map::new();
    for spec in specs.iter().filter(|spec| !spec.is_output()) {
        if let Some(default) = &spec.default {
            merged.insert(spec.name.clone(), default.clone());
        }
    }
    if let Some(obj) = params.as_object() {
        for (key, value) in obj {
            merged.insert(key.clone(), value.clone());
        }
    }
    Value::Object(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validate_ndvi_band_params() {
        let specs = vec![
            ParameterSpec {
                name: "red_band".into(),
                param_type: ParamType::Band,
                description: "red".into(),
                direction: ParamDirection::Input,
                required: false,
                default: Some(json!(1)),
                minimum: None,
                maximum: None,
                role: Some("red".into()),
                filename_template: None,
                subpath: None,
            },
            ParameterSpec {
                name: "nir_band".into(),
                param_type: ParamType::Band,
                description: "nir".into(),
                direction: ParamDirection::Input,
                required: false,
                default: Some(json!(2)),
                minimum: None,
                maximum: None,
                role: Some("nir".into()),
                filename_template: None,
                subpath: None,
            },
        ];

        validate_params_against_specs(&specs, &json!({"red_band": 3, "nir_band": 4}))
            .expect("valid bands");
        assert!(validate_params_against_specs(&specs, &json!({"red_band": 0})).is_err());
    }

    #[test]
    fn validate_skips_output_parameters() {
        let specs = vec![
            ParameterSpec {
                name: "values".into(),
                param_type: ParamType::NumberList,
                description: "samples".into(),
                direction: ParamDirection::Input,
                required: true,
                default: None,
                minimum: None,
                maximum: None,
                role: None,
                filename_template: None,
                subpath: None,
            },
            ParameterSpec {
                name: "statistics".into(),
                param_type: ParamType::OutputJson,
                description: "result file".into(),
                direction: ParamDirection::Output,
                required: true,
                default: None,
                minimum: None,
                maximum: None,
                role: None,
                filename_template: Some("zonal_stats.json".into()),
                subpath: Some("jobs".into()),
            },
        ];

        validate_params_against_specs(&specs, &json!({"values": [1.0, 2.0]})).expect("inputs only");
        assert!(validate_params_against_specs(&specs, &json!({"statistics": "ignored"})).is_err());
    }

    #[test]
    fn plugin_descriptor_deserializes_split_parameters() {
        let json = json!({
            "id": "zonal_stats",
            "version": "1.0.0",
            "model_kind": "prpm",
            "inputs": [{
                "name": "values",
                "param_type": "number_list",
                "description": "samples",
                "direction": "input",
                "required": false
            }],
            "outputs": [{
                "name": "statistics",
                "param_type": "output_json",
                "description": "result",
                "direction": "output",
                "required": true,
                "filename_template": "zonal_stats.json",
                "subpath": "jobs"
            }]
        });

        let descriptor: PluginDescriptor = serde_json::from_value(json).expect("deserialize");
        assert_eq!(descriptor.inputs.len(), 1);
        assert_eq!(descriptor.outputs.len(), 1);
        assert_eq!(descriptor.outputs[0].param_type, ParamType::OutputJson);
    }
}
