//! OGC API response models (CoverageJSON stub, process metadata, map landing).

use serde::{Deserialize, Serialize};

/// Mantle's default OGC collection id (aligned with STAC `mantle` collection).
pub const DEFAULT_COLLECTION_ID: &str = "mantle";

/// Default Web Mercator tile matrix set identifier.
pub const WEB_MERCATOR_TILE_MATRIX_SET: &str = "WebMercatorQuad";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OgcLink {
    pub rel: String,
    pub href: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapMetadata {
    pub id: String,
    pub title: String,
    pub description: String,
    pub links: Vec<OgcLink>,
}

pub fn map_metadata(collection_id: &str) -> MapMetadata {
    MapMetadata {
        id: collection_id.to_string(),
        title: format!("Mantle map — {collection_id}"),
        description: "OGC API – Maps layer backed by the Mantle render AST pipeline.".into(),
        links: vec![
            ogc_link(
                "self",
                &format!("/ogc/maps/{collection_id}"),
                Some("application/json"),
            ),
            ogc_link(
                "tiles",
                &format!(
                    "/ogc/maps/{collection_id}/tiles/{WEB_MERCATOR_TILE_MATRIX_SET}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}"
                ),
                Some("image/webp"),
            ),
            ogc_link(
                "render-plan",
                &format!("/ogc/maps/{collection_id}/plan"),
                Some("application/json"),
            ),
        ],
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessDescription {
    pub id: String,
    pub title: String,
    pub description: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessList {
    pub processes: Vec<ProcessDescription>,
    pub links: Vec<OgcLink>,
}

pub fn process_list() -> ProcessList {
    ProcessList {
        processes: vec![
            ProcessDescription {
                id: "ndvi".into(),
                title: "NDVI".into(),
                description: "Normalized difference vegetation index from red and NIR bands."
                    .into(),
                version: "1.0.0".into(),
            },
            ProcessDescription {
                id: "zonal-stats".into(),
                title: "Zonal statistics".into(),
                description: "Aggregate raster values over vector zones.".into(),
                version: "1.0.0".into(),
            },
            ProcessDescription {
                id: "cube-slice".into(),
                title: "Cube slice".into(),
                description: "Extract a multidimensional slice from an Icechunk dataset.".into(),
                version: "1.0.0".into(),
            },
            ProcessDescription {
                id: "edr-point".into(),
                title: "EDR point query".into(),
                description: "Point value extraction for multidimensional coverages.".into(),
                version: "1.0.0".into(),
            },
        ],
        links: vec![
            ogc_link("self", "/ogc/processes", Some("application/json")),
            ogc_link(
                "execute",
                "/ogc/processes/{processId}/execution",
                Some("application/json"),
            ),
        ],
    }
}

/// OGC CoverageJSON stub for sync EDR point responses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoverageJsonStub {
    #[serde(rename = "type")]
    pub type_: String,
    pub domain: CoverageDomain,
    pub ranges: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stub: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoverageDomain {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(rename = "domainType")]
    pub domain_type: String,
    pub axes: CoverageAxes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoverageAxes {
    pub x: AxisValues,
    pub y: AxisValues,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t: Option<AxisValues>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AxisValues {
    pub values: Vec<f64>,
}

/// Build a CoverageJSON stub for a point query (sync fast path).
pub fn coverage_json_stub(
    lon: f64,
    lat: f64,
    variables: &[String],
    datetime: Option<&str>,
    is_stub: bool,
) -> CoverageJsonStub {
    let mut ranges = serde_json::Map::new();
    if variables.is_empty() {
        ranges.insert(
            "value".into(),
            serde_json::json!({
                "type": "NdArray",
                "dataType": "float",
                "axisNames": ["t"],
                "shape": [1],
                "values": [0.0]
            }),
        );
    } else {
        for var in variables {
            ranges.insert(
                var.clone(),
                serde_json::json!({
                    "type": "NdArray",
                    "dataType": "float",
                    "axisNames": ["t"],
                    "shape": [1],
                    "values": [0.0]
                }),
            );
        }
    }

    CoverageJsonStub {
        type_: "Coverage".into(),
        domain: CoverageDomain {
            type_: "Domain".into(),
            domain_type: "Point".into(),
            axes: CoverageAxes {
                x: AxisValues { values: vec![lon] },
                y: AxisValues { values: vec![lat] },
                t: datetime.map(|dt| AxisValues {
                    values: vec![parse_datetime_axis(dt)],
                }),
            },
        },
        ranges,
        stub: is_stub.then_some(true),
    }
}

fn parse_datetime_axis(dt: &str) -> f64 {
    chrono::DateTime::parse_from_rfc3339(dt)
        .map(|d| d.timestamp() as f64)
        .unwrap_or(0.0)
}

pub fn ogc_link(rel: &str, href: &str, media_type: Option<&str>) -> OgcLink {
    OgcLink {
        rel: rel.into(),
        href: href.into(),
        media_type: media_type.map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_json_stub_has_point_domain() {
        let cov = coverage_json_stub(-122.4, 37.8, &["temperature".into()], None, true);
        assert_eq!(cov.type_, "Coverage");
        assert_eq!(cov.domain.domain_type, "Point");
        assert_eq!(cov.domain.axes.x.values, vec![-122.4]);
        assert_eq!(cov.domain.axes.y.values, vec![37.8]);
        assert!(cov.ranges.contains_key("temperature"));
    }

    #[test]
    fn process_list_includes_ndvi() {
        let list = process_list();
        assert!(list.processes.iter().any(|p| p.id == "ndvi"));
    }
}
