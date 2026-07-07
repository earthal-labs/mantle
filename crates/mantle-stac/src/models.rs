use serde::{Deserialize, Serialize};

/// Mantle's single STAC collection id — all catalog services are exposed here.
pub const DEFAULT_COLLECTION_ID: &str = "mantle";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacCatalog {
    #[serde(rename = "type")]
    pub type_: String,
    pub stac_version: String,
    pub id: String,
    pub title: String,
    pub description: String,
    pub links: Vec<StacLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacCollection {
    #[serde(rename = "type")]
    pub type_: String,
    pub stac_version: String,
    pub id: String,
    pub title: String,
    pub description: String,
    pub license: String,
    pub links: Vec<StacLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacCollectionList {
    pub collections: Vec<StacCollection>,
    pub links: Vec<StacLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacItemCollection {
    #[serde(rename = "type")]
    pub type_: String,
    pub stac_version: String,
    pub features: Vec<StacItem>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "numberMatched")]
    pub number_matched: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "numberReturned")]
    pub number_returned: Option<u64>,
    pub links: Vec<StacLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacItem {
    #[serde(rename = "type")]
    pub type_: String,
    pub stac_version: String,
    pub id: String,
    pub collection: String,
    pub geometry: Option<serde_json::Value>,
    pub bbox: Option<Vec<f64>>,
    pub properties: serde_json::Value,
    pub assets: serde_json::Value,
    pub links: Vec<StacLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacLink {
    pub rel: String,
    pub href: String,
    #[serde(rename = "type")]
    pub media_type: Option<String>,
}

pub fn landing_catalog() -> StacCatalog {
    StacCatalog {
        type_: "Catalog".into(),
        stac_version: "1.0.0".into(),
        id: "mantle".into(),
        title: "Mantle STAC API".into(),
        description: "Cloud-native raster catalog exposed as STAC 1.0.".into(),
        links: vec![
            link("self", "/stac", Some("application/json")),
            link("root", "/stac", Some("application/json")),
            link("collections", "/stac/collections", Some("application/json")),
            link("search", "/stac/search", Some("application/geo+json")),
        ],
    }
}

pub fn default_collection() -> StacCollection {
    StacCollection {
        type_: "Collection".into(),
        stac_version: "1.0.0".into(),
        id: DEFAULT_COLLECTION_ID.into(),
        title: "Mantle Services".into(),
        description: "Raster services registered in the Mantle catalog (COG and Icechunk).".into(),
        license: "proprietary".into(),
        links: vec![
            link(
                "self",
                &format!("/stac/collections/{DEFAULT_COLLECTION_ID}"),
                Some("application/json"),
            ),
            link("root", "/stac", Some("application/json")),
            link("parent", "/stac/collections", Some("application/json")),
            link(
                "items",
                &format!("/stac/collections/{DEFAULT_COLLECTION_ID}/items"),
                Some("application/geo+json"),
            ),
        ],
    }
}

pub fn collection_list() -> StacCollectionList {
    StacCollectionList {
        collections: vec![default_collection()],
        links: vec![
            link("self", "/stac/collections", Some("application/json")),
            link("root", "/stac", Some("application/json")),
        ],
    }
}

pub fn link(rel: &str, href: &str, media_type: Option<&str>) -> StacLink {
    StacLink {
        rel: rel.into(),
        href: href.into(),
        media_type: media_type.map(str::to_string),
    }
}
