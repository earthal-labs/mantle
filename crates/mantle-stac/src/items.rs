use crate::models::{link, StacItem, StacItemCollection, DEFAULT_COLLECTION_ID};
use mantle_arrow::{DatasetFormat, DatasetRef};

pub fn datasets_to_stac_items(datasets: &[DatasetRef]) -> Vec<StacItem> {
    datasets
        .iter()
        .map(dataset_to_stac_item)
        .collect()
}

pub fn dataset_to_stac_item(dataset: &DatasetRef) -> StacItem {
    let item_path = format!(
        "/stac/collections/{DEFAULT_COLLECTION_ID}/items/{}",
        dataset.id
    );
    let format_str = match dataset.format {
        DatasetFormat::Cog => "cog",
        DatasetFormat::Icechunk => "icechunk",
    };

    StacItem {
        type_: "Feature".into(),
        stac_version: "1.0.0".into(),
        id: dataset.id.to_string(),
        collection: DEFAULT_COLLECTION_ID.into(),
        geometry: None,
        bbox: None,
        properties: serde_json::json!({
            "title": dataset.name,
            "mantle:format": format_str,
            "proj:epsg": dataset.crs,
        }),
        assets: serde_json::json!({
            "data": {
                "href": dataset.storage_uri,
                "roles": ["data"],
                "title": dataset.name,
                "type": asset_media_type(dataset.format),
            }
        }),
        links: vec![
            link("self", &item_path, Some("application/geo+json")),
            link(
                "parent",
                &format!("/stac/collections/{DEFAULT_COLLECTION_ID}"),
                Some("application/json"),
            ),
            link("root", "/stac", Some("application/json")),
            link("collection", &format!("/stac/collections/{DEFAULT_COLLECTION_ID}"), Some("application/json")),
        ],
    }
}

fn asset_media_type(format: DatasetFormat) -> &'static str {
    match format {
        DatasetFormat::Cog => "image/tiff; application=geotiff; profile=cloud-optimized",
        DatasetFormat::Icechunk => "application/vnd+zarr",
    }
}

pub fn build_item_collection(features: Vec<StacItem>, matched: u64) -> StacItemCollection {
    let returned = features.len() as u64;
    StacItemCollection {
        type_: "FeatureCollection".into(),
        stac_version: "1.0.0".into(),
        features,
        number_matched: Some(matched),
        number_returned: Some(returned),
        links: vec![
            link("self", "/stac/search", Some("application/geo+json")),
            link("root", "/stac", Some("application/json")),
        ],
    }
}

pub fn build_collection_items(features: Vec<StacItem>, collection_id: &str) -> StacItemCollection {
    let returned = features.len() as u64;
    StacItemCollection {
        type_: "FeatureCollection".into(),
        stac_version: "1.0.0".into(),
        features,
        number_matched: Some(returned),
        number_returned: Some(returned),
        links: vec![
            link(
                "self",
                &format!("/stac/collections/{collection_id}/items"),
                Some("application/geo+json"),
            ),
            link(
                "parent",
                &format!("/stac/collections/{collection_id}"),
                Some("application/json"),
            ),
            link("root", "/stac", Some("application/json")),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_arrow::DatasetFormat;
    use uuid::Uuid;

    fn sample_dataset() -> DatasetRef {
        DatasetRef {
            id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            name: "sentinel-2-tile".into(),
            format: DatasetFormat::Cog,
            storage_uri: "s3://mantle-data/tiles/s2.tif".into(),
            crs: Some("EPSG:4326".into()),
        }
    }

    #[test]
    fn stac_item_has_required_fields() {
        let item = dataset_to_stac_item(&sample_dataset());
        assert_eq!(item.type_, "Feature");
        assert_eq!(item.stac_version, "1.0.0");
        assert_eq!(item.collection, DEFAULT_COLLECTION_ID);
        assert_eq!(item.id, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn stac_item_asset_points_to_storage_uri() {
        let item = dataset_to_stac_item(&sample_dataset());
        let assets = item.assets.as_object().expect("assets object");
        let data = assets.get("data").expect("data asset");
        assert_eq!(
            data.get("href").and_then(|v| v.as_str()),
            Some("s3://mantle-data/tiles/s2.tif")
        );
    }

    #[test]
    fn stac_item_serializes_to_geojson_shape() {
        let item = dataset_to_stac_item(&sample_dataset());
        let json = serde_json::to_value(&item).expect("serialize");
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("Feature"));
        assert!(json.get("stac_version").is_some());
        assert!(json.get("collection").is_some());
        assert!(json.get("assets").is_some());
        assert!(json.get("links").and_then(|v| v.as_array()).is_some_and(|a| !a.is_empty()));
    }

    #[test]
    fn item_collection_includes_context_fields() {
        let features = datasets_to_stac_items(&[sample_dataset()]);
        let collection = build_item_collection(features, 1);
        assert_eq!(collection.type_, "FeatureCollection");
        assert_eq!(collection.number_matched, Some(1));
        assert_eq!(collection.number_returned, Some(1));
    }
}
