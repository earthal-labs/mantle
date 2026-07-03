use chrono::{DateTime, Utc};
use geo_types::{coord, Rect};
use mantle_catalog::SpatialQuery;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StacSearchRequest {
    pub collections: Option<Vec<String>>,
    pub bbox: Option<Vec<f64>>,
    pub datetime: Option<String>,
    pub limit: Option<u32>,
    pub query: Option<Value>,
    /// CQL2-JSON filter (STAC Item Search `filter` body field).
    pub filter: Option<Value>,
}

impl StacSearchRequest {
    pub const ROUTE: &'static str = "/stac/search";
    pub const DEFAULT_LIMIT: u32 = 10;
    pub const MAX_LIMIT: u32 = 10_000;

    pub fn effective_limit(&self) -> u32 {
        self.limit
            .unwrap_or(Self::DEFAULT_LIMIT)
            .min(Self::MAX_LIMIT)
    }

    pub fn to_spatial_query(&self) -> SpatialQuery {
        let (datetime_start, datetime_end) = self
            .datetime
            .as_deref()
            .map(parse_datetime_range)
            .transpose()
            .ok()
            .flatten()
            .unwrap_or((None, None));

        SpatialQuery {
            bbox: self.bbox.as_ref().and_then(|b| parse_bbox(b.as_slice())),
            datetime_start,
            datetime_end,
            cloud_cover_max: extract_cloud_cover_max(self.query.as_ref())
                .or_else(|| extract_cloud_cover_from_cql(self.filter.as_ref())),
        }
    }

    pub fn matches_collections(&self, collection_id: &str) -> bool {
        match &self.collections {
            None => true,
            Some(ids) => ids.iter().any(|id| id == collection_id),
        }
    }
}

fn parse_bbox(bbox: &[f64]) -> Option<Rect<f64>> {
    if bbox.len() == 6 {
        // STAC 6-element bbox: minx, miny, [z|t], maxx, maxy, [z|t]
        Some(Rect::new(
            coord! { x: bbox[0], y: bbox[1] },
            coord! { x: bbox[3], y: bbox[4] },
        ))
    } else if bbox.len() >= 4 {
        Some(Rect::new(
            coord! { x: bbox[0], y: bbox[1] },
            coord! { x: bbox[2], y: bbox[3] },
        ))
    } else {
        None
    }
}

/// Parse STAC `datetime` — instant, closed/open interval (`start/end`, `../end`, `start/..`).
fn parse_datetime_range(value: &str) -> Result<(Option<DateTime<Utc>>, Option<DateTime<Utc>>), chrono::ParseError> {
    if value.contains('/') {
        let (start_raw, end_raw) = value.split_once('/').unwrap_or((value, ""));
        let start = if start_raw == ".." {
            None
        } else {
            Some(parse_datetime(start_raw)?)
        };
        let end = if end_raw == ".." {
            None
        } else {
            Some(parse_datetime(end_raw)?)
        };
        Ok((start, end))
    } else {
        let instant = parse_datetime(value)?;
        Ok((Some(instant), Some(instant)))
    }
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|dt| dt.with_timezone(&Utc))
}

/// STAC Item Search `query` object, e.g. `{ "eo:cloud_cover": { "lte": 20 } }`.
fn extract_cloud_cover_max(query: Option<&Value>) -> Option<f64> {
    let query = query?;
    let cloud = query.get("eo:cloud_cover")?;
    cloud_cover_lte(cloud)
}

/// CQL2-JSON filter — supports nested `and` and `<=` on `eo:cloud_cover`.
fn extract_cloud_cover_from_cql(filter: Option<&Value>) -> Option<f64> {
    let filter = filter?;
    walk_cql_for_cloud_cover(filter)
}

fn walk_cql_for_cloud_cover(value: &Value) -> Option<f64> {
    if let Some(op) = value.get("op").and_then(Value::as_str) {
        if op == "<=" || op == "lte" {
            if let Some(args) = value.get("args").and_then(Value::as_array) {
                if args.len() >= 2 && is_cloud_cover_property(&args[0]) {
                    return args[1].as_f64();
                }
            }
        }
        if op == "and" {
            if let Some(args) = value.get("args").and_then(Value::as_array) {
                for arg in args {
                    if let Some(max) = walk_cql_for_cloud_cover(arg) {
                        return Some(max);
                    }
                }
            }
        }
    }
    None
}

fn is_cloud_cover_property(value: &Value) -> bool {
    value
        .get("property")
        .and_then(Value::as_str)
        .is_some_and(|p| p == "eo:cloud_cover")
}

fn cloud_cover_lte(value: &Value) -> Option<f64> {
    value
        .get("lte")
        .and_then(Value::as_f64)
        .or_else(|| value.get("<=").and_then(Value::as_f64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::coord;

    #[test]
    fn bbox_maps_to_spatial_query() {
        let req = StacSearchRequest {
            bbox: Some(vec![-10.0, -5.0, 10.0, 5.0]),
            ..Default::default()
        };
        let query = req.to_spatial_query();
        let bbox = query.bbox.expect("bbox");
        assert_eq!(bbox.min(), coord! { x: -10.0, y: -5.0 });
        assert_eq!(bbox.max(), coord! { x: 10.0, y: 5.0 });
    }

    #[test]
    fn six_element_bbox_ignores_temporal_components() {
        let req = StacSearchRequest {
            bbox: Some(vec![-10.0, -5.0, 0.0, 10.0, 5.0, 1.0]),
            ..Default::default()
        };
        let query = req.to_spatial_query();
        let bbox = query.bbox.expect("bbox");
        assert_eq!(bbox.min().x, -10.0);
        assert_eq!(bbox.max().x, 10.0);
    }

    #[test]
    fn datetime_closed_interval() {
        let req = StacSearchRequest {
            datetime: Some("2020-01-01T00:00:00Z/2020-12-31T23:59:59Z".into()),
            ..Default::default()
        };
        let query = req.to_spatial_query();
        assert!(query.datetime_start.is_some());
        assert!(query.datetime_end.is_some());
    }

    #[test]
    fn datetime_open_start() {
        let req = StacSearchRequest {
            datetime: Some("../2020-12-31T23:59:59Z".into()),
            ..Default::default()
        };
        let query = req.to_spatial_query();
        assert!(query.datetime_start.is_none());
        assert!(query.datetime_end.is_some());
    }

    #[test]
    fn datetime_instant_sets_both_bounds() {
        let req = StacSearchRequest {
            datetime: Some("2021-06-15T12:00:00Z".into()),
            ..Default::default()
        };
        let query = req.to_spatial_query();
        assert_eq!(query.datetime_start, query.datetime_end);
    }

    #[test]
    fn query_cloud_cover_lte() {
        let req = StacSearchRequest {
            query: Some(serde_json::json!({ "eo:cloud_cover": { "lte": 15.5 } })),
            ..Default::default()
        };
        let query = req.to_spatial_query();
        assert_eq!(query.cloud_cover_max, Some(15.5));
    }

    #[test]
    fn cql2_cloud_cover_filter() {
        let req = StacSearchRequest {
            filter: Some(serde_json::json!({
                "op": "<=",
                "args": [
                    { "property": "eo:cloud_cover" },
                    20
                ]
            })),
            ..Default::default()
        };
        let query = req.to_spatial_query();
        assert_eq!(query.cloud_cover_max, Some(20.0));
    }

    #[test]
    fn collections_filter_matching() {
        let req = StacSearchRequest {
            collections: Some(vec!["mantle".into(), "other".into()]),
            ..Default::default()
        };
        assert!(req.matches_collections("mantle"));
        assert!(!req.matches_collections("missing"));
    }

    #[test]
    fn effective_limit_caps_at_max() {
        let req = StacSearchRequest {
            limit: Some(99_999),
            ..Default::default()
        };
        assert_eq!(req.effective_limit(), StacSearchRequest::MAX_LIMIT);
    }
}
