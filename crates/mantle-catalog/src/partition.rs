//! Partition key strategy for DuckLake footprint Parquet files.
//!
//! Footprints are partitioned by **acquisition month** (`YYYY-MM`) derived from the
//! dataset's `temporal_start` timestamp. When `temporal_start` is absent, the current
//! UTC month is used. Each insert creates a new Parquet object under
//! `{ducklake_data_path}partitions/{partition_key}/` — never rewriting existing
//! partition directories (append-only).

use chrono::{DateTime, Datelike, Utc};

/// Derive the DuckLake partition key from a dataset acquisition timestamp.
pub fn acquisition_month(temporal_start: Option<DateTime<Utc>>) -> String {
    let dt = temporal_start.unwrap_or_else(Utc::now);
    format!("{:04}-{:02}", dt.year(), dt.month())
}

/// Resolve the partition key for a footprint insert, preferring an explicit key.
pub fn resolve_partition_key(
    explicit: &str,
    temporal_start: Option<DateTime<Utc>>,
) -> String {
    if explicit.is_empty() {
        acquisition_month(temporal_start)
    } else {
        explicit.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn acquisition_month_from_temporal_start() {
        let ts = Utc.with_ymd_and_hms(2024, 7, 15, 12, 0, 0).unwrap();
        assert_eq!(acquisition_month(Some(ts)), "2024-07");
    }

    #[test]
    fn resolve_prefers_explicit_key() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(
            resolve_partition_key("custom-part", Some(ts)),
            "custom-part"
        );
    }

    #[test]
    fn resolve_falls_back_to_acquisition_month() {
        let ts = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();
        assert_eq!(resolve_partition_key("", Some(ts)), "2023-12");
    }
}
