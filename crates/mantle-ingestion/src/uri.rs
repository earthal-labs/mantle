//! Storage URI validation for cloud-reference ingestion.

use crate::IngestionError;

/// Supported remote reference schemes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceScheme {
    S3,
    Https,
}

/// Detected multidimensional / raster format from URI path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceFormat {
    Cog,
    NetCdf,
    Hdf5,
    Unknown,
}

/// Parsed, validated cloud reference URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedUri {
    pub raw: String,
    pub scheme: ReferenceScheme,
    pub format: ReferenceFormat,
}

/// Validate an external storage URI for Pathway B ingestion.
pub fn validate_storage_uri(uri: &str) -> Result<ValidatedUri, IngestionError> {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return Err(IngestionError::InvalidUri("storage_uri must not be empty".into()));
    }
    if trimmed.contains('\0') || trimmed.chars().any(char::is_control) {
        return Err(IngestionError::InvalidUri(
            "storage_uri contains invalid control characters".into(),
        ));
    }

    let (scheme, path_for_ext) = if let Some(rest) = trimmed.strip_prefix("s3://") {
        if rest.is_empty() || !rest.contains('/') {
            return Err(IngestionError::InvalidUri(
                "s3 URI must include bucket and key (s3://bucket/key)".into(),
            ));
        }
        (ReferenceScheme::S3, rest)
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        if rest.is_empty() {
            return Err(IngestionError::InvalidUri(
                "https URI must include a host".into(),
            ));
        }
        (ReferenceScheme::Https, rest)
    } else if trimmed.starts_with("http://") {
        return Err(IngestionError::InvalidUri(
            "http URIs are not permitted; use https://".into(),
        ));
    } else {
        return Err(IngestionError::InvalidUri(
            "storage_uri must use s3:// or https:// scheme".into(),
        ));
    };

    let format = detect_format(path_for_ext);

    Ok(ValidatedUri {
        raw: trimmed.to_string(),
        scheme,
        format,
    })
}

fn detect_format(path: &str) -> ReferenceFormat {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".nc") || lower.ends_with(".nc4") || lower.ends_with(".netcdf") {
        ReferenceFormat::NetCdf
    } else if lower.ends_with(".h5")
        || lower.ends_with(".hdf5")
        || lower.ends_with(".he5")
    {
        ReferenceFormat::Hdf5
    } else if lower.ends_with(".tif")
        || lower.ends_with(".tiff")
        || lower.ends_with(".cog")
        || lower.ends_with(".geotiff")
    {
        ReferenceFormat::Cog
    } else {
        ReferenceFormat::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_s3_cog_uri() {
        let uri = validate_storage_uri("s3://external-bucket/path/to/scene.tif").expect("valid");
        assert_eq!(uri.scheme, ReferenceScheme::S3);
        assert_eq!(uri.format, ReferenceFormat::Cog);
    }

    #[test]
    fn accepts_https_netcdf_uri() {
        let uri =
            validate_storage_uri("https://data.example.org/archive/sst_2024.nc").expect("valid");
        assert_eq!(uri.scheme, ReferenceScheme::Https);
        assert_eq!(uri.format, ReferenceFormat::NetCdf);
    }

    #[test]
    fn rejects_http_scheme() {
        let err = validate_storage_uri("http://insecure.example/data.tif").unwrap_err();
        assert!(matches!(err, IngestionError::InvalidUri(_)));
    }

    #[test]
    fn rejects_malformed_s3_uri() {
        let err = validate_storage_uri("s3://bucket-only").unwrap_err();
        assert!(matches!(err, IngestionError::InvalidUri(_)));
    }

    #[test]
    fn rejects_empty_uri() {
        assert!(validate_storage_uri("").is_err());
        assert!(validate_storage_uri("   ").is_err());
    }
}
