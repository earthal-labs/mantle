//! API health when compose stack is up. Run with:
//! `MANTLE_SMOKE_TEST=1 cargo test -p mantle-integration-tests stack_api_health --features integration`

#[cfg(feature = "integration")]
use mantle_integration::stack;

#[tokio::test]
#[cfg(feature = "integration")]
async fn stack_api_health() {
    if std::env::var("MANTLE_SMOKE_TEST").ok().as_deref() != Some("1") {
        eprintln!("skip stack_api_health (set MANTLE_SMOKE_TEST=1 with compose up)");
        return;
    }
    stack::require_api_healthy().await;
}

#[tokio::test]
#[cfg(not(feature = "integration"))]
async fn stack_api_health() {
    panic!("enable --features integration for stack_api_health");
}
