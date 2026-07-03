//! Cross-boundary flow: Process job 202, poll, S3 result. Run with `--features integration -- --ignored`. See docs/operations.md.

#[cfg(feature = "integration")]
use mantle_integration::stack;

#[tokio::test]
#[ignore = "requires full stack (api, redis, analytics-worker, ray); see docs/operations.md"]
#[cfg(feature = "integration")]
async fn process_job_async_flow() {
    stack::require_api_healthy().await;

    let accepted = stack::submit_process("ndvi", &[]).await;
    assert!(accepted.status_url.starts_with("/status/"));

    let status = stack::poll_job_until_terminal(accepted.job_id, 60).await;
    assert_eq!(
        status.state, "succeeded",
        "job failed: {:?}",
        status.error
    );
    assert!(
        status.result_url.as_deref().unwrap_or("").starts_with("s3://"),
        "expected S3 result_url, got {:?}",
        status.result_url
    );
}

#[tokio::test]
#[ignore = "requires full stack (api, redis, analytics-worker, ray); see docs/operations.md"]
#[cfg(not(feature = "integration"))]
async fn process_job_async_flow() {
    panic!("enable --features integration to compile the live process→poll flow");
}
