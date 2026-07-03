use mantle_config::MantleConfig;
use mantle_worker::CacheWarmer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("mantle_worker=info".parse()?))
        .init();

    let config_path = std::env::var("MANTLE_CONFIG").unwrap_or_else(|_| "config.toml".into());
    let config = MantleConfig::from_file(&config_path)?;
    let warmer = CacheWarmer::new(config).await?;
    warmer.run().await
}
