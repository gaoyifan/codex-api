#[tokio::main]
async fn main() -> anyhow::Result<()> {
    codex_api::run_cli().await
}
