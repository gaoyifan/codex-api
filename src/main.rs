use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "codex-api")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    codex_api::run(&args.config).await
}
