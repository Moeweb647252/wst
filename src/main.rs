use std::str::FromStr;

use clap::Parser;
use tracing::level_filters::LevelFilter;

mod client;
mod server;

#[derive(Parser)]
struct Args {
    #[arg(short, default_value_t = false)]
    client: bool,
    #[arg(short, default_value_t = false)]
    server: bool,
    #[arg(short)]
    bind: String,
    #[arg(short)]
    path: Option<String>,
    #[arg(short)]
    url: Option<String>,
    #[arg(short)]
    target: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    let args = Args::parse();
    if args.client {
        let url: &'static str = args.url.expect("url should be provided").leak();
        let bind = args.bind.parse().expect("bind should be provided");
        client::run_client(url, bind)
            .await
            .expect("Failed to run client");
    } else if args.server {
        let path: &'static str = args.path.expect("path should be provided").leak();
        let bind = args.bind.parse().expect("bind should be provided");
        let target = std::net::SocketAddr::from_str(
            args.target.expect("target should be provided").as_str(),
        )
        .expect("target should be valid socket addr");
        server::run_server(bind, target, path)
            .await
            .expect("Failed to run server");
    } else {
        panic!("client or server mode should be chosen")
    }
}
