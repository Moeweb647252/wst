use anyhow::{Context, Result, bail};
use http_body_util::channel::Channel;
use http_body_util::BodyExt;
use hyper::client::conn::http2;
use hyper::header::HOST;
use hyper::{Method, Request, StatusCode, Uri, Version};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tracing::{debug, error, info};

use crate::transport::{join_error_to_anyhow, pump_body_to_writer, pump_reader_to_body};

#[derive(Clone, Debug)]
struct Endpoint {
    authority: String,
    path_and_query: String,
    server_addr: SocketAddr,
}

async fn resolve_endpoint(url: &'static str) -> Result<Endpoint> {
    let uri: Uri = url.parse().context("url should be a valid http URI")?;
    let scheme = uri
        .scheme_str()
        .context("url should include a scheme such as http://")?;
    if scheme != "http" {
        bail!("only cleartext http:// is supported for HTTP/2");
    }

    let authority = uri
        .authority()
        .context("url should include host and port")?
        .as_str()
        .to_owned();
    let host = uri.host().context("url should include a host")?;
    let port = uri.port_u16().unwrap_or(80);
    let server_addr = tokio::net::lookup_host((host, port))
        .await?
        .next()
        .with_context(|| format!("failed to resolve {host}:{port}"))?;
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .unwrap_or_else(|| "/".to_owned());

    Ok(Endpoint {
        authority,
        path_and_query,
        server_addr,
    })
}

async fn handle_connect(conn: TcpStream, endpoint: Endpoint) -> Result<()> {
    debug!("HTTP/2 connecting");
    let stream = TcpStream::connect(endpoint.server_addr).await?;
    let (mut sender, connection) = http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(stream))
        .await?;
    let connection_task: JoinHandle<Result<()>> = tokio::spawn(async move {
        connection.await?;
        Ok(())
    });
    debug!("HTTP/2 connected");

    let (request_tx, request_body) = Channel::<bytes::Bytes, std::io::Error>::new(8);
    let request = Request::builder()
        .method(Method::POST)
        .uri(endpoint.path_and_query)
        .version(Version::HTTP_2)
        .header(HOST, endpoint.authority)
        .body(request_body.boxed())?;
    let response = sender.send_request(request).await?;
    if response.status() != StatusCode::OK {
        bail!("tunnel server returned unexpected status {}", response.status());
    }

    let (conn_rx, conn_tx) = conn.into_split();
    let mut request_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { pump_reader_to_body(conn_rx, request_tx).await });
    let mut response_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { pump_body_to_writer(response.into_body(), conn_tx).await });

    let result: Result<()> = tokio::select! {
        result = &mut request_handle => {
            response_handle.abort();
            let _ = response_handle.await;
            result.map_err(join_error_to_anyhow)?
        }
        result = &mut response_handle => {
            request_handle.abort();
            let _ = request_handle.await;
            result.map_err(join_error_to_anyhow)?
        }
    };

    connection_task.abort();
    let _ = connection_task.await;
    result
}

pub async fn run_client(url: &'static str, bind: SocketAddr) -> Result<()> {
    info!("Client started");
    let endpoint = resolve_endpoint(url).await?;
    let socket = TcpListener::bind(bind).await?;
    loop {
        let (conn, socket_addr) = socket.accept().await?;
        info!("New connection from {}", socket_addr);
        let endpoint = endpoint.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connect(conn, endpoint).await {
                error!("Error: {:?}", err);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_http_endpoint() -> Result<()> {
        let endpoint = resolve_endpoint("http://127.0.0.1:8080/tunnel?foo=bar").await?;
        assert_eq!(endpoint.authority, "127.0.0.1:8080");
        assert_eq!(endpoint.path_and_query, "/tunnel?foo=bar");
        assert_eq!(endpoint.server_addr, "127.0.0.1:8080".parse()?);
        Ok(())
    }
}
