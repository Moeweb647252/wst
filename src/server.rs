use anyhow::Result;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Response, StatusCode};
use hyper_tungstenite::tungstenite::Message;
use hyper_tungstenite::{self, is_upgrade_request, upgrade};
use hyper_tungstenite::{HyperWebsocket, hyper};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::ops::Not;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tracing::{error, info};

async fn handle_request(
    mut req: hyper::Request<Incoming>,
    target: SocketAddr,
    path: &'static str,
) -> Result<hyper::Response<Full<Bytes>>> {
    if req.uri().path().eq(path).not() {
        info!("Incoming path not match: {}", req.uri().path());
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::<Bytes>::from("Not Found"))?);
    }
    if is_upgrade_request(&req) {
        let (resp, ws) = upgrade(&mut req, None)?;
        tokio::spawn(async move {
            handle_redirect(ws, target)
                .await
                .inspect_err(|e| error!("Error: {:?}", e))
                .ok();
        });
        Ok(resp)
    } else {
        Ok(Response::builder()
            .status(StatusCode::UPGRADE_REQUIRED)
            .body(Full::<Bytes>::from("Upgrade Required"))?)
    }
}

async fn handle_redirect(ws: HyperWebsocket, target: SocketAddr) -> Result<()> {
    let conn = tokio::net::TcpStream::connect(target).await?;
    let (mut conn_rx, mut conn_tx) = conn.into_split();
    let ws = ws.await?;
    let (mut ws_tx, mut ws_rx) = ws.split();
    let mut ws_rx_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        while let Some(message) = ws_rx.next().await {
            match message? {
                Message::Binary(msg) => {
                    conn_tx.write_all(&msg).await?;
                }
                _ => {}
            }
        }
        Ok(())
    });
    let mut conn_rx_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        loop {
            let mut buffer = BytesMut::with_capacity(16 * 1024);
            let read = conn_rx.read_buf(&mut buffer).await?;
            if read == 0 {
                break;
            }
            ws_tx.send(Message::Binary(buffer.into())).await?;
        }
        Ok(())
    });
    tokio::select! {
        result = &mut ws_rx_handle => {
            conn_rx_handle.abort();
            let _ = conn_rx_handle.await;
            result??;
        }
        result = &mut conn_rx_handle => {
            ws_rx_handle.abort();
            let _ = ws_rx_handle.await;
            result??;
        }
    }
    Ok(())
}

pub async fn run_server(bind: SocketAddr, target: SocketAddr, path: &'static str) -> Result<()> {
    info!("Server started");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let mut http = hyper::server::conn::http1::Builder::new();
    http.keep_alive(true);
    loop {
        let (socket, socket_addr) = listener.accept().await?;
        info!("New connection from {}", socket_addr);
        let connection = http
            .serve_connection(
                TokioIo::new(socket),
                service_fn(move |req| handle_request(req, target, path)),
            )
            .with_upgrades();
        tokio::spawn(async move {
            connection
                .await
                .inspect_err(|e| error!("Error: {:?}", e))
                .ok();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn websocket_upgrade_completes() -> Result<()> {
        let target_listener = TcpListener::bind("127.0.0.1:0").await?;
        let target_addr = target_listener.local_addr()?;
        let target_task: JoinHandle<Result<()>> = tokio::spawn(async move {
            let (mut socket, _) = target_listener.accept().await?;
            let mut buffer = [0_u8; 3];
            socket.read_exact(&mut buffer).await?;
            assert_eq!(buffer, [1, 2, 3]);
            Ok(())
        });

        let server_listener = TcpListener::bind("127.0.0.1:0").await?;
        let server_addr = server_listener.local_addr()?;
        let server_task: JoinHandle<Result<()>> = tokio::spawn(async move {
            let (socket, _) = server_listener.accept().await?;
            let mut http = hyper::server::conn::http1::Builder::new();
            http.keep_alive(true);
            http.serve_connection(
                TokioIo::new(socket),
                service_fn(move |req| handle_request(req, target_addr, "/ws")),
            )
            .with_upgrades()
            .await?;
            Ok(())
        });

        let url = format!("ws://{server_addr}/ws");
        let (mut ws, response) = tokio_tungstenite::connect_async(url).await?;
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        ws.send(Message::Binary(vec![1, 2, 3].into())).await?;
        ws.close(None).await?;

        target_task.await??;
        server_task.await??;
        Ok(())
    }
}
