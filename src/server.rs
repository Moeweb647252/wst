use anyhow::Result;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use std::ops::Not;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::transport::{
    TunnelBody, join_error_to_anyhow, pump_body_to_writer, pump_reader_to_body, static_response,
    streaming_response,
};

async fn handle_request(
    req: Request<Incoming>,
    target: SocketAddr,
    path: &'static str,
) -> Result<Response<TunnelBody>> {
    if req.uri().path().eq(path).not() {
        info!("Incoming path not match: {}", req.uri().path());
        return Ok(static_response(StatusCode::NOT_FOUND, "Not Found"));
    }
    if req.method() != Method::POST {
        return Ok(static_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Method Not Allowed",
        ));
    }

    let req_body = req.into_body();
    let conn = match tokio::net::TcpStream::connect(target).await {
        Ok(conn) => conn,
        Err(err) => {
            error!("Failed to connect target {target}: {err:?}");
            return Ok(static_response(StatusCode::BAD_GATEWAY, "Bad Gateway"));
        }
    };
    let (conn_rx, conn_tx) = conn.into_split();
    let (body_tx, response) = streaming_response();

    tokio::spawn(async move {
        let mut request_handle: JoinHandle<Result<()>> =
            tokio::spawn(async move { pump_body_to_writer(req_body, conn_tx).await });
        let mut response_handle: JoinHandle<Result<()>> =
            tokio::spawn(async move { pump_reader_to_body(conn_rx, body_tx).await });

        let result = tokio::select! {
            result = &mut request_handle => {
                response_handle.abort();
                let _ = response_handle.await;
                result.map_err(join_error_to_anyhow).and_then(|result| result)
            }
            result = &mut response_handle => {
                request_handle.abort();
                let _ = request_handle.await;
                result.map_err(join_error_to_anyhow).and_then(|result| result)
            }
        };

        if let Err(err) = result {
            error!("Tunnel relay failed: {err:?}");
        }
    });

    Ok(response)
}

pub async fn run_server(bind: SocketAddr, target: SocketAddr, path: &'static str) -> Result<()> {
    info!("Server started");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    loop {
        let (socket, socket_addr) = listener.accept().await?;
        info!("New connection from {}", socket_addr);
        let builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        let connection = builder.serve_connection(
            TokioIo::new(socket),
            service_fn(move |req| handle_request(req, target, path)),
        );
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
    use crate::transport::encode_payload;
    use http_body_util::BodyExt;
    use http_body_util::channel::Channel;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn http2_tunnel_forwards_request_body() -> Result<()> {
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
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(socket),
                    service_fn(move |req| handle_request(req, target_addr, "/ws")),
                )
                .await?;
            Ok(())
        });

        let stream = tokio::net::TcpStream::connect(server_addr).await?;
        let (mut sender, connection) =
            hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                .handshake(TokioIo::new(stream))
                .await?;
        let connection_task: JoinHandle<Result<()>> = tokio::spawn(async move {
            connection.await?;
            Ok(())
        });
        let (mut request_tx, request_body) = Channel::<bytes::Bytes, std::io::Error>::new(8);
        let response = sender
            .send_request(
                Request::builder()
                    .method(Method::POST)
                    .uri("/ws")
                    .header(hyper::header::HOST, server_addr.to_string())
                    .body(request_body.boxed())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);

        request_tx.send_data(encode_payload(&[1, 2, 3])).await?;
        drop(request_tx);
        response.into_body().collect().await?;

        target_task.await??;
        connection_task.abort();
        let _ = connection_task.await;
        server_task.await??;
        Ok(())
    }
}
