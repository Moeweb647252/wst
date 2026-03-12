use anyhow::Result;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info};

async fn handle_connect(conn: TcpStream, url: &'static str) -> Result<()> {
    debug!("Websocket connecting");
    let (ws, _) = tokio_tungstenite::connect_async(url).await?;
    debug!("Websocket connected");
    let (mut ws_tx, mut ws_rx) = ws.split();
    let (mut conn_rx, mut conn_tx) = conn.into_split();
    ws_tx.send(Message::Text("Hello".into())).await?;
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

pub async fn run_client(url: &'static str, bind: SocketAddr) -> Result<()> {
    info!("Client started");
    let socket = TcpListener::bind(bind).await?;
    loop {
        let (conn, socket_addr) = socket.accept().await?;
        info!("New connection from {}", socket_addr);
        tokio::spawn(async move {
            handle_connect(conn, url)
                .await
                .inspect(|e| tracing::error!("Error: {:?}", e))
        });
    }
}
