use std::io;

use anyhow::{Result, anyhow, bail, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use http_body_util::channel::{Channel, Sender};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Response, StatusCode};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const BODY_CHANNEL_CAPACITY: usize = 8;
const FRAME_HEADER_LEN: usize = 4;
const READ_BUFFER_CAPACITY: usize = 16 * 1024;

pub type TunnelBody = BoxBody<Bytes, io::Error>;
pub type TunnelBodySender = Sender<Bytes, io::Error>;

pub fn static_response(status: StatusCode, body: &'static str) -> Response<TunnelBody> {
    Response::builder()
        .status(status)
        .body(
            Full::new(Bytes::from_static(body.as_bytes()))
                .map_err(|err| match err {})
                .boxed(),
        )
        .expect("static response should be valid")
}

pub fn streaming_response() -> (TunnelBodySender, Response<TunnelBody>) {
    let (tx, body) = Channel::new(BODY_CHANNEL_CAPACITY);
    let response = Response::builder()
        .status(StatusCode::OK)
        .body(body.boxed())
        .expect("streaming response should be valid");
    (tx, response)
}

pub async fn pump_reader_to_body<R>(mut reader: R, mut sender: TunnelBodySender) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    loop {
        let mut buffer = BytesMut::with_capacity(READ_BUFFER_CAPACITY);
        let read = reader.read_buf(&mut buffer).await?;
        if read == 0 {
            break;
        }
        sender
            .send_data(encode_payload(&buffer))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "http body receiver closed"))?;
    }
    Ok(())
}

pub async fn pump_body_to_writer<W>(body: Incoming, mut writer: W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut stream = body.into_data_stream();
    let mut pending = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        pending.extend_from_slice(chunk.as_ref());
        while let Some(frame) = try_decode_frame(&mut pending)? {
            let decompressed = lz4_flex::decompress_size_prepended(&frame)?;
            writer.write_all(&decompressed).await?;
        }
    }
    ensure!(pending.is_empty(), "incomplete frame left in http/2 body");
    writer.shutdown().await?;
    Ok(())
}

pub fn encode_payload(payload: &[u8]) -> Bytes {
    let compressed = lz4_flex::compress_prepend_size(payload);
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_LEN + compressed.len());
    frame.put_u32(
        compressed
            .len()
            .try_into()
            .expect("compressed payload should fit into u32"),
    );
    frame.extend_from_slice(&compressed);
    frame.freeze()
}

fn try_decode_frame(buffer: &mut BytesMut) -> Result<Option<Bytes>> {
    if buffer.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let frame_len = u32::from_be_bytes(
        buffer[..FRAME_HEADER_LEN]
            .try_into()
            .expect("frame header should be complete"),
    ) as usize;
    if frame_len == 0 {
        bail!("received empty frame");
    }
    if buffer.len() < FRAME_HEADER_LEN + frame_len {
        return Ok(None);
    }
    buffer.advance(FRAME_HEADER_LEN);
    Ok(Some(buffer.split_to(frame_len).freeze()))
}

pub fn join_error_to_anyhow(err: tokio::task::JoinError) -> anyhow::Error {
    anyhow!(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_handles_fragmented_input() -> Result<()> {
        let encoded = encode_payload(b"hello http2");
        let split_at = 5;
        let mut pending = BytesMut::new();

        pending.extend_from_slice(&encoded[..split_at]);
        assert!(try_decode_frame(&mut pending)?.is_none());

        pending.extend_from_slice(&encoded[split_at..]);
        let frame = try_decode_frame(&mut pending)?.expect("complete frame expected");
        let decompressed = lz4_flex::decompress_size_prepended(&frame)?;

        assert_eq!(decompressed, b"hello http2");
        assert!(pending.is_empty());
        Ok(())
    }
}
