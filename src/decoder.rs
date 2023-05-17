use std::io::ErrorKind;

use anyhow::{bail, Result};
use async_recursion::async_recursion;
use tokio::io::AsyncReadExt;

use crate::resp_protocol::RespValue;

const SIMPLE_STRING_MAGIC: u8 = '+' as u8;
// const ERROR_MAGIC: u8 = '-' as u8;
// const INTEGER_MAGIC: u8 = ':' as u8;
const BULK_STRING_MAGIC: u8 = '$' as u8;
const ARRAY_MAGIC: u8 = '*' as u8;

pub struct Decoder<R>
where
    R: AsyncReadExt + Unpin + Send,
{
    reader: R,
}

impl<R> Decoder<R>
where
    R: AsyncReadExt + Unpin + Send,
{
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub async fn next(&mut self) -> Option<Result<RespValue>> {
        match self.reader.read_u8().await {
            Ok(magic) => Some(decode_magic(&mut self.reader, magic).await),
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => None, // no data to read
            Err(err) => Some(Err(err.into())), // different error, propagate
        }
    }
}

#[async_recursion]
pub async fn decode<R>(reader: &mut R) -> Result<RespValue>
where
    R: AsyncReadExt + Unpin + Send,
{
    let magic = reader.read_u8().await?;
    decode_magic(reader, magic).await
}

async fn decode_magic<R>(reader: &mut R, magic: u8) -> Result<RespValue>
where
    R: AsyncReadExt + Unpin + Send,
{
    match magic {
        SIMPLE_STRING_MAGIC => decode_simple_string(reader).await,
        BULK_STRING_MAGIC => decode_bulk_string(reader).await,
        ARRAY_MAGIC => decode_array(reader).await,
        magic => bail!("invalid magic byte: {}", magic),
    }
}

async fn decode_simple_string<R>(reader: &mut R) -> Result<RespValue>
where
    R: AsyncReadExt + Unpin,
{
    Ok(RespValue::SimpleString(
        read_crlf_terminated_string(reader).await?,
    ))
}

async fn decode_bulk_string<R>(reader: &mut R) -> Result<RespValue>
where
    R: AsyncReadExt + Unpin,
{
    let n = read_crlf_terminated_string(reader)
        .await?
        .parse::<usize>()?;
    let mut buffer = vec![0; n + 2]; // includes CRLF
    reader.read_exact(&mut buffer).await?;
    buffer.truncate(n); // remove CRLF
    Ok(RespValue::BulkString(String::from_utf8(buffer)?))
}

async fn decode_array<R>(reader: &mut R) -> Result<RespValue>
where
    R: AsyncReadExt + Unpin + Send,
{
    let n = read_crlf_terminated_string(reader)
        .await?
        .parse::<usize>()?;
    let mut elements = Vec::with_capacity(n);
    for _ in 0..n {
        elements.push(decode(reader).await?);
    }
    Ok(RespValue::Array(elements))
}

async fn read_crlf_terminated_string<R>(reader: &mut R) -> Result<String>
where
    R: AsyncReadExt + Unpin,
{
    let mut buffer = Vec::new();
    loop {
        buffer.push(reader.read_u8().await?);
        let n = buffer.len();
        if n >= 2 && buffer[n - 2] == '\r' as u8 && buffer[n - 1] == '\n' as u8 {
            buffer.truncate(n - 2);
            return Ok(String::from_utf8(buffer)?);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[tokio::test]
    async fn test_decode_simple_string() -> Result<()> {
        let iter = "+PING\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().await.unwrap()?;
        assert_eq!(decoded, RespValue::SimpleString("PING".into()));
        Ok(())
    }

    #[tokio::test]
    async fn test_decode_bulk_string_empty() -> Result<()> {
        let iter = "$0\r\n\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().await.unwrap()?;
        assert_eq!(decoded, RespValue::BulkString("".into()));
        Ok(())
    }

    #[tokio::test]
    async fn test_decode_bulk_string() -> Result<()> {
        let iter = "$5\r\nhello\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().await.unwrap()?;
        assert_eq!(decoded, RespValue::BulkString("hello".into()));
        Ok(())
    }

    #[tokio::test]
    async fn test_decode_array_empty() -> Result<()> {
        let iter = "*0\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().await.unwrap()?;
        assert_eq!(decoded, RespValue::Array(vec![]));
        Ok(())
    }

    #[tokio::test]
    async fn test_decode_array_bulk_strings() -> Result<()> {
        let iter = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().await.unwrap()?;
        assert_eq!(
            decoded,
            RespValue::Array(vec![
                RespValue::BulkString("hello".into()),
                RespValue::BulkString("world".into())
            ])
        );
        Ok(())
    }
}
