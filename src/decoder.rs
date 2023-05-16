use anyhow::{anyhow, bail, Result};
use std::io::Read;

use crate::resp_protocol::RespValue;

const SIMPLE_STRING_MAGIC: u8 = '+' as u8;
// const ERROR_MAGIC: u8 = '-' as u8;
// const INTEGER_MAGIC: u8 = ':' as u8;
const BULK_STRING_MAGIC: u8 = '$' as u8;
const ARRAY_MAGIC: u8 = '*' as u8;

pub struct Decoder<R: Read> {
    iter: std::io::Bytes<R>,
}

impl<R: Read> Decoder<R> {
    pub fn new(stream: R) -> Self {
        Self {
            iter: stream.bytes(),
        }
    }

    pub fn decode(&mut self, magic: u8) -> Result<RespValue> {
        match magic {
            SIMPLE_STRING_MAGIC => self.decode_simple_string(),
            BULK_STRING_MAGIC => self.decode_bulk_string(),
            ARRAY_MAGIC => self.decode_array(),
            _ => bail!("invalid magic byte: {}", magic),
        }
    }

    fn decode_simple_string(&mut self) -> Result<RespValue> {
        Ok(RespValue::SimpleString(self.read_crcf_terminated_string()?))
    }

    fn decode_bulk_string(&mut self) -> Result<RespValue> {
        let n = self.read_crcf_terminated_string()?.parse::<usize>()?;
        let mut string = Vec::with_capacity(n);
        for _ in 0..n {
            let b = self
                .iter
                .next()
                .ok_or(anyhow!("missing bulk string data"))??;
            string.push(b);
        }
        // consume CRLF
        self.iter.next();
        self.iter.next();
        Ok(RespValue::BulkString(String::from_utf8(string)?))
    }

    fn decode_array(&mut self) -> Result<RespValue> {
        let n = self.read_crcf_terminated_string()?.parse::<usize>()?;
        let mut elements = Vec::with_capacity(n);
        for _ in 0..n {
            let magic = self.iter.next().ok_or(anyhow!("missing array element"))??;
            elements.push(self.decode(magic)?);
        }
        Ok(RespValue::Array(elements))
    }

    fn read_crcf_terminated_string(&mut self) -> Result<String> {
        let mut buffer = Vec::new();
        while let Some(res) = self.iter.next() {
            buffer.push(res?);
            let n = buffer.len();
            if n >= 2 && buffer[n - 2] == '\r' as u8 && buffer[n - 1] == '\n' as u8 {
                buffer.truncate(n - 2);
                return Ok(String::from_utf8(buffer)?);
            }
        }
        bail!("incomplete string");
    }
}

impl<R: Read> Iterator for Decoder<R> {
    type Item = Result<RespValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let first_byte = self.iter.next()?;
        Some(match first_byte {
            Ok(magic) => self.decode(magic),
            Err(err) => Err(err.into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_decode_simple_string() -> Result<()> {
        let iter = "+PING\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().unwrap()?;
        assert_eq!(decoded, RespValue::SimpleString("PING".into()));
        Ok(())
    }

    #[test]
    fn test_decode_bulk_string_empty() -> Result<()> {
        let iter = "$0\r\n\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().unwrap()?;
        assert_eq!(decoded, RespValue::BulkString("".into()));
        Ok(())
    }

    #[test]
    fn test_decode_bulk_string() -> Result<()> {
        let iter = "$5\r\nhello\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().unwrap()?;
        assert_eq!(decoded, RespValue::BulkString("hello".into()));
        Ok(())
    }

    #[test]
    fn test_decode_array_empty() -> Result<()> {
        let iter = "*0\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().unwrap()?;
        assert_eq!(decoded, RespValue::Array(vec![]));
        Ok(())
    }

    #[test]
    fn test_decode_array_bulk_strings() -> Result<()> {
        let iter = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let mut decoder = Decoder::new(iter);
        let decoded = decoder.next().unwrap()?;
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
