use anyhow::{bail, Result};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

use crate::{decoder::Decoder, resp_protocol::RespValue};

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr).await?,
        })
    }

    pub async fn listen(&mut self) -> Result<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            tokio::spawn(async move {
                if let Err(err) = handle_connection(stream).await {
                    println!("{:?}", err);
                }
            });
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    let (reader, mut writer) = stream.split();
    let mut decoder = Decoder::new(BufReader::new(reader));
    loop {
        match decoder.next().await {
            Some(Ok(RespValue::Array(commands))) => {
                for command in commands {
                    if let RespValue::BulkString(cmd) = command {
                        process_command(&mut writer, &cmd).await?;
                    } else {
                        bail!("unexpected command element type from client: {:?}", command);
                    }
                }
            }
            Some(Ok(_)) => bail!("unexpected root value type from client"),
            Some(Err(err)) => return Err(err),
            None => return Ok(()), // end of stream
        }
    }
}

async fn process_command<W>(writer: &mut W, cmd: &str) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    match cmd.to_uppercase().as_str() {
        "PING" => {
            writer.write_all("+PONG\r\n".as_bytes()).await?;
        }
        _ => println!("Unexpected command: {}", cmd),
    };
    Ok(())
}
