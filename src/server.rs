use std::{
    io::{BufReader, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

use anyhow::{bail, Result};

use crate::{decoder::Decoder, resp_protocol::RespValue};

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr)?,
        })
    }

    pub fn listen(&mut self) -> Result<()> {
        for stream in self.listener.incoming() {
            handle_connection(stream?)?;
        }
        Ok(())
    }
}

fn process_command(stream: &mut TcpStream, cmd: &str) -> Result<()> {
    match cmd.to_uppercase().as_str() {
        "PING" => {
            stream.write("+PONG\r\n".as_bytes())?;
        }
        _ => println!("Unexpected command: {}", cmd),
    };
    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    for item in Decoder::new(BufReader::new(stream.try_clone()?)) {
        if let RespValue::Array(commands) = item? {
            for command in commands {
                if let RespValue::BulkString(cmd) = command {
                    process_command(&mut stream, &cmd)?;
                } else {
                    bail!("unexpected command type from client: {:?}", command);
                }
            }
        } else {
            bail!("unexpected root value from client");
        }
    }
    Ok(())
}
