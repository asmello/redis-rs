mod decoder;
mod resp_protocol;
mod server;

use anyhow::Result;
use server::Server;

fn main() -> Result<()> {
    Server::new("127.0.0.1:6379")?.listen()?;
    Ok(())
}
