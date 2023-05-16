#[derive(Debug, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RespValue>),
}
