use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::str::from_utf8;

use crate::errors::RESPError;

const NULL: &[u8] = b"$-1\r\n";
const OK: &[u8] = b"+OK\r\n";

pub type KVStore = HashMap<String, String>;

#[derive(Debug, PartialEq, Clone)]
pub enum RedisValue {
    String(String),
    Error(String),
    Int(i64),
    Array(Vec<RedisValue>),
    NullArray,
    NullBulkString,
}

pub fn parse(buf: &[u8], pos: usize) -> RedisResult {
    if buf.is_empty() {
        return Ok(None);
    }
    match buf[pos] {
        b'*' => array(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b'+' => simple_string(buf, pos + 1),
        b'-' => error(buf, pos + 1),
        b':' => redis_int(buf, pos + 1),
        _ => Err(RESPError::UnknownStartingByte),
    }
}

pub fn execute(
    stream: &mut TcpStream,
    msg: &[RedisValue],
    store: &mut KVStore,
) -> Result<(), RESPError> {
    if msg.is_empty() {
        return Err(RESPError::UnknownStartingByte);
    }

    let cmd = match msg.first().unwrap() {
        RedisValue::String(str) => str,
        _ => return Err(RESPError::InvalidCommand),
    };

    match cmd.as_str() {
        "PING" => stream.write_all(b"+PONG\r\n")?,
        "ECHO" => {
            if msg.len() < 2 {
                return Err(RESPError::InvalidArguments);
            }
            if let Some(RedisValue::String(str)) = msg.get(1) {
                let length = str.len();
                stream.write_all(format!("${}\r\n{}\r\n", length, str).as_bytes())?;
            }
        }
        "GET" => {
            if msg.len() < 2 {
                return Err(RESPError::InvalidArguments);
            }
            if let Some(RedisValue::String(key)) = msg.get(1) {
                match store.get(key) {
                    Some(value) => {
                        let len = value.len();
                        stream.write_all(format!("${}\r\n{}\r\n", len, value).as_bytes())?;
                    },
                    None => stream.write_all(NULL)?,
                }
            } else {
                return Err(RESPError::InvalidArguments);
            }
        },
        "SET" => {
            if msg.len() < 3 {
                return Err(RESPError::InvalidArguments);
            }
            let key = match msg.get(1).unwrap() {
                RedisValue::String(key) => key,
                _ => return Err(RESPError::InvalidArguments)
            };
            let value = match msg.get(2).unwrap() {
                RedisValue::String(value) => value,
                _ => return Err(RESPError::InvalidArguments)
            };
            store.insert(key.to_owned(), value.to_owned());
            stream.write_all(OK)?;

        },
        _ => return Err(RESPError::InvalidCommand),
    }
    Ok(())
}

type RedisResult = Result<Option<(usize, RedisValue)>, RESPError>;

struct BufSplit(usize, usize);

impl BufSplit {
    /// Get a lifetime appropriate slice of the underlying buffer.
    ///
    /// Constant time.
    #[inline]
    fn as_slice<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
        &buf[self.0..self.1]
    }
}

// Get a word from `buf` starting at `pos`
fn word(buf: &[u8], pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() <= pos {
        return None;
    }
    let mut end = pos;
    while buf[end] != b'\r' {
        end += 1;
        if buf.len() < end + 1 {
            return None;
        }
    }
    Some((end + 2, BufSplit(pos, end)))
}

fn simple_string(buf: &[u8], pos: usize) -> RedisResult {
    match word(buf, pos) {
        Some((pos, word)) => {
            let str = from_utf8(word.as_slice(buf)).unwrap();
            let res = RedisValue::String(str.to_string());
            Ok(Some((pos, res)))
        }
        None => Ok(None),
    }
}

fn bulk_string(buf: &[u8], pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, RedisValue::NullBulkString))),
        Some((pos, size)) if size >= 0 => {
            let total_size = pos + size as usize;
            if buf.len() < total_size + 2 {
                Ok(None)
            } else {
                simple_string(buf, pos)
            }
        }
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Ok(None),
    }
}

fn error(buf: &[u8], pos: usize) -> RedisResult {
    match word(buf, pos) {
        Some((pos, word)) => {
            let str = from_utf8(word.as_slice(buf)).unwrap();
            let res = RedisValue::Error(str.to_string());
            Ok(Some((pos, res)))
        }
        None => Ok(None),
    }
}

fn int(buf: &[u8], pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    match word(buf, pos) {
        Some((pos, word)) => {
            let s =
                std::str::from_utf8(word.as_slice(buf)).map_err(|_| RESPError::IntParseFailure)?;
            let i = str::parse(s).map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        }
        None => Ok(None),
    }
}

fn redis_int(buf: &[u8], pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, i)) => Ok(Some((pos, RedisValue::Int(i)))),
        None => Ok(None),
    }
}

fn array(buf: &[u8], pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, RedisValue::NullArray))),
        Some((pos, arr_size)) if arr_size >= 0 => {
            let mut res: Vec<RedisValue> = Vec::with_capacity(arr_size as usize);
            let mut curr_pos = pos;
            for _ in 0..arr_size {
                match parse(buf, curr_pos)? {
                    Some((pos, val)) => {
                        res.push(val);
                        curr_pos = pos;
                    }
                    None => return Err(RESPError::UnexpectedEnd),
                }
            }
            Ok(Some((pos, RedisValue::Array(res))))
        }
        Some((_pos, bad_size)) => Err(RESPError::BadArraySize(bad_size)),
        None => Ok(None),
    }
}
