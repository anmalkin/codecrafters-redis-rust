use std::io::Write;
use std::net::TcpStream;
use std::str::from_utf8;

use crate::errors::RESPError;

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
    args: &[RedisValue],
) -> Result<(), RESPError> {
    if args.is_empty() {
        return Err(RESPError::InvalidCommand);
    }
    let cmd = args.first().unwrap();
    if let RedisValue::String(str) = cmd {
        match &str[..] {
            "PING" => stream.write_all(b"+PONG\r\n")?,
            "ECHO" => {
                if args.is_empty() {
                    return Err(RESPError::InvalidCommand);
                }
                if let RedisValue::String(str) = args.first().unwrap() {
                    let length = str.len();
                    stream.write_all(format!("${}\r\n{}\r\n", length, str).as_bytes())?;
                }
            }
            _ => return Err(RESPError::InvalidCommand),
        }
    }
    Err(RESPError::InvalidCommand)
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
