use bytes::{Bytes, BytesMut};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() -> std::io::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                std::thread::spawn(move || handle_connection(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> Result<(), RESPError> {
    let mut buf = BytesMut::with_capacity(512);
    loop {
        let n = stream.read(&mut buf)?;
        println!("received {} bytes", n);
        println!("len of buf = {}", buf.len());
        let b = Bytes::from(buf.clone());

        if n == 0 {
            break;
        }

        let cmd: RedisCommand;
        let args: &[RedisValue];

        if let Some((_pos, RedisValue::Array(vec))) = parse(&b)? {
            cmd = command(&vec)?;
            args = &vec[1..];
            run(&mut stream, cmd, args)?;
        } else {
            return Err(RESPError::InvalidCommand);
        }
    }
    Ok(())
}

#[derive(PartialEq, Clone)]
pub enum RedisValue {
    String(Bytes),
    Error(Bytes),
    Int(i64),
    Array(Vec<RedisValue>),
    NullArray,
    NullBulkString,
}

pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
    InvalidCommand,
}

pub enum RedisCommand {
    PING,
    ECHO,
}

impl From<std::io::Error> for RESPError {
    fn from(value: std::io::Error) -> Self {
        RESPError::IOError(value)
    }
}

type RedisResult = Result<Option<(usize, RedisValue)>, RESPError>;

struct BufSplit(usize, usize);

impl BufSplit {
    /// Get a lifetime appropriate slice of the underlying buffer.
    ///
    /// Constant time.
    #[inline]
    fn as_slice<'a>(&self, buf: &'a Bytes) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    /// Get a Bytes object representing the appropriate slice
    /// of bytes.
    ///
    /// Constant time.
    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

// Get a word from `buf` starting at `pos`
fn word(buf: &Bytes, pos: usize) -> Option<(usize, BufSplit)> {
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

fn simple_string(buf: &Bytes, pos: usize) -> RedisResult {
    match word(buf, pos) {
        Some((pos, word)) => {
            let res = RedisValue::String(word.as_bytes(buf));
            Ok(Some((pos, res)))
        }
        None => Ok(None),
    }
}

fn bulk_string(buf: &Bytes, pos: usize) -> RedisResult {
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

fn error(buf: &Bytes, pos: usize) -> RedisResult {
    match word(buf, pos) {
        Some((pos, word)) => {
            let res = RedisValue::Error(word.as_bytes(buf));
            Ok(Some((pos, res)))
        }
        None => Ok(None),
    }
}

fn int(buf: &Bytes, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
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

fn redis_int(buf: &Bytes, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, i)) => Ok(Some((pos, RedisValue::Int(i)))),
        None => Ok(None),
    }
}

fn array(buf: &Bytes, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, RedisValue::NullArray))),
        Some((pos, arr_size)) if arr_size >= 0 => {
            let mut res: Vec<RedisValue> = Vec::with_capacity(arr_size as usize);
            let mut next_pos = pos;
            for _ in 0..arr_size {
                match parse(&buf.slice(next_pos..))? {
                    Some((pos, val)) => {
                        res.push(val);
                        next_pos = pos;
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

fn parse(buf: &Bytes) -> RedisResult {
    if buf.is_empty() {
        return Ok(None);
    }
    match buf[0] {
        b'*' => array(buf, 1),
        b'$' => bulk_string(buf, 1),
        b'+' => simple_string(buf, 1),
        b'-' => error(buf, 1),
        b':' => redis_int(buf, 1),
        _ => Err(RESPError::UnknownStartingByte),
    }
}

fn command(arr: &[RedisValue]) -> Result<RedisCommand, RESPError> {
    if arr.is_empty() {
        return Err(RESPError::InvalidCommand);
    }
    let cmd = arr.first().unwrap();
    if let RedisValue::String(str) = cmd {
        match std::str::from_utf8(str).unwrap() {
            "ping" => Ok(RedisCommand::PING),
            "echo" => Ok(RedisCommand::ECHO),
            _ => Err(RESPError::InvalidCommand),
        }
    } else {
        Err(RESPError::InvalidCommand)
    }
}

fn run(stream: &mut TcpStream, cmd: RedisCommand, args: &[RedisValue]) -> Result<(), RESPError> {
    match cmd {
        RedisCommand::PING => stream.write_all(b"+PONG\r\n")?,
        RedisCommand::ECHO => {
            if args.is_empty() {
                return Err(RESPError::InvalidCommand);
            }
            if let RedisValue::String(str) = args.get(1).unwrap() {
                let response = std::str::from_utf8(str).unwrap();
                stream.write_all(format!("{}\r\n", response).as_bytes())?;
            }
        }
    }
    Ok(())
}
