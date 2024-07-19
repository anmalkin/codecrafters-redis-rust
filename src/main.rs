use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    io::{BufRead, Read, Write},
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

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(512);
    loop {
        let n = stream.read(&mut buf)?;
        println!("received {} bytes", n);
        println!("len of buf = {}", buf.len());
        let mut iter = buf.iter();
        let data_type = iter.next().unwrap();
        match data_type {
            b'*' => {}
            _ => {}
        }

        if n == 0 {
            break;
        }

        stream.write_all(b"+PONG\r\n")?;
    }
    Ok(())
}


// Get a word from `buf` starting at `pos`
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
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

#[derive(PartialEq, Clone)]
pub enum RedisValue {
    String(Bytes),
    Error(Bytes),
    Int(i64),
    Array(Vec<RedisValue>),
    NullArray,
    NullBulkString,
}

// Used for zero-copy Redis values
struct BufSplit(usize, usize);

enum RedisBufSplit {
    String(BufSplit),
    Error(BufSplit),
    Int(i64),
    Array(Vec<RedisBufSplit>),
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
}

type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;
