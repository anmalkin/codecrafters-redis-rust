use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::thread;

mod errors;
mod parser;

use crate::parser::{parse, execute, RedisValue};

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || handle_connection(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection(mut stream: TcpStream) {
    let mut buf = vec![0; 512];
    loop {
        let n = stream
            .read(buf.as_mut_slice())
            .expect("Failed to read from stream.");
        println!("received {} bytes", n);

        if n == 0 {
            break;
        }

        let args = match parse(buf.as_slice(), 0) {
            Ok(Some((_pos, RedisValue::Array(vec)))) => vec,
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {
                println!("Invalid message");
                break;
            }
        };

        if let Err(e) = execute(&mut stream, &args) {
            println!("Error: {}", e);
            break;
        }
    }
}
