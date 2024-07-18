use std::{io::{Read, Write}, net::TcpListener};

fn main() -> std::io::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0; 1024];
                let n = stream.read(&mut buf)?;
                println!("received {} bytes", n);
                let buf: &[u8] = b"+PONG\r\n";
                stream.write_all(buf)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
