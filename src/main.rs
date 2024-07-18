use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() -> std::io::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                std::thread::spawn(move || handle_connection(&mut stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut buf = [0; 512];
    loop {
        let n = stream.read(&mut buf)?;
        println!("received {} bytes", n);

        if n == 0 {
            break;
        }

        stream.write_all(b"+PONG\r\n")?;
    }
    Ok(())
}
