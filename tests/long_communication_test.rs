use std::io::{BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

struct ProcessGuard(Child);

#[test]
fn test_server_client_integration() {
    let bin = env!("CARGO_BIN_EXE_long_communication");

    let mut server = ProcessGuard(
        Command::new(bin)
            .args(["-s", "-d", "example.com", "-p", "10053", "--once"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server"),
    );

    thread::sleep(Duration::from_millis(200));

    let mut client = ProcessGuard(
        Command::new(bin)
            .args(["-d", "example.com", "-n", "127.0.0.1:10053", "--once"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start client"),
    );

    {
        let client_stdin = client
            .0
            .stdin
            .as_mut()
            .expect("Failed to open client stdin");
        client_stdin
            .write_all(b"hello\n")
            .expect("Failed to write to client");
        drop(client.0.stdin.take());
    }

    thread::sleep(Duration::from_millis(1000));

    let _ = client.0.kill();
    let _ = client.0.wait();

    let _ = server.0.kill();
    let _ = server.0.wait();
    let server_stdout = server.0.stdout.as_mut().expect("Failed to capture stdout");
    let mut server_reader = BufReader::new(server_stdout);
    let mut all_output = String::new();
    server_reader
        .read_to_string(&mut all_output)
        .expect("Failed to read server output");

    println!("Server output:\n{}", all_output);

    assert!(
        all_output.contains("hello"),
        "Server should receive 'hello'"
    );
    assert!(
        all_output.contains("HELLO"),
        "Server should send capitalized 'HELLO'"
    );

    let client_stdout = client
        .0
        .stdout
        .as_mut()
        .expect("Failed to capture client stdout");
    let mut client_reader = BufReader::new(client_stdout);
    let mut client_output = String::new();
    let _ = client_reader.read_to_string(&mut client_output);

    println!("Client output:\n{}", client_output);

    assert!(
        client_output.contains("HELLO"),
        "Client should receive 'HELLO', got: {}",
        client_output
    );
}
