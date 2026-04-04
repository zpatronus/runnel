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
        let msg = "a".repeat(10000);
        client_stdin
            .write_all(format!("{}\n", msg).as_bytes())
            .expect("Failed to write to client");
        drop(client.0.stdin.take());
    }

    thread::sleep(Duration::from_secs(15));

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

    println!("Server output length: {} bytes", all_output.len());

    assert!(
        all_output.contains("Received:"),
        "Server should receive the message"
    );
    assert!(
        all_output.contains("Sent:"),
        "Server should send capitalized response"
    );

    let client_stdout = client
        .0
        .stdout
        .as_mut()
        .expect("Failed to capture client stdout");
    let mut client_reader = BufReader::new(client_stdout);
    let mut client_output = String::new();
    let _ = client_reader.read_to_string(&mut client_output);

    println!("Client output length: {} bytes", client_output.len());

    assert!(
        client_output.contains("Received response:"),
        "Client should receive the response, got: {}",
        &client_output[..client_output.len().min(200)]
    );

    let expected_response = "A".repeat(10000);
    assert!(
        client_output.contains(&expected_response),
        "Client should receive the expected response"
    );
}

#[test]
fn test_server_multi_client_integration() {
    let bin = env!("CARGO_BIN_EXE_long_communication");

    let mut server = ProcessGuard(
        Command::new(bin)
            .args(["-s", "-d", "example.com", "-p", "10054"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server"),
    );

    thread::sleep(Duration::from_millis(200));

    let mut client1 = ProcessGuard(
        Command::new(bin)
            .args(["-d", "example.com", "-n", "127.0.0.1:10054", "--once"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start client1"),
    );
    let mut client2 = ProcessGuard(
        Command::new(bin)
            .args(["-d", "example.com", "-n", "127.0.0.1:10054", "--once"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start client2"),
    );

    {
        let client1_stdin = client1
            .0
            .stdin
            .as_mut()
            .expect("Failed to open client1 stdin");
        let msg = "a".repeat(10000);
        client1_stdin
            .write_all(format!("{}\n", msg).as_bytes())
            .expect("Failed to write to client1");
        drop(client1.0.stdin.take());
    }
    {
        let client2_stdin = client2
            .0
            .stdin
            .as_mut()
            .expect("Failed to open client2 stdin");
        let msg = "b".repeat(10000);
        client2_stdin
            .write_all(format!("{}\n", msg).as_bytes())
            .expect("Failed to write to client2");
        drop(client2.0.stdin.take());
    }
    thread::sleep(Duration::from_secs(15));

    let _ = client1.0.kill();
    let _ = client1.0.wait();
    let _ = client2.0.kill();
    let _ = client2.0.wait();

    let _ = server.0.kill();
    let _ = server.0.wait();
    let server_stdout = server.0.stdout.as_mut().expect("Failed to capture stdout");
    let mut server_reader = BufReader::new(server_stdout);
    let mut all_output = String::new();
    server_reader
        .read_to_string(&mut all_output)
        .expect("Failed to read server output");

    println!("Server output length: {} bytes", all_output.len());

    assert!(
        all_output.contains("Received:"),
        "Server should receive messages"
    );
    assert!(all_output.contains("Sent:"), "Server should send responses");

    let client1_stdout = client1
        .0
        .stdout
        .as_mut()
        .expect("Failed to capture client1 stdout");
    let mut client1_reader = BufReader::new(client1_stdout);
    let mut client1_output = String::new();
    let _ = client1_reader.read_to_string(&mut client1_output);

    println!("client1 output length: {} bytes", client1_output.len());

    assert!(
        client1_output.contains("Received response:"),
        "client1 should receive the response, got: {}",
        &client1_output[..client1_output.len().min(200)]
    );

    let expected_response = "A".repeat(10000);
    assert!(
        client1_output.contains(&expected_response),
        "client1 should receive the expected response"
    );

    let client2_stdout = client2
        .0
        .stdout
        .as_mut()
        .expect("Failed to capture client2 stdout");
    let mut client2_reader = BufReader::new(client2_stdout);
    let mut client2_output = String::new();
    let _ = client2_reader.read_to_string(&mut client2_output);

    println!("client2 output length: {} bytes", client2_output.len());
    assert!(
        client2_output.contains("Received response:"),
        "client2 should receive the response, got: {}",
        &client2_output[..client2_output.len().min(200)]
    );

    let expected_response = "B".repeat(10000);
    assert!(
        client2_output.contains(&expected_response),
        "client2 should receive the expected response"
    );
}
