//! Phase 2: Long Communication through DNS Tunnel
//! This is a demonstration of using the `runnel` library to implement a long communication through a DNS tunnel. Unlike the short communication demo (Phase 1) which handles single-packet exchanges directly over UDP, this program uses the `RunnelServer` and `RunnelClient` session layer to manage multi-packet conversations with conversation IDs, enabling proper multi-client support and reliable data streaming over DNS. The server can handle multiple concurrent clients, each identified by a unique conversation ID, and processes data by converting it to uppercase before sending it back.

use anyhow::Result;
use clap::Parser;
use runnel::runnel_session::{RunnelClient, RunnelServer};
use std::io::Write;
use std::time::Duration;
use tokio::time;

/// Command-line arguments for the long communication program. Similar to the short communication program, it can run in either server mode or client mode, and requires a domain suffix for encoding/decoding DNS packets. Unlike Phase 1, the client supports multiple DNS server addresses for redundancy, and the communication is managed through the `RunnelServer`/`RunnelClient` session layer which handles conversation IDs and multi-packet data streaming.
#[derive(Debug, Parser)]
#[command(name = "long-communication")]
#[command(about = "Long communication through DNS tunnel in Rust")]
struct Args {
    /// Run as server, otherwise run as client
    #[arg(short = 's', long)]
    server: bool,

    /// Domain suffix for DNS encoding (e.g., "example.com")
    #[arg(short = 'd', long)]
    domain: String,

    /// Server listen port (only for server mode)
    #[arg(short = 'p', long, default_value = "5353")]
    port: u16,

    /// DNS server addresses to connect to (only for client mode, supports multiple for redundancy)
    #[arg(short = 'n', long, default_values = ["8.8.8.8:53", "8.8.4.4:53"])]
    dns: Vec<String>,

    /// Exit after sending/receiving one message (for testing)
    #[arg(short = 'o', long)]
    once: bool,
}

/// The main entry point of the program. It parses command-line arguments and runs either the server or client logic based on the provided flags. The server uses `RunnelServer` to manage multiple concurrent client conversations, while the client uses `RunnelClient` to establish a session that handles multi-packet data streaming over DNS.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.server {
        println!(
            "Running in server mode on port {} with domain suffix {}",
            args.port, args.domain
        );
        run_server(args).await;
    } else {
        println!(
            "Running in client mode, sending to DNS servers {:?} with domain suffix {}",
            args.dns, args.domain
        );
        run_client(args).await?;
    }
    Ok(())
}

/// Runs the server logic using the `RunnelServer` session layer. The server binds to the specified port and manages multiple concurrent client conversations, each identified by a unique conversation ID. It polls active conversations for incoming messages, decodes the data, processes it (in this case, converts it to uppercase), and sends the response back through the same conversation. The server continues to run until interrupted or until the `once` flag is set, which allows it to exit after handling one complete message exchange.
async fn run_server(args: Args) {
    let bind_addr = format!("0.0.0.0:{}", args.port);
    let server = RunnelServer::new(&bind_addr, &args.domain)
        .await
        .expect("Failed to start server");

    println!("Server listening on {}", bind_addr);

    loop {
        time::sleep(Duration::from_millis(5)).await;
        for conv in server.active_convs() {
            if let Some(msg) = server.recv(conv) {
                println!("Received: {}", String::from_utf8_lossy(&msg));
                let capitalized: Vec<u8> = msg.iter().map(|b| b.to_ascii_uppercase()).collect();
                server
                    .send(conv, &capitalized)
                    .expect("Failed to send response");
                println!("Sent: {}", String::from_utf8_lossy(&capitalized));
                if args.once {
                    time::sleep(Duration::from_millis(500)).await;
                    return;
                }
            }
        }
    }
}

/// Runs the client logic using the `RunnelClient` session layer. The client connects to one or more DNS servers and establishes a session that handles multi-packet data streaming. It reads user input from the console, sends it through the session, and polls for the response. Unlike the short communication client which directly encodes/decodes individual DNS packets, the `RunnelClient` abstracts away the packet-level details and manages the conversation state automatically. The client continues to run until interrupted or until the `once` flag is set.
async fn run_client(args: Args) -> Result<()> {
    let mut client = RunnelClient::new(args.dns, &args.domain).await?;

    loop {
        print!("> ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim_end();

        client.send(input.as_bytes())?;

        loop {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = client.recv() {
                println!("Received response: {}", String::from_utf8_lossy(&msg));
                break;
            }
        }

        if args.once {
            break;
        }
    }
    Ok(())
}
