use anyhow::Result;
use clap::Parser;
use runnel::{
    dns_endec::{DnsRequest, DnsResponse},
    udp::{Client, Server},
};
use std::io::Write;

#[derive(Debug, Parser)]
#[command(name = "short-communication")]
#[command(about = "Short communication through DNS tunnel in Rust")]
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

    /// DNS server address to connect to (only for client mode)
    #[arg(short = 'n', long, default_value = "8.8.8.8:53")]
    dns: String,

    /// Exit after sending/receiving one message (for testing)
    #[arg(short = 'o', long)]
    once: bool,
}

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
            "Running in client mode, sending to DNS server {} with domain suffix {}",
            args.dns, args.domain
        );
        run_client(args).await?;
    }
    Ok(())
}

async fn run_server(args: Args) {
    let port = args.port;
    let bind_addr = format!("0.0.0.0:{}", port);
    let request_decoder =
        DnsRequest::new(&args.domain).expect("Failed to create DNS request decoder");
    let response_encoder = DnsResponse::new();
    let server = Server::new(&bind_addr)
        .await
        .expect("Failed to start server");

    println!("Server listening on {}", bind_addr);

    loop {
        // listen and respond with content reversed
        let (packet, reply) = match server.recv().await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("Failed to receive data: {}", e);
                continue;
            }
        };
        let data = match request_decoder.decode_packet(&packet) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Failed to decode packet: {}", e);
                continue;
            }
        };
        println!("Received data: {}", String::from_utf8_lossy(&data));
        let reversed = data.iter().rev().cloned().collect::<Vec<u8>>();
        let response_packet = match response_encoder.encode_packet(&packet, &reversed) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to encode response packet: {}", e);
                continue;
            }
        };
        match reply.send(&response_packet).await {
            Ok(_) => println!("Sent response: {}", String::from_utf8_lossy(&reversed)),
            Err(e) => eprintln!("Failed to send response: {}", e),
        }

        if args.once {
            break;
        }
    }
}

async fn run_client(args: Args) -> Result<()> {
    let dns_server = &args.dns;

    let request_encoder =
        DnsRequest::new(&args.domain).expect("Failed to create DNS request encoder");
    let response_decoder = DnsResponse::new();

    let client = Client::new("0.0.0.0:0", dns_server)
        .await
        .expect("Failed to bind client socket");

    loop {
        // read input and send, print response
        print!("> ");
        std::io::stdout().flush().expect("Failed to flush stdout");
        let mut input = String::new();
        match std::io::stdin().read_line(&mut input) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to read input: {}", e);
                continue;
            }
        }

        let input = input.trim_end().as_bytes();

        let packet = match request_encoder.encode_packet(input) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to encode request packet: {}", e);
                continue;
            }
        };

        if let Err(e) = client.send(&packet).await {
            eprintln!("Failed to send packet: {}", e);
            continue;
        }

        let response_packet = match client.recv().await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to receive response: {}", e);
                continue;
            }
        };

        let response_data = match response_decoder.decode_packet(&response_packet) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Failed to decode response packet: {}", e);
                continue;
            }
        };

        println!(
            "Received response: {}",
            String::from_utf8_lossy(&response_data)
        );

        if args.once {
            break;
        }
    }
    Ok(())
}
