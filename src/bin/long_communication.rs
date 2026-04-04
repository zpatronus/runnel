use anyhow::Result;
use clap::Parser;
use runnel::runnel_session::{RunnelClient, RunnelServer};
use std::io::Write;
use std::time::Duration;
use tokio::time;

#[derive(Debug, Parser)]
#[command(name = "long-communication")]
#[command(about = "Long communication through DNS tunnel in Rust")]
struct Args {
    #[arg(short = 's', long)]
    server: bool,

    #[arg(short = 'd', long)]
    domain: String,

    #[arg(short = 'p', long, default_value = "5353")]
    port: u16,

    #[arg(short = 'n', long, default_values = ["8.8.8.8:53", "8.8.4.4:53"])]
    dns: Vec<String>,

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
            "Running in client mode, sending to DNS servers {:?} with domain suffix {}",
            args.dns, args.domain
        );
        run_client(args).await?;
    }
    Ok(())
}

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
                server.send(conv, &capitalized).expect("Failed to send response");
                println!("Sent: {}", String::from_utf8_lossy(&capitalized));
                if args.once {
                    time::sleep(Duration::from_millis(500)).await;
                    return;
                }
            }
        }
    }
}

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
