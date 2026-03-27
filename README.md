![](./assets/cover_short.png)

# runnel - DNS Tunnel in Rust

## Disclaimer

This project is for educational purposes only. It is not intended to be used for any malicious activities, such as bypassing network restrictions without permission. Always ensure that you have the necessary permissions before using or deploying any tools that can bypass network restrictions.

## What is runnel and why you might be interested in it?

`runnel` is a DNS tunnel implemented in Rust. A DNS tunnel is a technique that allows data to be transmitted over DNS queries and responses. Unlike other protocols such as HTTP or HTTPS, DNS traffic is often allowed through firewalls and network filters. Also, the destination of DNS queries is some public DNS server whose IP is on white list with high probability. These features make DNS tunnels **a powerful tool for bypassing network restrictions**.

For example, Oct 2025, Ramsay Leung posted in his blog [A Story About Bypassing Air Canada's In-flight Network Restrictions](https://ramsayleung.github.io/en/post/2025/a_story_about_bypassing_air_canadas_in-flight_network_restrictions/) that he successfully got free WiFi on Air Canada flights by tunneling his internet traffic through DNS queries and responses.

For more technical details about this project, please refer to the [Technical Notes](./DEVELOPERS.md).

## Phase 1: Short Communication through DNS Tunnel

In this phase, a binary, `short_communication`,is implemented to enable short communication through a DNS tunnel. `short` is defined as messages that can fit into a single DNS packet, whose limit (depending on domain suffix) is around 150 bytes for request and response.

### Usage

A help message is provided for `short_communication`:

```bash
cargo run --bin short_communication -- -h
```

#### Server Mode

To run it in server mode, provide the `-s` flag, the domain suffix, and optionally specify the port to listen on (default is 5353):

```bash
cargo run --bin short_communication -- -s -d example.com -p 10053
```

#### Client Mode

To run it in client mode, provide the domain suffix and the DNS server address (the DNS server that the client will query, which can be a public DNS server or the server you just started locally in server mode):

```bash
cargo run --bin short_communication -- -d example.com --dns 127.0.0.1:10053
```

### Local Demonstration

This binary allows user to input messages in the client terminal, which will be sent to the server through DNS tunnel.

As a demonstration, the server responds with the reversed message received from the client.

![](./assets/phase-1_client_server.png)

You may also verify using `dig` command that the server receives the request and responds correctly (`19260817` encodes to base32 `GE4TENRQHAYTO` and `g4ytqmbwgi4tc` decodes to `71806291`):

```bash
dig @127.0.0.1 -p 10053 GE4TENRQHAYTO.example.com TXT
```

![](./assets/phase-1_dig_server.png)

### Actually Deploying Server to be an Authoritative DNS Server

To make it more than a local toy, you might want to deploy the server to a publicly accessible machine, let it listen to port 53, and point a domain's NS record to it. This way, you can use the client from anywhere in the world **using public DNS servers** (which also makes it possible to concurrently querying multiple public DNS servers to speed things up/avoid rate limiting) to communicate with our server.

Run a DNS server on port 53 usually requires root privileges, so you might want to use `sudo` to run the server:

```bash
sudo ./target/debug/short_communication -s -d n.marky.top -p 53
```

Run the client with Cloudflare's public DNS server:

```bash
cargo run --bin short_communication -- -d n.marky.top -n 1.1.1.1:53
```

![](./assets/phase-1_client_server_public.png)

Or use `dig`:

```bash
dig @1.1.1.1 GE4TENRQHAYTO.n.marky.top TXT
```

![](./assets/phase-1_dig_server_public.png)

You can also try to communicate with `n.marky.top`. I will try my best to keep the authoritative server running. It runs on a [RISC-V dev board](https://wiki.sipeed.com/hardware/en/lichee/RV_Nano/1_intro.html) and freezes from time to time. Not the most stable server.

## For Developers

[Technical Notes](./DEVELOPERS.md)

## For 98-008 Teaching Group

[Notes and links for checkpoints](./for_98-008_teaching_group.md)
