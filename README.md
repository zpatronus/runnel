![](./assets/cover_short.png)

# runnel - DNS Tunnel in Rust

## Phase 1: Short Communication through DNS Tunnel

In this phase, a binary, `short_communication`,is implemented to enable short communication through a DNS tunnel. `short` is defined as messages that can fit into a single DNS packet, whose limit (depending on domain suffix) is around 150 bytes for request and response.

### Usage

A help message is provided for `short_communication`:

```bash
cargo run --bin short_communication -- -h
```

To run it in server mode, provide the `-s` flag, the domain suffix, and optionally specify the port to listen on (default is 5353):

```bash
cargo run --bin short_communication -- -s -d example.com -p 10053
```

To run it in client mode, provide the domain suffix and the DNS server address:

```bash
cargo run --bin short_communication -- -d example.com --dns 127.0.0.1:10053
```

This binary allows user to input messages in the client terminal, which will be sent to the server through DNS tunnel.

As a demonstration, the server responds with the reversed message received from the client.

![](./assets/phase-1_client_server.png)

You may also verify using `dig` command that the server receives the request and responds correctly (`19260817` encodes to base32 `GE4TENRQHAYTO` and `g4ytqmbwgi4tc` decodes to `71806291`):

![](./assets/phase-1_dig_server.png)

## For Developers

[Technical Notes](./DEVELOPERS.md)

## For 98-008 Teaching Group

[Notes and links for checkpoints](./for_98-008_teaching_group.md)
