# For 98-008 Teaching Group

## Check Points

- [Week 1 Check Point](https://github.com/zpatronus/runnel/tree/week1-checkpoint)
  - Explored async io and udp stuff provided by tokio, network stuff provided by std, and base32 endec provided by base32.
  - Implemented the most bottom layer (udp module) to enable easy request and response for client and server in the next phase.
  - Implemented base32 domain name endec to support request through domain name for client and server in the next phase.
  - Played around with Rust's license management system cargo about.
- [Week 2 Check Point](https://github.com/zpatronus/runnel/tree/week2-checkpoint)
  - Implemented DNS packet encoding/decoding (dns_endec module) to support encoding/decoding of DNS queries and responses.
  - Implemented Phase 1 short communication binary with both server and client modes for DNS tunnel communication.
  - Added integration tests to verify the DNS tunnel works correctly.
  - Enhanced documentation (README, DEVELOPERS.md) with usage instructions, deployment guidance, and architecture diagrams.
  - Added clap dependency for command-line argument parsing.

## Code Requirement Checklist

- [x] Must be written in Rust.
- [x] At least 3 structs and 5 functions: Too many structs and functions.
- [x] Manually implement at least one trait: `Display` for `DnsRequest`.
- [x] Uses at least one of Vec, HashMap, or other std data structure: Vec everywhere.
- [x] Uses Option and/or Result: Too many Result and Option.
- [x] Your code is split up into 2 or more modules and/or crates: Too many modules and crates.
- [x] 50% line coverage: Way above the minimum.
- [x] Rustdoc comments with doc tests: Full documentation.
- [x] Error handling: Uses anyhow throughout.
- [x] Ecosystem breadth: Entry points via clap, async via tokio, external integration with DNS, data processing via base32.

Advanced Rust

- [x] Uses smart pointers: `Arc` in UDP server for shared socket.
