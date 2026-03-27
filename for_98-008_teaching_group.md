# For 98-008 Teaching Group

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
