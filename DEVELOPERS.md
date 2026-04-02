# Technical Notes

## Phase 1

![](./assets/phase-1-diagram.png)

In Phase 1, `short_communication` supports request and response that can fit into a single DNS packet after base32 encoding. There's no fragmentation, assembly, encryption, authentication, or reliability mechanism implemented in this phase.

Messages are encoded in base32, and are included in subdomain labels for requests, and in TXT records for responses.

## Phase 2

### Reliable Long Message Transmission through KCP

KCP is an alternative to TCP, designed for low-latency and high-throughput communication over unreliable networks. It provides features like fragmentation, assembly, retransmission, and congestion control.

The crate [kcp](https://crates.io/crates/kcp) provides core KCP functionalities. Specifically, it provides a `message` semantics where messages are delivered atomically (in full or not at all) and in order. It allows decoupling of the underlying transport protocol, enables us to use a DNS tunnel to transmit its packets.

For more details, see the [kcp crate documentation](https://docs.rs/kcp/latest/kcp/struct.Kcp.html).

However, its implementation has a hardcoded message size limit of 127 * MSS. In the context of DNS tunneling, the MTU is typically around 150 bytes, which results in a MSS of around 120 bytes, meaning the maximum message size is around 15 KB. This is insufficient.

My solution is to break a long message into 127*MSS chunks, and view each chunks as a "message" in KCP's perspective. Each chunk is prefixed with a single byte, 1 if more chunks follow, 0 for the final chunk.

Since KCP guarantees in-order delivery, the receiver can simply read chunks until it encounters a chunk with the bit 0, and then reassemble the original message. This way, I can support messages of arbitrary size.

![](./assets/phase-2-kcp_session.png)
