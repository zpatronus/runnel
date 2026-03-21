use crate::b32_endec::B32Endec;
use anyhow::Result;
use hickory_proto::op::{message::Message, query::Query};
use hickory_proto::rr::{domain::Name, record_type::RecordType};
use std::str::FromStr;

pub struct DnsRequest {
    b32_endec: B32Endec,
}

impl DnsRequest {
    pub fn new(domain_suffix: &str) -> Result<Self> {
        let domain_suffix_with_dot = if !domain_suffix.ends_with(".") {
            format!("{}.", domain_suffix)
        } else {
            domain_suffix.to_string()
        };
        Ok(Self {
            b32_endec: B32Endec::new(&domain_suffix_with_dot)?,
        })
    }

    pub fn encode_packet(&self, data: &[u8]) -> Result<Vec<u8>> {
        let domain = self.b32_endec.encode(data)?;
        let name = Name::from_str(&domain)?;
        let query = Query::query(name, RecordType::TXT);
        let mut message = Message::new();
        message.add_query(query);
        let packet = message.to_vec()?;
        Ok(packet)
    }

    pub fn decode_packet(&self, packet: &[u8]) -> Result<Vec<u8>> {
        let message = Message::from_vec(packet)?;
        let query = message
            .query()
            .ok_or_else(|| anyhow::anyhow!("No query in packet"))?;
        let name = query.name();
        let domain = name.to_utf8();
        let data = self.b32_endec.decode(&domain)?;

        Ok(data)
    }
}

#[cfg(test)]
mod dns_endec_tests {
    use anyhow::Ok;

    use super::*;

    #[test]
    fn test_dns_endec() -> Result<()> {
        let encoder = DnsRequest::new("example.com")?;
        let data = b"Hello, DNS!";
        let packet = encoder.encode_packet(data)?;
        let decoded_data = encoder.decode_packet(&packet)?;

        assert_eq!(data.to_vec(), decoded_data);

        let encoder = DnsRequest::new("example.com.")?;
        let packet = encoder.encode_packet(data)?;
        let decoded_data = encoder.decode_packet(&packet)?;
        assert_eq!(data.to_vec(), decoded_data);

        Ok(())
    }

    #[test]
    fn test_no_query() -> Result<()> {
        let encoder = DnsRequest::new("example.com")?;
        let message = Message::new();
        let packet = message.to_vec()?;
        assert!(encoder.decode_packet(&packet).is_err());
        Ok(())
    }
}
