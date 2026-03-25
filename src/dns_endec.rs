use crate::b32_endec::{B32DomainEndec, B32ResponseEndec};
use anyhow::{Result, bail};
use hickory_proto::op::{MessageType, message::Message, query::Query};
use hickory_proto::rr::{
    Record, domain::Name, rdata::TXT, record_data::RData, record_type::RecordType,
};
use std::str::FromStr;

pub struct DnsRequest {
    b32_domain_endec: B32DomainEndec,
}

impl DnsRequest {
    pub fn new(domain_suffix: &str) -> Result<Self> {
        let domain_suffix_with_dot = if !domain_suffix.ends_with(".") {
            format!("{}.", domain_suffix)
        } else {
            domain_suffix.to_string()
        };
        Ok(Self {
            b32_domain_endec: B32DomainEndec::new(&domain_suffix_with_dot)?,
        })
    }

    pub fn encode_packet(&self, data: &[u8]) -> Result<Vec<u8>> {
        let domain = self.b32_domain_endec.encode(data)?;
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
        let data = self.b32_domain_endec.decode(&domain)?;

        Ok(data)
    }
}

pub struct DnsResponse {
    b32_response_endec: B32ResponseEndec,
}

impl DnsResponse {
    pub fn new() -> Self {
        Self {
            b32_response_endec: B32ResponseEndec::new(),
        }
    }

    pub fn encode_packet(&self, request: Message, response_data: &[u8]) -> Result<Vec<u8>> {
        let id = request.id();
        let query = request
            .query()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("No query in request"))?;
        let encoded = self.b32_response_endec.encode(response_data)?;
        let encoded_str = std::str::from_utf8(&encoded)?;
        let name = query.name().clone();
        let txt_data = TXT::new(vec![encoded_str.to_string()]);
        let rdata = RData::TXT(txt_data);
        let record = Record::from_rdata(name, 0, rdata);
        let mut response = Message::new();
        response.set_id(id);
        response.set_message_type(MessageType::Response);
        response.add_query(query);
        response.add_answer(record);
        let packet = response.to_vec()?;
        Ok(packet)
    }

    pub fn decode_packet(&self, packet: &[u8]) -> Result<Vec<u8>> {
        let message = Message::from_vec(packet)?;
        let answers = message.answers();
        if answers.is_empty() {
            bail!("No answers in packet");
        }
        if answers.len() > 1 {
            bail!("Multiple answers in packet, expected only one");
        }
        let answer = &answers[0];
        if answer.record_type() != RecordType::TXT {
            bail!("Expected TXT record in answer");
        }
        let rdata = answer.data();
        let txt = match rdata {
            RData::TXT(txt) => txt,
            _ => bail!("Expected TXT record in answer"),
        };
        let txt_data = txt.txt_data();
        if txt_data.len() != 1 {
            bail!("Expected exactly one TXT string in answer");
        }
        let encoded_bytes: &[u8] = &txt_data[0];
        let decoded = self.b32_response_endec.decode(encoded_bytes)?;
        Ok(decoded)
    }
}

#[cfg(test)]
mod dns_request_endec_tests {
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

#[cfg(test)]
mod dns_response_endec_tests {
    use anyhow::Ok;

    use super::*;

    #[test]
    fn test_dns_response_endec() -> Result<()> {
        let request_encoder = DnsRequest::new("example.com")?;
        let response_encoder = DnsResponse::new();
        let request_data = b"Hello, DNS!";
        let request_packet = request_encoder.encode_packet(request_data)?;
        let response_data = b"Hello, Client!";
        let response_packet =
            response_encoder.encode_packet(Message::from_vec(&request_packet)?, response_data)?;
        let decoded_response_data = response_encoder.decode_packet(&response_packet)?;

        assert_eq!(response_data.to_vec(), decoded_response_data);
        Ok(())
    }
}

#[cfg(test)]
mod dns_request_response_integration_tests {
    use anyhow::Ok;

    use super::*;

    #[test]
    fn test_dns_request_response_integration() -> Result<()> {
        let request_encoder = DnsRequest::new("example.com")?;
        let response_encoder = DnsResponse::new();
        let request_max_len = request_encoder.b32_domain_endec.max_data_len();
        let response_max_len = response_encoder.b32_response_endec.max_data_len();
        let content_length = request_max_len.min(response_max_len);
        let request_data = b"ABCD".repeat(content_length / 4);
        let request_packet = request_encoder.encode_packet(&request_data)?;
        let received_request_data = request_encoder.decode_packet(&request_packet)?;
        let response_data = received_request_data
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<u8>>();
        let response_packet =
            response_encoder.encode_packet(Message::from_vec(&request_packet)?, &response_data)?;
        let received_response_data = response_encoder.decode_packet(&response_packet)?;
        let rev_received_response_data = received_response_data
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<u8>>();

        assert_eq!(request_data, rev_received_response_data);
        Ok(())
    }
}
