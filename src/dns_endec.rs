//! DNS request and response encoder/decoder using base32 encoding for the data. The `DnsRequest` struct encodes data into a domain name in a DNS query, while the `DnsResponse` struct encodes data into a TXT record in a DNS response. Both encoders/decoders handle the limitations of DNS record sizes and ensure that the encoded data is in lowercase.
use crate::b32_endec::{B32DomainEndec, B32ResponseEndec};
use anyhow::{Result, bail};
use hickory_proto::op::{MessageType, message::Message, query::Query};
use hickory_proto::rr::{
    Record, domain::Name, rdata::TXT, record_data::RData, record_type::RecordType,
};
use std::fmt;
use std::str::FromStr;

/// A DNS request encoder/decoder that encodes data into a domain name in a DNS query using base32 encoding, and decodes data from such a domain name. The encoded data will be in lowercase, and the maximum length of the encoded data is determined by the limitations of DNS label lengths and total domain name length.    
pub struct DnsRequest {
    /// The base32 encoder/decoder used to encode data into domain labels and decode data from domain labels. It handles the logic of splitting the encoded data into labels of appropriate length and ensuring that the total length of the domain name does not exceed DNS limits.
    b32_domain_endec: B32DomainEndec,
}

impl fmt::Display for DnsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DnsRequest Encoder/Decoder with domain suffix: {}, max payload length: {}",
            self.b32_domain_endec.suffix(),
            self.max_data_len()
        )
    }
}

impl DnsRequest {
    /// Creates a new `DnsRequest` encoder/decoder with the specified domain suffix. The suffix must not be empty and must not exceed 253 characters (including the trailing dot). The suffix will be normalized to lowercase and ensured to end with a dot.
    ///
    /// # Example
    /// ```
    /// let encoder = DnsRequest::new("example.com")?;
    /// ```
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

    /// Encodes the given data into a DNS query packet, where the data is encoded into a domain name using base32 encoding. The resulting domain name will consist of labels of up to 63 characters, followed by the specified suffix. The total length of the domain name must not exceed 253 characters. The encoded data will be in lowercase.
    ///
    /// # Example
    /// ```
    /// let packet = encoder.encode_packet(b"Hello, DNS!")?;
    /// ```
    pub fn encode_packet(&self, data: &[u8]) -> Result<Vec<u8>> {
        let domain = self.b32_domain_endec.encode(data)?;
        let name = Name::from_str(&domain)?;
        let query = Query::query(name, RecordType::TXT);
        let mut message = Message::new();
        message.set_recursion_desired(true);
        message.add_query(query);
        let packet = message.to_vec()?;
        Ok(packet)
    }

    /// Decodes data from a DNS query packet that was encoded using the `encode_packet` method. The method extracts the domain name from the query, checks that it ends with the specified suffix, and decodes the labels before the suffix from base32 to retrieve the original data. The decoding is case-insensitive.
    /// # Example
    /// ```
    /// let data = encoder.decode_packet(&packet)?;
    /// ```
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

    /// Returns the maximum length of the data that can be encoded in a DNS query using this encoder, based on the limitations of DNS label lengths and total domain name length. This is determined by the maximum number of characters that can be used for the encoded data while still fitting within the DNS limits when combined with the specified suffix.
    pub fn max_data_len(&self) -> usize {
        self.b32_domain_endec.max_data_len()
    }
}

/// A DNS response encoder/decoder that encodes data into a TXT record in a DNS response using base32 encoding, and decodes data from such a TXT record. The encoded data will be in lowercase, and the maximum length of the encoded data is determined by the limitations of DNS record sizes and the overhead of base32 encoding.
pub struct DnsResponse {
    /// The base32 encoder/decoder used to encode data into the TXT record and decode data from the TXT record. It handles the logic of ensuring that the encoded data fits within the limits of a DNS response and is properly formatted for inclusion in a TXT record.
    b32_response_endec: B32ResponseEndec,
}

impl DnsResponse {
    /// Creates a new `DnsResponse` encoder/decoder with the default maximum total length of 253 characters for the encoded data. This allows for some overhead in the DNS response while still fitting within the typical limits of DNS record sizes.
    ///
    /// # Example
    /// ```
    /// let encoder = DnsResponse::new();
    /// ```
    pub fn new() -> Self {
        Self {
            b32_response_endec: B32ResponseEndec::new(),
        }
    }

    /// Encodes the given data into a DNS response packet, where the data is encoded into a TXT record using base32 encoding. The method takes the original request packet to extract the query and construct a corresponding response. The encoded data will be in lowercase, and the total length of the encoded data must not exceed the maximum total length allowed by this encoder.
    ///
    /// # Example
    /// ```
    /// let response_packet = encoder.encode_packet(&request_packet, b"Hello, Client!")
    /// ```
    pub fn encode_packet(&self, request: &[u8], response_data: &[u8]) -> Result<Vec<u8>> {
        let request = Message::from_vec(request)?;
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

    /// Decodes data from a DNS response packet that was encoded using the `encode_packet` method. The method extracts the TXT record from the answer section of the response, checks that it is properly formatted, and decodes the content from base32 to retrieve the original data. The decoding is case-insensitive.
    ///
    /// # Example
    /// ```
    /// let data = encoder.decode_packet(&response_packet)?;
    /// ```
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

    /// Returns the maximum length of the data that can be encoded in a DNS response using this encoder, based on the limitations of DNS record sizes and the overhead of base32 encoding. This is determined by the maximum number of characters that can be used for the encoded data while still fitting within the limits of a DNS response when combined with the necessary DNS record formatting.
    pub fn max_data_len(&self) -> usize {
        self.b32_response_endec.max_data_len()
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
        let message = Message::from_vec(&packet)?;
        assert!(message.recursion_desired());
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

    #[test]
    fn test_display_trait() -> Result<()> {
        let encoder = DnsRequest::new("example.com")?;
        let display_output = format!("{}", encoder);
        println!("{}", display_output);
        assert!(
            display_output.contains("DnsRequest Encoder/Decoder with domain suffix: example.com.")
        );
        assert!(display_output.contains("max payload length:"));
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
        let response_packet = response_encoder.encode_packet(&request_packet, response_data)?;
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
        let request_max_len = request_encoder.max_data_len();
        let response_max_len = response_encoder.max_data_len();
        let content_length = request_max_len.min(response_max_len);
        let request_data = b"ABCD".repeat(content_length / 4);
        let request_packet = request_encoder.encode_packet(&request_data)?;
        let received_request_data = request_encoder.decode_packet(&request_packet)?;
        let response_data = received_request_data
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<u8>>();
        let response_packet = response_encoder.encode_packet(&request_packet, &response_data)?;
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
