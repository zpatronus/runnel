//! Base32 encoding and decoding for DNS labels and responses.
use anyhow::{Ok, Result, bail};

/// A DNS encoder/decoder that encodes data into the labels of a domain name using base32 encoding, and decodes data from such a domain name.
pub struct B32DomainEndec {
    /// The domain suffix that will be appended to the encoded labels. The suffix must end with a dot and will be normalized to lowercase. The maximum length of the suffix is 253 characters (including the trailing dot), and it must not be empty.
    suffix: String,
    /// The maximum length of a single label in a domain name, which is 63 characters according to DNS specifications.
    max_label_len: usize,
    /// The maximum total length of a domain name, which is 253 characters according to DNS specifications (including the trailing dot).
    max_total_len: usize,
}

impl B32DomainEndec {
    /// Creates a new `B32DomainEndec` with the specified domain suffix. The suffix must not be empty and must not exceed 253 characters (including the trailing dot). The suffix will be normalized to lowercase and ensured to end with a dot.
    ///
    /// # Example
    /// ```
    /// let endec = B32DomainEndec::new("example.com")?;
    /// ```
    pub fn new(suffix: &str) -> Result<Self> {
        let suffix_with_dot = if !suffix.ends_with(".") {
            format!("{}.", suffix)
        } else {
            suffix.to_string()
        };

        if suffix_with_dot.len() > 253 {
            bail!("Suffix too long");
        }

        if suffix_with_dot.len() == 1 {
            bail!("Suffix must not be empty");
        }

        let endec = Self {
            suffix: suffix_with_dot.to_lowercase(),
            max_label_len: 63,
            max_total_len: 253,
        };

        if endec.max_data_len() < 1 {
            bail!("Suffix too long to allow any data encoding");
        }

        Ok(endec)
    }

    /// Encodes the given data into a domain name using base32 encoding. The resulting domain name will consist of labels of up to 63 characters, followed by the specified suffix. The total length of the domain name must not exceed 253 characters. The encoded data will be in lowercase.
    ///
    /// # Example
    /// ```
    /// let domain = endec.encode(b"Hello, World!")?;
    /// ```
    pub fn encode(&self, data: &[u8]) -> Result<String> {
        let encoded =
            base32::encode(base32::Alphabet::Rfc4648 { padding: false }, data).to_lowercase();

        let available_len = self.max_total_len - self.suffix.len() - 1;
        let labels: Vec<&str> = encoded
            .as_bytes()
            .chunks(self.max_label_len)
            .map(|chunk| std::str::from_utf8(chunk).expect("Invalid UTF-8"))
            .collect();

        let dots_between_labels = labels.len() - 1;
        let labels_len = encoded.len() + dots_between_labels;

        if labels_len > available_len {
            bail!(
                "Data too long to encode in DNS labels. Available: {}, Required: {}",
                available_len,
                labels_len
            );
        }

        Ok(format!("{}.{}", labels.join("."), self.suffix))
    }

    /// Decodes data from a domain name that was encoded using the `encode` method. The domain name must end with the specified suffix, and the labels before the suffix will be concatenated and decoded from base32. The decoding is case-insensitive.
    ///
    /// # Example
    /// ```
    /// let data = endec.decode("jbswy3dpfqqfo33snrscc.example.com.")?;
    /// ```
    pub fn decode(&self, domain: &str) -> Result<Vec<u8>> {
        let mut domain = domain.to_lowercase();
        if !domain.ends_with('.') {
            domain = format!("{}.", domain);
        }

        if !domain.ends_with(&format!(".{}", self.suffix)) {
            bail!("Domain does not end with the expected suffix");
        }

        let prefix = &domain[..domain.len() - self.suffix.len() - 1];
        let labels: Vec<&str> = prefix.split('.').collect();
        let encoded = labels.concat().to_uppercase();

        let bytes = base32::decode(base32::Alphabet::Rfc4648 { padding: false }, &encoded)
            .ok_or_else(|| anyhow::anyhow!("Invalid base32 encoding"))?;
        Ok(bytes)
    }

    /// Returns the domain suffix for this encoder/decoder.
    ///
    /// # Example
    /// ```
    /// let suffix = endec.suffix();
    /// ```
    pub fn suffix(&self) -> &str {
        &self.suffix
    }

    /// Returns the maximum length of data that can be encoded in a domain name with the current suffix, taking into account the limitations of label lengths and total domain name length.
    ///
    /// # Example
    /// ```
    /// let max_data_len = endec.max_data_len();
    /// ```
    pub fn max_data_len(&self) -> usize {
        let available_len = self.max_total_len - self.suffix.len() - 1;
        let max_chars = (available_len / 64) * 63 + (available_len % 64);

        (max_chars) * 5 / 8
    }
}

/// A DNS encoder/decoder that encodes data into the TXT record of a DNS response using base32 encoding, and decodes data from such a TXT record. The encoded data will be in lowercase, and the maximum length of the encoded data is determined by the maximum length of a TXT record in a DNS response (255 bytes for the entire TXT record, including length byte).
pub struct B32ResponseEndec {
    /// The maximum total length of the encoded data in the TXT record, which must be less than or equal to 253 characters to fit within the DNS response limits.
    max_total_len: usize,
}

impl B32ResponseEndec {
    /// Creates a new `B32ResponseEndec` with the default maximum total length of 253 characters for the encoded data. This allows for some overhead in the DNS response while still fitting within the typical limits of DNS record sizes.
    ///
    /// # Example
    /// ```
    /// let endec = B32ResponseEndec::new();
    /// ```
    pub fn new() -> Self {
        Self { max_total_len: 253 }
    }

    /// Returns the maximum length of data that can be encoded in a DNS response using this encoder, taking into account the limitations of DNS record sizes and the overhead of base32 encoding.
    ///
    /// # Example
    /// ```
    /// let max_data_len = endec.max_data_len();
    /// ```
    pub fn max_data_len(&self) -> usize {
        (self.max_total_len) * 5 / 8
    }

    /// Encodes the given data into a byte vector that can be included in a DNS response, using base32 encoding. The encoded data will be in lowercase, and the total length of the encoded data must not exceed the maximum total length allowed by this encoder.
    ///
    /// # Example
    /// ```
    /// let encoded = endec.encode(b"Hello, World!")?;
    /// ```
    pub fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let encoded =
            base32::encode(base32::Alphabet::Rfc4648 { padding: false }, data).to_lowercase();
        if encoded.len() > self.max_total_len {
            bail!(
                "Data too long to encode in DNS response. Available: {}, Required: {}",
                self.max_total_len,
                encoded.len()
            );
        }
        Ok(encoded.into_bytes())
    }

    /// Decodes data from a byte vector that was encoded using the `encode` method. The input is expected to be a UTF-8 encoded string in lowercase, which will be decoded from base32. The decoding is case-insensitive.
    ///
    /// # Example
    /// ```
    /// let decoded = endec.decode(b"jbswy3dpfqqfo33snrscc")?;
    /// ```
    pub fn decode(&self, content: &[u8]) -> Result<Vec<u8>> {
        let encoded = std::str::from_utf8(content)?.to_uppercase();
        let bytes = base32::decode(base32::Alphabet::Rfc4648 { padding: false }, &encoded)
            .ok_or_else(|| anyhow::anyhow!("Invalid base32 encoding"))?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod b32_domain_endec_tests {
    use super::*;

    #[test]
    fn test_endec_success() -> Result<()> {
        let encoder = B32DomainEndec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        assert_eq!(domain, "jbswy3dpfqqfo33snrscc.example.com.");
        let decoded = encoder.decode(&domain)?;
        assert_eq!(data.to_vec(), decoded);
        Ok(())
    }

    #[test]
    fn test_endec_kinds_of_suffix() -> Result<()> {
        assert!(B32DomainEndec::new("").is_err());
        assert!(B32DomainEndec::new("example.com.").is_ok());
        assert!(B32DomainEndec::new(&"a".repeat(254)).is_err());
        Ok(())
    }

    #[test]
    fn test_endec_suffix_too_long() -> Result<()> {
        let long_suffix = "a".repeat(246) + ".com";
        assert!(B32DomainEndec::new(&long_suffix).is_err());
        Ok(())
    }

    #[test]
    fn test_encoder_too_long() -> Result<()> {
        let encoder = B32DomainEndec::new("example.com")?;
        let data = vec![0u8; 200];
        assert!(encoder.encode(&data).is_err());
        Ok(())
    }

    #[test]
    fn test_decoder_wrong_suffix() -> Result<()> {
        let encoder = B32DomainEndec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        let decoder = B32DomainEndec::new("other.com")?;
        assert!(decoder.decode(&domain).is_err());
        Ok(())
    }

    #[test]
    fn test_decoder_invalid_base32() -> Result<()> {
        let decoder = B32DomainEndec::new("example.com")?;
        let domain = "!.example.com";
        assert!(decoder.decode(domain).is_err());
        Ok(())
    }

    #[test]
    fn test_invariant_to_random_uppercase() -> Result<()> {
        let encoder = B32DomainEndec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        let random_uppercase_domain = domain
            .chars()
            .map(|c| {
                if rand::random() {
                    c.to_ascii_uppercase()
                } else {
                    c.to_ascii_lowercase()
                }
            })
            .collect::<String>();
        let decoded = encoder.decode(&random_uppercase_domain)?;
        assert_eq!(data.to_vec(), decoded);
        Ok(())
    }

    #[test]
    fn test_size() -> Result<()> {
        for i in 1..100 {
            let middle: Vec<&str> = (0..i).map(|_| "a").collect();
            let suffix = format!("{}.com", middle.join("."));
            let encoder = B32DomainEndec::new(&suffix)?;
            let max_size = encoder.max_data_len();
            let success_data = vec![0u8; max_size];
            let unsuccess_data = vec![0u8; max_size + 1];
            assert!(encoder.encode(&success_data).is_ok());
            assert!(encoder.encode(&unsuccess_data).is_err());
        }

        Ok(())
    }
}

#[cfg(test)]
mod b32_response_endec_tests {
    use super::*;

    #[test]
    fn test_endec_success() -> Result<()> {
        let encoder = B32ResponseEndec::new();
        let data = b"Hello, World!";
        let encoded = encoder.encode(data)?;
        let decoded = encoder.decode(&encoded)?;
        assert_eq!(data.to_vec(), decoded);
        Ok(())
    }

    #[test]
    fn test_encode_length() -> Result<()> {
        let encoder = B32ResponseEndec::new();
        let max_data_len = encoder.max_data_len();
        let data = vec![0u8; max_data_len];
        assert!(encoder.encode(&data).is_ok());
        let too_long_data = vec![0u8; max_data_len + 1];
        assert!(encoder.encode(&too_long_data).is_err());
        Ok(())
    }
}
