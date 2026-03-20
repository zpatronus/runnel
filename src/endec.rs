use std::fmt::format;

use anyhow::{Ok, Result, bail};
use base32::encode;

pub struct DnsEndec {
    suffix: String,
    max_label_len: usize,
    max_total_len: usize,
}

impl DnsEndec {
    pub fn new(suffix: &str) -> Result<Self> {
        if suffix.len() > 253 {
            bail!("Suffix too long");
        }
        if suffix.is_empty() || suffix.ends_with('.') {
            bail!("Suffix must not be empty or end with a dot");
        }

        Ok(Self {
            suffix: suffix.to_string().to_lowercase(),
            max_label_len: 63,
            max_total_len: 253,
        })
    }

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

    pub fn decode(&self, domain: &str) -> Result<Vec<u8>> {
        let domain = domain.to_lowercase();
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endec_success() -> Result<()> {
        let encoder = DnsEndec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        let decoded = encoder.decode(&domain)?;
        assert_eq!(data.to_vec(), decoded);
        Ok(())
    }

    #[test]
    fn test_endec_bad_suffix() {
        assert!(DnsEndec::new("").is_err());
        assert!(DnsEndec::new("example.com.").is_err());
        assert!(DnsEndec::new(&"a".repeat(254)).is_err());
    }

    #[test]
    fn test_encoder_too_long() -> Result<()> {
        let encoder = DnsEndec::new("example.com")?;
        let data = vec![0u8; 200];
        assert!(encoder.encode(&data).is_err());
        Ok(())
    }

    #[test]
    fn test_decoder_wrong_suffix() -> Result<()> {
        let encoder = DnsEndec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        let decoder = DnsEndec::new("other.com")?;
        assert!(decoder.decode(&domain).is_err());
        Ok(())
    }

    #[test]
    fn test_decoder_invalid_base32() -> Result<()> {
        let decoder = DnsEndec::new("example.com")?;
        let domain = "!.example.com";
        assert!(decoder.decode(domain).is_err());
        Ok(())
    }

    #[test]
    fn test_invariant_to_random_uppercase() -> Result<()> {
        let encoder = DnsEndec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        let random_uppercase_domain = domain
            .chars()
            .map(|c| {
                if c.is_ascii_lowercase() {
                    c.to_ascii_uppercase()
                } else {
                    c
                }
            })
            .collect::<String>();
        let decoded = encoder.decode(&random_uppercase_domain)?;
        assert_eq!(data.to_vec(), decoded);
        Ok(())
    }
}
