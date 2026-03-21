use std::fmt::format;

use anyhow::{Ok, Result, bail};
use base32::encode;

pub struct B32Endec {
    suffix: String,
    max_label_len: usize,
    max_total_len: usize,
}

impl B32Endec {
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

    pub fn max_data_len(&self) -> usize {
        let available_len = self.max_total_len - self.suffix.len() - 1;
        let max_chars = (available_len / 64) * 63 + (available_len % 64);

        (max_chars) * 5 / 8
    }
}

#[cfg(test)]
mod b32_endec_tests {
    use super::*;

    #[test]
    fn test_endec_success() -> Result<()> {
        let encoder = B32Endec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        assert_eq!(domain, "jbswy3dpfqqfo33snrscc.example.com.");
        let decoded = encoder.decode(&domain)?;
        assert_eq!(data.to_vec(), decoded);
        Ok(())
    }

    #[test]
    fn test_endec_kinds_of_suffix() -> Result<()> {
        assert!(B32Endec::new("").is_err());
        assert!(B32Endec::new("example.com.").is_ok());
        assert!(B32Endec::new(&"a".repeat(254)).is_err());
        Ok(())
    }

    #[test]
    fn test_endec_suffix_too_long() -> Result<()> {
        let long_suffix = "a".repeat(246) + ".com";
        assert!(B32Endec::new(&long_suffix).is_err());
        Ok(())
    }

    #[test]
    fn test_encoder_too_long() -> Result<()> {
        let encoder = B32Endec::new("example.com")?;
        let data = vec![0u8; 200];
        assert!(encoder.encode(&data).is_err());
        Ok(())
    }

    #[test]
    fn test_decoder_wrong_suffix() -> Result<()> {
        let encoder = B32Endec::new("example.com")?;
        let data = b"Hello, World!";
        let domain = encoder.encode(data)?;
        let decoder = B32Endec::new("other.com")?;
        assert!(decoder.decode(&domain).is_err());
        Ok(())
    }

    #[test]
    fn test_decoder_invalid_base32() -> Result<()> {
        let decoder = B32Endec::new("example.com")?;
        let domain = "!.example.com";
        assert!(decoder.decode(domain).is_err());
        Ok(())
    }

    #[test]
    fn test_invariant_to_random_uppercase() -> Result<()> {
        let encoder = B32Endec::new("example.com")?;
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
            let encoder = B32Endec::new(&suffix)?;
            let max_size = encoder.max_data_len();
            let success_data = vec![0u8; max_size];
            let unsuccess_data = vec![0u8; max_size + 1];
            assert!(encoder.encode(&success_data).is_ok());
            assert!(encoder.encode(&unsuccess_data).is_err());
        }

        Ok(())
    }
}
