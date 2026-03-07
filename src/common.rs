/// Extract domain from an email address. Handles "Name <addr>" format.
pub fn extract_domain(addr: &str) -> &str {
    let inner = if let (Some(s), Some(e)) = (addr.find('<'), addr.rfind('>')) {
        addr[s + 1..e].trim()
    } else {
        addr.trim()
    };
    if let Some(at) = inner.rfind('@') {
        &inner[at + 1..]
    } else {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_plain_address() {
        assert_eq!(extract_domain("user@example.com"), "example.com");
    }

    #[test]
    fn domain_name_addr_format() {
        assert_eq!(extract_domain("John Doe <user@example.com>"), "example.com");
    }

    #[test]
    fn domain_no_at_sign() {
        assert_eq!(extract_domain("notanemail"), "");
    }

    #[test]
    fn domain_empty_string() {
        assert_eq!(extract_domain(""), "");
    }

    #[test]
    fn domain_quoted_display_name() {
        assert_eq!(extract_domain("\"Foo Bar\" <foo@bar.com>"), "bar.com");
    }

    #[test]
    fn domain_subdomain() {
        assert_eq!(extract_domain("user@mail.sub.example.com"), "mail.sub.example.com");
    }

    #[test]
    fn domain_angle_brackets_only() {
        assert_eq!(extract_domain("<user@example.com>"), "example.com");
    }

    #[test]
    fn domain_whitespace_around_address() {
        assert_eq!(extract_domain("  user@example.com  "), "example.com");
    }

    #[test]
    fn domain_multiple_at_signs_uses_last() {
        // rfind('@') — returns the last @
        // inner = "a@b@example.com" → rfind('@') = position of last @ → "example.com"
        assert_eq!(extract_domain("a@b@example.com"), "example.com");
    }

    #[test]
    fn domain_only_at_sign() {
        // "@" → rfind('@') = 0 → &inner[1..] = ""
        assert_eq!(extract_domain("@"), "");
    }

    #[test]
    fn domain_preserves_case() {
        // extract_domain does NOT lowercase; callers are responsible for that
        assert_eq!(extract_domain("User@EXAMPLE.COM"), "EXAMPLE.COM");
    }
}
