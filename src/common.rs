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
