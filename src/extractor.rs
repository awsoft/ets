use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// All recognised tag category names — used for normalisation/clamping
// ---------------------------------------------------------------------------
const TAG_CATEGORIES: &[&str] = &[
    "action_required", "personal", "financial", "investment",
    "job", "kids", "travel", "marketing", "social", "newsletter",
];

// Static regex compiled once for get_from_name (Fix 5)
static FROM_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^"?([^"<]+?)"?\s*<"#).unwrap()
});

// Static default tags for unclassified (no template matched) — cloned when needed (Fix 11)
static DEFAULT_TAGS: LazyLock<HashMap<String, f64>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("personal".to_string(), 0.3);
    m.insert("action_required".to_string(), 0.2);
    m
});

// ---------------------------------------------------------------------------
// UTF-8-safe truncation helper (Fix 1)
// ---------------------------------------------------------------------------

/// Truncate `s` to at most `max_bytes` bytes without splitting a UTF-8 character.
fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if max_bytes >= s.len() {
        return s;
    }
    // Walk backwards from max_bytes to find a char boundary
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

// ---------------------------------------------------------------------------
// JSON schema structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct TemplatesFile {
    pub templates: Vec<Template>,
    #[serde(default)]
    pub tag_rules: Vec<TagRule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Template {
    pub id: String,
    pub name: String,
    pub priority: i32,
    #[serde(rename = "type")]
    pub email_type: String,
    pub detect: DetectRules,
    pub extract: HashMap<String, FieldDef>,
    /// Base tag weights for this template type (0.0–1.0 per category).
    #[serde(default)]
    pub tags: HashMap<String, f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DetectRules {
    pub sender_domain: Option<String>,
    pub sender_contains: Option<String>,
    pub subject_regex: Option<String>,
    pub snippet_regex: Option<String>,
    pub any: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub source: Option<Value>, // string or array of strings
    pub regex: Option<String>,
    pub enum_map: Option<HashMap<String, String>>,
    #[serde(rename = "static")]
    pub static_value: Option<Value>,
    pub max_chars: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TagRule {
    pub id: String,
    #[serde(rename = "match")]
    pub match_rules: TagMatch,
    pub adjust: HashMap<String, f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TagMatch {
    pub subject_regex: Option<String>,
}

// ---------------------------------------------------------------------------
// Compiled forms
// ---------------------------------------------------------------------------

struct CompiledTemplate {
    template: Template,
    subject_re: Option<Regex>,
    snippet_re: Option<Regex>,
    field_regexes: HashMap<String, Regex>,
    enum_regexes: HashMap<String, Vec<(Regex, String)>>,
}

struct CompiledTagRule {
    _id: String,
    pattern: Option<Regex>,
    adjust: HashMap<String, f64>,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

pub struct TemplateEngine {
    templates: Vec<CompiledTemplate>,
    tag_rules: Vec<CompiledTagRule>,
}

impl TemplateEngine {
    pub fn load(templates_path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(templates_path)
            .with_context(|| format!("Cannot read templates: {}", templates_path.display()))?;
        let mut file: TemplatesFile =
            serde_json::from_str(&raw).with_context(|| "Templates JSON parse error")?;

        // Sort templates by priority descending
        file.templates.sort_by(|a, b| b.priority.cmp(&a.priority));

        // Compile templates
        let mut compiled_templates = Vec::new();
        for tmpl in file.templates {
            let subject_re = tmpl
                .detect
                .subject_regex
                .as_deref()
                .and_then(|p| Regex::new(p).ok());
            let snippet_re = tmpl
                .detect
                .snippet_regex
                .as_deref()
                .and_then(|p| Regex::new(p).ok());

            let mut field_regexes: HashMap<String, Regex> = HashMap::new();
            let mut enum_regexes: HashMap<String, Vec<(Regex, String)>> = HashMap::new();

            for (name, field) in &tmpl.extract {
                if let Some(pat) = &field.regex {
                    if let Ok(re) = Regex::new(pat) {
                        field_regexes.insert(name.clone(), re);
                    }
                }
                if let Some(enum_map) = &field.enum_map {
                    let pairs: Vec<(Regex, String)> = enum_map
                        .iter()
                        .filter_map(|(pat, val)| {
                            Regex::new(pat).ok().map(|re| (re, val.clone()))
                        })
                        .collect();
                    if !pairs.is_empty() {
                        enum_regexes.insert(name.clone(), pairs);
                    }
                }
            }

            compiled_templates.push(CompiledTemplate {
                template: tmpl,
                subject_re,
                snippet_re,
                field_regexes,
                enum_regexes,
            });
        }

        // Compile tag rules
        let compiled_tag_rules = file
            .tag_rules
            .into_iter()
            .map(|rule| {
                let pattern = rule
                    .match_rules
                    .subject_regex
                    .as_deref()
                    .and_then(|p| Regex::new(p).ok());
                CompiledTagRule {
                    _id: rule.id,
                    pattern,
                    adjust: rule.adjust,
                }
            })
            .collect();

        Ok(TemplateEngine {
            templates: compiled_templates,
            tag_rules: compiled_tag_rules,
        })
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Delegate to shared common::extract_domain and return owned String.
    fn get_domain(from: &str) -> String {
        crate::common::extract_domain(from).to_string()
    }

    /// Extract display name from email. Uses a module-level compiled regex (Fix 5).
    fn get_from_name(email: &Value) -> String {
        if let Some(name) = email.get("from_name").and_then(|v| v.as_str()) {
            let t = name.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
        let from = email.get("from").and_then(|v| v.as_str()).unwrap_or("");
        if let Some(cap) = FROM_NAME_RE.captures(from) {
            return cap[1].trim().to_string();
        }
        let local = from.split('@').next().unwrap_or(from);
        local.replace(['.', '_', '-'], " ")
    }

    fn get_source_text<'a>(source: &str, email: &'a Value, from_name: &'a str) -> &'a str {
        match source {
            "subject" => email.get("subject").and_then(|v| v.as_str()).unwrap_or(""),
            "snippet" => email.get("snippet").and_then(|v| v.as_str()).unwrap_or(""),
            "sender" => email.get("from").and_then(|v| v.as_str()).unwrap_or(""),
            "from_name" => from_name,
            _ => "",
        }
    }

    /// Check whether an email matches a template's detect rules.
    /// Uses u8 counters instead of Vec<bool> allocation (Fix 6).
    fn matches_detect(&self, ct: &CompiledTemplate, email: &Value) -> bool {
        let detect = &ct.template.detect;
        let any_mode = detect.any.unwrap_or(false);
        let from = email.get("from").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
        let domain = Self::get_domain(&from);
        let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
        let snippet = email.get("snippet").and_then(|v| v.as_str()).unwrap_or("");

        let mut total: u8 = 0;
        let mut pass: u8 = 0;

        if let Some(d) = &detect.sender_domain {
            total += 1;
            if domain == d.to_lowercase() { pass += 1; }
        }
        if let Some(sub) = &detect.sender_contains {
            total += 1;
            if from.contains(&sub.to_lowercase()) { pass += 1; }
        }
        if let Some(re) = &ct.subject_re {
            total += 1;
            if re.is_match(subject) { pass += 1; }
        }
        if let Some(re) = &ct.snippet_re {
            total += 1;
            if re.is_match(snippet) { pass += 1; }
        }

        if total == 0 { return true; }
        if any_mode { pass > 0 } else { pass == total }
    }

    fn apply_field(
        &self,
        ct: &CompiledTemplate,
        field_name: &str,
        field_def: &FieldDef,
        email: &Value,
        from_name: &str,
    ) -> Option<Value> {
        if let Some(sv) = &field_def.static_value {
            return Some(sv.clone());
        }

        let sources: Vec<&str> = match &field_def.source {
            Some(Value::String(s)) => vec![s.as_str()],
            Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str()).collect(),
            _ => vec!["snippet"],
        };

        let max_chars = field_def.max_chars;
        let has_regex = field_def.regex.is_some();
        let has_enum = field_def.enum_map.is_some();

        for src in &sources {
            let text = if *src == "from_name" {
                from_name
            } else {
                Self::get_source_text(src, email, from_name)
            };
            if text.is_empty() {
                continue;
            }

            if has_regex {
                if let Some(re) = ct.field_regexes.get(field_name) {
                    if let Some(caps) = re.captures(text) {
                        let val = caps.get(1).or_else(|| caps.get(0)).map(|m| m.as_str().trim())?;
                        // Fix 1: use safe_truncate to avoid splitting multi-byte UTF-8 chars
                        let val = if let Some(max) = max_chars {
                            safe_truncate(val, max)
                        } else {
                            val
                        };
                        return Some(Value::String(val.to_string()));
                    }
                }
            }

            if has_enum {
                if let Some(pairs) = ct.enum_regexes.get(field_name) {
                    for (re, value) in pairs {
                        if re.is_match(text) {
                            return Some(Value::String(value.clone()));
                        }
                    }
                }
            }

            if !has_regex && !has_enum {
                if let Some(max) = max_chars {
                    // Fix 1: safe_truncate guards against mid-char byte slicing
                    return Some(Value::String(
                        safe_truncate(text, max).trim().to_string(),
                    ));
                }
            }
        }
        None
    }

    // ------------------------------------------------------------------
    // Tag computation
    // ------------------------------------------------------------------

    /// Apply base template tags + tag_rules adjustments (max merge) → final tags map.
    fn compute_tags(&self, email: &Value, base_tags: &HashMap<String, f64>) -> HashMap<String, f64> {
        let mut tags: HashMap<String, f64> = base_tags.clone();

        let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
        let snippet = email.get("snippet").and_then(|v| v.as_str()).unwrap_or("");
        // Check rules against subject; also check snippet for broader coverage
        let check_texts = [subject, snippet];

        for rule in &self.tag_rules {
            if let Some(re) = &rule.pattern {
                let matched = check_texts.iter().any(|t| !t.is_empty() && re.is_match(t));
                if matched {
                    for (category, &value) in &rule.adjust {
                        let current = tags.get(category.as_str()).copied().unwrap_or(0.0);
                        tags.insert(category.clone(), current.max(value));
                    }
                }
            }
        }

        // Normalise: keep only known categories, clamp to [0.0, 1.0], drop zeros
        let mut result = HashMap::new();
        for &cat in TAG_CATEGORIES {
            let v = tags.get(cat).copied().unwrap_or(0.0).clamp(0.0, 1.0);
            if v > 0.0 {
                result.insert(cat.to_string(), v);
            }
        }
        result
    }

    /// Derive snippet policy from final tags.
    /// Returns ("full"|"short"|"omitted", effective_cap)
    fn snippet_policy(tags: &HashMap<String, f64>) -> (&'static str, usize) {
        let action_req = tags.get("action_required").copied().unwrap_or(0.0);
        let personal   = tags.get("personal").copied().unwrap_or(0.0);
        let investment = tags.get("investment").copied().unwrap_or(0.0);

        if action_req >= 0.6 || personal >= 0.7 {
            ("full", usize::MAX)
        } else if action_req >= 0.3 || personal >= 0.4 || investment >= 0.7 {
            ("short", 100)
        } else {
            ("omitted", 0)
        }
    }

    // ------------------------------------------------------------------
    // Batch extraction
    // ------------------------------------------------------------------

    pub fn extract_batch(&self, filter_output: &Value, snippet_cap: usize, explain: bool) -> Value {
        let start = std::time::Instant::now();

        let empty = vec![];
        let passed   = filter_output.get("passed").and_then(|v| v.as_array()).unwrap_or(&empty);
        let uncertain = filter_output.get("uncertain").and_then(|v| v.as_array()).unwrap_or(&empty);
        let blocked  = filter_output.get("blocked").and_then(|v| v.as_array()).unwrap_or(&empty);

        let mut results: Vec<Value> = Vec::new();
        let mut extracted_structured: usize = 0;
        let mut snippet_only: usize = 0;
        let mut tags_only: usize = 0;

        for (bucket, emails) in [("passed", passed), ("uncertain", uncertain)] {
            for email in emails {
                let from_name = Self::get_from_name(email);
                let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
                let snippet = email.get("snippet").and_then(|v| v.as_str()).unwrap_or("");
                let from    = email.get("from").and_then(|v| v.as_str()).unwrap_or("");
                let date    = email.get("date").and_then(|v| v.as_str()).unwrap_or("");
                let id      = email.get("id").and_then(|v| v.as_str()).unwrap_or("");

                let mut matched_template: Option<&str> = None;
                let mut email_type = "unclassified";
                // Clone from static DEFAULT_TAGS (Fix 11)
                let mut base_tags: HashMap<String, f64> = DEFAULT_TAGS.clone();
                let mut extracted = serde_json::Map::new();

                for ct in &self.templates {
                    if self.matches_detect(ct, email) {
                        matched_template = Some(ct.template.id.as_str());
                        email_type = ct.template.email_type.as_str();
                        base_tags = ct.template.tags.clone();

                        for (fname, fdef) in &ct.template.extract {
                            if let Some(val) = self.apply_field(ct, fname, fdef, email, &from_name) {
                                extracted.insert(fname.clone(), val);
                            }
                        }
                        break;
                    }
                }

                // Compute final tags (base + tag_rules via max merge)
                let final_tags = self.compute_tags(email, &base_tags);
                let (policy, tag_cap) = Self::snippet_policy(&final_tags);

                // Apply snippet: tag policy cap, bounded by caller's snippet_cap for full.
                // Fix 1: use safe_truncate to prevent panics on multi-byte UTF-8 text.
                let final_snippet: Value = if policy == "omitted" {
                    Value::Null
                } else {
                    let cap = if policy == "full" {
                        snippet_cap
                    } else {
                        tag_cap.min(snippet_cap)
                    };
                    if snippet.is_empty() {
                        Value::Null
                    } else {
                        Value::String(safe_truncate(snippet, cap).to_string())
                    }
                };

                // Build tags JSON object (only non-zero values)
                let tags_json: serde_json::Map<String, Value> = final_tags
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::from(*v)))
                    .collect();

                let has_extracted = !extracted.is_empty();
                match policy {
                    "omitted" => tags_only += 1,
                    _ if email_type != "unclassified" && has_extracted => extracted_structured += 1,
                    _ => snippet_only += 1,
                }

                let mut record = serde_json::json!({
                    "id": id,
                    "from": from,
                    "subject": subject,
                    "date": date,
                    "type": email_type,
                    "tags": Value::Object(tags_json),
                    "snippet": final_snippet,
                    "snippet_policy_applied": policy,
                    "source_bucket": bucket,
                    "matched_template": matched_template,
                });

                if has_extracted {
                    record["extracted"] = Value::Object(extracted);
                }
                if explain {
                    record["_matched_template"] =
                        matched_template.map(Value::from).unwrap_or(Value::Null);
                }

                results.push(record);
            }
        }

        let results_len  = results.len();
        let blocked_len  = blocked.len();
        let elapsed_ms   = start.elapsed().as_millis() as u64;

        serde_json::json!({
            "emails": results,
            "stats": {
                "total_in": results_len,
                "blocked_dropped": blocked_len,
                "extracted_structured": extracted_structured,
                "snippet_only": snippet_only,
                "tags_only": tags_only,
                "snippet_cap": snippet_cap,
                "elapsed_ms": elapsed_ms
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_templates_file(json_str: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("ets_extractor_tmpl_{}.json", n));
        std::fs::write(&path, json_str).unwrap();
        path
    }

    fn make_engine(json_str: &str) -> TemplateEngine {
        let path = temp_templates_file(json_str);
        let engine = TemplateEngine::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        engine
    }

    fn default_engine() -> TemplateEngine {
        make_engine(TEST_TEMPLATES_JSON)
    }

    /// Wrap emails into the filter_output format extract_batch expects.
    fn filter_output_with(passed: Vec<Value>) -> Value {
        json!({
            "passed": passed,
            "blocked": [],
            "uncertain": [],
            "stats": {
                "total": passed.len(),
                "passed": passed.len(),
                "blocked": 0,
                "uncertain": 0,
                "rules_loaded": 0,
                "elapsed_ms": 0
            }
        })
    }

    fn filter_output_with_uncertain(uncertain: Vec<Value>) -> Value {
        json!({
            "passed": [],
            "blocked": [],
            "uncertain": uncertain,
            "stats": {}
        })
    }

    fn email(from: &str, subject: &str, snippet: &str) -> Value {
        json!({
            "id": "test-id",
            "from": from,
            "subject": subject,
            "snippet": snippet,
            "date": "2025-01-01T00:00:00Z"
        })
    }

    const TEST_TEMPLATES_JSON: &str = r#"{
        "templates": [
            {
                "id": "bank-tmpl",
                "name": "Bank Notification",
                "priority": 100,
                "type": "bank_notification",
                "detect": {"sender_domain": "mybank.com"},
                "extract": {
                    "notice_type": {"static": "bank"},
                    "amount": {"source": "snippet", "regex": "\\$([\\d,]+\\.\\d{2})"},
                    "summary": {"source": "subject", "max_chars": 20},
                    "direction": {
                        "source": "subject",
                        "enum_map": {"(?i)deposit": "inbound", "(?i)withdraw|payment": "outbound"}
                    }
                },
                "tags": {"financial": 0.8, "action_required": 0.5}
            },
            {
                "id": "newsletter-tmpl",
                "name": "Newsletter",
                "priority": 50,
                "type": "newsletter",
                "detect": {
                    "sender_contains": "newsletter@",
                    "subject_regex": "(?i)(weekly|monthly|digest)"
                },
                "extract": {},
                "tags": {"newsletter": 0.9, "marketing": 0.3}
            },
            {
                "id": "shipping-tmpl",
                "name": "Shipping Notification",
                "priority": 75,
                "type": "shipping",
                "detect": {
                    "any": true,
                    "subject_regex": "(?i)(shipped|tracking|delivery)"
                },
                "extract": {},
                "tags": {"travel": 0.3, "action_required": 0.2}
            },
            {
                "id": "multi-source-tmpl",
                "name": "Multi Source Test",
                "priority": 60,
                "type": "test_multi",
                "detect": {"sender_domain": "multisource.com"},
                "extract": {
                    "name_field": {"source": ["from_name", "subject"], "max_chars": 50}
                },
                "tags": {"personal": 0.4}
            }
        ],
        "tag_rules": [
            {
                "id": "tag-urgent",
                "match": {"subject_regex": "(?i)(urgent|action required|immediate)"},
                "adjust": {"action_required": 0.9, "personal": 0.3}
            },
            {
                "id": "tag-investment",
                "match": {"subject_regex": "(?i)(stock|portfolio|dividend|market)"},
                "adjust": {"investment": 0.85, "newsletter": 0.3}
            }
        ]
    }"#;

    // ---- Template loading ----

    #[test]
    fn load_templates_from_json() {
        let engine = default_engine();
        assert_eq!(engine.templates.len(), 4);
    }

    #[test]
    fn templates_sorted_by_priority_descending() {
        let engine = default_engine();
        let priorities: Vec<i32> = engine
            .templates
            .iter()
            .map(|ct| ct.template.priority)
            .collect();
        for window in priorities.windows(2) {
            assert!(
                window[0] >= window[1],
                "templates should be sorted descending: {:?}",
                priorities
            );
        }
    }

    #[test]
    fn load_templates_invalid_path_returns_error() {
        let result = TemplateEngine::load(std::path::Path::new("/nonexistent/templates.json"));
        assert!(result.is_err());
    }

    // ---- UTF-8 safe truncation ----

    #[test]
    fn safe_truncate_ascii_exact() {
        assert_eq!(safe_truncate("hello world", 5), "hello");
    }

    #[test]
    fn safe_truncate_no_truncation_needed() {
        assert_eq!(safe_truncate("hello", 100), "hello");
    }

    #[test]
    fn safe_truncate_exact_length() {
        let s = "hello";
        assert_eq!(safe_truncate(s, 5), "hello");
    }

    #[test]
    fn safe_truncate_empty_string() {
        assert_eq!(safe_truncate("", 10), "");
    }

    #[test]
    fn safe_truncate_zero_max() {
        assert_eq!(safe_truncate("hello", 0), "");
    }

    #[test]
    fn safe_truncate_emoji_no_split() {
        // "🎉" = 4 bytes; "abc🎉" = 7 bytes
        // truncating at 5 should give "abc" (3 bytes, not split the 4-byte emoji)
        let s = "abc🎉";
        assert_eq!(safe_truncate(s, 5), "abc");
        assert_eq!(safe_truncate(s, 4), "abc");
        assert_eq!(safe_truncate(s, 7), "abc🎉");
        assert_eq!(safe_truncate(s, 8), "abc🎉");
    }

    #[test]
    fn safe_truncate_accented_chars_no_split() {
        // "é" = 2 bytes (U+00E9); "café" = c(1)+a(1)+f(1)+é(2) = 5 bytes
        let s = "café";
        assert_eq!(safe_truncate(s, 5), "café");   // fits exactly
        assert_eq!(safe_truncate(s, 4), "caf");    // "é" at bytes 3-4, truncate to 3
        assert_eq!(safe_truncate(s, 3), "caf");
        assert_eq!(safe_truncate(s, 2), "ca");
    }

    #[test]
    fn safe_truncate_cjk_no_split() {
        // CJK chars are 3 bytes each; "你好世界" = 12 bytes
        let s = "你好世界";
        assert_eq!(safe_truncate(s, 12), "你好世界");
        assert_eq!(safe_truncate(s, 6), "你好");    // exact boundary
        assert_eq!(safe_truncate(s, 7), "你好");    // byte 7 is inside "世"
        assert_eq!(safe_truncate(s, 8), "你好");    // byte 8 is still inside "世"
        assert_eq!(safe_truncate(s, 3), "你");      // exact boundary at 3
        assert_eq!(safe_truncate(s, 4), "你");      // byte 4 is inside "好"
    }

    #[test]
    fn safe_truncate_mixed_multibyte() {
        // "Hello 世界!" = 5 + 1 + 3 + 3 + 1 = 13 bytes
        let s = "Hello 世界!";
        assert_eq!(safe_truncate(s, 6), "Hello ");  // boundary after space
        assert_eq!(safe_truncate(s, 9), "Hello 世"); // 6 + 3 = 9, exact boundary
        assert_eq!(safe_truncate(s, 10), "Hello 世"); // 10 is inside 界
    }

    // ---- Template detection ----

    #[test]
    fn detect_by_sender_domain() {
        let engine = default_engine();
        let e = email("alerts@mybank.com", "Account update", "Your balance changed");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let emails = result["emails"].as_array().unwrap();
        assert_eq!(emails.len(), 1);
        assert_eq!(emails[0]["type"].as_str().unwrap(), "bank_notification");
        assert_eq!(emails[0]["matched_template"].as_str().unwrap(), "bank-tmpl");
    }

    #[test]
    fn detect_by_sender_contains() {
        let engine = default_engine();
        // Both conditions must match (no any:true): sender_contains AND subject_regex
        let e = email("newsletter@brand.com", "Weekly digest is here", "");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let emails = result["emails"].as_array().unwrap();
        assert_eq!(emails[0]["matched_template"].as_str().unwrap(), "newsletter-tmpl");
    }

    #[test]
    fn detect_by_sender_contains_fails_if_subject_not_matching() {
        let engine = default_engine();
        // sender_contains matches but subject_regex doesn't → should NOT match newsletter
        let e = email("newsletter@brand.com", "Hello there", "");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let emails = result["emails"].as_array().unwrap();
        // Should not match newsletter-tmpl (both conditions required)
        assert_ne!(
            emails[0]["matched_template"].as_str().unwrap_or(""),
            "newsletter-tmpl"
        );
    }

    #[test]
    fn detect_any_mode_single_condition_matches() {
        let engine = default_engine();
        // shipping-tmpl: any=true, subject_regex="shipped|tracking|delivery"
        // Any email with that subject should match, regardless of domain
        let e = email("unknown@randomsender.com", "Your package has been shipped!", "");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let emails = result["emails"].as_array().unwrap();
        assert_eq!(emails[0]["matched_template"].as_str().unwrap(), "shipping-tmpl");
    }

    #[test]
    fn detect_no_match_returns_unclassified() {
        let engine = default_engine();
        let e = email("someone@unknown.org", "Hello there", "Just a regular email");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let emails = result["emails"].as_array().unwrap();
        assert_eq!(emails[0]["type"].as_str().unwrap(), "unclassified");
        assert!(emails[0]["matched_template"].is_null());
    }

    #[test]
    fn highest_priority_template_wins() {
        let engine = default_engine();
        // bank-tmpl (priority 100) beats shipping-tmpl (priority 75)
        // Even if both could match
        let e = email("alerts@mybank.com", "Your package has been shipped", "");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let emails = result["emails"].as_array().unwrap();
        // bank-tmpl is priority 100 and matches by sender_domain
        assert_eq!(emails[0]["matched_template"].as_str().unwrap(), "bank-tmpl");
    }

    // ---- Field extraction ----

    #[test]
    fn static_field_extraction() {
        let engine = default_engine();
        let e = email("alerts@mybank.com", "Deposit received", "$100.00 deposited");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        assert_eq!(extracted["notice_type"].as_str().unwrap(), "bank");
    }

    #[test]
    fn regex_capture_extraction() {
        let engine = default_engine();
        let e = email("alerts@mybank.com", "Payment received", "Your deposit of $1,234.56 was processed");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        assert_eq!(extracted["amount"].as_str().unwrap(), "1,234.56");
    }

    #[test]
    fn max_chars_truncation_field() {
        let engine = default_engine();
        // summary has max_chars: 20
        let e = email("alerts@mybank.com", "This is a very long subject line that exceeds twenty chars", "");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        let summary = extracted["summary"].as_str().unwrap();
        assert!(summary.len() <= 20, "summary should be truncated to 20 bytes");
    }

    #[test]
    fn enum_map_first_match_wins() {
        let engine = default_engine();
        // "Deposit confirmation" should match "(?i)deposit" → "inbound"
        let e = email("alerts@mybank.com", "Deposit confirmation", "$500.00 received");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        assert_eq!(extracted["direction"].as_str().unwrap(), "inbound");
    }

    #[test]
    fn enum_map_second_match_if_first_fails() {
        let engine = default_engine();
        // "Payment processed" should match "(?i)withdraw|payment" → "outbound"
        let e = email("alerts@mybank.com", "Payment processed", "$200.00 sent");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        assert_eq!(extracted["direction"].as_str().unwrap(), "outbound");
    }

    #[test]
    fn multi_source_fallback_uses_first_nonempty() {
        let engine = default_engine();
        // multi-source-tmpl: source = ["from_name", "subject"]
        // If from_name is present, use it
        let e = json!({
            "id": "t1",
            "from": "Alice <alice@multisource.com>",
            "from_name": "Alice Smith",
            "subject": "Hello",
            "snippet": "",
            "date": ""
        });
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        // from_name = "Alice Smith" → used first
        assert_eq!(extracted["name_field"].as_str().unwrap(), "Alice Smith");
    }

    #[test]
    fn multi_source_fallback_to_second_source() {
        let engine = default_engine();
        // get_from_name always provides a computed fallback (local part of email address).
        // To force the "from_name" source to be empty, use an email with no "from" field
        // and no "from_name" field so the computed name is also "".
        let e = json!({
            "id": "t2",
            "from": "",
            "subject": "FallbackSubject",
            "snippet": "",
            "date": ""
        });
        // We also need the email to match the multisource.com template detect rule,
        // but with empty "from" the sender_domain won't match.
        // Use a custom template for this test instead.
        let tmpl_json = r#"{
            "templates": [{
                "id": "ms2-tmpl",
                "name": "Multi Source 2",
                "priority": 100,
                "type": "test_ms2",
                "detect": {"any": true, "subject_regex": "(?i)FallbackSubject"},
                "extract": {
                    "label": {"source": ["from_name", "subject"], "max_chars": 100}
                },
                "tags": {}
            }],
            "tag_rules": []
        }"#;
        let custom_engine = make_engine(tmpl_json);
        // from="" → get_from_name returns "" → source "from_name" is empty → falls back to "subject"
        let fo = filter_output_with(vec![e]);
        let result = custom_engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        assert_eq!(extracted["label"].as_str().unwrap(), "FallbackSubject");
    }

    // ---- UTF-8-safe truncation in field extraction ----

    #[test]
    fn max_chars_safe_with_multibyte_subject() {
        let json = r#"{
            "templates": [{
                "id": "mb-tmpl",
                "name": "Multibyte",
                "priority": 100,
                "type": "multibyte_test",
                "detect": {"sender_domain": "mb.com"},
                "extract": {
                    "title": {"source": "subject", "max_chars": 6}
                },
                "tags": {}
            }],
            "tag_rules": []
        }"#;
        let engine = make_engine(json);
        // "你好世界" = 12 bytes, max_chars=6 → "你好" (2 × 3-byte chars)
        let e = email("user@mb.com", "你好世界", "");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        let title = extracted["title"].as_str().unwrap();
        assert!(title.len() <= 6, "truncated bytes must be <= 6");
        assert!(std::str::from_utf8(title.as_bytes()).is_ok(), "must be valid UTF-8");
    }

    #[test]
    fn max_chars_safe_with_emoji_snippet() {
        let json = r#"{
            "templates": [{
                "id": "emoji-tmpl",
                "name": "Emoji Test",
                "priority": 100,
                "type": "emoji_test",
                "detect": {"sender_domain": "emoji.com"},
                "extract": {
                    "msg": {"source": "snippet", "max_chars": 5}
                },
                "tags": {}
            }],
            "tag_rules": []
        }"#;
        let engine = make_engine(json);
        // "Hi 🎉 bye" — "Hi 🎉" is 3+1+4 = 8 bytes; max_chars=5 → "Hi " (3 bytes, stops before 🎉)
        let e = email("user@emoji.com", "", "Hi 🎉 bye");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let extracted = &result["emails"][0]["extracted"];
        let msg = extracted["msg"].as_str().unwrap();
        assert!(msg.len() <= 5);
        assert!(std::str::from_utf8(msg.as_bytes()).is_ok());
    }

    // ---- Snippet policy ----

    #[test]
    fn snippet_policy_full_action_required() {
        let mut tags = HashMap::new();
        tags.insert("action_required".to_string(), 0.6);
        assert_eq!(TemplateEngine::snippet_policy(&tags).0, "full");
    }

    #[test]
    fn snippet_policy_full_personal() {
        let mut tags = HashMap::new();
        tags.insert("personal".to_string(), 0.7);
        assert_eq!(TemplateEngine::snippet_policy(&tags).0, "full");
    }

    #[test]
    fn snippet_policy_short_action_required_medium() {
        let mut tags = HashMap::new();
        tags.insert("action_required".to_string(), 0.4);
        let (policy, cap) = TemplateEngine::snippet_policy(&tags);
        assert_eq!(policy, "short");
        assert_eq!(cap, 100);
    }

    #[test]
    fn snippet_policy_short_personal_medium() {
        let mut tags = HashMap::new();
        tags.insert("personal".to_string(), 0.5);
        let (policy, cap) = TemplateEngine::snippet_policy(&tags);
        assert_eq!(policy, "short");
        assert_eq!(cap, 100);
    }

    #[test]
    fn snippet_policy_short_investment_high() {
        let mut tags = HashMap::new();
        tags.insert("investment".to_string(), 0.7);
        let (policy, cap) = TemplateEngine::snippet_policy(&tags);
        assert_eq!(policy, "short");
        assert_eq!(cap, 100);
    }

    #[test]
    fn snippet_policy_omitted_all_low() {
        let tags: HashMap<String, f64> = HashMap::new();
        let (policy, cap) = TemplateEngine::snippet_policy(&tags);
        assert_eq!(policy, "omitted");
        assert_eq!(cap, 0);
    }

    #[test]
    fn snippet_policy_omitted_marketing_only() {
        let mut tags = HashMap::new();
        tags.insert("marketing".to_string(), 0.9);
        let (policy, _) = TemplateEngine::snippet_policy(&tags);
        assert_eq!(policy, "omitted");
    }

    // ---- Tag computation ----

    #[test]
    fn base_tags_used_when_no_tag_rules_match() {
        let engine = default_engine();
        let e = email("alerts@mybank.com", "Regular notice", "Nothing special");
        let base = {
            let mut m = HashMap::new();
            m.insert("financial".to_string(), 0.8);
            m.insert("action_required".to_string(), 0.5);
            m
        };
        let tags = engine.compute_tags(&e, &base);
        assert!((tags["financial"] - 0.8).abs() < 1e-10);
        assert!((tags["action_required"] - 0.5).abs() < 1e-10);
    }

    #[test]
    fn tag_rules_boost_via_max_merge() {
        let engine = default_engine();
        // tag-urgent rule: subject_regex "urgent" → adjust action_required to max(existing, 0.9)
        let e = email("alerts@mybank.com", "URGENT: Action required now", "");
        let base = {
            let mut m = HashMap::new();
            m.insert("action_required".to_string(), 0.5); // base is 0.5
            m
        };
        let tags = engine.compute_tags(&e, &base);
        // tag-urgent should boost action_required to 0.9 via max merge
        assert!(
            tags["action_required"] >= 0.9,
            "tag rule should raise action_required to 0.9, got {:?}",
            tags.get("action_required")
        );
    }

    #[test]
    fn tag_rules_do_not_lower_values() {
        let engine = default_engine();
        // Tag rule adjusts action_required to 0.9 and personal to 0.3
        // If base personal is already 0.7, max(0.7, 0.3) = 0.7 (no lowering)
        let e = email("alerts@mybank.com", "URGENT notice", "");
        let base = {
            let mut m = HashMap::new();
            m.insert("personal".to_string(), 0.7);
            m
        };
        let tags = engine.compute_tags(&e, &base);
        assert!(
            tags.get("personal").copied().unwrap_or(0.0) >= 0.7,
            "max merge must not lower an existing value"
        );
    }

    #[test]
    fn unknown_category_not_in_output_tags() {
        let engine = default_engine();
        let base = {
            let mut m = HashMap::new();
            m.insert("not_a_real_category".to_string(), 0.9);
            m
        };
        let e = email("user@example.com", "Hello", "");
        let tags = engine.compute_tags(&e, &base);
        assert!(
            !tags.contains_key("not_a_real_category"),
            "unknown category should be excluded from output"
        );
    }

    #[test]
    fn tag_values_clamped_to_0_1() {
        let engine = default_engine();
        let base = {
            let mut m = HashMap::new();
            m.insert("financial".to_string(), 1.5); // over 1.0
            m.insert("marketing".to_string(), -0.5); // under 0.0
            m
        };
        let e = email("user@example.com", "", "");
        let tags = engine.compute_tags(&e, &base);
        assert!(tags.get("financial").copied().unwrap_or(0.0) <= 1.0, "clamped to 1.0");
        // marketing = -0.5 clamped to 0.0 → dropped (only non-zero kept)
        assert!(tags.get("marketing").is_none(), "zero/negative values dropped");
    }

    // ---- From name extraction ----

    #[test]
    fn from_name_from_explicit_field() {
        let e = json!({"from": "user@example.com", "from_name": "Alice Smith"});
        assert_eq!(TemplateEngine::get_from_name(&e), "Alice Smith");
    }

    #[test]
    fn from_name_from_angle_bracket_format() {
        let e = json!({"from": "Alice Smith <alice@example.com>"});
        assert_eq!(TemplateEngine::get_from_name(&e), "Alice Smith");
    }

    #[test]
    fn from_name_from_quoted_display_name() {
        let e = json!({"from": "\"Alice Smith\" <alice@example.com>"});
        assert_eq!(TemplateEngine::get_from_name(&e), "Alice Smith");
    }

    #[test]
    fn from_name_falls_back_to_local_part() {
        let e = json!({"from": "alice.smith@example.com"});
        // No display name → local part with dots replaced by spaces
        let name = TemplateEngine::get_from_name(&e);
        assert_eq!(name, "alice smith");
    }

    #[test]
    fn from_name_underscore_replaced() {
        let e = json!({"from": "alice_smith@example.com"});
        let name = TemplateEngine::get_from_name(&e);
        assert_eq!(name, "alice smith");
    }

    #[test]
    fn from_name_empty_from_name_field_falls_back() {
        // from_name field is empty → should fall back to from field parsing
        let e = json!({"from": "Bob Jones <bob@example.com>", "from_name": ""});
        let name = TemplateEngine::get_from_name(&e);
        assert_eq!(name, "Bob Jones");
    }

    #[test]
    fn from_name_whitespace_only_falls_back() {
        let e = json!({"from": "Carol <carol@example.com>", "from_name": "   "});
        let name = TemplateEngine::get_from_name(&e);
        assert_eq!(name, "Carol");
    }

    #[test]
    fn from_name_missing_fields_no_panic() {
        let e = json!({});
        let name = TemplateEngine::get_from_name(&e);
        assert_eq!(name, ""); // from is "" → local is "" → no @ → just ""
    }

    // ---- Batch extraction stats ----

    #[test]
    fn batch_stats_empty_no_panic() {
        let engine = default_engine();
        let fo = filter_output_with(vec![]);
        let result = engine.extract_batch(&fo, 300, false);
        assert_eq!(result["stats"]["total_in"].as_u64().unwrap(), 0);
        assert_eq!(result["emails"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn batch_stats_blocked_dropped() {
        let engine = default_engine();
        let fo = json!({
            "passed": [],
            "blocked": [
                {"id":"b1","from":"bad@spam.com","subject":"Spam","snippet":""},
                {"id":"b2","from":"bad@spam.com","subject":"Spam","snippet":""}
            ],
            "uncertain": [],
            "stats": {}
        });
        let result = engine.extract_batch(&fo, 300, false);
        // blocked emails should not appear in emails array
        assert_eq!(result["emails"].as_array().unwrap().len(), 0);
        assert_eq!(result["stats"]["blocked_dropped"].as_u64().unwrap(), 2);
    }

    #[test]
    fn batch_includes_uncertain_emails() {
        let engine = default_engine();
        let fo = filter_output_with_uncertain(vec![
            email("user@unknown.com", "Hello", "")
        ]);
        let result = engine.extract_batch(&fo, 300, false);
        assert_eq!(result["emails"].as_array().unwrap().len(), 1);
        assert_eq!(result["emails"][0]["source_bucket"].as_str().unwrap(), "uncertain");
    }

    #[test]
    fn snippet_cap_applied_in_full_policy() {
        let engine = default_engine();
        // urgent subject → action_required = 0.9 → full snippet policy
        // But caller's snippet_cap = 10 limits it
        let long_snippet = "A".repeat(200);
        let e = email("alerts@mybank.com", "URGENT payment", &long_snippet);
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 10, false);
        let snippet = result["emails"][0]["snippet"].as_str().unwrap();
        assert!(snippet.len() <= 10, "snippet_cap should limit full-policy snippets");
    }

    #[test]
    fn snippet_omitted_for_low_tag_scores() {
        let engine = default_engine();
        // newsletter tags: newsletter=0.9, marketing=0.3 → omitted policy
        let e = email("newsletter@brand.com", "Weekly digest is here", "Long newsletter text here");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let emails = result["emails"].as_array().unwrap();
        // newsletter template should match and produce omitted snippet
        if emails[0]["matched_template"].as_str().unwrap_or("") == "newsletter-tmpl" {
            assert!(
                emails[0]["snippet"].is_null(),
                "newsletter emails should have omitted (null) snippet"
            );
        }
    }

    #[test]
    fn extract_batch_output_has_required_fields() {
        let engine = default_engine();
        let e = email("alerts@mybank.com", "Account notice", "Check your account");
        let fo = filter_output_with(vec![e]);
        let result = engine.extract_batch(&fo, 300, false);
        let rec = &result["emails"][0];
        assert!(rec.get("id").is_some());
        assert!(rec.get("from").is_some());
        assert!(rec.get("subject").is_some());
        assert!(rec.get("date").is_some());
        assert!(rec.get("type").is_some());
        assert!(rec.get("tags").is_some());
        assert!(rec.get("snippet_policy_applied").is_some());
        assert!(rec.get("source_bucket").is_some());
    }
}
