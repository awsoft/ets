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
