use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct TemplatesFile {
    pub templates: Vec<Template>,
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

struct CompiledTemplate {
    template: Template,
    subject_re: Option<Regex>,
    snippet_re: Option<Regex>,
    // field_name -> compiled extraction regex
    field_regexes: HashMap<String, Regex>,
    // field_name -> [(compiled_pattern, value)]
    enum_regexes: HashMap<String, Vec<(Regex, String)>>,
}

pub struct TemplateEngine {
    templates: Vec<CompiledTemplate>,
}

impl TemplateEngine {
    pub fn load(templates_path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(templates_path)
            .with_context(|| format!("Cannot read templates: {}", templates_path.display()))?;
        let mut file: TemplatesFile =
            serde_json::from_str(&raw).with_context(|| "Templates JSON parse error")?;

        // Sort by priority descending — highest priority matched first
        file.templates.sort_by(|a, b| b.priority.cmp(&a.priority));

        let mut compiled = Vec::new();
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

            compiled.push(CompiledTemplate {
                template: tmpl,
                subject_re,
                snippet_re,
                field_regexes,
                enum_regexes,
            });
        }

        Ok(TemplateEngine { templates: compiled })
    }

    /// Extract the domain from an email address (handles "Name <addr>" format).
    fn get_domain(from: &str) -> String {
        let addr = if let (Some(s), Some(e)) = (from.find('<'), from.rfind('>')) {
            from[s + 1..e].trim()
        } else {
            from.trim()
        };
        if let Some(at) = addr.rfind('@') {
            addr[at + 1..].to_lowercase()
        } else {
            String::new()
        }
    }

    /// Derive a display name from the email's from/from_name fields.
    fn get_from_name(email: &Value) -> String {
        if let Some(name) = email.get("from_name").and_then(|v| v.as_str()) {
            let t = name.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
        let from = email
            .get("from")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        // "Name <addr>" pattern
        if let Ok(re) = Regex::new(r#"^"?([^"<]+?)"?\s*<"#) {
            if let Some(cap) = re.captures(from) {
                return cap[1].trim().to_string();
            }
        }
        // Fall back to local part of address
        let local = from.split('@').next().unwrap_or(from);
        local.replace(['.', '_', '-'], " ")
    }

    /// Get the text for a named source field.
    fn get_source_text<'a>(source: &str, email: &'a Value, from_name: &'a str) -> &'a str {
        match source {
            "subject" => email
                .get("subject")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "snippet" => email
                .get("snippet")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "sender" => email.get("from").and_then(|v| v.as_str()).unwrap_or(""),
            "from_name" => from_name,
            _ => "",
        }
    }

    /// Check whether an email matches a template's detect rules.
    fn matches_detect(&self, ct: &CompiledTemplate, email: &Value) -> bool {
        let detect = &ct.template.detect;
        let any_mode = detect.any.unwrap_or(false);
        let from = email
            .get("from")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let domain = Self::get_domain(&from);
        let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
        let snippet = email.get("snippet").and_then(|v| v.as_str()).unwrap_or("");

        let mut checks: Vec<bool> = Vec::new();

        if let Some(d) = &detect.sender_domain {
            checks.push(domain == d.to_lowercase());
        }
        if let Some(sub) = &detect.sender_contains {
            checks.push(from.contains(&sub.to_lowercase()));
        }
        if let Some(re) = &ct.subject_re {
            checks.push(re.is_match(subject));
        }
        if let Some(re) = &ct.snippet_re {
            checks.push(re.is_match(snippet));
        }

        if checks.is_empty() {
            return true;
        }
        if any_mode {
            checks.iter().any(|&b| b)
        } else {
            checks.iter().all(|&b| b)
        }
    }

    /// Apply a single field definition to an email, returning the extracted value.
    fn apply_field(
        &self,
        ct: &CompiledTemplate,
        field_name: &str,
        field_def: &FieldDef,
        email: &Value,
        from_name: &str,
    ) -> Option<Value> {
        // Static value always wins
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

            // Regex: return first capture group or full match
            if has_regex {
                if let Some(re) = ct.field_regexes.get(field_name) {
                    if let Some(caps) = re.captures(text) {
                        let val = caps
                            .get(1)
                            .or_else(|| caps.get(0))
                            .map(|m| m.as_str().trim())?;
                        let val = if let Some(max) = max_chars {
                            &val[..val.len().min(max)]
                        } else {
                            val
                        };
                        return Some(Value::String(val.to_string()));
                    }
                }
            }

            // Enum map: first matching pattern wins
            if has_enum {
                if let Some(pairs) = ct.enum_regexes.get(field_name) {
                    for (re, value) in pairs {
                        if re.is_match(text) {
                            return Some(Value::String(value.clone()));
                        }
                    }
                }
            }

            // Pure truncation (no regex, no enum_map)
            if !has_regex && !has_enum {
                if let Some(max) = max_chars {
                    return Some(Value::String(
                        text[..text.len().min(max)].trim().to_string(),
                    ));
                }
            }
        }
        None
    }

    /// Run extraction over filter output. Processes passed + uncertain buckets;
    /// blocked emails are dropped (counted in stats).
    pub fn extract_batch(&self, filter_output: &Value, snippet_cap: usize, explain: bool) -> Value {
        let start = std::time::Instant::now();

        let empty = vec![];
        let passed = filter_output
            .get("passed")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty);
        let uncertain = filter_output
            .get("uncertain")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty);
        let blocked = filter_output
            .get("blocked")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty);

        let mut results: Vec<Value> = Vec::new();
        let mut extracted_structured: usize = 0;
        let mut snippet_only: usize = 0;

        for (bucket, emails) in [("passed", passed), ("uncertain", uncertain)] {
            for email in emails {
                let from_name = Self::get_from_name(email);
                let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
                let snippet = email.get("snippet").and_then(|v| v.as_str()).unwrap_or("");
                let from = email.get("from").and_then(|v| v.as_str()).unwrap_or("");
                let date = email.get("date").and_then(|v| v.as_str()).unwrap_or("");
                let id = email.get("id").and_then(|v| v.as_str()).unwrap_or("");

                let mut matched_template: Option<&str> = None;
                let mut email_type = "unclassified";
                let mut extracted = serde_json::Map::new();
                let mut effective_cap = snippet_cap;

                for ct in &self.templates {
                    if self.matches_detect(ct, email) {
                        matched_template = Some(ct.template.id.as_str());
                        email_type = ct.template.email_type.as_str();

                        // Financial alerts always get uncapped snippet
                        if email_type == "financial_alert" {
                            effective_cap = usize::MAX;
                        }

                        for (fname, fdef) in &ct.template.extract {
                            if let Some(val) =
                                self.apply_field(ct, fname, fdef, email, &from_name)
                            {
                                extracted.insert(fname.clone(), val);
                            }
                        }
                        break; // first match wins
                    }
                }

                let capped_snippet = if effective_cap < snippet.len() {
                    &snippet[..effective_cap]
                } else {
                    snippet
                };

                let has_extracted = !extracted.is_empty();
                if email_type != "unclassified" && has_extracted {
                    extracted_structured += 1;
                } else {
                    snippet_only += 1;
                }

                let mut record = serde_json::json!({
                    "id": id,
                    "from": from,
                    "subject": subject,
                    "date": date,
                    "type": email_type,
                    "snippet": capped_snippet,
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

        let results_len = results.len();
        let blocked_len = blocked.len();
        let elapsed_ms = start.elapsed().as_millis() as u64;

        serde_json::json!({
            "emails": results,
            "stats": {
                "total_in": results_len,
                "blocked_dropped": blocked_len,
                "extracted_structured": extracted_structured,
                "snippet_only": snippet_only,
                "snippet_cap": snippet_cap,
                "elapsed_ms": elapsed_ms
            }
        })
    }
}
