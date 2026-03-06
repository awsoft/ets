use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;

const HARD_OVERRIDE_MIN_WEIGHT: i32 = 90;

#[derive(Debug, Deserialize)]
pub struct RulesFile {
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Rule {
    pub id: String,
    pub action: String, // "allow" | "block"
    pub weight: i32,
    #[serde(rename = "match")]
    pub match_criteria: MatchCriteria,
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MatchCriteria {
    pub sender_domain: Option<String>,
    pub sender_contains: Option<String>,
    pub sender_exact: Option<String>,
    pub subject_regex: Option<String>,
    pub body_regex: Option<String>,
}

/// Pre-compiled, fast-lookup rule engine.
pub struct RuleEngine {
    pub rules: Vec<Rule>,

    // O(1) domain lookup — stores (weight, rule_id) so no secondary linear scan
    allow_domain_map: HashMap<String, (i32, String)>,
    block_domain_map: HashMap<String, (i32, String)>,

    // (substring, weight, rule_id)
    allow_senders: Vec<(String, i32, String)>,
    block_senders: Vec<(String, i32, String)>,

    // exact sender
    allow_exact: Vec<(String, i32, String)>,
    block_exact_set: HashSet<String>,
    block_exact_list: Vec<(String, i32, String)>,

    // (compiled_regex, weight, action, rule_id, field: "subject"|"body")
    regex_rules: Vec<(Regex, i32, String, String, String)>,

    pub threshold_block: i32,
    pub threshold_allow: i32,
}

impl RuleEngine {
    pub fn load(rules_path: &Path, threshold_block: i32, threshold_allow: i32) -> Result<Self> {
        let raw = std::fs::read_to_string(rules_path)
            .with_context(|| format!("Cannot read rules file: {}", rules_path.display()))?;
        let file: RulesFile =
            serde_json::from_str(&raw).with_context(|| "Rules JSON parse error")?;

        let mut engine = RuleEngine {
            allow_domain_map: HashMap::new(),
            block_domain_map: HashMap::new(),
            allow_senders: Vec::new(),
            block_senders: Vec::new(),
            allow_exact: Vec::new(),
            block_exact_set: HashSet::new(),
            block_exact_list: Vec::new(),
            regex_rules: Vec::new(),
            threshold_block,
            threshold_allow,
            rules: file.rules.clone(),
        };

        for rule in &file.rules {
            let action = rule.action.as_str();
            let weight = rule.weight;
            let id = rule.id.clone();
            let m = &rule.match_criteria;

            if let Some(domain) = &m.sender_domain {
                let d = domain.to_lowercase();
                if action == "allow" {
                    engine.allow_domain_map.insert(d, (weight, id.clone()));
                } else {
                    engine.block_domain_map.insert(d, (weight, id.clone()));
                }
            }
            if let Some(sub) = &m.sender_contains {
                let s = sub.to_lowercase();
                if action == "allow" {
                    engine.allow_senders.push((s, weight, id.clone()));
                } else {
                    engine.block_senders.push((s, weight, id.clone()));
                }
            }
            if let Some(exact) = &m.sender_exact {
                let e = exact.to_lowercase();
                if action == "allow" {
                    engine.allow_exact.push((e, weight, id.clone()));
                } else {
                    engine.block_exact_set.insert(e.clone());
                    engine.block_exact_list.push((e, weight, id.clone()));
                }
            }
            for (field, regex_str) in [
                ("subject", m.subject_regex.as_deref()),
                ("body", m.body_regex.as_deref()),
            ] {
                if let Some(pat) = regex_str {
                    match Regex::new(pat) {
                        Ok(re) => engine.regex_rules.push((
                            re,
                            weight,
                            action.to_string(),
                            id.clone(),
                            field.to_string(),
                        )),
                        Err(e) => eprintln!("[ETS] Bad regex in rule {}: {}", id, e),
                    }
                }
            }
        }
        Ok(engine)
    }

    /// Extract the domain portion of an email address.
    /// Handles both "addr@domain" and "Name <addr@domain>" formats.
    fn extract_domain(addr: &str) -> &str {
        // Strip angle-bracket wrapping first
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

    /// Score a single email. Never panics — malformed fields yield empty strings.
    /// Returns `(score, hard_allow, matched_rule_ids)`.
    pub fn score_email(&self, email: &Value) -> (i32, bool, Vec<String>) {
        let from = email
            .get("from")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let from_name = email
            .get("from_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let subject = email
            .get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let snippet = email
            .get("snippet")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Parsed email address (strip "Name <addr>" wrapper)
        let parsed_addr = if let Some(s) = from.find('<') {
            from[s + 1..].trim_end_matches('>').trim().to_string()
        } else {
            from.trim().to_string()
        };

        let domain = Self::extract_domain(&from).to_lowercase();
        let sender_search = format!("{} {}", parsed_addr, from_name);

        let mut score: i32 = 0;
        let mut matched: Vec<String> = Vec::new();
        let mut hard_allow = false;

        // Domain checks — O(1) via HashMap (weight+rule_id stored, no secondary scan)
        if !domain.is_empty() {
            if let Some((w, id)) = self.allow_domain_map.get(&domain) {
                score += w;
                matched.push(id.clone());
                if *w >= HARD_OVERRIDE_MIN_WEIGHT {
                    hard_allow = true;
                }
            }
            if let Some((w, id)) = self.block_domain_map.get(&domain) {
                score -= w;
                matched.push(id.clone());
            }
        }

        // Exact sender checks
        if self.block_exact_set.contains(&parsed_addr) {
            for (addr, w, id) in &self.block_exact_list {
                if *addr == parsed_addr {
                    score -= w;
                    matched.push(id.clone());
                }
            }
        }
        for (addr, w, id) in &self.allow_exact {
            if *addr == parsed_addr {
                score += w;
                matched.push(id.clone());
                if *w >= HARD_OVERRIDE_MIN_WEIGHT {
                    hard_allow = true;
                }
            }
        }

        // Sender-contains checks
        for (sub, w, id) in &self.block_senders {
            if sender_search.contains(sub.as_str()) {
                score -= w;
                matched.push(id.clone());
            }
        }
        for (sub, w, id) in &self.allow_senders {
            if sender_search.contains(sub.as_str()) {
                score += w;
                matched.push(id.clone());
                if *w >= HARD_OVERRIDE_MIN_WEIGHT {
                    hard_allow = true;
                }
            }
        }

        // Regex checks
        for (re, w, action, id, field) in &self.regex_rules {
            let text = if field == "subject" { subject } else { snippet };
            if re.is_match(text) {
                if action == "allow" {
                    score += w;
                    matched.push(id.clone());
                    if *w >= HARD_OVERRIDE_MIN_WEIGHT {
                        hard_allow = true;
                    }
                } else {
                    score -= w;
                    matched.push(id.clone());
                }
            }
        }

        // Dedup matched rule IDs (preserve order)
        let mut seen = HashSet::new();
        matched.retain(|id| seen.insert(id.clone()));

        (score, hard_allow, matched)
    }

    /// Filter a batch of emails. Returns (output JSON, per-rule hit counts).
    pub fn filter_batch(
        &self,
        emails: Vec<Value>,
        explain: bool,
    ) -> (Value, HashMap<String, usize>) {
        let start = std::time::Instant::now();
        let mut passed: Vec<Value> = Vec::new();
        let mut blocked: Vec<Value> = Vec::new();
        let mut uncertain: Vec<Value> = Vec::new();

        // Pre-populate hit map so all rules appear even with 0 hits
        let mut rule_hits: HashMap<String, usize> =
            self.rules.iter().map(|r| (r.id.clone(), 0)).collect();

        for mut email in emails {
            // score_email never panics — malformed fields become empty strings
            let (score, hard_allow, matched) = self.score_email(&email);

            for id in &matched {
                *rule_hits.entry(id.clone()).or_insert(0) += 1;
            }

            let decision = if hard_allow || score >= self.threshold_allow {
                "passed"
            } else if score <= self.threshold_block {
                "blocked"
            } else {
                "uncertain"
            };

            if let Some(obj) = email.as_object_mut() {
                obj.insert("score".to_string(), score.into());
                if explain {
                    obj.insert(
                        "matched_rules".to_string(),
                        serde_json::to_value(&matched).unwrap_or_default(),
                    );
                }
            }

            match decision {
                "passed" => passed.push(email),
                "blocked" => blocked.push(email),
                _ => uncertain.push(email),
            }
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let passed_len = passed.len();
        let blocked_len = blocked.len();
        let uncertain_len = uncertain.len();
        let total = passed_len + blocked_len + uncertain_len;

        let output = serde_json::json!({
            "passed": passed,
            "blocked": blocked,
            "uncertain": uncertain,
            "stats": {
                "total": total,
                "passed": passed_len,
                "blocked": blocked_len,
                "uncertain": uncertain_len,
                "rules_loaded": self.rules.len(),
                "elapsed_ms": elapsed_ms
            }
        });

        (output, rule_hits)
    }
}
