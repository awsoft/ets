use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;

const HARD_OVERRIDE_MIN_WEIGHT: i32 = 90;

/// Action enum — replaces the previous `String` field to eliminate error-prone string comparisons.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Allow,
    Block,
}

#[derive(Debug, Deserialize)]
pub struct RulesFile {
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Rule {
    pub id: String,
    pub action: Action,
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

    // O(1) exact sender lookup — HashMap<addr, (weight, rule_id)>
    allow_exact_map: HashMap<String, (i32, String)>,
    block_exact_map: HashMap<String, (i32, String)>,

    // (compiled_regex, weight, action, rule_id, field: "subject"|"body")
    regex_rules: Vec<(Regex, i32, Action, String, String)>,

    pub threshold_block: i32,
    pub threshold_allow: i32,
}

impl RuleEngine {
    /// Load rules from a primary file, optionally merging a local overrides file.
    /// Local rules with the same ID as a bundled rule replace the bundled version.
    pub fn load(rules_path: &Path, threshold_block: i32, threshold_allow: i32) -> Result<Self> {
        Self::load_with_local(rules_path, None, threshold_block, threshold_allow)
    }

    /// Load rules from a primary file + optional local overrides file.
    /// Local rules override bundled rules by ID (last wins).
    pub fn load_with_local(
        rules_path: &Path,
        local_rules_path: Option<&Path>,
        threshold_block: i32,
        threshold_allow: i32,
    ) -> Result<Self> {
        let raw = std::fs::read_to_string(rules_path)
            .with_context(|| format!("Cannot read rules file: {}", rules_path.display()))?;
        let file: RulesFile =
            serde_json::from_str(&raw).with_context(|| "Rules JSON parse error")?;

        // Start with bundled rules indexed by ID
        let mut rules_map: std::collections::BTreeMap<String, Rule> = std::collections::BTreeMap::new();
        for rule in file.rules {
            rules_map.insert(rule.id.clone(), rule);
        }

        // Merge local rules (override by ID, add new ones)
        if let Some(local_path) = local_rules_path {
            match std::fs::read_to_string(local_path) {
                Ok(local_raw) => {
                    match serde_json::from_str::<RulesFile>(&local_raw) {
                        Ok(local_file) => {
                            let count = local_file.rules.len();
                            for rule in local_file.rules {
                                rules_map.insert(rule.id.clone(), rule);
                            }
                            eprintln!("[ETS] Loaded {} local rules from {}", count, local_path.display());
                        }
                        Err(e) => {
                            eprintln!("[ETS] Warning: cannot parse local rules {}: {}", local_path.display(), e);
                        }
                    }
                }
                Err(_) => {
                    // Local file doesn't exist — that's fine, it's optional
                }
            }
        }

        let all_rules: Vec<Rule> = rules_map.into_values().collect();

        let mut engine = RuleEngine {
            allow_domain_map: HashMap::new(),
            block_domain_map: HashMap::new(),
            allow_senders: Vec::new(),
            block_senders: Vec::new(),
            allow_exact_map: HashMap::new(),
            block_exact_map: HashMap::new(),
            regex_rules: Vec::new(),
            threshold_block,
            threshold_allow,
            rules: all_rules.clone(),
        };

        for rule in &all_rules {
            let action = rule.action;
            let weight = rule.weight;
            let id = rule.id.clone();
            let m = &rule.match_criteria;

            if let Some(domain) = &m.sender_domain {
                let d = domain.to_lowercase();
                if action == Action::Allow {
                    engine.allow_domain_map.insert(d, (weight, id.clone()));
                } else {
                    engine.block_domain_map.insert(d, (weight, id.clone()));
                }
            }
            if let Some(sub) = &m.sender_contains {
                let s = sub.to_lowercase();
                if action == Action::Allow {
                    engine.allow_senders.push((s, weight, id.clone()));
                } else {
                    engine.block_senders.push((s, weight, id.clone()));
                }
            }
            if let Some(exact) = &m.sender_exact {
                let e = exact.to_lowercase();
                if action == Action::Allow {
                    engine.allow_exact_map.insert(e, (weight, id.clone()));
                } else {
                    engine.block_exact_map.insert(e, (weight, id.clone()));
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
                            action,
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

        // Use shared common::extract_domain; `from` is already lowercased
        let domain = crate::common::extract_domain(&from).to_string();
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

        // Exact sender checks — O(1) via HashMap
        if let Some((w, id)) = self.block_exact_map.get(&parsed_addr) {
            score -= w;
            matched.push(id.clone());
        }
        if let Some((w, id)) = self.allow_exact_map.get(&parsed_addr) {
            score += w;
            matched.push(id.clone());
            if *w >= HARD_OVERRIDE_MIN_WEIGHT {
                hard_allow = true;
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
                if *action == Action::Allow {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Write JSON to a uniquely-named temp file and return its path.
    fn temp_rules_file(json_str: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("ets_filter_rules_{}.json", n));
        std::fs::write(&path, json_str).unwrap();
        path
    }

    fn make_engine_from(rules_json: &str) -> RuleEngine {
        let path = temp_rules_file(rules_json);
        let engine = RuleEngine::load(&path, -50, 50).unwrap();
        let _ = std::fs::remove_file(&path);
        engine
    }

    fn default_engine() -> RuleEngine {
        make_engine_from(TEST_RULES_JSON)
    }

    fn email(from: &str, subject: &str, snippet: &str) -> Value {
        json!({
            "id": "test",
            "from": from,
            "subject": subject,
            "snippet": snippet
        })
    }

    // Minimal rules fixture covering all match types
    const TEST_RULES_JSON: &str = r#"{
        "rules": [
            {"id": "allow-trusted",  "action": "allow", "weight": 60, "match": {"sender_domain": "trusted.com"}},
            {"id": "block-spam",     "action": "block", "weight": 60, "match": {"sender_domain": "spam.com"}},
            {"id": "allow-contains", "action": "allow", "weight": 40, "match": {"sender_contains": "news@"}},
            {"id": "block-contains", "action": "block", "weight": 40, "match": {"sender_contains": "noreply@spam"}},
            {"id": "allow-exact",    "action": "allow", "weight": 55, "match": {"sender_exact": "vip@work.com"}},
            {"id": "block-exact",    "action": "block", "weight": 55, "match": {"sender_exact": "bad@actor.com"}},
            {"id": "allow-subject",  "action": "allow", "weight": 30, "match": {"subject_regex": "(?i)important"}},
            {"id": "block-body",     "action": "block", "weight": 30, "match": {"body_regex": "(?i)unsubscribe"}},
            {"id": "hard-allow",     "action": "allow", "weight": 90, "match": {"sender_domain": "bank.com"}}
        ]
    }"#;

    // ---- Rules loading ----

    #[test]
    fn load_rules_from_json() {
        let engine = default_engine();
        assert_eq!(engine.rules.len(), 9);
    }

    #[test]
    fn load_rules_invalid_path_returns_error() {
        let result = RuleEngine::load(std::path::Path::new("/nonexistent/rules.json"), -50, 50);
        assert!(result.is_err());
    }

    #[test]
    fn load_rules_invalid_json_returns_error() {
        let path = temp_rules_file("this is not json");
        let result = RuleEngine::load(&path, -50, 50);
        let _ = std::fs::remove_file(&path);
        assert!(result.is_err());
    }

    // ---- Action enum deserialization ----

    #[test]
    fn action_deserialization_allow() {
        let engine = default_engine();
        let rule = engine.rules.iter().find(|r| r.id == "allow-trusted").unwrap();
        assert_eq!(rule.action, Action::Allow);
    }

    #[test]
    fn action_deserialization_block() {
        let engine = default_engine();
        let rule = engine.rules.iter().find(|r| r.id == "block-spam").unwrap();
        assert_eq!(rule.action, Action::Block);
    }

    // ---- Domain matching ----

    #[test]
    fn domain_allow_exact_match() {
        let engine = default_engine();
        let e = email("user@trusted.com", "Hello", "");
        let (score, _, matched) = engine.score_email(&e);
        assert_eq!(score, 60);
        assert!(matched.contains(&"allow-trusted".to_string()));
    }

    #[test]
    fn domain_block_exact_match() {
        let engine = default_engine();
        let e = email("user@spam.com", "Hello", "");
        let (score, _, matched) = engine.score_email(&e);
        assert_eq!(score, -60);
        assert!(matched.contains(&"block-spam".to_string()));
    }

    #[test]
    fn domain_matching_is_case_insensitive() {
        let engine = default_engine();
        // from field is lowercased before domain extraction
        let e = email("User@TRUSTED.COM", "Hello", "");
        let (score, _, matched) = engine.score_email(&e);
        assert_eq!(score, 60);
        assert!(matched.contains(&"allow-trusted".to_string()));
    }

    #[test]
    fn domain_no_match_scores_zero() {
        let engine = default_engine();
        let e = email("user@other.com", "Hello", "");
        let (score, hard_allow, _) = engine.score_email(&e);
        assert_eq!(score, 0);
        assert!(!hard_allow);
    }

    #[test]
    fn domain_from_name_addr_format() {
        let engine = default_engine();
        let e = email("Spam Guy <user@spam.com>", "Offer", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score < 0);
        assert!(matched.contains(&"block-spam".to_string()));
    }

    // ---- Sender contains matching ----

    #[test]
    fn sender_contains_allow_match() {
        let engine = default_engine();
        let e = email("news@example.com", "Weekly digest", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score > 0);
        assert!(matched.contains(&"allow-contains".to_string()));
    }

    #[test]
    fn sender_contains_block_match() {
        let engine = default_engine();
        let e = email("noreply@spam.org", "Offer", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score < 0);
        assert!(matched.contains(&"block-contains".to_string()));
    }

    #[test]
    fn sender_contains_case_insensitive() {
        let engine = default_engine();
        // "news@" stored lowercase; from field lowercased → "NEWS@example.com" → "news@example.com"
        let e = email("NEWS@example.com", "Digest", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score > 0);
        assert!(matched.contains(&"allow-contains".to_string()));
    }

    // ---- Sender exact matching ----

    #[test]
    fn exact_sender_allow() {
        let engine = default_engine();
        let e = email("vip@work.com", "Report", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score > 0);
        assert!(matched.contains(&"allow-exact".to_string()));
    }

    #[test]
    fn exact_sender_block() {
        let engine = default_engine();
        let e = email("bad@actor.com", "Free money", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score < 0);
        assert!(matched.contains(&"block-exact".to_string()));
    }

    #[test]
    fn exact_sender_with_display_name() {
        let engine = default_engine();
        // "Name <addr>" format — parsed_addr should be extracted correctly
        let e = email("Bad Guy <bad@actor.com>", "Free money", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score < 0);
        assert!(matched.contains(&"block-exact".to_string()));
    }

    #[test]
    fn exact_sender_case_insensitive() {
        let engine = default_engine();
        let e = email("BAD@ACTOR.COM", "Free money", "");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score < 0);
        assert!(matched.contains(&"block-exact".to_string()));
    }

    // ---- Subject / body regex matching ----

    #[test]
    fn subject_regex_allow_match() {
        let engine = default_engine();
        let e = email("user@other.com", "Important update", "");
        let (score, _, matched) = engine.score_email(&e);
        assert_eq!(score, 30);
        assert!(matched.contains(&"allow-subject".to_string()));
    }

    #[test]
    fn subject_regex_case_insensitive_flag() {
        let engine = default_engine();
        let e = email("user@other.com", "IMPORTANT NOTICE", "");
        let (score, _, _) = engine.score_email(&e);
        assert_eq!(score, 30);
    }

    #[test]
    fn body_regex_block_match() {
        let engine = default_engine();
        let e = email("user@other.com", "Newsletter", "Click here to unsubscribe.");
        let (score, _, matched) = engine.score_email(&e);
        assert!(score < 0);
        assert!(matched.contains(&"block-body".to_string()));
    }

    // ---- Score accumulation with multiple rules ----

    #[test]
    fn multiple_rules_scores_accumulate() {
        let engine = default_engine();
        // trusted domain (+60) + important subject (+30) = 90
        let e = email("user@trusted.com", "Important notice", "");
        let (score, _, matched) = engine.score_email(&e);
        assert_eq!(score, 90);
        assert!(matched.contains(&"allow-trusted".to_string()));
        assert!(matched.contains(&"allow-subject".to_string()));
    }

    #[test]
    fn allow_and_block_rules_net_score() {
        let engine = default_engine();
        // trusted domain (+60) + unsubscribe body (-30) = 30
        let e = email("user@trusted.com", "Hello", "Click here to unsubscribe");
        let (score, _, _) = engine.score_email(&e);
        assert_eq!(score, 30);
    }

    // ---- Hard allow override ----

    #[test]
    fn hard_allow_set_for_weight_90() {
        let engine = default_engine();
        let e = email("user@bank.com", "Hello", "");
        let (score, hard_allow, _) = engine.score_email(&e);
        assert_eq!(score, 90);
        assert!(hard_allow, "weight >= 90 should trigger hard allow");
    }

    #[test]
    fn weight_89_does_not_hard_allow() {
        let rules_json = r#"{"rules": [
            {"id": "r1", "action": "allow", "weight": 89, "match": {"sender_domain": "almost.com"}}
        ]}"#;
        let engine = make_engine_from(rules_json);
        let e = email("user@almost.com", "Hello", "");
        let (_, hard_allow, _) = engine.score_email(&e);
        assert!(!hard_allow, "weight 89 should NOT trigger hard allow");
    }

    #[test]
    fn hard_allow_forces_pass_despite_low_net_score() {
        let rules_json = r#"{"rules": [
            {"id": "hard-allow", "action": "allow", "weight": 90, "match": {"sender_domain": "bank.com"}},
            {"id": "big-block",  "action": "block", "weight": 200, "match": {"body_regex": "(?i)spam"}}
        ]}"#;
        let engine = make_engine_from(rules_json);
        // score = 90 - 200 = -110, but hard_allow=true → should be "passed"
        let e = email("user@bank.com", "Notice", "This is spam content");
        let (score, hard_allow, _) = engine.score_email(&e);
        assert_eq!(score, -110);
        assert!(hard_allow);
        let (output, _) = engine.filter_batch(vec![e], false);
        assert_eq!(output["passed"].as_array().unwrap().len(), 1, "hard_allow must force into passed");
        assert_eq!(output["blocked"].as_array().unwrap().len(), 0);
    }

    // ---- Threshold-based bucketing ----

    #[test]
    fn bucket_passed_when_score_at_threshold_allow() {
        // threshold_allow = 50; score = 60 → passed
        let engine = default_engine();
        let e = email("user@trusted.com", "Hello", ""); // score = 60
        let (output, _) = engine.filter_batch(vec![e], false);
        assert_eq!(output["passed"].as_array().unwrap().len(), 1);
        assert_eq!(output["blocked"].as_array().unwrap().len(), 0);
        assert_eq!(output["uncertain"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn bucket_blocked_when_score_at_threshold_block() {
        // threshold_block = -50; score = -60 → blocked
        let engine = default_engine();
        let e = email("user@spam.com", "Hello", ""); // score = -60
        let (output, _) = engine.filter_batch(vec![e], false);
        assert_eq!(output["blocked"].as_array().unwrap().len(), 1);
        assert_eq!(output["passed"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn bucket_uncertain_between_thresholds() {
        // score = 40; between -50 and 50 → uncertain
        let engine = default_engine();
        let e = email("news@example.com", "Update", ""); // allow-contains: +40
        let (output, _) = engine.filter_batch(vec![e], false);
        assert_eq!(output["uncertain"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn bucket_exact_at_threshold_allow_passes() {
        // score exactly at threshold_allow (50) should pass
        let rules_json = r#"{"rules": [
            {"id": "r1", "action": "allow", "weight": 50, "match": {"sender_domain": "exact.com"}}
        ]}"#;
        let engine = make_engine_from(rules_json);
        let e = email("user@exact.com", "", "");
        let (output, _) = engine.filter_batch(vec![e], false);
        assert_eq!(output["passed"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn bucket_exact_at_threshold_block_blocks() {
        // score exactly at threshold_block (-50) should block
        let rules_json = r#"{"rules": [
            {"id": "r1", "action": "block", "weight": 50, "match": {"sender_domain": "exact.com"}}
        ]}"#;
        let engine = make_engine_from(rules_json);
        let e = email("user@exact.com", "", "");
        let (output, _) = engine.filter_batch(vec![e], false);
        assert_eq!(output["blocked"].as_array().unwrap().len(), 1);
    }

    // ---- Empty / malformed email fields (no panics) ----

    #[test]
    fn empty_email_object_no_panic() {
        let engine = default_engine();
        let e = json!({});
        let (score, hard_allow, matched) = engine.score_email(&e);
        assert_eq!(score, 0);
        assert!(!hard_allow);
        assert!(matched.is_empty());
    }

    #[test]
    fn null_fields_no_panic() {
        let engine = default_engine();
        let e = json!({"from": null, "subject": null, "snippet": null});
        let (score, _, _) = engine.score_email(&e);
        assert_eq!(score, 0);
    }

    #[test]
    fn missing_from_field_no_panic() {
        let engine = default_engine();
        let e = json!({"subject": "Important update", "snippet": ""});
        // subject regex should still fire
        let (score, _, matched) = engine.score_email(&e);
        assert_eq!(score, 30);
        assert!(matched.contains(&"allow-subject".to_string()));
    }

    #[test]
    fn empty_from_string_no_panic() {
        let engine = default_engine();
        let e = json!({"from": "", "subject": "", "snippet": ""});
        let (score, _, _) = engine.score_email(&e);
        assert_eq!(score, 0);
    }

    #[test]
    fn batch_filter_empty_vec_no_panic() {
        let engine = default_engine();
        let (output, rule_hits) = engine.filter_batch(vec![], false);
        assert_eq!(output["stats"]["total"].as_u64().unwrap(), 0);
        // All rules should still appear in rule_hits with 0 count
        assert_eq!(rule_hits.len(), 9);
        for count in rule_hits.values() {
            assert_eq!(*count, 0);
        }
    }

    // ---- Rule hit counting ----

    #[test]
    fn rule_hits_populated_correctly() {
        let engine = default_engine();
        let emails = vec![
            email("user@trusted.com", "Hello", ""),    // hits allow-trusted
            email("user@trusted.com", "Important", ""), // hits allow-trusted + allow-subject
            email("user@spam.com", "Hello", ""),        // hits block-spam
        ];
        let (_, rule_hits) = engine.filter_batch(emails, false);
        assert_eq!(*rule_hits.get("allow-trusted").unwrap(), 2);
        assert_eq!(*rule_hits.get("allow-subject").unwrap(), 1);
        assert_eq!(*rule_hits.get("block-spam").unwrap(), 1);
    }

    #[test]
    fn rule_hits_zero_for_unmatched_rules() {
        let engine = default_engine();
        let (_, rule_hits) = engine.filter_batch(vec![], false);
        // All rules pre-populated with 0
        assert!(rule_hits.contains_key("allow-trusted"));
        assert_eq!(*rule_hits.get("allow-trusted").unwrap(), 0);
        assert!(rule_hits.contains_key("block-spam"));
    }

    #[test]
    fn rule_ids_deduplicated_in_matched() {
        // A rule that matches both sender_contains AND domain should only appear once in matched
        let rules_json = r#"{"rules": [
            {"id": "r1", "action": "allow", "weight": 30, "match": {"sender_domain": "dual.com"}}
        ]}"#;
        let engine = make_engine_from(rules_json);
        let e = email("user@dual.com", "", "");
        let (_, _, matched) = engine.score_email(&e);
        let count = matched.iter().filter(|id| id.as_str() == "r1").count();
        assert_eq!(count, 1, "same rule should not appear twice in matched");
    }

    // ---- explain mode ----

    #[test]
    fn explain_mode_adds_matched_rules_to_output() {
        let engine = default_engine();
        let e = email("user@trusted.com", "Hello", "");
        let (output, _) = engine.filter_batch(vec![e], true);
        let passed = output["passed"].as_array().unwrap();
        assert_eq!(passed.len(), 1);
        assert!(passed[0]["matched_rules"].is_array(), "explain mode adds matched_rules array");
        let matched = passed[0]["matched_rules"].as_array().unwrap();
        assert!(matched.iter().any(|v| v.as_str() == Some("allow-trusted")));
    }

    #[test]
    fn non_explain_mode_no_matched_rules_field() {
        let engine = default_engine();
        let e = email("user@trusted.com", "Hello", "");
        let (output, _) = engine.filter_batch(vec![e], false);
        let passed = output["passed"].as_array().unwrap();
        assert!(!passed[0].as_object().unwrap().contains_key("matched_rules"));
    }

    // ---- Stats in output ----

    #[test]
    fn output_stats_counts_correct() {
        let engine = default_engine();
        let emails = vec![
            email("user@trusted.com", "Hello", ""),   // passed (60)
            email("user@spam.com", "Hello", ""),       // blocked (-60)
            email("news@example.com", "Update", ""),   // uncertain (40)
        ];
        let (output, _) = engine.filter_batch(emails, false);
        assert_eq!(output["stats"]["total"].as_u64().unwrap(), 3);
        assert_eq!(output["stats"]["passed"].as_u64().unwrap(), 1);
        assert_eq!(output["stats"]["blocked"].as_u64().unwrap(), 1);
        assert_eq!(output["stats"]["uncertain"].as_u64().unwrap(), 1);
        assert_eq!(output["stats"]["rules_loaded"].as_u64().unwrap(), 9);
    }
}
