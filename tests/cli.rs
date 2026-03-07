//! CLI integration tests for `ets` binary.
//! These tests invoke the compiled binary via `std::process::Command`.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Locate the `ets` binary built by cargo.
fn ets_bin() -> PathBuf {
    let mut p = std::env::current_exe().expect("current_exe");
    p.pop(); // remove test binary name
    if p.ends_with("deps") {
        p.pop(); // deps/ → debug/ or release/
    }
    p.join("ets")
}

/// Write content to a uniquely-named temp file and return the path.
fn temp_file(prefix: &str, ext: &str, content: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("ets_cli_{}_{}_{}", prefix, n, ext));
    std::fs::write(&path, content).unwrap();
    path
}

/// Temp path (no content written) for DB files.
fn temp_db_path() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ets_cli_test_{}.db", n))
}

/// Run `ets` with given args and stdin, returning (stdout, stderr, status).
fn run_ets(args: &[&str], env: &[(&str, &str)], stdin_data: &str) -> (String, String, bool) {
    let bin = ets_bin();
    let mut cmd = Command::new(&bin);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().unwrap_or_else(|e| panic!("Failed to spawn {}: {}", bin.display(), e));
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(stdin_data.as_bytes()).ok();
    }
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (stdout, stderr, output.status.success())
}

// ---- Fixture constants ----

const MINIMAL_RULES: &str = r#"{
  "rules": [
    {"id": "allow-trusted", "action": "allow", "weight": 60, "match": {"sender_domain": "trusted.com"}},
    {"id": "block-spam",    "action": "block", "weight": 60, "match": {"sender_domain": "spam.com"}},
    {"id": "allow-subject", "action": "allow", "weight": 40, "match": {"subject_regex": "(?i)important"}}
  ]
}"#;

const MINIMAL_TEMPLATES: &str = r#"{
  "templates": [
    {
      "id": "bank",
      "name": "Bank",
      "priority": 100,
      "type": "bank_notification",
      "detect": {"sender_domain": "bank.com"},
      "extract": {
        "notice": {"static": "bank_notice"}
      },
      "tags": {"financial": 0.8, "action_required": 0.6}
    }
  ],
  "tag_rules": []
}"#;

// ---- --version ----

#[test]
fn version_flag_shows_version() {
    let (stdout, stderr, ok) = run_ets(&["--version"], &[], "");
    // Some versions print to stdout, some to stderr
    let combined = format!("{}{}", stdout, stderr);
    assert!(ok, "ets --version should succeed");
    assert!(
        combined.contains("1.4") || combined.contains("ets"),
        "version output should contain version or program name: {:?}",
        combined
    );
}

// ---- filter subcommand ----

#[test]
fn filter_passes_trusted_domain_email() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let input = r#"[{"id":"1","from":"user@trusted.com","subject":"Hello","snippet":"Test body"}]"#;
    let (stdout, _stderr, ok) = run_ets(&["filter"], &env, input);
    assert!(ok, "filter should succeed");
    let val: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON output");
    assert_eq!(val["passed"].as_array().unwrap().len(), 1);
    assert_eq!(val["blocked"].as_array().unwrap().len(), 0);
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&templates);
    let _ = std::fs::remove_file(&db);
}

#[test]
fn filter_blocks_spam_domain_email() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let input = r#"[{"id":"2","from":"user@spam.com","subject":"Buy now","snippet":"Click here"}]"#;
    let (stdout, _stderr, ok) = run_ets(&["filter"], &env, input);
    assert!(ok);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(val["blocked"].as_array().unwrap().len(), 1);
    assert_eq!(val["passed"].as_array().unwrap().len(), 0);
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

#[test]
fn filter_output_has_stats_field() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let input = r#"[{"id":"1","from":"user@other.com","subject":"Hello","snippet":""}]"#;
    let (stdout, _, ok) = run_ets(&["filter"], &env, input);
    assert!(ok);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(val.get("stats").is_some(), "output must have stats field");
    assert!(val["stats"].get("total").is_some());
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

#[test]
fn filter_multiple_emails_in_batch() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let input = r#"[
        {"id":"1","from":"user@trusted.com","subject":"Hello","snippet":""},
        {"id":"2","from":"user@spam.com","subject":"Offer","snippet":""},
        {"id":"3","from":"user@other.com","subject":"Update","snippet":""}
    ]"#;
    let (stdout, _, ok) = run_ets(&["filter"], &env, input);
    assert!(ok);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(val["stats"]["total"].as_u64().unwrap(), 3);
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

#[test]
fn filter_explain_flag_adds_matched_rules() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let input = r#"[{"id":"1","from":"user@trusted.com","subject":"Hello","snippet":""}]"#;
    let (stdout, _, ok) = run_ets(&["filter", "--explain"], &env, input);
    assert!(ok);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let passed = val["passed"].as_array().unwrap();
    assert!(!passed.is_empty());
    assert!(
        passed[0].get("matched_rules").is_some(),
        "--explain should add matched_rules"
    );
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

// ---- extract subcommand ----

#[test]
fn extract_processes_filter_output() {
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let env: Vec<(&str, &str)> = vec![
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
    ];
    // extract subcommand reads filter output JSON from stdin
    let filter_output = r#"{
        "passed": [{"id":"1","from":"alert@bank.com","subject":"Account notice","snippet":"Check your balance","date":"2025-01-01"}],
        "blocked": [],
        "uncertain": [],
        "stats": {"total":1,"passed":1,"blocked":0,"uncertain":0,"rules_loaded":0,"elapsed_ms":0}
    }"#;
    let (stdout, _stderr, ok) = run_ets(&["extract"], &env, filter_output);
    assert!(ok, "extract subcommand should succeed");
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(val.get("emails").is_some());
    assert!(val.get("stats").is_some());
    let _ = std::fs::remove_file(&templates);
}

#[test]
fn extract_structured_fields_for_matched_template() {
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let env: Vec<(&str, &str)> = vec![
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
    ];
    let filter_output = r#"{
        "passed": [{"id":"1","from":"alert@bank.com","subject":"Account notice","snippet":"","date":""}],
        "blocked": [],
        "uncertain": [],
        "stats": {}
    }"#;
    let (stdout, _, ok) = run_ets(&["extract"], &env, filter_output);
    assert!(ok);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let emails = val["emails"].as_array().unwrap();
    assert_eq!(emails[0]["type"].as_str().unwrap(), "bank_notification");
    assert_eq!(
        emails[0]["extracted"]["notice"].as_str().unwrap(),
        "bank_notice"
    );
    let _ = std::fs::remove_file(&templates);
}

#[test]
fn extract_snippet_cap_arg() {
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let env: Vec<(&str, &str)> = vec![
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
    ];
    let long_snippet = "X".repeat(500);
    let filter_output = format!(
        r#"{{"passed":[{{"id":"1","from":"alert@bank.com","subject":"URGENT notice","snippet":"{}","date":""}}],"blocked":[],"uncertain":[],"stats":{{}}}}"#,
        long_snippet
    );
    let (stdout, _, ok) = run_ets(&["extract", "--snippet-cap", "50"], &env, &filter_output);
    assert!(ok);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let emails = val["emails"].as_array().unwrap();
    if let Some(snippet) = emails[0]["snippet"].as_str() {
        assert!(
            snippet.len() <= 50,
            "snippet-cap 50 should limit snippet to 50 bytes"
        );
    }
    let _ = std::fs::remove_file(&templates);
}

// ---- pipeline subcommand ----

#[test]
fn pipeline_filter_and_extract_in_one_pass() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let input = r#"[
        {"id":"1","from":"alert@bank.com","subject":"URGENT account notice","snippet":"Balance due","date":""},
        {"id":"2","from":"user@spam.com","subject":"Win prizes","snippet":"Click now","date":""}
    ]"#;
    let (stdout, _stderr, ok) = run_ets(&["pipeline"], &env, input);
    assert!(ok, "pipeline should succeed");
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(val.get("emails").is_some(), "pipeline output should have emails");
    assert!(val.get("stats").is_some(), "pipeline output should have stats");
    // bank.com email: trusted domain not in our rules but bank.com passes via pipeline filter
    // The output depends on filter rules — just verify structure
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&templates);
    let _ = std::fs::remove_file(&db);
}

#[test]
fn pipeline_output_structure_matches_extract() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let input = r#"[{"id":"1","from":"user@trusted.com","subject":"Important update","snippet":"Hello","date":""}]"#;
    let (stdout, _, ok) = run_ets(&["pipeline"], &env, input);
    assert!(ok);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    // Should have same structure as extract output
    assert!(val["emails"].is_array());
    assert!(val["stats"]["total_in"].is_number() || val["stats"]["blocked_dropped"].is_number());
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&templates);
    let _ = std::fs::remove_file(&db);
}

// ---- stats subcommand ----

#[test]
fn stats_returns_json_from_empty_db() {
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let (stdout, _, ok) = run_ets(&["stats"], &env, "");
    assert!(ok, "stats should succeed even on empty db");
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(val["total_runs"].as_u64().unwrap(), 0);
    assert_eq!(val["total_emails"].as_u64().unwrap(), 0);
    assert!(val["rule_hits"].is_array());
    let _ = std::fs::remove_file(&db);
}

#[test]
fn stats_reflects_filter_run() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    // Run filter to populate DB
    let input = r#"[
        {"id":"1","from":"user@trusted.com","subject":"Hello","snippet":""},
        {"id":"2","from":"user@spam.com","subject":"Buy","snippet":""}
    ]"#;
    let (_, _, ok) = run_ets(&["filter"], &env, input);
    assert!(ok, "filter should succeed");

    // Now check stats
    let (stdout, _, ok2) = run_ets(&["stats"], &env, "");
    assert!(ok2);
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(val["total_runs"].as_u64().unwrap(), 1);
    assert_eq!(val["total_emails"].as_u64().unwrap(), 2);
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

// ---- Bad input handling ----

#[test]
fn filter_invalid_json_fails() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let (_, _, ok) = run_ets(&["filter"], &env, "this is not json");
    assert!(!ok, "filter should fail on invalid JSON input");
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

#[test]
fn filter_empty_array_succeeds_with_zero_count() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let (stdout, _, ok) = run_ets(&["filter"], &env, "[]");
    assert!(ok, "empty array should be valid input");
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(val["stats"]["total"].as_u64().unwrap(), 0);
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

#[test]
fn filter_missing_rules_file_fails() {
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", "/nonexistent/path/rules.json"),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    let (_, _, ok) = run_ets(&["filter"], &env, "[]");
    assert!(!ok, "filter with missing rules file should fail");
}

#[test]
fn extract_invalid_json_fails() {
    let templates = temp_file("templates", "json", MINIMAL_TEMPLATES);
    let env: Vec<(&str, &str)> = vec![
        ("ETS_TEMPLATES_PATH", templates.to_str().unwrap()),
    ];
    let (_, _, ok) = run_ets(&["extract"], &env, "not valid json at all");
    assert!(!ok, "extract should fail on invalid JSON input");
    let _ = std::fs::remove_file(&templates);
}

#[test]
fn filter_json_object_not_array_fails() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    // filter expects a JSON array, not an object
    let (_, _, ok) = run_ets(&["filter"], &env, r#"{"key":"value"}"#);
    assert!(!ok, "filter should fail when given a JSON object instead of array");
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}

// ---- Custom threshold args ----

#[test]
fn custom_threshold_args_change_bucketing() {
    let rules = temp_file("rules", "json", MINIMAL_RULES);
    let db = temp_db_path();
    let env: Vec<(&str, &str)> = vec![
        ("ETS_RULES_PATH", rules.to_str().unwrap()),
        ("ETS_DB_PATH", db.to_str().unwrap()),
    ];
    // With default thresholds (-50/50), score=40 is uncertain.
    // With threshold_allow=30, score=40 should pass.
    // Note: use = syntax for numeric args to avoid Clap misinterpreting negative signs.
    let input = r#"[{"id":"1","from":"user@other.com","subject":"important notice","snippet":""}]"#;
    // subject regex matches "important notice": +40 → score=40; threshold_allow=30 → passed
    let (stdout, _, ok) = run_ets(
        &["--threshold-allow=30", "--threshold-block=-30", "filter"],
        &env,
        input,
    );
    assert!(ok, "filter with custom thresholds should succeed");
    let val: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        val["passed"].as_array().unwrap().len(),
        1,
        "with threshold_allow=30, score=40 should pass"
    );
    let _ = std::fs::remove_file(&rules);
    let _ = std::fs::remove_file(&db);
}
