mod db;
mod extractor;
mod filter;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::io::{self, Read};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Returns the plugin root directory (<plugin>/bin/ets → parent twice).
fn plugin_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(PathBuf::from)) // strip "ets" binary name → bin/
        .and_then(|p| p.parent().map(PathBuf::from)) // strip "bin/" → plugin root
        .unwrap_or_else(|| PathBuf::from("."))
}

fn default_rules_path() -> PathBuf {
    std::env::var("ETS_RULES_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| plugin_dir().join("email_rules.json"))
}

fn default_db_path() -> PathBuf {
    std::env::var("ETS_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_default()
                .join(".openclaw")
                .join("ets")
                .join("ets.db")
        })
}

fn default_templates_path() -> PathBuf {
    std::env::var("ETS_TEMPLATES_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| plugin_dir().join("extractor_templates.json"))
}

fn read_stdin() -> Result<String> {
    let mut s = String::new();
    io::stdin().read_to_string(&mut s)?;
    Ok(s)
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "ets", about = "ETS — Email Token Saver", version = "1.3.0")]
struct Cli {
    /// Path to email_rules.json
    #[arg(long, env = "ETS_RULES_PATH")]
    rules: Option<PathBuf>,

    /// Path to SQLite database
    #[arg(long, env = "ETS_DB_PATH")]
    db: Option<PathBuf>,

    /// Path to extractor_templates.json
    #[arg(long, env = "ETS_TEMPLATES_PATH")]
    templates: Option<PathBuf>,

    /// Score threshold below which emails are blocked (inclusive)
    #[arg(long, default_value = "-50")]
    threshold_block: i32,

    /// Score threshold above which emails pass (inclusive)
    #[arg(long, default_value = "50")]
    threshold_allow: i32,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Filter emails — reads JSON array from stdin, outputs filter result JSON
    Filter {
        #[arg(long)]
        explain: bool,
    },
    /// Extract structured fields — reads filter output JSON from stdin
    Extract {
        #[arg(long, default_value = "300")]
        snippet_cap: usize,
        #[arg(long)]
        explain: bool,
    },
    /// Filter + extract in a single pass (no two-process overhead)
    Pipeline {
        #[arg(long, default_value = "300")]
        snippet_cap: usize,
        #[arg(long)]
        explain: bool,
    },
    /// Print SQLite filter stats as JSON
    Stats,
    /// Sync rules file entries into SQLite rule_hits table
    SyncRules,
}

// ---------------------------------------------------------------------------
// DB write helper — explicit join, no sleep hack
// ---------------------------------------------------------------------------

fn record_run_in_thread(
    db_path: PathBuf,
    filter_output: &serde_json::Value,
    rule_hits: std::collections::HashMap<String, usize>,
) {
    let stats = filter_output["stats"].clone();
    // Spawn thread and join immediately — ensures DB write completes before
    // process exits, replacing the time.sleep(0.05) hack in the Python version.
    std::thread::spawn(move || {
        if let Ok(conn) = db::open(&db_path) {
            let _ = db::record_run(
                &conn,
                stats["total"].as_u64().unwrap_or(0) as usize,
                stats["passed"].as_u64().unwrap_or(0) as usize,
                stats["blocked"].as_u64().unwrap_or(0) as usize,
                stats["uncertain"].as_u64().unwrap_or(0) as usize,
                &rule_hits,
            );
        }
    })
    .join()
    .ok();
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let cli = Cli::parse();

    let rules_path = cli.rules.unwrap_or_else(default_rules_path);
    let db_path = cli.db.unwrap_or_else(default_db_path);
    let templates_path = cli.templates.unwrap_or_else(default_templates_path);

    match cli.command {
        Commands::Filter { explain } => {
            let engine = filter::RuleEngine::load(
                &rules_path,
                cli.threshold_block,
                cli.threshold_allow,
            )?;
            let raw = read_stdin()?;
            let emails: Vec<serde_json::Value> = serde_json::from_str(&raw)?;
            let (output, rule_hits) = engine.filter_batch(emails, explain);
            record_run_in_thread(db_path, &output, rule_hits);
            println!("{}", serde_json::to_string(&output)?);
        }

        Commands::Extract { snippet_cap, explain } => {
            // Reads filter output JSON from stdin — no temp file needed
            let engine = extractor::TemplateEngine::load(&templates_path)?;
            let raw = read_stdin()?;
            let filter_output: serde_json::Value = serde_json::from_str(&raw)?;
            let result = engine.extract_batch(&filter_output, snippet_cap, explain);
            println!("{}", serde_json::to_string(&result)?);
        }

        Commands::Pipeline { snippet_cap, explain } => {
            // Filter + extract in one process — no inter-process JSON serialization
            let filter_engine = filter::RuleEngine::load(
                &rules_path,
                cli.threshold_block,
                cli.threshold_allow,
            )?;
            let extract_engine = extractor::TemplateEngine::load(&templates_path)?;
            let raw = read_stdin()?;
            let emails: Vec<serde_json::Value> = serde_json::from_str(&raw)?;
            let (filter_output, rule_hits) = filter_engine.filter_batch(emails, explain);
            let result = extract_engine.extract_batch(&filter_output, snippet_cap, explain);
            record_run_in_thread(db_path, &filter_output, rule_hits);
            println!("{}", serde_json::to_string(&result)?);
        }

        Commands::Stats => {
            let conn = db::open(&db_path)?;
            let stats = db::get_stats(&conn)?;
            println!("{}", serde_json::to_string_pretty(&stats)?);
        }

        Commands::SyncRules => {
            let raw = std::fs::read_to_string(&rules_path)?;
            let data: serde_json::Value = serde_json::from_str(&raw)?;
            let rule_ids: Vec<String> = data["rules"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|r| r["id"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let conn = db::open(&db_path)?;
            db::sync_rules(&conn, &rule_ids)?;
            eprintln!(
                "[ETS] Synced {} rules to {}",
                rule_ids.len(),
                db_path.display()
            );
        }
    }

    Ok(())
}
