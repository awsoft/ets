use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;

pub fn open(db_path: &Path) -> Result<Connection> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    ensure_schema(&conn)?;
    Ok(conn)
}

fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS filter_runs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL    NOT NULL,
            total     INTEGER NOT NULL,
            passed    INTEGER NOT NULL,
            blocked   INTEGER NOT NULL,
            uncertain INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rule_hits (
            rule_id   TEXT    PRIMARY KEY,
            hit_count INTEGER NOT NULL DEFAULT 0,
            last_hit  REAL    NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    ",
    )?;
    Ok(())
}

pub fn record_run(
    conn: &Connection,
    total: usize,
    passed: usize,
    blocked: usize,
    uncertain: usize,
    hits: &HashMap<String, usize>,
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs_f64();

    tx.execute(
        "INSERT INTO filter_runs (timestamp, total, passed, blocked, uncertain) VALUES (?1,?2,?3,?4,?5)",
        params![now, total as i64, passed as i64, blocked as i64, uncertain as i64],
    )?;

    for (rule_id, count) in hits {
        if *count > 0 {
            tx.execute(
                "INSERT INTO rule_hits (rule_id, hit_count, last_hit) VALUES (?1, ?2, ?3)
                 ON CONFLICT(rule_id) DO UPDATE SET hit_count = hit_count + excluded.hit_count, last_hit = excluded.last_hit",
                params![rule_id, *count as i64, now],
            )?;
        }
    }

    tx.commit()?;
    Ok(())
}

pub fn get_stats(conn: &Connection) -> Result<serde_json::Value> {
    let row = conn.query_row(
        "SELECT COUNT(*) as runs, COALESCE(SUM(total),0), COALESCE(SUM(passed),0), COALESCE(SUM(blocked),0), COALESCE(SUM(uncertain),0) FROM filter_runs",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
            ))
        },
    )?;
    let (runs, total, passed, blocked, uncertain) = row;

    let mut stmt = conn.prepare(
        "SELECT rule_id, hit_count, last_hit FROM rule_hits ORDER BY hit_count DESC",
    )?;
    let rule_hits: Vec<serde_json::Value> = stmt
        .query_map([], |r| {
            Ok(serde_json::json!({
                "rule_id": r.get::<_,String>(0)?,
                "hit_count": r.get::<_,i64>(1)?,
                "last_hit": r.get::<_,f64>(2)?
            }))
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(serde_json::json!({
        "total_runs": runs,
        "total_emails": total,
        "total_passed": passed,
        "total_blocked": blocked,
        "total_uncertain": uncertain,
        "pass_rate": if total > 0 { (passed as f64 / total as f64 * 10000.0).round() / 10000.0 } else { 0.0 },
        "block_rate": if total > 0 { (blocked as f64 / total as f64 * 10000.0).round() / 10000.0 } else { 0.0 },
        "uncertain_rate": if total > 0 { (uncertain as f64 / total as f64 * 10000.0).round() / 10000.0 } else { 0.0 },
        "rule_hits": rule_hits
    }))
}

pub fn sync_rules(conn: &Connection, rule_ids: &[String]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs_f64();
    for id in rule_ids {
        tx.execute(
            "INSERT OR IGNORE INTO rule_hits (rule_id, hit_count, last_hit) VALUES (?1, 0, ?2)",
            params![id, now],
        )?;
    }
    tx.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('rules_synced_at', ?1)",
        params![now.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}
