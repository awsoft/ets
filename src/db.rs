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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::collections::HashMap;

    /// Open an in-memory SQLite connection with schema applied.
    fn mem_db() -> Connection {
        let conn = Connection::open(":memory:").unwrap();
        ensure_schema(&conn).unwrap();
        conn
    }

    // ---- Schema creation ----

    #[test]
    fn schema_creates_filter_runs_table() {
        let conn = mem_db();
        // Query the table — should not fail
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM filter_runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn schema_creates_rule_hits_table() {
        let conn = mem_db();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rule_hits", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn schema_creates_meta_table() {
        let conn = mem_db();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM meta", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn schema_idempotent_double_creation() {
        let conn = mem_db();
        // Calling ensure_schema again should not fail (CREATE TABLE IF NOT EXISTS)
        ensure_schema(&conn).unwrap();
    }

    // ---- record_run ----

    #[test]
    fn record_run_inserts_filter_run_row() {
        let conn = mem_db();
        let hits = HashMap::new();
        record_run(&conn, 10, 7, 2, 1, &hits).unwrap();

        let (total, passed, blocked, uncertain): (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT total, passed, blocked, uncertain FROM filter_runs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(total, 10);
        assert_eq!(passed, 7);
        assert_eq!(blocked, 2);
        assert_eq!(uncertain, 1);
    }

    #[test]
    fn record_run_multiple_rows_accumulate() {
        let conn = mem_db();
        let hits = HashMap::new();
        record_run(&conn, 10, 7, 2, 1, &hits).unwrap();
        record_run(&conn, 5, 3, 1, 1, &hits).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM filter_runs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn record_run_inserts_rule_hits() {
        let conn = mem_db();
        let mut hits = HashMap::new();
        hits.insert("rule-alpha".to_string(), 3usize);
        hits.insert("rule-beta".to_string(), 1usize);
        record_run(&conn, 5, 4, 1, 0, &hits).unwrap();

        let hit_count: i64 = conn
            .query_row(
                "SELECT hit_count FROM rule_hits WHERE rule_id = 'rule-alpha'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hit_count, 3);
    }

    #[test]
    fn record_run_zero_hits_not_inserted() {
        let conn = mem_db();
        let mut hits = HashMap::new();
        hits.insert("rule-zero".to_string(), 0usize);
        record_run(&conn, 1, 1, 0, 0, &hits).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM rule_hits WHERE rule_id = 'rule-zero'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "rules with 0 hits should not be inserted");
    }

    // ---- Rule hit upsert (increment on conflict) ----

    #[test]
    fn rule_hit_upsert_increments_on_conflict() {
        let conn = mem_db();

        let mut hits1 = HashMap::new();
        hits1.insert("rule-x".to_string(), 5usize);
        record_run(&conn, 10, 10, 0, 0, &hits1).unwrap();

        let mut hits2 = HashMap::new();
        hits2.insert("rule-x".to_string(), 3usize);
        record_run(&conn, 10, 10, 0, 0, &hits2).unwrap();

        let hit_count: i64 = conn
            .query_row(
                "SELECT hit_count FROM rule_hits WHERE rule_id = 'rule-x'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hit_count, 8, "upsert should accumulate: 5 + 3 = 8");
    }

    #[test]
    fn rule_hit_upsert_multiple_rules() {
        let conn = mem_db();
        let mut hits = HashMap::new();
        hits.insert("rule-a".to_string(), 2usize);
        hits.insert("rule-b".to_string(), 4usize);
        record_run(&conn, 6, 6, 0, 0, &hits).unwrap();

        let mut hits2 = HashMap::new();
        hits2.insert("rule-a".to_string(), 3usize);
        record_run(&conn, 3, 3, 0, 0, &hits2).unwrap();

        let a: i64 = conn.query_row("SELECT hit_count FROM rule_hits WHERE rule_id='rule-a'", [], |r| r.get(0)).unwrap();
        let b: i64 = conn.query_row("SELECT hit_count FROM rule_hits WHERE rule_id='rule-b'", [], |r| r.get(0)).unwrap();
        assert_eq!(a, 5);  // 2 + 3
        assert_eq!(b, 4);  // unchanged
    }

    // ---- Stats query ----

    #[test]
    fn get_stats_empty_db() {
        let conn = mem_db();
        let stats = get_stats(&conn).unwrap();
        assert_eq!(stats["total_runs"].as_u64().unwrap(), 0);
        assert_eq!(stats["total_emails"].as_u64().unwrap(), 0);
        assert_eq!(stats["total_passed"].as_u64().unwrap(), 0);
        assert_eq!(stats["total_blocked"].as_u64().unwrap(), 0);
        assert_eq!(stats["total_uncertain"].as_u64().unwrap(), 0);
        assert_eq!(stats["pass_rate"].as_f64().unwrap(), 0.0);
        let rule_hits = stats["rule_hits"].as_array().unwrap();
        assert!(rule_hits.is_empty());
    }

    #[test]
    fn get_stats_after_runs() {
        let conn = mem_db();
        let mut hits = HashMap::new();
        hits.insert("rule-1".to_string(), 3usize);
        record_run(&conn, 100, 60, 30, 10, &hits).unwrap();
        record_run(&conn, 50, 40, 5, 5, &HashMap::new()).unwrap();

        let stats = get_stats(&conn).unwrap();
        assert_eq!(stats["total_runs"].as_u64().unwrap(), 2);
        assert_eq!(stats["total_emails"].as_u64().unwrap(), 150);
        assert_eq!(stats["total_passed"].as_u64().unwrap(), 100);
        assert_eq!(stats["total_blocked"].as_u64().unwrap(), 35);
    }

    #[test]
    fn get_stats_pass_rate_calculated() {
        let conn = mem_db();
        record_run(&conn, 100, 75, 15, 10, &HashMap::new()).unwrap();
        let stats = get_stats(&conn).unwrap();
        let pass_rate = stats["pass_rate"].as_f64().unwrap();
        assert!((pass_rate - 0.75).abs() < 0.001, "pass rate should be 75%");
    }

    #[test]
    fn get_stats_rule_hits_sorted_by_count_desc() {
        let conn = mem_db();
        let mut hits = HashMap::new();
        hits.insert("low-rule".to_string(), 1usize);
        hits.insert("high-rule".to_string(), 10usize);
        hits.insert("mid-rule".to_string(), 5usize);
        record_run(&conn, 20, 20, 0, 0, &hits).unwrap();

        let stats = get_stats(&conn).unwrap();
        let rule_hits = stats["rule_hits"].as_array().unwrap();
        // First should be highest
        assert_eq!(rule_hits[0]["rule_id"].as_str().unwrap(), "high-rule");
        assert_eq!(rule_hits[0]["hit_count"].as_u64().unwrap(), 10);
    }

    // ---- Sync rules ----

    #[test]
    fn sync_rules_inserts_new_entries() {
        let conn = mem_db();
        let ids = vec!["rule-1".to_string(), "rule-2".to_string(), "rule-3".to_string()];
        sync_rules(&conn, &ids).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rule_hits", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn sync_rules_does_not_overwrite_existing_hit_counts() {
        let conn = mem_db();
        // Insert a rule with some hits first
        let mut hits = HashMap::new();
        hits.insert("existing-rule".to_string(), 7usize);
        record_run(&conn, 10, 10, 0, 0, &hits).unwrap();

        // Sync should not zero out the existing count
        sync_rules(&conn, &["existing-rule".to_string()]).unwrap();

        let hit_count: i64 = conn
            .query_row(
                "SELECT hit_count FROM rule_hits WHERE rule_id='existing-rule'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hit_count, 7, "sync_rules should preserve existing hit counts");
    }

    #[test]
    fn sync_rules_sets_meta_key() {
        let conn = mem_db();
        sync_rules(&conn, &["r1".to_string()]).unwrap();

        let val: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key='rules_synced_at'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!val.is_empty(), "rules_synced_at meta key should be set");
    }

    #[test]
    fn sync_rules_empty_list_no_panic() {
        let conn = mem_db();
        sync_rules(&conn, &[]).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rule_hits", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    // ---- open() creates directories ----

    #[test]
    fn open_creates_parent_directories() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CNT: AtomicU64 = AtomicU64::new(0);
        let n = CNT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join(format!("ets_db_test_{}", n))
            .join("nested")
            .join("subdir");
        let db_path = dir.join("test.db");
        let conn = open(&db_path).unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM filter_runs", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0);
        // Cleanup
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join(format!("ets_db_test_{}", n)));
    }
}
