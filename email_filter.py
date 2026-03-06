#!/usr/bin/env python3
"""
ETS — Email Token Saver
Rules-based email pre-filter to reduce LLM token usage.
"""

import argparse
import email.utils
import json
import os
import re
import sqlite3
import sys
import time
import threading
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
DEFAULT_RULES_PATH = Path(os.environ.get("ETS_RULES_PATH", SCRIPT_DIR / "email_rules.json"))
DEFAULT_DB_PATH = Path(os.environ.get("ETS_DB_PATH", Path.home() / ".openclaw" / "ets" / "ets.db"))

THRESHOLD_BLOCK_DEFAULT = -50
THRESHOLD_ALLOW_DEFAULT = 50
HARD_OVERRIDE_MIN_WEIGHT = 90  # allow rules >= this weight hard-override block score


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def open_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
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
    """)
    conn.commit()


def sync_rules_to_db(conn: sqlite3.Connection, rules: list[dict]) -> None:
    """Rebuild rule_hits rows for all current rules (preserve counts)."""
    now = time.time()
    existing = {r["rule_id"] for r in conn.execute("SELECT rule_id FROM rule_hits").fetchall()}
    new_ids = {r["id"] for r in rules}

    # Insert rows for new rules
    for rule in rules:
        if rule["id"] not in existing:
            conn.execute(
                "INSERT OR IGNORE INTO rule_hits (rule_id, hit_count, last_hit) VALUES (?, 0, ?)",
                (rule["id"], now),
            )

    # Remove rows for deleted rules
    for old_id in existing - new_ids:
        conn.execute("DELETE FROM rule_hits WHERE rule_id = ?", (old_id,))

    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('rules_synced_at', ?)",
        (str(now),),
    )
    conn.commit()


def record_run_async(db_path: Path, total: int, passed: int, blocked: int,
                     uncertain: int, rule_hit_counts: dict[str, int]) -> None:
    """Fire-and-forget: record run stats to SQLite without blocking."""
    def _write():
        try:
            conn = open_db(db_path)
            now = time.time()
            conn.execute(
                "INSERT INTO filter_runs (timestamp, total, passed, blocked, uncertain) VALUES (?,?,?,?,?)",
                (now, total, passed, blocked, uncertain),
            )
            for rule_id, count in rule_hit_counts.items():
                if count > 0:
                    conn.execute(
                        """INSERT INTO rule_hits (rule_id, hit_count, last_hit)
                           VALUES (?, ?, ?)
                           ON CONFLICT(rule_id) DO UPDATE SET
                             hit_count = hit_count + excluded.hit_count,
                             last_hit  = excluded.last_hit""",
                        (rule_id, count, now),
                    )
            conn.commit()
            conn.close()
        except Exception as exc:
            print(f"[ETS] DB write error: {exc}", file=sys.stderr)

    t = threading.Thread(target=_write, daemon=True)
    t.start()


def get_stats(db_path: Path) -> dict:
    conn = open_db(db_path)
    runs = conn.execute(
        "SELECT COUNT(*) as runs, SUM(total) as total, SUM(passed) as passed, "
        "SUM(blocked) as blocked, SUM(uncertain) as uncertain FROM filter_runs"
    ).fetchone()
    hits = conn.execute(
        "SELECT rule_id, hit_count, last_hit FROM rule_hits ORDER BY hit_count DESC"
    ).fetchall()
    conn.close()

    total_emails = runs["total"] or 0
    return {
        "total_runs": runs["runs"] or 0,
        "total_emails": total_emails,
        "total_passed": runs["passed"] or 0,
        "total_blocked": runs["blocked"] or 0,
        "total_uncertain": runs["uncertain"] or 0,
        "pass_rate": round((runs["passed"] or 0) / total_emails, 4) if total_emails else 0,
        "block_rate": round((runs["blocked"] or 0) / total_emails, 4) if total_emails else 0,
        "uncertain_rate": round((runs["uncertain"] or 0) / total_emails, 4) if total_emails else 0,
        "rule_hits": [
            {"rule_id": h["rule_id"], "hit_count": h["hit_count"],
             "last_hit": h["last_hit"]}
            for h in hits
        ],
    }


# ---------------------------------------------------------------------------
# Rule engine
# ---------------------------------------------------------------------------

class RuleEngine:
    def __init__(self, rules_path: Path, db_path: Path,
                 threshold_block: int = THRESHOLD_BLOCK_DEFAULT,
                 threshold_allow: int = THRESHOLD_ALLOW_DEFAULT):
        self.rules_path = rules_path
        self.db_path = db_path
        self.threshold_block = threshold_block
        self.threshold_allow = threshold_allow

        self.rules: list[dict] = []
        self.block_domains: set[str] = set()
        self.allow_domains: set[str] = set()
        self.block_senders: list[tuple[str, int, str]] = []   # (substring, weight, rule_id)
        self.allow_senders: list[tuple[str, int, str]] = []
        self.block_senders_exact: set[tuple[str, int, str]] = set()
        self.allow_senders_exact: list[tuple[str, int, str]] = []
        self.regex_rules: list[tuple[re.Pattern, int, str, str]] = []  # (pattern, weight, action, rule_id)

        self._load()

    def _load(self):
        if not self.rules_path.exists():
            raise FileNotFoundError(f"Rules file not found: {self.rules_path}")

        with open(self.rules_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.rules = data.get("rules", [])

        # Check if DB needs sync (rules newer than DB)
        db_mtime = self.db_path.stat().st_mtime if self.db_path.exists() else 0
        rules_mtime = self.rules_path.stat().st_mtime

        conn = open_db(self.db_path)
        if rules_mtime > db_mtime:
            sync_rules_to_db(conn, self.rules)
        conn.close()

        # Build fast lookup structures
        self.block_domains.clear()
        self.allow_domains.clear()
        self.block_senders.clear()
        self.allow_senders.clear()
        self.allow_senders_exact.clear()
        self.regex_rules.clear()
        exact_block: list[tuple[str, int, str]] = []

        for rule in self.rules:
            action = rule.get("action")
            weight = int(rule.get("weight", 50))
            rule_id = rule["id"]
            match = rule.get("match", {})

            if "sender_domain" in match:
                domain = match["sender_domain"].lower()
                if action == "block":
                    self.block_domains.add(domain)
                else:
                    self.allow_domains.add(domain)

            if "sender_contains" in match:
                sub = match["sender_contains"].lower()
                if action == "block":
                    self.block_senders.append((sub, weight, rule_id))
                else:
                    self.allow_senders.append((sub, weight, rule_id))

            if "sender_exact" in match:
                addr = match["sender_exact"].lower()
                if action == "block":
                    exact_block.append((addr, weight, rule_id))
                else:
                    self.allow_senders_exact.append((addr, weight, rule_id))

            if "subject_regex" in match:
                try:
                    pat = re.compile(match["subject_regex"])
                    self.regex_rules.append((pat, weight, action, rule_id, "subject"))
                except re.error as e:
                    print(f"[ETS] Bad subject_regex in rule {rule_id}: {e}", file=sys.stderr)

            if "body_regex" in match:
                try:
                    pat = re.compile(match["body_regex"])
                    self.regex_rules.append((pat, weight, action, rule_id, "body"))
                except re.error as e:
                    print(f"[ETS] Bad body_regex in rule {rule_id}: {e}", file=sys.stderr)

        # Convert exact block to set for O(1)
        self._block_exact_set: set[str] = {addr for addr, _, _ in exact_block}
        self._block_exact_list = exact_block

    def filter_email(self, email_obj: dict, explain: bool = False) -> dict:
        """Score and classify a single email. Returns enriched dict."""
        try:
            raw_from = email_obj.get("from", "") or ""
            from_name = email_obj.get("from_name", "") or ""
            subject = email_obj.get("subject", "") or ""
            snippet = email_obj.get("snippet", "") or ""

            # Parse sender
            _, parsed_addr = email.utils.parseaddr(raw_from)
            parsed_addr = parsed_addr.lower()
            domain = parsed_addr.split("@")[-1] if "@" in parsed_addr else ""
            sender_searchable = f"{parsed_addr} {from_name.lower()}"

        except Exception:
            # Malformed — return uncertain
            result = dict(email_obj)
            result["score"] = 0
            result["decision"] = "uncertain"
            if explain:
                result["matched_rules"] = []
            return result

        net_score = 0
        matched: list[str] = []
        hard_allow = False

        # --- Domain checks (exact, O(1)) ---
        if domain:
            if domain in self.allow_domains:
                # Find rule_id and weight for this domain
                for rule in self.rules:
                    m = rule.get("match", {})
                    if m.get("sender_domain", "").lower() == domain and rule["action"] == "allow":
                        w = rule["weight"]
                        net_score += w
                        matched.append(rule["id"])
                        if w >= HARD_OVERRIDE_MIN_WEIGHT:
                            hard_allow = True
                        break
            if domain in self.block_domains:
                for rule in self.rules:
                    m = rule.get("match", {})
                    if m.get("sender_domain", "").lower() == domain and rule["action"] == "block":
                        net_score -= rule["weight"]
                        matched.append(rule["id"])
                        break

        # --- Exact sender checks ---
        if parsed_addr:
            if parsed_addr in self._block_exact_set:
                for addr, w, rid in self._block_exact_list:
                    if addr == parsed_addr:
                        net_score -= w
                        matched.append(rid)
            for addr, w, rid in self.allow_senders_exact:
                if addr == parsed_addr:
                    net_score += w
                    matched.append(rid)
                    if w >= HARD_OVERRIDE_MIN_WEIGHT:
                        hard_allow = True

        # --- Sender contains checks ---
        for sub, w, rid in self.block_senders:
            if sub in sender_searchable:
                net_score -= w
                matched.append(rid)

        for sub, w, rid in self.allow_senders:
            if sub in sender_searchable:
                net_score += w
                matched.append(rid)
                if w >= HARD_OVERRIDE_MIN_WEIGHT:
                    hard_allow = True

        # --- Regex checks ---
        for entry in self.regex_rules:
            pat, w, action, rid, field = entry
            text = subject if field == "subject" else snippet
            if text and pat.search(text):
                if action == "allow":
                    net_score += w
                    matched.append(rid)
                    if w >= HARD_OVERRIDE_MIN_WEIGHT:
                        hard_allow = True
                else:
                    net_score -= w
                    matched.append(rid)

        # --- Decision ---
        if hard_allow:
            decision = "passed"
        elif net_score >= self.threshold_allow:
            decision = "passed"
        elif net_score <= self.threshold_block:
            decision = "blocked"
        else:
            decision = "uncertain"

        result = dict(email_obj)
        result["score"] = net_score
        result["decision"] = decision
        if explain:
            result["matched_rules"] = list(dict.fromkeys(matched))  # deduplicated, ordered

        return result, list(dict.fromkeys(matched))

    def filter_batch(self, emails: list[dict], explain: bool = False) -> dict:
        t_start = time.perf_counter()

        passed = []
        blocked = []
        uncertain = []
        rule_hit_counts: dict[str, int] = {r["id"]: 0 for r in self.rules}

        for em in emails:
            try:
                result, matched = self.filter_email(em, explain=explain)
                for rid in matched:
                    rule_hit_counts[rid] = rule_hit_counts.get(rid, 0) + 1
                decision = result.get("decision", "uncertain")
                if "decision" in result:
                    del result["decision"]  # clean output
                if decision == "passed":
                    passed.append(result)
                elif decision == "blocked":
                    blocked.append(result)
                else:
                    uncertain.append(result)
            except Exception as exc:
                print(f"[ETS] Error filtering email {em.get('id', '?')}: {exc}", file=sys.stderr)
                result = dict(em)
                result["score"] = 0
                if explain:
                    result["matched_rules"] = []
                uncertain.append(result)

        elapsed_ms = round((time.perf_counter() - t_start) * 1000)

        output = {
            "passed": passed,
            "blocked": blocked,
            "uncertain": uncertain,
            "stats": {
                "total": len(emails),
                "passed": len(passed),
                "blocked": len(blocked),
                "uncertain": len(uncertain),
                "rules_loaded": len(self.rules),
                "elapsed_ms": elapsed_ms,
            },
        }

        # Record async — don't block
        record_run_async(
            self.db_path,
            len(emails), len(passed), len(blocked), len(uncertain),
            rule_hit_counts,
        )

        return output


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETS — Email Token Saver: rules-based email pre-filter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", metavar="FILE",
                        help="JSON file with emails array (default: stdin)")
    parser.add_argument("--rules", metavar="PATH",
                        default=str(DEFAULT_RULES_PATH),
                        help=f"Rules JSON path (default: {DEFAULT_RULES_PATH})")
    parser.add_argument("--db", metavar="PATH",
                        default=str(DEFAULT_DB_PATH),
                        help=f"SQLite DB path (default: {DEFAULT_DB_PATH})")
    parser.add_argument("--explain", action="store_true",
                        help="Include matched_rules in output per email")
    parser.add_argument("--sync-rules", action="store_true",
                        help="Rebuild SQLite from rules file and exit")
    parser.add_argument("--stats", action="store_true",
                        help="Print rule hit stats as JSON and exit")
    parser.add_argument("--threshold-block", type=int, default=THRESHOLD_BLOCK_DEFAULT,
                        metavar="INT", help=f"Block threshold (default: {THRESHOLD_BLOCK_DEFAULT})")
    parser.add_argument("--threshold-allow", type=int, default=THRESHOLD_ALLOW_DEFAULT,
                        metavar="INT", help=f"Allow threshold (default: {THRESHOLD_ALLOW_DEFAULT})")
    return parser.parse_args()


def main():
    args = parse_args()
    rules_path = Path(args.rules).expanduser()
    db_path = Path(args.db).expanduser()

    # --stats: just print DB stats and exit
    if args.stats:
        stats = get_stats(db_path)
        print(json.dumps(stats, indent=2))
        return

    # --sync-rules: rebuild DB from rules file and exit
    if args.sync_rules:
        if not rules_path.exists():
            print(f"[ETS] Rules file not found: {rules_path}", file=sys.stderr)
            sys.exit(1)
        with open(rules_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        rules = data.get("rules", [])
        conn = open_db(db_path)
        sync_rules_to_db(conn, rules)
        conn.close()
        print(f"[ETS] Synced {len(rules)} rules to {db_path}", file=sys.stderr)
        return

    # Load engine
    try:
        engine = RuleEngine(
            rules_path=rules_path,
            db_path=db_path,
            threshold_block=args.threshold_block,
            threshold_allow=args.threshold_allow,
        )
    except FileNotFoundError as e:
        print(f"[ETS] {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ETS] Rules JSON parse error: {e}", file=sys.stderr)
        sys.exit(1)

    # Read input
    try:
        if args.input:
            with open(args.input, "r", encoding="utf-8") as f:
                emails = json.load(f)
        else:
            raw = sys.stdin.read()
            if not raw.strip():
                emails = []
            else:
                emails = json.loads(raw)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[ETS] Input error: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(emails, list):
        print("[ETS] Input must be a JSON array of email objects", file=sys.stderr)
        sys.exit(1)

    result = engine.filter_batch(emails, explain=args.explain)

    # Small delay to let async DB write thread finish if we have a tiny batch
    if len(emails) < 10:
        time.sleep(0.05)

    print(json.dumps(result))


if __name__ == "__main__":
    main()
