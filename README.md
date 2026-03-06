# ETS — Email Token Saver

An OpenClaw plugin that runs a rules-based email pre-filter before the LLM sees your inbox. Strips noise, extracts structured data, and dramatically reduces token usage in email monitoring workflows.

## What it does

**Without ETS:** Your hourly email cron feeds 25 raw emails to an LLM to decide what matters. ~800 tokens per check.

**With ETS:**
1. `ets_filter` — Rules engine blocks known noise (shipping updates, newsletters, promo emails) in <5ms. No LLM needed.
2. `ets_extract` — Classifies remaining emails by type (shipping/job/school/billing/financial) and extracts key fields. No LLM needed.
3. LLM sees a compact structured summary — typically 3-5 emails, ~50-150 tokens.

**Result: ~80-90% token reduction on email monitoring.**

## Install

```bash
openclaw plugins install @awsoft/openclaw-ets
```

Restart the Gateway after installing.

## Requirements

- Python 3.8+
- OpenClaw 2026.x+

## Configuration

Optional config under `plugins.entries.ets.config`:

| Field | Default | Description |
|-------|---------|-------------|
| `rulesPath` | `<plugin-dir>/email_rules.json` | Path to rules file |
| `dbPath` | `~/.openclaw/ets/ets.db` | SQLite database for stats/audit |
| `blockThreshold` | `-50` | Score below this → blocked |
| `allowThreshold` | `50` | Score above this → passed |
| `snippetCap` | `300` | Max chars per email snippet |

## Agent tools

| Tool | Description |
|------|-------------|
| `ets_filter` | Filter raw email array. Returns passed/blocked/uncertain buckets. |
| `ets_extract` | Classify and extract structured fields from filter output. |
| `ets_add_rule` | Add a block or allow rule to the rules file. |
| `ets_list_rules` | List current rules. |
| `ets_stats` | Get filter statistics (hit counts, run history). |

## Slash commands

| Command | Description |
|---------|-------------|
| `/ets stats` | Show filter statistics |
| `/ets rules` | List all rules |
| `/ets pipeline` | Show pipeline config |
| `/ets version` | Show version and rules count |

## Recommended cron prompt (after ETS)

```
Run the email pipeline:
1. Fetch Yahoo: `himalaya envelope list --page-size 30 -o json`
2. Fetch Gmail: `GOG_KEYRING_PASSWORD="..." gog gmail search "newer_than:1h" --limit 30 -p`
3. Normalize both to ETS input format and call ets_filter
4. Pass filter output to ets_extract
5. Review the extracted emails array. Flag only what needs the user's attention.
```

## Adding rules

Via agent tool:
> "Block all emails from Groupon"
> → agent calls `ets_add_rule`

Via slash command (planned): `/ets block groupon.com`

Via direct edit: modify `email_rules.json` — changes take effect on next filter run.

## License

MIT
