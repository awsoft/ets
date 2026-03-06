![ETS — Email Token Saver](https://raw.githubusercontent.com/awsoft/ets/main/logo.png)

[![npm](https://img.shields.io/npm/v/@awsoft/ets)](https://www.npmjs.com/package/@awsoft/ets)
[![license](https://img.shields.io/badge/license-MIT-blue)](./LICENSE)

# ETS — Email Token Saver
> Rules-based email pre-filter for OpenClaw. Strips noise, extracts structure, and categorizes intent — before the LLM sees a single byte.

---

## What it does

**Without ETS:** Your hourly email cron feeds 35 raw emails (~12,000 tokens) directly to the LLM.

**With ETS:**

```
Raw inbox (35 emails, ~12,000 tokens)
        │
        ▼
  [Stage 1] Filter        ← rules engine, <5ms, no LLM
  Block known noise
  24 blocked → gone
        │
        ▼
  [Stage 2] Fetch bodies  ← only for 11 survivors
        │
        ▼
  [Stage 3] Extract +     ← template engine, <2ms, no LLM
  Categorize
  Tags: action_required, personal, financial...
  Snippet policy: full / 100 chars / omitted
        │
        ▼
  LLM sees 11 emails      ← ~400 tokens total
```

**Result: ~80–95% token reduction on email monitoring.**

---

## Install

```bash
openclaw plugins install @awsoft/ets
```

Restart the Gateway after installing.

---

## Requirements

- OpenClaw
- Rust / cargo

---

## How it works

### Stage 1 — Filter

The rules engine scores each email against your block/allow rules. Each rule has a **match** condition, an **action** (`block` or `allow`), and a **weight** (1–100).

- Score ≤ `-50` → blocked, dropped
- Score ≥ `50` → passed
- In between → uncertain (passed with lower confidence)
- Allow rules with weight ≥ 90 are hard overrides — they pass regardless of block score

### Stage 2 — Extract

The template engine matches surviving emails against 20+ built-in templates in priority order. Built-in templates cover:

| Sender | Types |
|--------|-------|
| Amazon, Walmart, eBay | shipping, order_confirm |
| UPS, FedEx, USPS, DHL | shipping |
| PayPal, Stripe | billing |
| GitHub | notifications |
| Generic | financial_alert, subscription, job, billing, calendar_invite |

Each template extracts structured fields — tracking numbers, order numbers, amounts, alert types — with no configuration required for supported senders.

### Stage 3 — Categorize

Every email gets weighted tag scores across 10 categories:

| Tag | Meaning |
|-----|---------|
| `action_required` | Needs a response or action |
| `personal` | From a real person, not automated |
| `financial` | Money, banking, billing |
| `investment` | Stocks, markets, portfolio |
| `job` | Employment, recruiting, career |
| `kids` | School, sports, activities |
| `travel` | Flights, hotels, car rentals |
| `marketing` | Promotional / advertising |
| `social` | Social platforms, community |
| `newsletter` | Informational digest, no action needed |

Tags come from two sources: **template base weights** (e.g. `financial_alert` starts at `action_required: 1.0`) and **cross-cutting tag rules** (10 subject/snippet patterns that adjust scores across any type). Adjustments use max merge — tags only go up.

---

## Snippet policy

Tag scores drive how much of each email the LLM actually sees:

| Policy | Condition | LLM receives |
|--------|-----------|--------------|
| `full` | `action_required ≥ 0.6` OR `personal ≥ 0.7` | Full snippet |
| `short` | `action_required ≥ 0.3` OR `personal ≥ 0.4` OR `investment ≥ 0.7` | First 100 chars |
| `omitted` | Everything else | `null` — zero tokens |

A delivered UPS package: `snippet: null`. A fraud alert: full body. A "hey can you call me?" text: full body. All automatic.

---

## Configuration

Optional config under `plugins.entries.ets.config`:

| Field | Default | Description |
|-------|---------|-------------|
| `rulesPath` | `<plugin-dir>/email_rules.json` | Path to rules file |
| `dbPath` | `~/.openclaw/ets/ets.db` | SQLite stats database |
| `blockThreshold` | `-50` | Score at or below this → blocked |
| `allowThreshold` | `50` | Score at or above this → passed |
| `snippetCap` | `300` | Max chars for full-policy snippets |

---

## Agent tools

| Tool | Description |
|------|-------------|
| `ets_filter` | Filter a raw email array → passed/blocked/uncertain buckets |
| `ets_extract` | Classify, extract fields, apply tags and snippet policy |
| `ets_add_rule` | Add a block or allow rule to `email_rules.json` |
| `ets_list_rules` | List current rules |
| `ets_stats` | Rule hit counts, run history, pass/block rates |
| `ets_add_extractor` | Add a new extraction template |

---

## Slash commands

| Command | Description |
|---------|-------------|
| `/ets stats` | Filter statistics |
| `/ets rules` | List all rules |
| `/ets pipeline` | Pipeline config and engine status |
| `/ets version` | Version and rule count |

---

## Default rules

**ETS ships with zero block rules.** Block rules are personal — what's noise for one person is signal for another. Six universal allow rules ship by default (financial alerts, job milestones, etc.).

Add your own:

```
"Block all emails from Groupon"
```

Or via tool:

```js
ets_add_rule({
  id: "block-groupon",
  action: "block",
  weight: 80,
  match: { sender_domain: "groupon.com" },
  reason: "Promo spam"
})
```

Or edit `email_rules.json` directly — changes take effect on the next run.

---

## Extending extractors

Add support for a new email type without touching any code:

```js
ets_add_extractor({ template: {
  "id": "etsy-order",
  "name": "Etsy Order Confirmation",
  "priority": 105,
  "type": "order_confirm",
  "detect": {
    "sender_domain": "etsy.com",
    "subject_regex": "(?i)(order confirmed|you bought|receipt)"
  },
  "tags": { "financial": 0.3, "action_required": 0.0 },
  "extract": {
    "order_number": { "source": ["subject", "snippet"], "regex": "#(\\d{8,})" },
    "total": { "source": "snippet", "regex": "\\$[\\d,]+\\.?\\d*" },
    "item_hint": { "source": "snippet", "max_chars": 80 }
  }
}})
```

**Priority guide:**
- `≥ 110` — Specific sender + specific subject
- `100–109` — Any email from a known sender domain
- `75–99` — High-confidence generic type
- `50–74` — Broad generic type
- `< 50` — Fallback / catch-all

---

## Binary CLI

```bash
# Filter
echo '[{...}]' | bin/ets filter

# Extract
echo '[{...}]' | bin/ets filter | bin/ets extract

# Single-pass pipeline
echo '[{...}]' | bin/ets pipeline --snippet-cap 300

# Stats
bin/ets stats

# Explain mode
echo '[{...}]' | bin/ets pipeline --explain
```

Build from source:

```bash
cargo build --release && cp target/release/ets bin/ets
```

---

## License

MIT
