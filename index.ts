/**
 * ETS — Email Token Saver
 * OpenClaw plugin: wraps the Rust `ets` binary (or Python scripts as fallback).
 *
 * Binary preference:
 *   1. bin/ets  (Rust — fast, single-process pipeline)
 *   2. email_filter.py / email_extractor.py  (Python fallback)
 */

import { spawnSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const PLUGIN_DIR: string = (() => {
  try {
    if (typeof __filename !== "undefined") {
      return path.dirname(__filename);
    }
  } catch (_) {}
  try {
    return path.dirname(new URL(import.meta.url).pathname);
  } catch (_) {}
  return path.join(os.homedir(), ".openclaw", "extensions", "ets");
})();

const BINARY_PATH = path.join(PLUGIN_DIR, "bin", "ets");
const FILTER_SCRIPT = path.join(PLUGIN_DIR, "email_filter.py");
const EXTRACTOR_SCRIPT = path.join(PLUGIN_DIR, "email_extractor.py");

const DEFAULT_RULES_PATH = path.join(PLUGIN_DIR, "email_rules.json");
const DEFAULT_DB_PATH = path.join(os.homedir(), ".openclaw", "ets", "ets.db");
const DEFAULT_TEMPLATES_PATH = path.join(PLUGIN_DIR, "extractor_templates.json");

const PLUGIN_VERSION = "1.2.0";

// ---------------------------------------------------------------------------
// Binary check
// ---------------------------------------------------------------------------

function hasBinary(): boolean {
  try {
    fs.accessSync(BINARY_PATH, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Unified spawn helper (#7 — single function, no duplicate runFilter/runExtractor)
// ---------------------------------------------------------------------------

interface SpawnResult {
  stdout: string;
  stderr: string;
  status: number | null;
}

/**
 * Run an ETS operation. Uses the Rust binary when available; falls back to Python.
 *
 * @param args  Subcommand + flags. First element is the subcommand:
 *              "filter" | "extract" | "pipeline" | "stats" | "sync-rules"
 *              When falling back to Python, "filter"→email_filter.py,
 *              "extract"→email_extractor.py.
 * @param input Optional stdin payload (JSON string).
 * @param env   Extra environment variables.
 */
function runEts(
  args: string[],
  input?: string,
  env?: NodeJS.ProcessEnv
): SpawnResult {
  const mergedEnv = { ...process.env, ...env };

  if (hasBinary()) {
    // Rust binary — subcommand is first arg, global flags prepended
    const result = spawnSync(
      BINARY_PATH,
      ["--rules", rulesPathResolved, "--db", dbPathResolved, ...args],
      { input, encoding: "utf8", env: mergedEnv, maxBuffer: 50 * 1024 * 1024 }
    );
    return {
      stdout: result.stdout ?? "",
      stderr: result.stderr ?? "",
      status: result.status,
    };
  }

  // Python fallback — map subcommand to script + translate flags
  const [subcmd, ...rest] = args;
  if (subcmd === "stats") {
    const result = spawnSync(
      "python3",
      [FILTER_SCRIPT, "--stats", "--rules", rulesPathResolved, "--db", dbPathResolved],
      { encoding: "utf8", env: mergedEnv, maxBuffer: 50 * 1024 * 1024 }
    );
    return { stdout: result.stdout ?? "", stderr: result.stderr ?? "", status: result.status };
  }
  if (subcmd === "sync-rules") {
    const result = spawnSync(
      "python3",
      [FILTER_SCRIPT, "--sync-rules", "--rules", rulesPathResolved, "--db", dbPathResolved],
      { encoding: "utf8", env: mergedEnv, maxBuffer: 50 * 1024 * 1024 }
    );
    return { stdout: result.stdout ?? "", stderr: result.stderr ?? "", status: result.status };
  }
  if (subcmd === "filter") {
    const result = spawnSync(
      "python3",
      [FILTER_SCRIPT, "--rules", rulesPathResolved, "--db", dbPathResolved, ...rest],
      { input, encoding: "utf8", env: mergedEnv, maxBuffer: 50 * 1024 * 1024 }
    );
    return { stdout: result.stdout ?? "", stderr: result.stderr ?? "", status: result.status };
  }
  if (subcmd === "extract") {
    // Python extractor reads from stdin (no temp file — #4)
    const result = spawnSync(
      "python3",
      [EXTRACTOR_SCRIPT, ...rest],
      { input, encoding: "utf8", env: mergedEnv, maxBuffer: 50 * 1024 * 1024 }
    );
    return { stdout: result.stdout ?? "", stderr: result.stderr ?? "", status: result.status };
  }
  if (subcmd === "pipeline") {
    // Python has no pipeline subcommand — run filter then extract sequentially
    const filterResult = spawnSync(
      "python3",
      [FILTER_SCRIPT, "--rules", rulesPathResolved, "--db", dbPathResolved,
       ...rest.filter(a => a === "--explain")],
      { input, encoding: "utf8", env: mergedEnv, maxBuffer: 50 * 1024 * 1024 }
    );
    if ((filterResult.status ?? 1) !== 0) {
      return { stdout: "", stderr: filterResult.stderr ?? "", status: filterResult.status };
    }
    const extractResult = spawnSync(
      "python3",
      [EXTRACTOR_SCRIPT, ...rest],
      { input: filterResult.stdout, encoding: "utf8", env: mergedEnv, maxBuffer: 50 * 1024 * 1024 }
    );
    return {
      stdout: extractResult.stdout ?? "",
      stderr: extractResult.stderr ?? "",
      status: extractResult.status,
    };
  }

  return { stdout: "", stderr: `Unknown subcommand: ${subcmd}`, status: 1 };
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface FilterResult {
  passed: EmailObj[];
  blocked: EmailObj[];
  uncertain: EmailObj[];
  stats: Record<string, number>;
}

interface ExtractResult {
  emails: ExtractedEmail[];
  stats: Record<string, number>;
}

interface ExtractedEmail {
  id: string;
  from: string;
  subject: string;
  date: string;
  type: string;
  extracted?: Record<string, unknown>;
  snippet: string;
  source_bucket: string;
  matched_template?: string | null;
}

interface EmailObj {
  id?: string;
  from?: string;
  from_name?: string;
  subject?: string;
  date?: string;
  snippet?: string;
  score?: number;
  matched_rules?: string[];
  [key: string]: unknown;
}

interface Rule {
  id: string;
  action: "allow" | "block";
  weight: number;
  match: Record<string, string>;
  reason: string;
}

interface ExtractorTemplate {
  id: string;
  name: string;
  priority: number;
  type: string;
  detect: Record<string, unknown>;
  extract: Record<string, unknown>;
}

interface TemplatesFile {
  _meta: Record<string, unknown>;
  templates: ExtractorTemplate[];
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

function loadRules(rulesPath: string): { version: number; rules: Rule[]; _meta?: Record<string, unknown> } {
  const raw = fs.readFileSync(rulesPath, "utf8");
  return JSON.parse(raw);
}

function loadTemplates(templatesPath: string): TemplatesFile {
  const raw = fs.readFileSync(templatesPath, "utf8");
  return JSON.parse(raw);
}

// ---------------------------------------------------------------------------
// Plugin registration
// ---------------------------------------------------------------------------

// These are set during register() and used by runEts() above.
// Declared at module scope so runEts (defined above) can reference them.
let rulesPathResolved: string = DEFAULT_RULES_PATH;
let dbPathResolved: string = DEFAULT_DB_PATH;

export default function register(api: any): void {
  const cfg: Record<string, any> =
    api.config?.plugins?.entries?.ets?.config ?? {};

  // Proper ~ expansion anchored at start of string (#6)
  rulesPathResolved = cfg.rulesPath
    ? path.resolve(String(cfg.rulesPath).replace(/^~/, os.homedir()))
    : DEFAULT_RULES_PATH;
  dbPathResolved = cfg.dbPath
    ? path.resolve(String(cfg.dbPath).replace(/^~/, os.homedir()))
    : DEFAULT_DB_PATH;
  const templatesPath: string = cfg.templatesPath
    ? path.resolve(String(cfg.templatesPath).replace(/^~/, os.homedir()))
    : DEFAULT_TEMPLATES_PATH;

  const thresholdBlock: number = cfg.blockThreshold ?? -50;
  const thresholdAllow: number = cfg.allowThreshold ?? 50;

  const binaryAvailable = hasBinary();

  // -------------------------------------------------------------------------
  // Tool: ets_filter
  // -------------------------------------------------------------------------
  api.registerTool({
    name: "ets_filter",
    description:
      "Pre-filter a batch of emails through the ETS rules engine. " +
      "Returns emails split into passed/blocked/uncertain buckets with scores. " +
      "Use this before passing emails to the LLM to reduce token usage.",
    parameters: {
      type: "object",
      properties: {
        emails: {
          type: "array",
          description:
            "Array of email objects. Each must have: id, from, subject, date, snippet. " +
            "Optional: from_name.",
          items: {
            type: "object",
            properties: {
              id: { type: "string" },
              from: { type: "string" },
              from_name: { type: "string" },
              subject: { type: "string" },
              date: { type: "string" },
              snippet: { type: "string" },
            },
            required: ["id", "from", "subject"],
          },
        },
        explain: {
          type: "boolean",
          description: "If true, include matched_rules array in each email result.",
          default: false,
        },
      },
      required: ["emails"],
    },
    handler: async (params: { emails: EmailObj[]; explain?: boolean }) => {
      const { emails, explain = false } = params;

      if (!Array.isArray(emails) || emails.length === 0) {
        return {
          passed: [], blocked: [], uncertain: [],
          stats: { total: 0, passed: 0, blocked: 0, uncertain: 0, rules_loaded: 0, elapsed_ms: 0 },
        };
      }

      const args = [
        "filter",
        "--threshold-block", String(thresholdBlock),
        "--threshold-allow", String(thresholdAllow),
        ...(explain ? ["--explain"] : []),
      ];

      const { stdout, stderr, status } = runEts(args, JSON.stringify(emails));

      if (status !== 0) {
        throw new Error(`ETS filter failed (exit ${status}): ${stderr.slice(0, 500)}`);
      }

      try {
        return JSON.parse(stdout) as FilterResult;
      } catch (e) {
        throw new Error(
          `Failed to parse filter output: ${(e as Error).message}\nRaw: ${stdout.slice(0, 500)}`
        );
      }
    },
  });

  // -------------------------------------------------------------------------
  // Tool: ets_extract
  // -------------------------------------------------------------------------
  api.registerTool({
    name: "ets_extract",
    description:
      "Run pre-LLM email extraction on filter output. Classifies emails by type and extracts key fields. " +
      "Pass the output of ets_filter as input. Returns compact structured summaries — no LLM involved. " +
      "When the Rust binary is available, extraction runs in the same process as filtering (no overhead).",
    parameters: {
      type: "object",
      properties: {
        filterOutput: {
          type: "object",
          description:
            "The full output object from ets_filter (contains passed, blocked, uncertain, stats).",
        },
        snippetCap: {
          type: "number",
          description: "Max chars for snippet fallback (default: 300). Financial alerts always get full snippet.",
          default: 300,
        },
        explain: {
          type: "boolean",
          description: "If true, include extraction debug fields in each email.",
          default: false,
        },
      },
      required: ["filterOutput"],
    },
    handler: async (params: {
      filterOutput: FilterResult;
      snippetCap?: number;
      explain?: boolean;
    }) => {
      const { filterOutput, snippetCap = 300, explain = false } = params;

      if (!filterOutput || typeof filterOutput !== "object") {
        throw new Error("filterOutput must be the object returned by ets_filter");
      }

      // Rust binary: read filter output from stdin — no temp file (#4)
      // Python fallback: also reads from stdin (extractor.py supports stdin)
      const args = [
        "extract",
        "--snippet-cap", String(snippetCap),
        ...(explain ? ["--explain"] : []),
      ];

      const { stdout, stderr, status } = runEts(
        args,
        JSON.stringify(filterOutput), // piped to stdin, not written to disk
        { ETS_SNIPPET_CAP: String(snippetCap) }
      );

      if (status !== 0) {
        throw new Error(`ETS extractor failed (exit ${status}): ${stderr.slice(0, 500)}`);
      }

      try {
        return JSON.parse(stdout) as ExtractResult;
      } catch (e) {
        throw new Error(
          `Failed to parse extractor output: ${(e as Error).message}\nRaw: ${stdout.slice(0, 500)}`
        );
      }
    },
  });

  // -------------------------------------------------------------------------
  // Tool: ets_add_rule
  // -------------------------------------------------------------------------
  api.registerTool({
    name: "ets_add_rule",
    description:
      "Add a new filtering rule to email_rules.json. " +
      "Automatically syncs the rule to the SQLite database.",
    parameters: {
      type: "object",
      properties: {
        id: {
          type: "string",
          description: "Unique rule ID (kebab-case, e.g. block-acme-spam)",
        },
        action: {
          type: "string",
          enum: ["allow", "block"],
          description: "Whether to allow or block matching emails",
        },
        weight: {
          type: "number",
          description:
            "Rule weight 1-100. Higher = stronger signal. Weights >= 90 on allow rules act as hard overrides.",
          minimum: 1,
          maximum: 100,
        },
        match: {
          type: "object",
          description:
            "Match criteria. Keys: sender_domain, sender_contains, sender_exact, subject_regex, body_regex. " +
            "Only one key per rule is recommended.",
          additionalProperties: { type: "string" },
        },
        reason: {
          type: "string",
          description: "Human-readable reason for this rule.",
        },
      },
      required: ["id", "action", "weight", "match", "reason"],
    },
    handler: async (params: {
      id: string;
      action: "allow" | "block";
      weight: number;
      match: Record<string, string>;
      reason: string;
    }) => {
      const { id, action, weight, match, reason } = params;

      if (!/^[a-z0-9][a-z0-9-]*$/.test(id)) {
        throw new Error("Rule ID must be kebab-case (lowercase letters, numbers, hyphens)");
      }
      if (!["allow", "block"].includes(action)) {
        throw new Error("Action must be 'allow' or 'block'");
      }
      if (weight < 1 || weight > 100) {
        throw new Error("Weight must be between 1 and 100");
      }
      if (!match || Object.keys(match).length === 0) {
        throw new Error("Match criteria must have at least one key");
      }

      const validMatchKeys = [
        "sender_domain", "sender_contains", "sender_exact", "subject_regex", "body_regex",
      ];
      for (const key of Object.keys(match)) {
        if (!validMatchKeys.includes(key)) {
          throw new Error(`Invalid match key: ${key}. Valid keys: ${validMatchKeys.join(", ")}`);
        }
      }

      const data = loadRules(rulesPathResolved);

      if (data.rules.some((r: Rule) => r.id === id)) {
        throw new Error(`Rule with ID '${id}' already exists. Use a different ID.`);
      }

      const newRule: Rule = { id, action, weight, match, reason };
      data.rules.push(newRule);
      fs.writeFileSync(rulesPathResolved, JSON.stringify(data, null, 2), "utf8");

      // Sync to DB using unified runEts (#9)
      const { status, stderr } = runEts(["sync-rules"]);
      if (status !== 0) {
        console.error(`[ETS] sync-rules warning: ${stderr}`);
      }

      return { ok: true, total_rules: data.rules.length, rule: newRule };
    },
  });

  // -------------------------------------------------------------------------
  // Tool: ets_list_rules
  // -------------------------------------------------------------------------
  api.registerTool({
    name: "ets_list_rules",
    description: "List current ETS filtering rules.",
    parameters: {
      type: "object",
      properties: {
        action_filter: {
          type: "string",
          enum: ["allow", "block", "all"],
          description: "Filter by action type (default: all)",
          default: "all",
        },
      },
    },
    handler: async (params: { action_filter?: string }) => {
      const { action_filter = "all" } = params;
      const data = loadRules(rulesPathResolved);
      const rules =
        action_filter === "all"
          ? data.rules
          : data.rules.filter((r: Rule) => r.action === action_filter);
      return { rules, total: rules.length };
    },
  });

  // -------------------------------------------------------------------------
  // Tool: ets_stats (#9 — uses runEts(["stats"]) not a raw Python subprocess)
  // -------------------------------------------------------------------------
  api.registerTool({
    name: "ets_stats",
    description:
      "Get ETS filter statistics: rule hit counts, total runs, pass/block/uncertain rates.",
    parameters: {
      type: "object",
      properties: {},
    },
    handler: async (_params: Record<string, never>) => {
      const { stdout, stderr, status } = runEts(["stats"]);
      if (status !== 0) {
        throw new Error(`ETS stats failed (exit ${status}): ${stderr.slice(0, 500)}`);
      }
      try {
        return JSON.parse(stdout);
      } catch {
        throw new Error(`Failed to parse stats output: ${stdout.slice(0, 200)}`);
      }
    },
  });

  // -------------------------------------------------------------------------
  // Tool: ets_add_extractor
  // -------------------------------------------------------------------------
  api.registerTool({
    name: "ets_add_extractor",
    description:
      "Add a new email extraction template to ETS. Use this to teach ETS how to extract " +
      "structured data from a new email type or sender. Templates are matched in priority order " +
      "(highest first). Site-specific extractors should have priority >= 100; generic type " +
      "extractors should use priority 50-99.",
    parameters: {
      type: "object",
      properties: {
        template: {
          type: "object",
          description:
            "Full template object. Required fields: id (snake_case), name, priority (integer), " +
            "type (shipping|order_confirm|billing|job|financial_alert|calendar_invite|subscription|travel|unclassified), " +
            "detect (at least one rule: sender_domain|sender_contains|subject_regex|snippet_regex), " +
            "extract (at least one field with static|regex|enum_map).",
        },
        validate: {
          type: "boolean",
          description: "Validate required fields before writing (default: true).",
          default: true,
        },
      },
      required: ["template"],
    },
    handler: async (params: { template: ExtractorTemplate; validate?: boolean }) => {
      const { template, validate = true } = params;

      if (validate) {
        for (const field of ["id", "name", "priority", "type", "detect", "extract"]) {
          if (!(field in template)) {
            throw new Error(`Missing required field: ${field}`);
          }
        }
        if (!/^[a-z0-9][a-z0-9\-_]*$/.test(template.id)) {
          throw new Error("Template id must be snake_case or kebab-case");
        }
        const validTypes = [
          "shipping", "order_confirm", "billing", "job", "financial_alert",
          "calendar_invite", "subscription", "travel", "school_notice", "unclassified",
        ];
        if (!validTypes.includes(template.type)) {
          throw new Error(`Invalid type '${template.type}'. Valid types: ${validTypes.join(", ")}`);
        }
        const detectKeys = ["sender_domain", "sender_contains", "subject_regex", "snippet_regex"];
        const hasDetect = detectKeys.some(k => k in (template.detect || {}));
        if (!hasDetect) {
          throw new Error(`detect must have at least one rule: ${detectKeys.join(", ")}`);
        }
        if (!template.extract || Object.keys(template.extract).length === 0) {
          throw new Error("extract must have at least one field");
        }
        if (typeof template.priority !== "number" || template.priority < 1) {
          throw new Error("priority must be a positive integer");
        }
      }

      const data = loadTemplates(templatesPath);
      if (data.templates.some((t: ExtractorTemplate) => t.id === template.id)) {
        throw new Error(`Template with id '${template.id}' already exists.`);
      }

      data.templates.push(template);
      fs.writeFileSync(templatesPath, JSON.stringify(data, null, 2), "utf8");

      return {
        success: true,
        template_id: template.id,
        total_templates: data.templates.length,
        message: `Template '${template.id}' added successfully.`,
      };
    },
  });

  // -------------------------------------------------------------------------
  // Slash command: /ets [stats|rules|version|pipeline]
  // -------------------------------------------------------------------------
  api.registerCommand({
    name: "ets",
    acceptsArgs: true,
    description: "ETS Email Token Saver — /ets stats | /ets rules | /ets version | /ets pipeline",
    handler: async (_context: any, args: string) => {
      const sub = (args ?? "").trim().toLowerCase();

      // Load rules once at the top — not per-branch (#8)
      let rulesData: { rules: Rule[]; _meta?: Record<string, unknown> } = { rules: [] };
      try {
        rulesData = loadRules(rulesPathResolved);
      } catch {
        return `❌ Cannot read rules file: \`${rulesPathResolved}\``;
      }

      if (sub === "pipeline") {
        const meta = (rulesData._meta ?? {}) as Record<string, unknown>;
        const snippetCap: number = (meta.snippet_cap as number) ?? 300;
        const pipeline: string[] = (meta.pipeline as string[]) ?? ["filter", "extract"];
        const pipelineStr = pipeline.map((s: string, i: number) => `${i + 1}. **${s}**`).join(" → ");
        const allowCount = rulesData.rules.filter((r: Rule) => r.action === "allow").length;
        const blockCount = rulesData.rules.filter((r: Rule) => r.action === "block").length;

        let templateCount = 0;
        try {
          const tmplData = loadTemplates(templatesPath);
          templateCount = tmplData.templates.length;
        } catch (_) {}

        return [
          `**ETS Pipeline Config** (v${PLUGIN_VERSION})`,
          ``,
          `**Engine:** ${binaryAvailable ? "🦀 Rust binary" : "🐍 Python fallback"}`,
          `**Pipeline stages:** ${pipelineStr}`,
          `**Filter rules:** ${rulesData.rules.length} total (${allowCount} allow, ${blockCount} block)`,
          `**Extractor templates:** ${templateCount} loaded`,
          `**Snippet cap:** ${snippetCap} chars (financial alerts: unlimited)`,
          `**Thresholds:** block ≤ ${thresholdBlock}, allow ≥ ${thresholdAllow}`,
          ``,
          `**Rules path:** \`${rulesPathResolved}\``,
          `**Templates path:** \`${templatesPath}\``,
          `**DB path:** \`${dbPathResolved}\``,
          `**Binary path:** \`${BINARY_PATH}\``,
        ].join("\n");
      }

      if (sub === "version") {
        let templateCount = 0;
        try {
          const tmplData = loadTemplates(templatesPath);
          templateCount = tmplData.templates.length;
        } catch (_) {}
        return (
          `**ETS — Email Token Saver** v${PLUGIN_VERSION}\n` +
          `Engine: ${binaryAvailable ? "🦀 Rust binary" : "🐍 Python fallback"}\n` +
          `Filter rules: ${rulesData.rules.length}\n` +
          `Extractor templates: ${templateCount}\n` +
          `Rules path: \`${rulesPathResolved}\`\n` +
          `DB path: \`${dbPathResolved}\``
        );
      }

      if (sub === "rules") {
        const allowRules = rulesData.rules.filter((r: Rule) => r.action === "allow");
        const blockRules = rulesData.rules.filter((r: Rule) => r.action === "block");
        const fmt = (r: Rule) => `• **${r.id}** (weight: ${r.weight}) — ${r.reason}`;

        return [
          `**ETS Rules** (${rulesData.rules.length} total)`,
          "",
          `**✅ Allow rules (${allowRules.length}):**`,
          ...allowRules.map(fmt),
          "",
          `**🚫 Block rules (${blockRules.length}):**`,
          ...blockRules.map(fmt),
        ].join("\n");
      }

      // Default: stats
      const { stdout, stderr, status } = runEts(["stats"]); // (#9)
      if (status !== 0) {
        return `❌ ETS stats error: ${stderr.slice(0, 300)}`;
      }
      let s: any;
      try {
        s = JSON.parse(stdout);
      } catch {
        return `❌ Failed to parse stats output`;
      }

      const pct = (n: number, total: number) =>
        total > 0 ? ` (${Math.round((n / total) * 100)}%)` : "";

      const top5 = (s.rule_hits ?? [])
        .slice(0, 5)
        .map((h: any) => `  • ${h.rule_id}: ${h.hit_count} hits`)
        .join("\n");

      return [
        `**ETS Filter Stats** (${binaryAvailable ? "🦀 Rust" : "🐍 Python"})`,
        `Total runs: ${s.total_runs}`,
        `Total emails processed: ${s.total_emails}`,
        `Passed: ${s.total_passed}${pct(s.total_passed, s.total_emails)}`,
        `Blocked: ${s.total_blocked}${pct(s.total_blocked, s.total_emails)}`,
        `Uncertain: ${s.total_uncertain}${pct(s.total_uncertain, s.total_emails)}`,
        top5 ? `\n**Top 5 rules by hits:**\n${top5}` : "",
      ]
        .filter(Boolean)
        .join("\n");
    },
  });
}
