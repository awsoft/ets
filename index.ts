/**
 * ETS — Email Token Saver
 * OpenClaw plugin: wraps email_filter.py as agent tools + slash commands.
 */

import { spawnSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

// jiti sets __filename; fall back to import.meta when available
const PLUGIN_DIR: string = (() => {
  try {
    // jiti context
    if (typeof __filename !== "undefined") {
      return path.dirname(__filename);
    }
  } catch (_) {}
  try {
    return path.dirname(new URL(import.meta.url).pathname);
  } catch (_) {}
  return path.join(os.homedir(), ".openclaw", "extensions", "ets");
})();

const DEFAULT_RULES_PATH = path.join(PLUGIN_DIR, "email_rules.json");
const DEFAULT_DB_PATH = path.join(os.homedir(), ".openclaw", "ets", "ets.db");
const FILTER_SCRIPT = path.join(PLUGIN_DIR, "email_filter.py");
const EXTRACTOR_SCRIPT = path.join(PLUGIN_DIR, "email_extractor.py");

const PLUGIN_VERSION = "1.1.0";
const DEFAULT_TEMPLATES_PATH = path.join(PLUGIN_DIR, "extractor_templates.json");

// ---------------------------------------------------------------------------
// Helpers
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
  extraction_methods?: Record<string, string>;
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

function runFilter(
  args: string[],
  input?: string,
  env?: NodeJS.ProcessEnv
): { stdout: string; stderr: string; status: number | null } {
  const result = spawnSync("python3", [FILTER_SCRIPT, ...args], {
    input: input,
    encoding: "utf8",
    env: { ...process.env, ...env },
    maxBuffer: 50 * 1024 * 1024, // 50 MB
  });

  return {
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
    status: result.status,
  };
}

function runExtractor(
  args: string[],
  input?: string,
  env?: NodeJS.ProcessEnv
): { stdout: string; stderr: string; status: number | null } {
  const result = spawnSync("python3", [EXTRACTOR_SCRIPT, ...args], {
    input: input,
    encoding: "utf8",
    env: { ...process.env, ...env },
    maxBuffer: 50 * 1024 * 1024, // 50 MB
  });

  return {
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
    status: result.status,
  };
}

function parseFilterOutput(stdout: string): FilterResult {
  try {
    return JSON.parse(stdout) as FilterResult;
  } catch (e) {
    throw new Error(`Failed to parse filter output: ${(e as Error).message}\nRaw: ${stdout.slice(0, 500)}`);
  }
}

function loadRules(rulesPath: string): { version: number; rules: Rule[] } {
  const raw = fs.readFileSync(rulesPath, "utf8");
  return JSON.parse(raw);
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

function loadTemplates(templatesPath: string): TemplatesFile {
  const raw = fs.readFileSync(templatesPath, "utf8");
  return JSON.parse(raw);
}

interface Rule {
  id: string;
  action: "allow" | "block";
  weight: number;
  match: Record<string, string>;
  reason: string;
}

// ---------------------------------------------------------------------------
// Plugin registration
// ---------------------------------------------------------------------------

export default function register(api: any): void {
  // Resolve config with defaults
  const cfg: Record<string, any> =
    api.config?.plugins?.entries?.ets?.config ?? {};
  const rulesPath: string = cfg.rulesPath
    ? String(cfg.rulesPath).replace("~", os.homedir())
    : DEFAULT_RULES_PATH;
  const dbPath: string = cfg.dbPath
    ? String(cfg.dbPath).replace("~", os.homedir())
    : DEFAULT_DB_PATH;

  const thresholdBlock: number = cfg.blockThreshold ?? -50;
  const thresholdAllow: number = cfg.allowThreshold ?? 50;

  const baseEnv = {
    ETS_RULES_PATH: rulesPath,
    ETS_DB_PATH: dbPath,
  };

  const baseArgs = [
    "--rules", rulesPath,
    "--db", dbPath,
    "--threshold-block", String(thresholdBlock),
    "--threshold-allow", String(thresholdAllow),
  ];

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
        return { passed: [], blocked: [], uncertain: [], stats: { total: 0, passed: 0, blocked: 0, uncertain: 0, rules_loaded: 0, elapsed_ms: 0 } };
      }

      const args = [...baseArgs];
      if (explain) args.push("--explain");

      const { stdout, stderr, status } = runFilter(
        args,
        JSON.stringify(emails),
        baseEnv
      );

      if (status !== 0) {
        throw new Error(`ETS filter failed (exit ${status}): ${stderr.slice(0, 500)}`);
      }

      return parseFilterOutput(stdout);
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
          description: "Rule weight 1-100. Higher = stronger signal. Weights >= 90 on allow rules act as hard overrides.",
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

      // Validate
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

      const validMatchKeys = ["sender_domain", "sender_contains", "sender_exact", "subject_regex", "body_regex"];
      for (const key of Object.keys(match)) {
        if (!validMatchKeys.includes(key)) {
          throw new Error(`Invalid match key: ${key}. Valid keys: ${validMatchKeys.join(", ")}`);
        }
      }

      // Load, check for duplicate, append, save
      const data = loadRules(rulesPath);

      if (data.rules.some((r: Rule) => r.id === id)) {
        throw new Error(`Rule with ID '${id}' already exists. Use a different ID.`);
      }

      const newRule: Rule = { id, action, weight, match, reason };
      data.rules.push(newRule);

      fs.writeFileSync(rulesPath, JSON.stringify(data, null, 2), "utf8");

      // Sync to DB
      const { status, stderr } = runFilter(
        ["--sync-rules", "--rules", rulesPath, "--db", dbPath],
        undefined,
        baseEnv
      );
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
      const data = loadRules(rulesPath);
      const rules =
        action_filter === "all"
          ? data.rules
          : data.rules.filter((r: Rule) => r.action === action_filter);
      return { rules, total: rules.length };
    },
  });

  // -------------------------------------------------------------------------
  // Tool: ets_stats
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
      const { stdout, stderr, status } = runFilter(
        ["--stats", "--rules", rulesPath, "--db", dbPath],
        undefined,
        baseEnv
      );
      if (status !== 0) {
        throw new Error(`ETS stats failed (exit ${status}): ${stderr.slice(0, 500)}`);
      }
      try {
        return JSON.parse(stdout);
      } catch (e) {
        throw new Error(`Failed to parse stats output: ${stdout.slice(0, 200)}`);
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
      "Pass the output of ets_filter as input. Returns compact structured summaries — no LLM involved.",
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
          description: "If true, include extraction_methods debug field in each email.",
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

      // Write filterOutput to a temp file
      const tmpFile = path.join(os.tmpdir(), `ets_filter_out_${Date.now()}.json`);
      try {
        fs.writeFileSync(tmpFile, JSON.stringify(filterOutput), "utf8");

        const args: string[] = [
          "--input", tmpFile,
          "--snippet-cap", String(snippetCap),
        ];
        if (explain) args.push("--explain");

        const { stdout, stderr, status } = runExtractor(args, undefined, {
          ETS_SNIPPET_CAP: String(snippetCap),
        });

        if (status !== 0) {
          throw new Error(`ETS extractor failed (exit ${status}): ${stderr.slice(0, 500)}`);
        }

        try {
          return JSON.parse(stdout) as ExtractResult;
        } catch (e) {
          throw new Error(`Failed to parse extractor output: ${(e as Error).message}\nRaw: ${stdout.slice(0, 500)}`);
        }
      } finally {
        try { fs.unlinkSync(tmpFile); } catch (_) {}
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
      const templatesPath = DEFAULT_TEMPLATES_PATH;

      if (validate) {
        // Required top-level fields
        for (const field of ["id", "name", "priority", "type", "detect", "extract"]) {
          if (!(field in template)) {
            throw new Error(`Missing required field: ${field}`);
          }
        }

        // id must be snake_case
        if (!/^[a-z0-9][a-z0-9\-_]*$/.test(template.id)) {
          throw new Error("Template id must be snake_case or kebab-case (lowercase letters, numbers, hyphens, underscores)");
        }

        // Valid types
        const validTypes = ["shipping", "order_confirm", "billing", "job", "financial_alert",
          "calendar_invite", "subscription", "travel", "school_notice", "unclassified"];
        if (!validTypes.includes(template.type)) {
          throw new Error(`Invalid type '${template.type}'. Valid types: ${validTypes.join(", ")}`);
        }

        // detect must have at least one rule
        const detectKeys = ["sender_domain", "sender_contains", "subject_regex", "snippet_regex"];
        const hasDetect = detectKeys.some(k => k in (template.detect || {}));
        if (!hasDetect) {
          throw new Error(`detect must have at least one rule: ${detectKeys.join(", ")}`);
        }

        // extract must have at least one field
        if (!template.extract || Object.keys(template.extract).length === 0) {
          throw new Error("extract must have at least one field");
        }

        // priority must be a positive integer
        if (typeof template.priority !== "number" || template.priority < 1) {
          throw new Error("priority must be a positive integer");
        }
      }

      const data = loadTemplates(templatesPath);

      // Check for duplicate id
      if (data.templates.some((t: ExtractorTemplate) => t.id === template.id)) {
        throw new Error(`Template with id '${template.id}' already exists. Use a different id.`);
      }

      data.templates.push(template);
      fs.writeFileSync(templatesPath, JSON.stringify(data, null, 2), "utf8");

      return {
        success: true,
        template_id: template.id,
        total_templates: data.templates.length,
        message: `Template '${template.id}' added successfully. ETS will use it on the next extraction run.`,
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
    handler: async (context: any, args: string) => {
      const sub = (args ?? "").trim().toLowerCase();

      if (sub === "pipeline") {
        const data = loadRules(rulesPath) as any;
        const meta = data._meta ?? {};
        const snippetCap: number = meta.snippet_cap ?? 300;
        const pipeline: string[] = meta.pipeline ?? ["filter", "extract"];
        const pipelineStr = pipeline.map((s: string, i: number) => `${i + 1}. **${s}**`).join(" → ");
        const rulesData = data.rules ?? [];
        const allowCount = rulesData.filter((r: Rule) => r.action === "allow").length;
        const blockCount = rulesData.filter((r: Rule) => r.action === "block").length;

        let templateCount = 0;
        try {
          const tmplData = loadTemplates(DEFAULT_TEMPLATES_PATH);
          templateCount = tmplData.templates.length;
        } catch (_) {}

        return [
          `**ETS Pipeline Config** (v${PLUGIN_VERSION})`,
          ``,
          `**Pipeline stages:** ${pipelineStr}`,
          `**Filter rules:** ${rulesData.length} total (${allowCount} allow, ${blockCount} block)`,
          `**Extractor templates:** ${templateCount} loaded`,
          `**Snippet cap:** ${snippetCap} chars (financial alerts: unlimited)`,
          `**Thresholds:** block ≤ ${thresholdBlock}, allow ≥ ${thresholdAllow}`,
          ``,
          `**Rules path:** \`${rulesPath}\``,
          `**Templates path:** \`${DEFAULT_TEMPLATES_PATH}\``,
          `**DB path:** \`${dbPath}\``,
          `**Filter script:** \`${FILTER_SCRIPT}\``,
          `**Extractor script:** \`${EXTRACTOR_SCRIPT}\``,
        ].join("\n");
      }

      if (sub === "version") {
        const data = loadRules(rulesPath);
        let templateCount = 0;
        try {
          const tmplData = loadTemplates(DEFAULT_TEMPLATES_PATH);
          templateCount = tmplData.templates.length;
        } catch (_) {}
        return `**ETS — Email Token Saver** v${PLUGIN_VERSION}\n` +
               `Filter rules: ${data.rules.length}\n` +
               `Extractor templates: ${templateCount}\n` +
               `Rules path: \`${rulesPath}\`\n` +
               `DB path: \`${dbPath}\``;
      }

      if (sub === "rules") {
        const data = loadRules(rulesPath);
        const allowRules = data.rules.filter((r: Rule) => r.action === "allow");
        const blockRules = data.rules.filter((r: Rule) => r.action === "block");

        const fmt = (r: Rule) =>
          `• **${r.id}** (weight: ${r.weight}) — ${r.reason}`;

        return [
          `**ETS Rules** (${data.rules.length} total)`,
          "",
          `**✅ Allow rules (${allowRules.length}):**`,
          ...allowRules.map(fmt),
          "",
          `**🚫 Block rules (${blockRules.length}):**`,
          ...blockRules.map(fmt),
        ].join("\n");
      }

      if (sub === "stats" || sub === "") {
        const { stdout, stderr, status } = runFilter(
          ["--stats", "--rules", rulesPath, "--db", dbPath],
          undefined,
          baseEnv
        );
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
          `**ETS Filter Stats**`,
          `Total runs: ${s.total_runs}`,
          `Total emails processed: ${s.total_emails}`,
          `Passed: ${s.total_passed}${pct(s.total_passed, s.total_emails)}`,
          `Blocked: ${s.total_blocked}${pct(s.total_blocked, s.total_emails)}`,
          `Uncertain: ${s.total_uncertain}${pct(s.total_uncertain, s.total_emails)}`,
          top5 ? `\n**Top 5 rules by hits:**\n${top5}` : "",
        ]
          .filter(Boolean)
          .join("\n");
      }

      return `Unknown subcommand: \`${sub}\`. Usage: \`/ets stats\` | \`/ets rules\` | \`/ets version\` | \`/ets pipeline\``;

    },
  });
}
