#!/usr/bin/env python3
"""
ETS Email Extractor — template-driven extraction engine v1.1.0
Loads extractor_templates.json and applies templates in priority order.
Add new extractors by editing extractor_templates.json or using ets_add_extractor.

Usage:
  python3 email_extractor.py [--input FILE] [--snippet-cap INT] [--explain]
  echo '<filter_json>' | python3 email_extractor.py
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Module-level compiled regex — not recompiled on every _get_domain() call (#10)
_DOMAIN_RE = re.compile(r"@([\w.\-]+)")

# ---------------------------------------------------------------------------
# Template Engine
# ---------------------------------------------------------------------------

class TemplateEngine:
    """
    Loads extractor_templates.json, pre-compiles all regexes, and applies
    templates in priority order to classify and extract email fields.
    """

    def __init__(self, templates_path: str) -> None:
        with open(templates_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        raw_templates: List[Dict] = data.get("templates", [])
        # Sort by priority descending (highest priority = checked first)
        self._templates = sorted(raw_templates, key=lambda t: -int(t.get("priority", 50)))
        self._compiled: Dict[str, Optional[re.Pattern]] = {}
        self._precompile_all()

    # ------------------------------------------------------------------
    # Regex compilation
    # ------------------------------------------------------------------

    def _precompile_all(self) -> None:
        for tmpl in self._templates:
            detect = tmpl.get("detect", {})
            for key in ("subject_regex", "snippet_regex"):
                if key in detect:
                    self._ensure_compiled(detect[key])
            for field_def in tmpl.get("extract", {}).values():
                if "regex" in field_def:
                    self._ensure_compiled(field_def["regex"])
                for pattern in field_def.get("enum_map", {}).keys():
                    self._ensure_compiled(pattern)

    def _ensure_compiled(self, pattern: str) -> None:
        if pattern not in self._compiled:
            try:
                self._compiled[pattern] = re.compile(pattern)
            except re.error:
                self._compiled[pattern] = None

    def _search(self, pattern: str, text: str) -> Optional[re.Match]:
        compiled = self._compiled.get(pattern)
        if compiled and text:
            return compiled.search(text)
        return None

    # ------------------------------------------------------------------
    # Source text helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_domain(from_addr: str) -> str:
        # Uses module-level _DOMAIN_RE — not recompiled on every call (#10)
        m = _DOMAIN_RE.search(from_addr or "")
        return m.group(1).lower() if m else ""

    @staticmethod
    def _get_from_name(email: Dict[str, Any]) -> str:
        name = (email.get("from_name") or "").strip()
        if name:
            return name
        addr = email.get("from", "")
        m = re.match(r'^"?([^"<]+)"?\s*<', addr)
        if m:
            return m.group(1).strip()
        local = addr.split("@")[0] if "@" in addr else addr
        return local.replace(".", " ").replace("_", " ").replace("-", " ").title()

    def _get_source_text(self, source: str, email: Dict[str, Any]) -> str:
        if source == "subject":
            return email.get("subject", "") or ""
        if source == "snippet":
            return email.get("snippet", "") or ""
        if source == "sender":
            return email.get("from", "") or ""
        if source == "from_name":
            return self._get_from_name(email)
        return ""

    # ------------------------------------------------------------------
    # Detect matching
    # ------------------------------------------------------------------

    def _matches_detect(
        self, template: Dict[str, Any], email: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        detect = template.get("detect", {})
        if not detect:
            return True, []

        from_addr = email.get("from", "") or ""
        domain = self._get_domain(from_addr)
        subject = email.get("subject", "") or ""
        snippet = email.get("snippet", "") or ""

        any_mode = detect.get("any", False)
        matched: List[str] = []
        results: List[bool] = []

        if "sender_domain" in detect:
            ok = domain == detect["sender_domain"]
            results.append(ok)
            if ok:
                matched.append(f"sender_domain={detect['sender_domain']}")

        if "sender_contains" in detect:
            ok = detect["sender_contains"].lower() in from_addr.lower()
            results.append(ok)
            if ok:
                matched.append(f"sender_contains={detect['sender_contains']}")

        if "subject_regex" in detect:
            ok = bool(self._search(detect["subject_regex"], subject))
            results.append(ok)
            if ok:
                matched.append("subject_regex")

        if "snippet_regex" in detect:
            ok = bool(self._search(detect["snippet_regex"], snippet))
            results.append(ok)
            if ok:
                matched.append("snippet_regex")

        if not results:
            return True, []

        passed = any(results) if any_mode else all(results)
        return passed, matched

    # ------------------------------------------------------------------
    # Extract field application
    # ------------------------------------------------------------------

    def _apply_field(
        self, field_def: Dict[str, Any], email: Dict[str, Any]
    ) -> Optional[Any]:
        # Static value — always wins, no source needed
        if "static" in field_def:
            return field_def["static"]

        sources = field_def.get("source", "snippet")
        if isinstance(sources, str):
            sources = [sources]

        max_chars: Optional[int] = field_def.get("max_chars")
        has_regex = "regex" in field_def
        has_enum = "enum_map" in field_def

        for src in sources:
            text = self._get_source_text(src, email)
            if not text:
                continue

            # Regex: return first capture group (or full match)
            if has_regex:
                m = self._search(field_def["regex"], text)
                if m:
                    try:
                        val = m.group(1).strip()
                    except IndexError:
                        val = m.group(0).strip()
                    if max_chars:
                        val = val[:max_chars]
                    return val

            # Enum map: first matching pattern wins
            if has_enum:
                for pattern, value in field_def["enum_map"].items():
                    if self._search(pattern, text):
                        return value

            # Pure truncation (no regex, no enum_map) — return clipped source
            if not has_regex and not has_enum and max_chars:
                return text[:max_chars].strip()

        return None

    # ------------------------------------------------------------------
    # Classify + extract one email
    # ------------------------------------------------------------------

    def classify_and_extract(
        self,
        email: Dict[str, Any],
        snippet_cap: int,
        explain: bool,
    ) -> Dict[str, Any]:
        subject = email.get("subject", "") or ""
        snippet = email.get("snippet", "") or ""
        from_addr = email.get("from", "") or ""

        matched_template_id: Optional[str] = None
        matched_rules: List[str] = []
        extracted: Dict[str, Any] = {}
        etype = "unclassified"
        effective_cap = snippet_cap

        for template in self._templates:
            ok, rules = self._matches_detect(template, email)
            if ok:
                matched_template_id = template["id"]
                matched_rules = rules
                etype = template.get("type", "unclassified")

                # Financial alerts always get full snippet
                if etype == "financial_alert":
                    effective_cap = 9999

                for field_name, field_def in template.get("extract", {}).items():
                    val = self._apply_field(field_def, email)
                    if val is not None:
                        extracted[field_name] = val

                break  # first match wins

        record: Dict[str, Any] = {
            "id": email.get("id", ""),
            "from": from_addr,
            "subject": subject,
            "date": email.get("date", ""),
            "type": etype,
        }

        if extracted:
            record["extracted"] = extracted

        capped = snippet[:effective_cap] if snippet and effective_cap < 9999 else snippet
        record["snippet"] = capped
        record["source_bucket"] = email.get("_source_bucket", "unknown")
        record["matched_template"] = matched_template_id

        if explain:
            record["_matched_template"] = matched_template_id
            record["_detect_rules_matched"] = matched_rules

        return record


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def _load_engine(templates_path: Optional[str] = None) -> TemplateEngine:
    # Use pathlib.Path consistently throughout (#11)
    if not templates_path:
        templates_path = os.environ.get("ETS_TEMPLATES_PATH", "")
    path = Path(templates_path) if templates_path else (
        Path(__file__).parent.resolve() / "extractor_templates.json"
    )
    if not path.exists():
        print(
            json.dumps({"error": f"Templates file not found: {path}"}),
            file=sys.stderr,
        )
        sys.exit(1)
    return TemplateEngine(str(path))


def run(
    filter_output: Dict[str, Any],
    snippet_cap: int,
    explain: bool,
    engine: Optional[TemplateEngine] = None,
) -> Dict[str, Any]:
    t0 = time.perf_counter()

    if engine is None:
        engine = _load_engine()

    passed: List[Dict] = filter_output.get("passed", []) or []
    uncertain: List[Dict] = filter_output.get("uncertain", []) or []
    blocked: List[Dict] = filter_output.get("blocked", []) or []

    for e in passed:
        e["_source_bucket"] = "passed"
    for e in uncertain:
        e["_source_bucket"] = "uncertain"

    emails_to_process = passed + uncertain
    results: List[Dict] = []
    extracted_structured = 0
    snippet_only = 0

    for email in emails_to_process:
        record = engine.classify_and_extract(email, snippet_cap, explain)
        results.append(record)
        if record.get("type") != "unclassified" and record.get("extracted"):
            extracted_structured += 1
        else:
            snippet_only += 1

    elapsed_ms = round((time.perf_counter() - t0) * 1000)

    stats = {
        "total_in": len(emails_to_process),
        "blocked_dropped": len(blocked),
        "extracted_structured": extracted_structured,
        "snippet_only": snippet_only,
        "snippet_cap": snippet_cap,
        "elapsed_ms": elapsed_ms,
    }

    return {"emails": results, "stats": stats}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETS Email Extractor — template-driven pipeline stage 2 (no LLM)",
    )
    parser.add_argument(
        "--input",
        metavar="FILE",
        help="JSON file (email_filter.py output) or omit for stdin",
    )
    parser.add_argument(
        "--snippet-cap",
        metavar="INT",
        type=int,
        default=int(os.environ.get("ETS_SNIPPET_CAP", "300")),
        help="Max chars for snippet (default: 300, env: ETS_SNIPPET_CAP)",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Include _matched_template and _detect_rules_matched in output (debug)",
    )
    parser.add_argument(
        "--templates",
        metavar="FILE",
        default=os.environ.get("ETS_TEMPLATES_PATH", ""),
        help="Path to extractor_templates.json (env: ETS_TEMPLATES_PATH)",
    )
    args = parser.parse_args()

    # Read input
    if args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            raw = f.read()
    else:
        raw = sys.stdin.read()

    try:
        filter_output = json.loads(raw)
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON input: {e}"}), file=sys.stderr)
        sys.exit(1)

    engine = _load_engine(args.templates or None)
    result = run(filter_output, snippet_cap=args.snippet_cap, explain=args.explain, engine=engine)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
