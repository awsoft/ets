#!/usr/bin/env python3
"""
ETS Email Extractor — Pipeline stage 2
Classifies emails by type and extracts key fields.
Zero LLM usage — pure regex + pattern matching.

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
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Pre-compiled patterns (module-level, compiled once)
# ---------------------------------------------------------------------------

# Shipping detection
_RE_SHIPPING = re.compile(
    r"(?i)(shipped|out\s+for\s+delivery|delivered|on\s+its\s+way|arrival|tracking\s+(number|#|no))",
)

# Carrier detection (domain-based + subject-based)
_RE_CARRIER_UPS = re.compile(r"(?i)(ups[\.\-_@]|ups\.com|united\s+parcel)", re.IGNORECASE)
_RE_CARRIER_FEDEX = re.compile(r"(?i)(fedex[\.\-_@]|fedex\.com|federal\s+express)", re.IGNORECASE)
_RE_CARRIER_USPS = re.compile(r"(?i)(usps[\.\-_@]|usps\.com|postal\s+service|uspsdelivery)", re.IGNORECASE)
_RE_CARRIER_AMAZON = re.compile(r"(?i)(amazon[\.\-_@]|amazon\.com|amzn\.)", re.IGNORECASE)
_RE_CARRIER_DHL = re.compile(r"(?i)(dhl[\.\-_@]|dhl\.com)", re.IGNORECASE)

# Shipping status
_RE_STATUS_DELIVERED = re.compile(r"(?i)\b(delivered|delivery\s+complete|has\s+been\s+delivered)\b")
_RE_STATUS_OFD = re.compile(r"(?i)\b(out\s+for\s+delivery|on\s+its\s+way|with\s+delivery\s+driver)\b")
_RE_STATUS_SHIPPED = re.compile(r"(?i)\b(shipped|has\s+shipped|in\s+transit|on\s+the\s+way)\b")

# ETA patterns — look near delivery/arrive/by keywords
_RE_ETA_TODAY = re.compile(r"(?i)\b(today|this\s+afternoon|this\s+evening|by\s+9|by\s+8|by\s+7|by\s+6|by\s+5)\b")
_RE_ETA_TOMORROW = re.compile(r"(?i)\btomorrow\b")
_RE_ETA_DATE = re.compile(
    r"(?i)\b(?:by|arrive[sd]?|deliver(?:ed|y)\s+(?:by|on)|estimated\s+(?:delivery|arrival)(?:\s+(?:by|on))?)\s+"
    r"((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
    r"|(?:January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}"
    r"|\d{1,2}/\d{1,2}(?:/\d{2,4})?)\b"
)

# Tracking number — common formats
_RE_TRACKING = re.compile(
    r"\b("
    r"1Z[A-Z0-9]{16}"               # UPS
    r"|[0-9]{12,22}"                 # FedEx / USPS numeric
    r"|[A-Z]{2}\d{9}[A-Z]{2}"       # USPS international
    r"|TBA\d{12}"                    # Amazon
    r")\b"
)

# Order confirm detection
_RE_ORDER_CONFIRM = re.compile(
    r"(?i)(order\s+(confirmed|received)|thank\s+you\s+for\s+(your\s+order|shopping)|order\s+#)",
)
_RE_ORDER_NUMBER = re.compile(r"(?i)(?:order\s*#?\s*|#\s*)(\w[\w\-]{2,20})")
_RE_ORDER_TOTAL = re.compile(r"\$\s*[\d,]+(?:\.\d{2})?")

# Billing detection
_RE_BILLING = re.compile(
    r"(?i)\b(bill(?:ing)?|invoice|statement|payment\s+due|amount\s+due)\b",
)
_RE_AMOUNT = re.compile(r"\$\s*[\d,]+(?:\.\d{2})?")
_RE_DUE_DATE = re.compile(
    r"(?i)(?:due|payment\s+(?:by|on)|pay\s+by)\s+"
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(?:,?\s+\d{4})?"
    r"|\d{1,2}/\d{1,2}(?:/\d{2,4})?"
    r"|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
)

# School detection
SCHOOL_DOMAINS = {"indiancreekschool.org"}
_RE_SCHOOL_SUBJECT = re.compile(
    r"(?i)\b(class|grade|school|student|parent|permission|field\s+trip|pickup|dismissal)\b",
)
_RE_FIRST_SENTENCE = re.compile(r"^([^.!?]{1,120}[.!?]?)")

# Job detection
_RE_JOB = re.compile(
    r"(?i)\b(job|position|role|offer|interview|application|recruiter|opportunity|career)\b",
)
_RE_JOB_OFFER = re.compile(r"(?i)\b(offer\s+letter|job\s+offer|we.re\s+(pleased|excited)\s+to\s+offer)\b")
_RE_JOB_INTERVIEW = re.compile(r"(?i)\b(interview|schedule\s+a\s+call|speak\s+with\s+you)\b")
_RE_JOB_UPDATE = re.compile(r"(?i)\b(application\s+(update|status|received)|we\s+received\s+your\s+application)\b")
_RE_JOB_ROLE = re.compile(
    r"(?i)(?:for\s+(?:the\s+)?(?:role|position|job)\s+of\s+|"
    r"(?:as\s+(?:a|an)\s+)|"
    r"(?:open(?:ing)?\s+for\s+)|"
    r"(?:hiring\s+(?:a|an)\s+))"
    r"([A-Z][a-zA-Z\s]{2,40}?)(?:\s+(?:at|with|@|\-)|\.|,|$)"
)
# "X opportunity/role/position" — capture up to 3 words before trigger, filter noise
_RE_JOB_ROLE_TRIGGER = re.compile(
    r"(?i)((?:\b[A-Za-z]+\b\s+){1,4}?)\b(opportunity|role|position|opening)\b"
)
_RE_JOB_TITLE_KEYWORDS = re.compile(
    r"(?i)\b(Director|Manager|Engineer|Developer|Analyst|Designer|Architect|"
    r"VP|President|Lead|Head|Chief|Officer|Coordinator|Specialist|Consultant|"
    r"Recruiter|Principal|Staff|Senior|Junior|CTO|CFO|COO|CEO|CRO|CMO)\b"
)
_ROLE_NOISE_WORDS = frozenset({
    "exciting", "great", "new", "this", "the", "a", "an", "for", "our", "your",
    "unique", "amazing", "perfect", "ideal", "potential", "interested", "open",
    "available", "hi", "hello", "dear", "about", "about", "incredible",
})

# Financial alert detection
_RE_FINANCIAL = re.compile(
    r"(?i)\b(fraud|alert|suspicious|unusual\s+activity|account\s+locked|"
    r"payment\s+failed|declined|security\s+alert|security\s+notice|"
    r"unauthorized|compromised|verify\s+your)\b",
)
_RE_ALERT_TYPE_FRAUD = re.compile(r"(?i)\b(fraud|fraudulent|suspicious\s+(?:activity|charge|transaction))\b")
_RE_ALERT_TYPE_LOCKED = re.compile(r"(?i)\b(account\s+(?:locked|suspended|disabled|compromised))\b")
_RE_ALERT_TYPE_PAYMENT = re.compile(r"(?i)\b(payment\s+(?:failed|declined)|declined\s+payment)\b")
_RE_ALERT_TYPE_UNUSUAL = re.compile(r"(?i)\b(unusual\s+activity|unusual\s+sign.in|unrecognized)\b")
_RE_ALERT_TYPE_SECURITY = re.compile(r"(?i)\b(security\s+(?:alert|notice|warning)|verify\s+your|unauthorized)\b")
_RE_ACCOUNT_HINT = re.compile(r"(?:ending\s+in|x+|ending\s*:?\s*|\*+)(\d{4})\b", re.IGNORECASE)

# Calendar invite detection
_RE_CALENDAR = re.compile(
    r"(?i)\b(invitation|invited|join\s+us|rsvp|save\s+the\s+date|calendar\s+(?:event|invite))\b",
)
_RE_ICS_HINT = re.compile(r"(?i)(\.ics|calendar\s+attachment|ical)", re.IGNORECASE)

# Date patterns for calendar/eta
_RE_DATE_GENERIC = re.compile(
    r"\b((?:January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(?:,?\s+\d{4})?"
    r"|\d{1,2}/\d{1,2}(?:/\d{2,4})?"
    r"|(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday))"
    r"(?:\s+at\s+\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))?"
)

# Location hint: simple address pattern
_RE_LOCATION = re.compile(
    r"\b(\d{1,5}\s+[A-Z][a-zA-Z\s]{3,40}(?:Street|St|Avenue|Ave|Blvd|Boulevard|Drive|Dr|Road|Rd|Lane|Ln|Way|Court|Ct|Place|Pl)\.?(?:,\s*[A-Z][a-zA-Z\s]+)?)",
    re.IGNORECASE,
)
_RE_LOCATION_VENUE = re.compile(
    r"(?:at|@|location:|venue:)\s+([A-Z][a-zA-Z\s&',\.]{3,60}?)(?:\.|,|\n|$)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _get_domain(from_addr: str) -> str:
    """Extract domain from email address."""
    m = re.search(r"@([\w.\-]+)", from_addr or "")
    return m.group(1).lower() if m else ""


def _get_from_name(email: Dict[str, Any]) -> str:
    """Get sender name, falling back to parsing from address."""
    name = (email.get("from_name") or "").strip()
    if name:
        return name
    addr = email.get("from", "")
    # Try to parse display name from "Name <addr>" format
    m = re.match(r'^"?([^"<]+)"?\s*<', addr)
    if m:
        return m.group(1).strip()
    # Fall back to local part of address
    local = addr.split("@")[0] if "@" in addr else addr
    return local.replace(".", " ").replace("_", " ").replace("-", " ").title()


def _cap_snippet(snippet: str, cap: int) -> str:
    """Cap snippet to max chars."""
    if not snippet:
        return ""
    if cap >= 9999:
        return snippet
    return snippet[:cap]


def _first_non_empty(*values: Optional[str]) -> str:
    """Return first non-empty string."""
    for v in values:
        if v:
            return v.strip()
    return ""


def _extract_amount(text: str) -> Optional[str]:
    """Find first dollar amount in text."""
    m = _RE_AMOUNT.search(text)
    return m.group(0).strip() if m else None


def _extract_date_generic(text: str) -> Optional[str]:
    """Find first generic date pattern in text."""
    m = _RE_DATE_GENERIC.search(text)
    return m.group(0).strip() if m else None


# ---------------------------------------------------------------------------
# Per-type extraction
# ---------------------------------------------------------------------------

def _extract_shipping(email: Dict[str, Any], text: str, explain: bool) -> Tuple[Dict, str]:
    sender = email.get("from", "")
    subject = email.get("subject", "")
    combined = f"{sender} {subject} {text}"

    # Carrier
    carrier = None
    carrier_method = None
    for name, pattern in [
        ("UPS", _RE_CARRIER_UPS),
        ("FedEx", _RE_CARRIER_FEDEX),
        ("USPS", _RE_CARRIER_USPS),
        ("Amazon", _RE_CARRIER_AMAZON),
        ("DHL", _RE_CARRIER_DHL),
    ]:
        if pattern.search(combined):
            carrier = name
            carrier_method = "regex"
            break

    # Status — check subject first (more reliable), then snippet
    # For snippet, exclude "will be delivered" (future tense = not yet delivered)
    _re_future_delivered = re.compile(r"(?i)\bwill\s+be\s+delivered\b")
    status = None
    status_method = None
    if _RE_STATUS_OFD.search(subject):
        status = "out_for_delivery"
        status_method = "regex_subject"
    elif _RE_STATUS_DELIVERED.search(subject):
        status = "delivered"
        status_method = "regex_subject"
    elif _RE_STATUS_SHIPPED.search(subject):
        status = "shipped"
        status_method = "regex_subject"
    elif _RE_STATUS_OFD.search(text):
        status = "out_for_delivery"
        status_method = "regex_snippet"
    elif _RE_STATUS_DELIVERED.search(text) and not _re_future_delivered.search(text):
        status = "delivered"
        status_method = "regex_snippet"
    elif _RE_STATUS_SHIPPED.search(text):
        status = "shipped"
        status_method = "regex_snippet"

    # ETA
    eta = None
    eta_method = None
    if _RE_ETA_TODAY.search(text):
        eta = "today"
        eta_method = "regex"
    elif _RE_ETA_TOMORROW.search(text):
        eta = "tomorrow"
        eta_method = "regex"
    else:
        m = _RE_ETA_DATE.search(text)
        if m:
            eta = m.group(1).strip()
            eta_method = "regex"

    # Tracking number
    tracking = None
    tracking_method = None
    m = _RE_TRACKING.search(text)
    if m:
        tracking = m.group(0)
        tracking_method = "regex"

    extracted: Dict[str, Any] = {}
    if carrier:
        extracted["carrier"] = carrier
    if status:
        extracted["status"] = status
    if eta:
        extracted["eta"] = eta
    if tracking:
        extracted["tracking_number"] = tracking

    if explain:
        methods: Dict[str, Any] = {}
        if carrier_method:
            methods["carrier"] = carrier_method
        if status_method:
            methods["status"] = status_method
        if eta_method:
            methods["eta"] = eta_method
        if tracking_method:
            methods["tracking_number"] = tracking_method
        return extracted, "shipping", methods

    return extracted, "shipping", {}


def _extract_order_confirm(email: Dict[str, Any], text: str, explain: bool) -> Tuple[Dict, str, Dict]:
    subject = email.get("subject", "")
    combined = f"{subject} {text}"

    merchant = _get_from_name(email)

    order_number = None
    m = _RE_ORDER_NUMBER.search(combined)
    if m:
        order_number = m.group(1)

    total = _extract_amount(combined)

    extracted: Dict[str, Any] = {"merchant": merchant}
    if order_number:
        extracted["order_number"] = order_number
    if total:
        extracted["total"] = total

    methods: Dict[str, Any] = {}
    if explain:
        methods["merchant"] = "from_name"
        if order_number:
            methods["order_number"] = "regex"
        if total:
            methods["total"] = "regex"

    return extracted, "order_confirm", methods


def _extract_billing(email: Dict[str, Any], text: str, explain: bool) -> Tuple[Dict, str, Dict]:
    subject = email.get("subject", "")
    combined = f"{subject} {text}"

    provider = _get_from_name(email)
    amount = _extract_amount(combined)

    due_date = None
    m = _RE_DUE_DATE.search(combined)
    if m:
        due_date = m.group(1).strip()

    extracted: Dict[str, Any] = {"provider": provider}
    if amount:
        extracted["amount"] = amount
    if due_date:
        extracted["due_date"] = due_date

    methods: Dict[str, Any] = {}
    if explain:
        methods["provider"] = "from_name"
        if amount:
            methods["amount"] = "regex"
        if due_date:
            methods["due_date"] = "regex"

    return extracted, "billing", methods


def _extract_school_notice(email: Dict[str, Any], text: str, explain: bool) -> Tuple[Dict, str, Dict]:
    sender_name = _get_from_name(email)

    first_sentence = ""
    m = _RE_FIRST_SENTENCE.match(text.strip())
    if m:
        first_sentence = m.group(1)[:120].strip()

    extracted: Dict[str, Any] = {"sender_name": sender_name}
    if first_sentence:
        extracted["first_sentence"] = first_sentence

    methods: Dict[str, Any] = {}
    if explain:
        methods["sender_name"] = "from_name"
        if first_sentence:
            methods["first_sentence"] = "regex"

    return extracted, "school_notice", methods


def _extract_job(email: Dict[str, Any], text: str, explain: bool) -> Tuple[Dict, str, Dict]:
    subject = email.get("subject", "")
    combined = f"{subject} {text}"

    company = _get_from_name(email)

    # Classify action
    action = "recruiter_outreach"  # default
    action_method = "default"
    if _RE_JOB_OFFER.search(combined):
        action = "offer"
        action_method = "regex"
    elif _RE_JOB_INTERVIEW.search(combined):
        action = "interview"
        action_method = "regex"
    elif _RE_JOB_UPDATE.search(combined):
        action = "application_update"
        action_method = "regex"

    # Extract role — try patterns in order of specificity
    role = None
    role_method = None

    # Pattern 1: "for the role of X" / "hiring a X" / "as a X"
    m = _RE_JOB_ROLE.search(combined)
    if m:
        role = m.group(1).strip()
        role_method = "regex_pattern"

    # Pattern 2: "X opportunity/role/position" — grab words before trigger, filter noise
    if not role:
        m2 = _RE_JOB_ROLE_TRIGGER.search(combined)
        if m2:
            prefix_words = [w for w in m2.group(1).split() if w.lower() not in _ROLE_NOISE_WORDS]
            # Only use if any word is a known title keyword
            if prefix_words and _RE_JOB_TITLE_KEYWORDS.search(" ".join(prefix_words)):
                role = " ".join(prefix_words).strip(" .,!?")
                role_method = "trigger_pattern"

    # Pattern 3: keyword in subject — grab 1-2 words before/after the keyword
    if not role:
        # Search subject only (more reliable than full combined)
        m3 = _RE_JOB_TITLE_KEYWORDS.search(subject)
        if m3:
            # Expand slightly: grab up to 2 words before the keyword match in the subject
            before = subject[:m3.start()].split()
            after = subject[m3.end():].split()
            title_parts = []
            # Take up to 2 non-noise words before
            for w in reversed(before[-2:]):
                if w.lower() not in _ROLE_NOISE_WORDS:
                    title_parts.insert(0, w)
            title_parts.append(m3.group(0))  # the keyword itself
            # Take 1 non-noise word after if it looks like a title continuation
            for w in after[:2]:
                if w.lower() not in _ROLE_NOISE_WORDS and re.match(r"(?i)^[A-Za-z]+$", w):
                    title_parts.append(w)
                    break
            role = " ".join(title_parts).strip(" .,!?")
            role_method = "keyword_subject"

    extracted: Dict[str, Any] = {"company": company, "action": action}
    if role:
        extracted["role"] = role

    methods: Dict[str, Any] = {}
    if explain:
        methods["company"] = "from_name"
        methods["action"] = action_method
        if role_method:
            methods["role"] = role_method

    return extracted, "job", methods


def _extract_financial_alert(email: Dict[str, Any], text: str, explain: bool) -> Tuple[Dict, str, Dict]:
    subject = email.get("subject", "")
    combined = f"{subject} {text}"

    # Alert type
    alert_type = "security_alert"  # fallback
    alert_method = "default"
    if _RE_ALERT_TYPE_FRAUD.search(combined):
        alert_type = "fraud"
        alert_method = "regex"
    elif _RE_ALERT_TYPE_LOCKED.search(combined):
        alert_type = "account_locked"
        alert_method = "regex"
    elif _RE_ALERT_TYPE_PAYMENT.search(combined):
        alert_type = "payment_failed"
        alert_method = "regex"
    elif _RE_ALERT_TYPE_UNUSUAL.search(combined):
        alert_type = "unusual_activity"
        alert_method = "regex"
    elif _RE_ALERT_TYPE_SECURITY.search(combined):
        alert_type = "security_alert"
        alert_method = "regex"

    # Account hint
    account_hint = None
    m = _RE_ACCOUNT_HINT.search(combined)
    if m:
        account_hint = m.group(1)

    # Amount
    amount = _extract_amount(combined)

    extracted: Dict[str, Any] = {"alert_type": alert_type, "urgent": True}
    if account_hint:
        extracted["account_hint"] = account_hint
    if amount:
        extracted["amount"] = amount

    methods: Dict[str, Any] = {}
    if explain:
        methods["alert_type"] = alert_method
        if account_hint:
            methods["account_hint"] = "regex"
        if amount:
            methods["amount"] = "regex"

    return extracted, "financial_alert", methods


def _extract_calendar_invite(email: Dict[str, Any], text: str, explain: bool) -> Tuple[Dict, str, Dict]:
    subject = email.get("subject", "")
    combined = f"{subject} {text}"

    # Event name: try to pull from subject (clean up common prefixes)
    event_name = re.sub(
        r"(?i)^(?:invitation|invited|join\s+us\s+(?:for|at)|rsvp\s+(?:for|to)|save\s+the\s+date[:\-\s]+)",
        "",
        subject,
    ).strip(" :–-")
    if not event_name:
        event_name = subject

    # Date hint
    date_hint = _extract_date_generic(combined)

    # Location hint: try address first, then venue keyword
    location_hint = None
    m = _RE_LOCATION.search(combined)
    if m:
        location_hint = m.group(0).strip()
    else:
        m2 = _RE_LOCATION_VENUE.search(combined)
        if m2:
            location_hint = m2.group(1).strip()

    extracted: Dict[str, Any] = {"event_name": event_name}
    if date_hint:
        extracted["date_hint"] = date_hint
    if location_hint:
        extracted["location_hint"] = location_hint

    methods: Dict[str, Any] = {}
    if explain:
        methods["event_name"] = "subject_parse"
        if date_hint:
            methods["date_hint"] = "regex"
        if location_hint:
            methods["location_hint"] = "regex"

    return extracted, "calendar_invite", methods


# ---------------------------------------------------------------------------
# Classifier — priority order
# ---------------------------------------------------------------------------

def classify_and_extract(
    email: Dict[str, Any],
    snippet_cap: int,
    explain: bool,
) -> Dict[str, Any]:
    """Classify email to ONE type and extract fields. Returns output dict."""
    subject = email.get("subject", "")
    snippet = email.get("snippet", "") or ""
    from_addr = email.get("from", "") or ""
    domain = _get_domain(from_addr)

    # Text for pattern matching (subject + snippet combined)
    text_for_match = snippet

    # Determine snippet cap for this email
    effective_cap = snippet_cap

    # --- Priority 1: financial_alert ---
    if _RE_FINANCIAL.search(subject) or _RE_FINANCIAL.search(snippet):
        effective_cap = 9999  # full snippet
        extracted, etype, methods = _extract_financial_alert(email, text_for_match, explain)

    # --- Priority 2: job ---
    elif _RE_JOB.search(subject):
        extracted, etype, methods = _extract_job(email, text_for_match, explain)

    # --- Priority 3: school_notice ---
    elif domain in SCHOOL_DOMAINS or _RE_SCHOOL_SUBJECT.search(subject):
        extracted, etype, methods = _extract_school_notice(email, text_for_match, explain)

    # --- Priority 4: shipping ---
    elif _RE_SHIPPING.search(subject) or _RE_SHIPPING.search(snippet):
        extracted, etype, methods = _extract_shipping(email, text_for_match, explain)

    # --- Priority 5: order_confirm ---
    elif _RE_ORDER_CONFIRM.search(subject):
        extracted, etype, methods = _extract_order_confirm(email, text_for_match, explain)

    # --- Priority 6: billing ---
    elif _RE_BILLING.search(subject):
        extracted, etype, methods = _extract_billing(email, text_for_match, explain)

    # --- Priority 7: calendar_invite ---
    elif _RE_CALENDAR.search(subject) or _RE_ICS_HINT.search(snippet):
        extracted, etype, methods = _extract_calendar_invite(email, text_for_match, explain)

    # --- Priority 8: unclassified ---
    else:
        extracted = {}
        etype = "unclassified"
        methods = {}

    # Build output record
    record: Dict[str, Any] = {
        "id": email.get("id", ""),
        "from": from_addr,
        "subject": subject,
        "date": email.get("date", ""),
        "type": etype,
    }

    if extracted:
        record["extracted"] = extracted

    record["snippet"] = _cap_snippet(snippet, effective_cap)
    record["source_bucket"] = email.get("_source_bucket", "unknown")

    if explain and methods:
        record["extraction_methods"] = methods

    return record


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(
    filter_output: Dict[str, Any],
    snippet_cap: int,
    explain: bool,
) -> Dict[str, Any]:
    t0 = time.perf_counter()

    passed: List[Dict] = filter_output.get("passed", []) or []
    uncertain: List[Dict] = filter_output.get("uncertain", []) or []
    blocked: List[Dict] = filter_output.get("blocked", []) or []
    upstream_stats: Dict = filter_output.get("stats", {}) or {}

    # Tag source bucket before processing
    for e in passed:
        e["_source_bucket"] = "passed"
    for e in uncertain:
        e["_source_bucket"] = "uncertain"

    emails_to_process = passed + uncertain
    results: List[Dict] = []
    extracted_structured = 0
    snippet_only = 0

    for email in emails_to_process:
        record = classify_and_extract(email, snippet_cap, explain)
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
        description="ETS Email Extractor — stage 2 pipeline (no LLM)",
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
        help="Include extraction_methods in output (debug)",
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

    result = run(filter_output, snippet_cap=args.snippet_cap, explain=args.explain)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
