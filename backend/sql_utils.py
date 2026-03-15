import re
from typing import Optional

RESERVED_WORDS = {
    "select", "from", "where", "order", "group", "by", "join", "left", "right",
    "inner", "outer", "full", "on", "limit", "offset", "union", "distinct",
    "having", "as", "and", "or", "not", "null", "is", "like", "in", "table", "view"
}

IDENT_SAFE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

IDENT_PART_RE = r'(?:\"(?:[^\"]|\"\")+\")|(?:`[^`]+`)|(?:\[[^\]]+\])|(?:[A-Za-z_][A-Za-z0-9_-]*)'
IDENT_RE = rf'{IDENT_PART_RE}(?:\s*\.\s*{IDENT_PART_RE})*'
ALIAS_RE = rf'(?:{IDENT_PART_RE})'

SIMPLE_SELECT_RE = re.compile(
    rf"^\s*SELECT\s+\*\s+FROM\s+({IDENT_RE})(?:\s+(?:AS\s+)?{ALIAS_RE})?\s*$",
    re.IGNORECASE,
)
SIMPLE_SELECT_WHERE_RE = re.compile(
    rf"^\s*SELECT\s+\*\s+FROM\s+({IDENT_RE})(?:\s+(?:AS\s+)?{ALIAS_RE})?\s+WHERE\s+(.+)\s*$",
    re.IGNORECASE,
)
WHERE_EXTRACT_RE = re.compile(r"^\s*SELECT\s+\*\s+FROM\s+.+?\s+WHERE\s+(.+)\s*$", re.IGNORECASE)


def _escape_ident(name: str) -> str:
    return name.replace('"', '""')


def is_quoted_identifier(name: str) -> bool:
    if not name:
        return False
    return (
        (name.startswith('"') and name.endswith('"')) or
        (name.startswith('`') and name.endswith('`')) or
        (name.startswith('[') and name.endswith(']'))
    )


def unquote_identifier(name: str) -> str:
    if not name:
        return name
    trimmed = name.strip()
    if trimmed.startswith('"') and trimmed.endswith('"'):
        inner = trimmed[1:-1]
        return inner.replace('""', '"')
    if trimmed.startswith('`') and trimmed.endswith('`'):
        return trimmed[1:-1]
    if trimmed.startswith('[') and trimmed.endswith(']'):
        return trimmed[1:-1]
    return trimmed


def needs_quoting(name: str) -> bool:
    if not name:
        return True
    if is_quoted_identifier(name):
        return False
    if not IDENT_SAFE_RE.match(name):
        return True
    return name.lower() in RESERVED_WORDS


def is_reserved_identifier(name: str) -> bool:
    if not name:
        return False
    raw = unquote_identifier(name).strip().lower()
    return raw in RESERVED_WORDS


def quote_identifier(name: str) -> str:
    if not name:
        return name
    trimmed = name.strip()
    if trimmed == "*":
        return trimmed
    if is_quoted_identifier(trimmed):
        return trimmed
    if "." in trimmed:
        parts = [p.strip() for p in trimmed.split(".")]
        return ".".join(_quote_simple(p) for p in parts)
    return _quote_simple(trimmed)


def _quote_simple(name: str) -> str:
    if not name:
        return name
    if is_quoted_identifier(name):
        return name
    if needs_quoting(name):
        return f"\"{_escape_ident(name)}\""
    return name


def quote_table_ref(table_ref: Optional[str]) -> Optional[str]:
    if not table_ref:
        return table_ref
    trimmed = table_ref.strip()
    if trimmed.startswith("(") or re.search(r"\s", trimmed):
        return table_ref
    return quote_identifier(trimmed)
