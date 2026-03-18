from __future__ import annotations

import ast
import datetime
import math
import re
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Sequence, Tuple
from urllib.parse import urlparse

DEFAULT_LOCAL_CORS_ORIGINS: Tuple[str, ...] = (
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:1420",
    "http://localhost:1420",
    "http://127.0.0.1:4173",
    "http://localhost:4173",
)

GENERIC_UPLOAD_CONTENT_TYPES = {
    "",
    "application/octet-stream",
    "binary/octet-stream",
}

ALLOWED_UPLOAD_CONTENT_TYPES: Dict[str, Tuple[str, ...]] = {
    ".csv": (
        "text/csv",
        "application/csv",
        "text/plain",
        "application/vnd.ms-excel",
    ),
    ".xlsx": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/zip",
    ),
    ".xls": (
        "application/vnd.ms-excel",
        "application/octet-stream",
    ),
    ".parquet": (
        "application/vnd.apache.parquet",
        "application/x-parquet",
        "application/octet-stream",
    ),
    ".pq": (
        "application/vnd.apache.parquet",
        "application/x-parquet",
        "application/octet-stream",
    ),
}

BLOCKED_DOUBLE_EXTENSIONS = {
    ".app",
    ".bat",
    ".cmd",
    ".com",
    ".cpl",
    ".dll",
    ".exe",
    ".hta",
    ".js",
    ".jse",
    ".lnk",
    ".msi",
    ".ps1",
    ".scr",
    ".sh",
    ".vbe",
    ".vbs",
}

READ_ONLY_SQL_START_KEYWORDS = {"select", "with"}
BLOCKED_SQL_KEYWORDS = (
    "alter",
    "attach",
    "call",
    "copy",
    "create",
    "delete",
    "detach",
    "drop",
    "export",
    "import",
    "insert",
    "install",
    "load",
    "merge",
    "pragma",
    "replace",
    "set",
    "truncate",
    "update",
    "use",
    "vacuum",
)

SAFE_TRANSFORM_BUILTINS: Dict[str, Any] = {
    "Exception": Exception,
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "isinstance": isinstance,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "range": range,
    "round": round,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
}

SAFE_TRANSFORM_GLOBALS: Dict[str, Any] = {
    "__builtins__": SAFE_TRANSFORM_BUILTINS,
    "datetime": datetime,
    "math": math,
    "re": re,
}

_DANGEROUS_NAME_RE = re.compile(r"^__.*__$")
_BLOCKED_TRANSFORM_NAMES = {
    "__builtins__",
    "__import__",
    "compile",
    "delattr",
    "dir",
    "eval",
    "exec",
    "getattr",
    "globals",
    "help",
    "input",
    "locals",
    "memoryview",
    "object",
    "open",
    "setattr",
    "super",
    "type",
    "vars",
}
_BLOCKED_TRANSFORM_ATTRIBUTES = {
    "__class__",
    "__closure__",
    "__code__",
    "__dict__",
    "__func__",
    "__globals__",
    "__mro__",
    "__self__",
    "__subclasses__",
}


def normalize_server(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized.lower() in {"mock", "mockserver"}:
        return "mockServer"
    if not normalized:
        return "mockServer"
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "mockServer"
    return normalized.rstrip("/")


def parse_cors_origins(raw: Any, default: Optional[Sequence[str]] = None) -> Tuple[str, ...]:
    if isinstance(raw, str):
        values = [item.strip() for item in raw.split(",") if item.strip()]
    elif raw is None:
        values = []
    else:
        values = [str(item).strip() for item in raw if str(item).strip()]

    if not values:
        values = list(default or DEFAULT_LOCAL_CORS_ORIGINS)

    seen = set()
    normalized: list[str] = []
    for origin in values:
        parsed = urlparse(origin)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        candidate = f"{parsed.scheme}://{parsed.netloc}"
        if candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return tuple(normalized)


def validate_upload_metadata(
    *,
    filename: str,
    content_type: Optional[str],
    content: bytes,
    max_bytes: int,
    allowed_extensions: Iterable[str],
) -> str:
    normalized_name = (filename or "").strip()
    if not normalized_name:
        raise ValueError("Uploaded file must include a filename")

    suffixes = [suffix.lower() for suffix in Path(normalized_name).suffixes]
    if not suffixes:
        raise ValueError("Unsupported file format. Please upload CSV, Excel, or Parquet.")
    extension = suffixes[-1]
    allowed_set = {str(item).lower() for item in allowed_extensions}
    if extension not in allowed_set:
        raise ValueError("Unsupported file format. Please upload CSV, Excel, or Parquet.")
    if any(suffix in BLOCKED_DOUBLE_EXTENSIONS for suffix in suffixes[:-1]):
        raise ValueError("Dangerous file extension detected")
    if len(content) > max_bytes:
        raise ValueError(f"File is too large. Max size is {max_bytes} bytes.")

    normalized_type = (content_type or "").strip().lower()
    allowed_content_types = ALLOWED_UPLOAD_CONTENT_TYPES.get(extension, ())
    if normalized_type and normalized_type not in GENERIC_UPLOAD_CONTENT_TYPES and normalized_type not in allowed_content_types:
        raise ValueError("File content type does not match the file extension")
    if extension == ".csv" and b"\x00" in content[:65536]:
        raise ValueError("CSV contains binary content")
    return extension


def _strip_sql_comments(query: str) -> str:
    result: list[str] = []
    i = 0
    in_single = False
    in_double = False
    while i < len(query):
        ch = query[i]
        nxt = query[i + 1] if i + 1 < len(query) else ""
        if not in_double and ch == "'" and (i == 0 or query[i - 1] != "\\"):
            in_single = not in_single
            result.append(ch)
            i += 1
            continue
        if not in_single and ch == '"' and (i == 0 or query[i - 1] != "\\"):
            in_double = not in_double
            result.append(ch)
            i += 1
            continue
        if not in_single and not in_double and ch == "-" and nxt == "-":
            i += 2
            while i < len(query) and query[i] != "\n":
                i += 1
            continue
        if not in_single and not in_double and ch == "/" and nxt == "*":
            i += 2
            while i + 1 < len(query) and not (query[i] == "*" and query[i + 1] == "/"):
                i += 1
            i += 2
            continue
        result.append(ch)
        i += 1
    return "".join(result)


def _remove_sql_string_literals(query: str) -> str:
    result: list[str] = []
    i = 0
    in_single = False
    in_double = False
    while i < len(query):
        ch = query[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            result.append(" ")
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            result.append(" ")
            i += 1
            continue
        result.append(" " if in_single or in_double else ch)
        i += 1
    return "".join(result)


def sanitize_read_only_sql(query: str) -> str:
    clean_query = (query or "").strip()
    if not clean_query:
        raise ValueError("Query is required")

    without_comments = _strip_sql_comments(clean_query).strip()
    while without_comments.endswith(";"):
        without_comments = without_comments[:-1].rstrip()
    if not without_comments:
        raise ValueError("Query is required")

    without_literals = _remove_sql_string_literals(without_comments)
    if ";" in without_literals:
        raise ValueError("Only a single read-only query is allowed")

    first_word_match = re.match(r"^\s*([a-zA-Z_]+)", without_literals)
    first_word = (first_word_match.group(1).lower() if first_word_match else "")
    if first_word not in READ_ONLY_SQL_START_KEYWORDS:
        raise ValueError("Only read-only SELECT queries are allowed")

    lowered = without_literals.lower()
    for keyword in BLOCKED_SQL_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", lowered):
            raise ValueError("Only read-only SELECT queries are allowed")
    return without_comments


class RestrictedTransformValidator(ast.NodeVisitor):
    def visit_Import(self, node: ast.Import) -> None:
        raise ValueError("Import statements are not allowed in Python transforms")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        raise ValueError("Import statements are not allowed in Python transforms")

    def visit_Global(self, node: ast.Global) -> None:
        raise ValueError("Global statements are not allowed in Python transforms")

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        raise ValueError("Nonlocal statements are not allowed in Python transforms")

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        raise ValueError("Class definitions are not allowed in Python transforms")

    def visit_Lambda(self, node: ast.Lambda) -> None:
        raise ValueError("Lambda expressions are not allowed in Python transforms")

    def visit_With(self, node: ast.With) -> None:
        raise ValueError("Context managers are not allowed in Python transforms")

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        raise ValueError("Async context managers are not allowed in Python transforms")

    def visit_Try(self, node: ast.Try) -> None:
        raise ValueError("Try/except blocks are not allowed in Python transforms")

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in _BLOCKED_TRANSFORM_NAMES or _DANGEROUS_NAME_RE.match(node.id):
            raise ValueError(f"Blocked name in Python transform: {node.id}")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in _BLOCKED_TRANSFORM_ATTRIBUTES or node.attr.startswith("__"):
            raise ValueError(f"Blocked attribute in Python transform: {node.attr}")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name):
            if node.func.id in _BLOCKED_TRANSFORM_NAMES:
                raise ValueError(f"Blocked function in Python transform: {node.func.id}")
        elif isinstance(node.func, ast.Attribute):
            if node.func.attr in _BLOCKED_TRANSFORM_ATTRIBUTES or node.func.attr.startswith("__"):
                raise ValueError(f"Blocked function in Python transform: {node.func.attr}")
        self.generic_visit(node)


def compile_python_transform(expression: str, *, allow_unsafe: bool = False) -> Callable[[Dict[str, Any]], Any]:
    source = (expression or "").strip()
    if not source:
        raise ValueError("Python transform expression is required")

    if allow_unsafe:
        compiled = compile(source, "<transform>", "exec")
        scope: Dict[str, Any] = {}
        exec(compiled, {"__builtins__": SAFE_TRANSFORM_BUILTINS, **SAFE_TRANSFORM_GLOBALS}, scope)
        return _select_transform_callable(scope)

    tree = ast.parse(source, mode="exec")
    RestrictedTransformValidator().visit(tree)
    compiled = compile(tree, "<transform>", "exec")
    scope: Dict[str, Any] = {}
    exec(compiled, dict(SAFE_TRANSFORM_GLOBALS), scope)
    return _select_transform_callable(scope)


def _select_transform_callable(scope: Dict[str, Any]) -> Callable[[Dict[str, Any]], Any]:
    if "transform" in scope and callable(scope["transform"]):
        return scope["transform"]
    for value in scope.values():
        if callable(value):
            return value
    raise ValueError("Python transform must define a callable")
