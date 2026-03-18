from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from collections import defaultdict, deque
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional, Tuple

REQUEST_ID_HEADER = "X-Request-ID"
V2_API_VERSION = "v2"
LEGACY_SUNSET = "Wed, 31 Dec 2026 23:59:59 GMT"
LEGACY_DOC_LINK = '</docs/api-contract-strategy.md>; rel="deprecation"; type="text/markdown"'

_request_id_var: ContextVar[str] = ContextVar("request_id", default="")


ERROR_CODE_CATALOG: Dict[str, Dict[str, str]] = {
    "AUTH_DISABLED": {"category": "auth", "message": "Authentication endpoints are disabled"},
    "AUTH_UNAUTHORIZED": {"category": "auth", "message": "Unauthorized"},
    "AUTH_INVALID_CREDENTIALS": {"category": "auth", "message": "Invalid email or password"},
    "PERM_PROJECT_READ": {"category": "permission", "message": "Insufficient project permission"},
    "PERM_PROJECT_WRITE": {"category": "permission", "message": "Insufficient project permission"},
    "PERM_PROJECT_MANAGE": {"category": "permission", "message": "Insufficient project permission"},
    "PERM_ORG_READ": {"category": "permission", "message": "Insufficient organization permission"},
    "PERM_ORG_WRITE": {"category": "permission", "message": "Insufficient organization permission"},
    "PERM_ORG_MANAGE": {"category": "permission", "message": "Insufficient organization permission"},
    "PROJECT_NOT_FOUND": {"category": "not_found", "message": "Project not found"},
    "PROJECT_STATE_NOT_FOUND": {"category": "not_found", "message": "Project state not found"},
    "PROJECT_STATE_CONFLICT": {"category": "conflict", "message": "Version conflict"},
    "PROJECT_COMMIT_INVALID": {"category": "validation", "message": "Invalid project commit payload"},
    "PROJECT_MEMBER_INVALID": {"category": "validation", "message": "Invalid project member payload"},
    "PROJECT_CREATE_INVALID": {"category": "validation", "message": "Invalid project payload"},
    "VALIDATION_ERROR": {"category": "validation", "message": "Validation failed"},
    "RATE_LIMIT_EXCEEDED": {"category": "rate_limit", "message": "Too many requests"},
    "IDEMPOTENCY_KEY_REUSED": {"category": "conflict", "message": "Idempotency key was reused with a different payload"},
    "LEGACY_SESSION_DISABLED": {"category": "deprecated", "message": "Legacy session compatibility is disabled"},
    "LEGACY_SESSION_AUTH_REQUIRED": {"category": "auth", "message": "Legacy project-backed session access requires authentication"},
    "INTERNAL_ERROR": {"category": "system", "message": "Internal server error"},
}


def ensure_request_id(request_id: Optional[str] = None) -> str:
    value = (request_id or "").strip() or uuid.uuid4().hex
    _request_id_var.set(value)
    return value


def get_request_id() -> str:
    return _request_id_var.get("") or ensure_request_id()


def success_envelope(data: Any, *, meta: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None) -> Dict[str, Any]:
    merged_meta = {
        "api_version": V2_API_VERSION,
        **(meta or {}),
    }
    return {
        "data": data,
        "error": None,
        "meta": merged_meta,
        "request_id": request_id or get_request_id(),
    }


def error_envelope(
    code: str,
    message: str,
    *,
    status_code: int,
    category: Optional[str] = None,
    details: Optional[Any] = None,
    meta: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    merged_meta = {
        "api_version": V2_API_VERSION,
        "status_code": status_code,
        **(meta or {}),
    }
    payload: Dict[str, Any] = {
        "data": None,
        "error": {
            "code": code,
            "message": message,
            "category": category or ERROR_CODE_CATALOG.get(code, {}).get("category") or "unknown",
        },
        "meta": merged_meta,
        "request_id": request_id or get_request_id(),
    }
    if details is not None:
        payload["error"]["details"] = details
    return payload


def normalize_error_detail(detail: Any, *, status_code: int) -> Tuple[str, str, str, Any]:
    if isinstance(detail, dict):
        code = str(detail.get("code") or "INTERNAL_ERROR")
        message = str(detail.get("message") or ERROR_CODE_CATALOG.get(code, {}).get("message") or "Request failed")
        category = str(detail.get("category") or ERROR_CODE_CATALOG.get(code, {}).get("category") or "unknown")
        details = detail.get("data")
        return code, message, category, details

    if isinstance(detail, list):
        return "VALIDATION_ERROR", "Validation failed", "validation", detail

    message = str(detail or ERROR_CODE_CATALOG["INTERNAL_ERROR"]["message"])
    return "INTERNAL_ERROR", message, "system", None


def add_request_headers(headers: Dict[str, str], *, deprecated: bool = False) -> Dict[str, str]:
    headers.setdefault(REQUEST_ID_HEADER, get_request_id())
    if deprecated:
        headers.setdefault("Deprecation", "true")
        headers.setdefault("Sunset", LEGACY_SUNSET)
        headers.setdefault("Link", LEGACY_DOC_LINK)
        headers.setdefault("X-API-Deprecated", "true")
    return headers


def _stable_hash(payload: Any) -> str:
    if payload is None:
        return "null"
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass
class IdempotencyRecord:
    status_code: int
    payload_hash: str
    response_payload: Any
    created_at: float


class IdempotencyStore:
    def __init__(self, ttl_seconds: int = 3600):
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._records: Dict[str, IdempotencyRecord] = {}
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        expired = [
            key
            for key, record in self._records.items()
            if now - record.created_at > self.ttl_seconds
        ]
        for key in expired:
            self._records.pop(key, None)

    def get(self, *, scope: str, subject: str, key: str, payload: Any) -> Optional[IdempotencyRecord]:
        cache_key = f"{scope}:{subject}:{key}"
        payload_hash = _stable_hash(payload)
        now = time.time()
        with self._lock:
            self._prune(now)
            record = self._records.get(cache_key)
            if not record:
                return None
            if record.payload_hash != payload_hash:
                raise ValueError("Idempotency key was reused with a different payload")
            return record

    def store(self, *, scope: str, subject: str, key: str, payload: Any, status_code: int, response_payload: Any) -> IdempotencyRecord:
        cache_key = f"{scope}:{subject}:{key}"
        now = time.time()
        record = IdempotencyRecord(
            status_code=status_code,
            payload_hash=_stable_hash(payload),
            response_payload=response_payload,
            created_at=now,
        )
        with self._lock:
            self._prune(now)
            self._records[cache_key] = record
        return record


class RateLimiter:
    def __init__(self) -> None:
        self._events: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def hit(self, *, scope: str, subject: str, limit: int, window_seconds: int) -> Optional[int]:
        if limit <= 0 or window_seconds <= 0:
            return None
        now = time.time()
        bucket_key = f"{scope}:{subject}"
        with self._lock:
            bucket = self._events[bucket_key]
            while bucket and now - bucket[0] >= window_seconds:
                bucket.popleft()
            if len(bucket) >= limit:
                retry_after = max(1, int(window_seconds - (now - bucket[0])))
                return retry_after
            bucket.append(now)
        return None
