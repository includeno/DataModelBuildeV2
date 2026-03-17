import hashlib
import json
import os
import secrets
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional


BACKEND_ENV = (os.environ.get("BACKEND_ENV") or "production").strip().lower()
IS_TEST_ENV = BACKEND_ENV == "test" or bool(os.environ.get("PYTEST_CURRENT_TEST"))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_ROOT = os.path.join(REPO_ROOT, "data")
DEFAULT_DB = "collab_test.sqlite3" if IS_TEST_ENV else "collab.sqlite3"
COLLAB_DB_PATH = os.environ.get("COLLAB_DB_PATH", os.path.join(DATA_ROOT, DEFAULT_DB))

TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60
VALID_ROLES = ("owner", "admin", "editor", "viewer")
WRITE_ROLES = ("owner", "admin", "editor")
MANAGE_MEMBER_ROLES = ("owner", "admin")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _password_hash(password: str, salt_hex: Optional[str] = None) -> str:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"pbkdf2_sha256${salt.hex()}${derived.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        algo, salt_hex, expected = stored.split("$", 2)
        if algo != "pbkdf2_sha256":
            return False
        actual = _password_hash(password, salt_hex).split("$", 2)[2]
        return secrets.compare_digest(actual, expected)
    except Exception:
        return False


class CollabStorage:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or COLLAB_DB_PATH
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS auth_tokens (
                    token TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_auth_tokens_user_id ON auth_tokens(user_id);

                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    created_by TEXT NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY(created_by) REFERENCES users(id) ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS project_members (
                    project_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    added_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY(project_id, user_id),
                    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_project_members_user_id ON project_members(user_id);

                CREATE TABLE IF NOT EXISTS project_states (
                    project_id TEXT PRIMARY KEY,
                    version INTEGER NOT NULL DEFAULT 0,
                    state_json TEXT NOT NULL DEFAULT '{}',
                    updated_by TEXT,
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS project_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    client_op_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    UNIQUE(project_id, client_op_id),
                    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
                    FOREIGN KEY(created_by) REFERENCES users(id) ON DELETE RESTRICT
                );
                CREATE INDEX IF NOT EXISTS idx_project_events_project_version
                ON project_events(project_id, version);
                """
            )

    def clear(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                DELETE FROM auth_tokens;
                DELETE FROM project_events;
                DELETE FROM project_states;
                DELETE FROM project_members;
                DELETE FROM projects;
                DELETE FROM users;
                """
            )

    def _row_to_user(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "email": row["email"],
            "displayName": row["display_name"] or "",
            "createdAt": row["created_at"],
        }

    def _row_to_project(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"] or "",
            "createdBy": row["created_by"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "archived": bool(row["archived"]),
            "role": row["role"],
        }

    def register_user(self, email: str, password: str, display_name: str = "") -> Dict[str, Any]:
        normalized = _normalize_email(email)
        if not normalized:
            raise ValueError("email is required")
        if len(password or "") < 8:
            raise ValueError("password must be at least 8 characters")
        now = _now_ms()
        user_id = f"usr_{uuid.uuid4().hex[:12]}"

        with self._connect() as conn:
            exists = conn.execute("SELECT 1 FROM users WHERE email = ?", (normalized,)).fetchone()
            if exists:
                raise ValueError("email already exists")
            conn.execute(
                """
                INSERT INTO users (id, email, password_hash, display_name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, normalized, _password_hash(password), (display_name or "").strip(), now, now),
            )
            row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            return self._row_to_user(row)

    def authenticate_user(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        normalized = _normalize_email(email)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE email = ?", (normalized,)).fetchone()
            if not row:
                return None
            if not _verify_password(password, row["password_hash"]):
                return None
            return self._row_to_user(row)

    def issue_token(self, user_id: str, ttl_seconds: int = TOKEN_TTL_SECONDS) -> Dict[str, Any]:
        now = _now_ms()
        expires_at = now + (ttl_seconds * 1000)
        token = secrets.token_urlsafe(32)
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO auth_tokens (token, user_id, created_at, expires_at, revoked) VALUES (?, ?, ?, ?, 0)",
                (token, user_id, now, expires_at),
            )
        return {"accessToken": token, "tokenType": "Bearer", "expiresAt": expires_at}

    def revoke_token(self, token: str) -> None:
        if not token:
            return
        with self._connect() as conn:
            conn.execute("UPDATE auth_tokens SET revoked = 1 WHERE token = ?", (token,))

    def get_user_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        if not token:
            return None
        now = _now_ms()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT u.*
                FROM auth_tokens t
                JOIN users u ON u.id = t.user_id
                WHERE t.token = ? AND t.revoked = 0 AND t.expires_at > ?
                """,
                (token, now),
            ).fetchone()
            if not row:
                return None
            return self._row_to_user(row)

    def create_project(self, user_id: str, name: str, description: str = "") -> Dict[str, Any]:
        clean_name = (name or "").strip()
        if not clean_name:
            raise ValueError("name is required")
        if len(clean_name) > 120:
            raise ValueError("name is too long")
        now = _now_ms()
        project_id = f"prj_{uuid.uuid4().hex[:12]}"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO projects (id, name, description, created_by, archived, created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, ?, ?)
                """,
                (project_id, clean_name, (description or "").strip(), user_id, now, now),
            )
            conn.execute(
                """
                INSERT INTO project_members (project_id, user_id, role, added_by, created_at, updated_at)
                VALUES (?, ?, 'owner', ?, ?, ?)
                """,
                (project_id, user_id, user_id, now, now),
            )
            conn.execute(
                """
                INSERT INTO project_states (project_id, version, state_json, updated_by, updated_at)
                VALUES (?, 0, '{}', ?, ?)
                """,
                (project_id, user_id, now),
            )
            row = conn.execute(
                """
                SELECT p.*, pm.role
                FROM projects p
                JOIN project_members pm ON p.id = pm.project_id
                WHERE p.id = ? AND pm.user_id = ?
                """,
                (project_id, user_id),
            ).fetchone()
            return self._row_to_project(row)

    def list_projects_for_user(self, user_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT p.*, pm.role
                FROM projects p
                JOIN project_members pm ON p.id = pm.project_id
                WHERE pm.user_id = ? AND p.archived = 0
                ORDER BY p.updated_at DESC, p.created_at DESC
                """,
                (user_id,),
            ).fetchall()
            return [self._row_to_project(r) for r in rows]

    def get_project_for_user(self, project_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT p.*, pm.role
                FROM projects p
                JOIN project_members pm ON p.id = pm.project_id
                WHERE p.id = ? AND pm.user_id = ?
                """,
                (project_id, user_id),
            ).fetchone()
            if not row:
                return None
            return self._row_to_project(row)

    def _get_project_role(self, conn: sqlite3.Connection, project_id: str, user_id: str) -> Optional[str]:
        row = conn.execute(
            "SELECT role FROM project_members WHERE project_id = ? AND user_id = ?",
            (project_id, user_id),
        ).fetchone()
        return row["role"] if row else None

    def add_project_member(self, project_id: str, actor_user_id: str, member_email: str, role: str) -> Dict[str, Any]:
        normalized_role = (role or "").strip().lower()
        if normalized_role not in VALID_ROLES:
            raise ValueError("invalid role")
        target_email = _normalize_email(member_email)
        if not target_email:
            raise ValueError("memberEmail is required")

        now = _now_ms()
        with self._connect() as conn:
            actor_role = self._get_project_role(conn, project_id, actor_user_id)
            if actor_role not in MANAGE_MEMBER_ROLES:
                raise PermissionError("permission denied")

            target = conn.execute("SELECT * FROM users WHERE email = ?", (target_email,)).fetchone()
            if not target:
                raise ValueError("target user not found")

            conn.execute(
                """
                INSERT INTO project_members (project_id, user_id, role, added_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id, user_id)
                DO UPDATE SET role = excluded.role, updated_at = excluded.updated_at, added_by = excluded.added_by
                """,
                (project_id, target["id"], normalized_role, actor_user_id, now, now),
            )

            row = conn.execute(
                """
                SELECT u.id, u.email, u.display_name, pm.role, pm.created_at, pm.updated_at
                FROM project_members pm
                JOIN users u ON u.id = pm.user_id
                WHERE pm.project_id = ? AND pm.user_id = ?
                """,
                (project_id, target["id"]),
            ).fetchone()
            return {
                "userId": row["id"],
                "email": row["email"],
                "displayName": row["display_name"] or "",
                "role": row["role"],
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
            }

    def list_project_members(self, project_id: str, user_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            role = self._get_project_role(conn, project_id, user_id)
            if not role:
                raise PermissionError("permission denied")
            rows = conn.execute(
                """
                SELECT u.id, u.email, u.display_name, pm.role, pm.created_at, pm.updated_at
                FROM project_members pm
                JOIN users u ON u.id = pm.user_id
                WHERE pm.project_id = ?
                ORDER BY pm.created_at ASC
                """,
                (project_id,),
            ).fetchall()
            return [
                {
                    "userId": r["id"],
                    "email": r["email"],
                    "displayName": r["display_name"] or "",
                    "role": r["role"],
                    "createdAt": r["created_at"],
                    "updatedAt": r["updated_at"],
                }
                for r in rows
            ]

    def update_project_member_role(
        self,
        project_id: str,
        actor_user_id: str,
        member_user_id: str,
        role: str,
    ) -> Dict[str, Any]:
        normalized_role = (role or "").strip().lower()
        if normalized_role not in VALID_ROLES:
            raise ValueError("invalid role")
        now = _now_ms()
        with self._connect() as conn:
            actor_role = self._get_project_role(conn, project_id, actor_user_id)
            if actor_role not in MANAGE_MEMBER_ROLES:
                raise PermissionError("permission denied")

            existing = conn.execute(
                "SELECT 1 FROM project_members WHERE project_id = ? AND user_id = ?",
                (project_id, member_user_id),
            ).fetchone()
            if not existing:
                raise ValueError("member not found")

            conn.execute(
                """
                UPDATE project_members
                SET role = ?, updated_at = ?, added_by = ?
                WHERE project_id = ? AND user_id = ?
                """,
                (normalized_role, now, actor_user_id, project_id, member_user_id),
            )
            row = conn.execute(
                """
                SELECT u.id, u.email, u.display_name, pm.role, pm.created_at, pm.updated_at
                FROM project_members pm
                JOIN users u ON u.id = pm.user_id
                WHERE pm.project_id = ? AND pm.user_id = ?
                """,
                (project_id, member_user_id),
            ).fetchone()
            return {
                "userId": row["id"],
                "email": row["email"],
                "displayName": row["display_name"] or "",
                "role": row["role"],
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
            }

    def get_project_state(self, project_id: str, user_id: str) -> Dict[str, Any]:
        with self._connect() as conn:
            role = self._get_project_role(conn, project_id, user_id)
            if not role:
                raise PermissionError("permission denied")
            row = conn.execute(
                "SELECT version, state_json, updated_by, updated_at FROM project_states WHERE project_id = ?",
                (project_id,),
            ).fetchone()
            if not row:
                raise ValueError("project state not found")
            state = json.loads(row["state_json"] or "{}")
            return {
                "projectId": project_id,
                "version": int(row["version"]),
                "state": state,
                "updatedBy": row["updated_by"],
                "updatedAt": row["updated_at"],
            }

    def _apply_patches(self, base_state: Dict[str, Any], patches: List[Dict[str, Any]]) -> Dict[str, Any]:
        state = json.loads(json.dumps(base_state))
        for patch in patches:
            op = str(patch.get("op") or "").strip().lower()
            if op == "replace_state":
                payload = patch.get("state")
                if not isinstance(payload, dict):
                    raise ValueError("replace_state requires object state")
                state = payload
            elif op == "set_top_level":
                key = str(patch.get("key") or "").strip()
                if not key:
                    raise ValueError("set_top_level requires key")
                state[key] = patch.get("value")
            else:
                raise ValueError(f"unsupported patch op: {op}")
        return state

    def commit_project_state(
        self,
        project_id: str,
        user_id: str,
        base_version: int,
        state: Optional[Dict[str, Any]],
        client_op_id: Optional[str] = None,
        patches: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if base_version is None:
            raise ValueError("baseVersion is required")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                role = self._get_project_role(conn, project_id, user_id)
                if role not in WRITE_ROLES:
                    raise PermissionError("permission denied")

                current = conn.execute(
                    "SELECT version, state_json FROM project_states WHERE project_id = ?",
                    (project_id,),
                ).fetchone()
                if not current:
                    raise ValueError("project state not found")

                if client_op_id:
                    existing = conn.execute(
                        "SELECT version FROM project_events WHERE project_id = ? AND client_op_id = ?",
                        (project_id, client_op_id),
                    ).fetchone()
                    if existing:
                        latest = conn.execute(
                            "SELECT version, state_json, updated_by, updated_at FROM project_states WHERE project_id = ?",
                            (project_id,),
                        ).fetchone()
                        conn.execute("COMMIT")
                        return {
                            "projectId": project_id,
                            "version": int(latest["version"]),
                            "state": json.loads(latest["state_json"] or "{}"),
                            "updatedBy": latest["updated_by"],
                            "updatedAt": latest["updated_at"],
                            "conflict": False,
                            "idempotent": True,
                        }

                current_version = int(current["version"])
                current_state = json.loads(current["state_json"] or "{}")

                if int(base_version) != current_version:
                    conn.execute("ROLLBACK")
                    return {
                        "projectId": project_id,
                        "conflict": True,
                        "latestVersion": current_version,
                        "state": current_state,
                    }

                if state is not None:
                    if not isinstance(state, dict):
                        raise ValueError("state must be an object")
                    next_state = state
                elif patches:
                    next_state = self._apply_patches(current_state, patches)
                else:
                    next_state = current_state

                next_version = current_version + 1
                now = _now_ms()
                payload_json = json.dumps(
                    {
                        "baseVersion": current_version,
                        "nextVersion": next_version,
                        "patches": patches or [],
                    },
                    ensure_ascii=False,
                )
                conn.execute(
                    """
                    UPDATE project_states
                    SET version = ?, state_json = ?, updated_by = ?, updated_at = ?
                    WHERE project_id = ?
                    """,
                    (next_version, json.dumps(next_state, ensure_ascii=False), user_id, now, project_id),
                )
                conn.execute(
                    """
                    INSERT INTO project_events (project_id, version, client_op_id, event_type, payload_json, created_by, created_at)
                    VALUES (?, ?, ?, 'state_commit', ?, ?, ?)
                    """,
                    (project_id, next_version, client_op_id, payload_json, user_id, now),
                )
                conn.execute(
                    "UPDATE projects SET updated_at = ? WHERE id = ?",
                    (now, project_id),
                )
                conn.execute("COMMIT")
                return {
                    "projectId": project_id,
                    "version": next_version,
                    "state": next_state,
                    "updatedBy": user_id,
                    "updatedAt": now,
                    "conflict": False,
                    "idempotent": False,
                }
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def list_project_events(self, project_id: str, user_id: str, from_version: int = 0, limit: int = 100) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit), 500))
        with self._connect() as conn:
            role = self._get_project_role(conn, project_id, user_id)
            if not role:
                raise PermissionError("permission denied")
            rows = conn.execute(
                """
                SELECT version, client_op_id, event_type, payload_json, created_by, created_at
                FROM project_events
                WHERE project_id = ? AND version > ?
                ORDER BY version ASC
                LIMIT ?
                """,
                (project_id, int(from_version), safe_limit),
            ).fetchall()
            state = conn.execute(
                "SELECT version FROM project_states WHERE project_id = ?",
                (project_id,),
            ).fetchone()
            events: List[Dict[str, Any]] = []
            for r in rows:
                payload = {}
                try:
                    payload = json.loads(r["payload_json"] or "{}")
                except Exception:
                    payload = {}
                events.append(
                    {
                        "version": int(r["version"]),
                        "clientOpId": r["client_op_id"],
                        "eventType": r["event_type"],
                        "payload": payload,
                        "createdBy": r["created_by"],
                        "createdAt": r["created_at"],
                    }
                )
            return {
                "projectId": project_id,
                "fromVersion": int(from_version),
                "latestVersion": int(state["version"]) if state else 0,
                "events": events,
            }


collab_storage = CollabStorage()
