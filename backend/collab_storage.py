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

ACCESS_TOKEN_TTL_SECONDS = 60 * 60
REFRESH_TOKEN_TTL_SECONDS = 30 * 24 * 60 * 60
STATE_SNAPSHOT_INTERVAL = 50
VALID_PROJECT_ROLES = ("owner", "admin", "editor", "viewer")
WRITE_PROJECT_ROLES = ("owner", "admin", "editor")
MANAGE_PROJECT_MEMBER_ROLES = ("owner", "admin")
VALID_ORG_ROLES = ("owner", "admin", "member")
MANAGE_ORG_MEMBER_ROLES = ("owner", "admin")


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


def _validate_password_policy(password: str) -> None:
    raw = password or ""
    if len(raw) < 8:
        raise ValueError("password must be at least 8 characters")
    if not any(ch.islower() for ch in raw):
        raise ValueError("password must include a lowercase letter")
    if not any(ch.isupper() for ch in raw):
        raise ValueError("password must include an uppercase letter")
    if not any(ch.isdigit() for ch in raw):
        raise ValueError("password must include a digit")


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

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> List[str]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [r[1] for r in rows]

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        cols = self._table_columns(conn, table)
        if column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
                    token_type TEXT NOT NULL DEFAULT 'access',
                    parent_token TEXT,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_auth_tokens_user_id ON auth_tokens(user_id);

                CREATE TABLE IF NOT EXISTS organizations (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    owner_user_id TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    deleted_at INTEGER,
                    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS organization_members (
                    organization_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    invited_by TEXT NOT NULL,
                    joined_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    deleted_at INTEGER,
                    PRIMARY KEY(organization_id, user_id),
                    FOREIGN KEY(organization_id) REFERENCES organizations(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_org_members_user_id ON organization_members(user_id);

                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    org_id TEXT,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    created_by TEXT NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0,
                    deleted_at INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    FOREIGN KEY(created_by) REFERENCES users(id) ON DELETE RESTRICT,
                    FOREIGN KEY(org_id) REFERENCES organizations(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS project_members (
                    project_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    added_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    deleted_at INTEGER,
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

            # Backward-compatible migrations for existing local DB files.
            self._ensure_column(conn, "auth_tokens", "token_type", "TEXT NOT NULL DEFAULT 'access'")
            self._ensure_column(conn, "auth_tokens", "parent_token", "TEXT")
            self._ensure_column(conn, "projects", "org_id", "TEXT")
            self._ensure_column(conn, "projects", "deleted_at", "INTEGER")
            self._ensure_column(conn, "project_members", "deleted_at", "INTEGER")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_tokens_type ON auth_tokens(token_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_projects_org_id ON projects(org_id)")

            self._backfill_org_relationships(conn)

    def _row_to_user(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "email": row["email"],
            "displayName": row["display_name"] or "",
            "createdAt": row["created_at"],
        }

    def _row_to_organization(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "ownerUserId": row["owner_user_id"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "role": row["role"],
        }

    def _row_to_project(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "id": row["id"],
            "orgId": row["org_id"],
            "name": row["name"],
            "description": row["description"] or "",
            "createdBy": row["created_by"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "archived": bool(row["archived"]),
            "role": row["role"],
        }

    def _get_default_org_for_user(self, conn: sqlite3.Connection, user_id: str) -> Optional[str]:
        row = conn.execute(
            """
            SELECT o.id
            FROM organizations o
            JOIN organization_members om ON o.id = om.organization_id
            WHERE om.user_id = ?
              AND om.deleted_at IS NULL
              AND o.deleted_at IS NULL
            ORDER BY CASE WHEN om.role = 'owner' THEN 0 WHEN om.role = 'admin' THEN 1 ELSE 2 END,
                     om.joined_at ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        return row["id"] if row else None

    def _ensure_personal_org(self, conn: sqlite3.Connection, user_row: sqlite3.Row) -> str:
        user_id = user_row["id"]
        existing_org = conn.execute(
            """
            SELECT o.id
            FROM organizations o
            JOIN organization_members om ON o.id = om.organization_id
            WHERE om.user_id = ?
              AND om.role = 'owner'
              AND om.deleted_at IS NULL
              AND o.deleted_at IS NULL
            ORDER BY om.joined_at ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        if existing_org:
            return existing_org["id"]

        now = _now_ms()
        display = (user_row["display_name"] or "").strip()
        email = user_row["email"]
        org_name = f"{display or email} Personal Space"
        org_id = f"org_{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO organizations (id, name, owner_user_id, created_at, updated_at, deleted_at)
            VALUES (?, ?, ?, ?, ?, NULL)
            """,
            (org_id, org_name[:120], user_id, now, now),
        )
        conn.execute(
            """
            INSERT INTO organization_members (organization_id, user_id, role, invited_by, joined_at, updated_at, deleted_at)
            VALUES (?, ?, 'owner', ?, ?, ?, NULL)
            """,
            (org_id, user_id, user_id, now, now),
        )
        return org_id

    def _ensure_user_bootstrap(self, conn: sqlite3.Connection, user_id: str) -> str:
        user_row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user_row:
            raise ValueError("user not found")
        return self._ensure_personal_org(conn, user_row)

    def _backfill_org_relationships(self, conn: sqlite3.Connection) -> None:
        users = conn.execute("SELECT * FROM users").fetchall()
        for user in users:
            self._ensure_personal_org(conn, user)

        projects = conn.execute("SELECT id, created_by, org_id FROM projects").fetchall()
        for project in projects:
            if project["org_id"]:
                continue
            owner = conn.execute("SELECT * FROM users WHERE id = ?", (project["created_by"],)).fetchone()
            if not owner:
                continue
            org_id = self._ensure_personal_org(conn, owner)
            conn.execute("UPDATE projects SET org_id = ? WHERE id = ?", (org_id, project["id"]))

    def clear(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                DELETE FROM auth_tokens;
                DELETE FROM project_events;
                DELETE FROM project_states;
                DELETE FROM project_members;
                DELETE FROM projects;
                DELETE FROM organization_members;
                DELETE FROM organizations;
                DELETE FROM users;
                """
            )

    # --- Auth ---

    def register_user(self, email: str, password: str, display_name: str = "") -> Dict[str, Any]:
        normalized = _normalize_email(email)
        if not normalized:
            raise ValueError("email is required")
        _validate_password_policy(password)

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
            user_row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            self._ensure_personal_org(conn, user_row)
            return self._row_to_user(user_row)

    def authenticate_user(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        normalized = _normalize_email(email)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE email = ?", (normalized,)).fetchone()
            if not row:
                return None
            if not _verify_password(password, row["password_hash"]):
                return None
            self._ensure_personal_org(conn, row)
            return self._row_to_user(row)

    def _issue_token(
        self,
        conn: sqlite3.Connection,
        user_id: str,
        token_type: str,
        ttl_seconds: int,
        parent_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _now_ms()
        expires_at = now + (ttl_seconds * 1000)
        token = secrets.token_urlsafe(48)
        conn.execute(
            """
            INSERT INTO auth_tokens (token, user_id, token_type, parent_token, created_at, expires_at, revoked)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (token, user_id, token_type, parent_token, now, expires_at),
        )
        return {"token": token, "expiresAt": expires_at}

    def issue_auth_tokens(self, user_id: str) -> Dict[str, Any]:
        with self._connect() as conn:
            access = self._issue_token(conn, user_id, "access", ACCESS_TOKEN_TTL_SECONDS)
            refresh = self._issue_token(conn, user_id, "refresh", REFRESH_TOKEN_TTL_SECONDS)
            return {
                "accessToken": access["token"],
                "refreshToken": refresh["token"],
                "tokenType": "Bearer",
                "expiresAt": access["expiresAt"],
                "refreshExpiresAt": refresh["expiresAt"],
            }

    def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        token = (refresh_token or "").strip()
        if not token:
            raise ValueError("refreshToken is required")

        now = _now_ms()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM auth_tokens
                WHERE token = ? AND token_type = 'refresh' AND revoked = 0 AND expires_at > ?
                """,
                (token, now),
            ).fetchone()
            if not row:
                raise ValueError("invalid refresh token")

            access = self._issue_token(conn, row["user_id"], "access", ACCESS_TOKEN_TTL_SECONDS, parent_token=token)
            return {
                "accessToken": access["token"],
                "refreshToken": token,
                "tokenType": "Bearer",
                "expiresAt": access["expiresAt"],
                "refreshExpiresAt": row["expires_at"],
            }

    def revoke_token(self, token: str) -> None:
        raw = (token or "").strip()
        if not raw:
            return
        with self._connect() as conn:
            conn.execute("UPDATE auth_tokens SET revoked = 1 WHERE token = ?", (raw,))

    def get_user_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        raw = (token or "").strip()
        if not raw:
            return None
        now = _now_ms()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT u.*
                FROM auth_tokens t
                JOIN users u ON u.id = t.user_id
                WHERE t.token = ?
                  AND t.token_type = 'access'
                  AND t.revoked = 0
                  AND t.expires_at > ?
                """,
                (raw, now),
            ).fetchone()
            if not row:
                return None
            return self._row_to_user(row)

    # --- Organization ---

    def _get_org_role(self, conn: sqlite3.Connection, organization_id: str, user_id: str) -> Optional[str]:
        row = conn.execute(
            """
            SELECT role
            FROM organization_members
            WHERE organization_id = ? AND user_id = ? AND deleted_at IS NULL
            """,
            (organization_id, user_id),
        ).fetchone()
        return row["role"] if row else None

    def create_organization(self, user_id: str, name: str) -> Dict[str, Any]:
        clean_name = (name or "").strip()
        if not clean_name:
            raise ValueError("name is required")
        if len(clean_name) > 120:
            raise ValueError("name is too long")

        now = _now_ms()
        org_id = f"org_{uuid.uuid4().hex[:12]}"
        with self._connect() as conn:
            self._ensure_user_bootstrap(conn, user_id)
            conn.execute(
                """
                INSERT INTO organizations (id, name, owner_user_id, created_at, updated_at, deleted_at)
                VALUES (?, ?, ?, ?, ?, NULL)
                """,
                (org_id, clean_name, user_id, now, now),
            )
            conn.execute(
                """
                INSERT INTO organization_members (organization_id, user_id, role, invited_by, joined_at, updated_at, deleted_at)
                VALUES (?, ?, 'owner', ?, ?, ?, NULL)
                """,
                (org_id, user_id, user_id, now, now),
            )
            row = conn.execute(
                """
                SELECT o.*, om.role
                FROM organizations o
                JOIN organization_members om ON o.id = om.organization_id
                WHERE o.id = ? AND om.user_id = ?
                """,
                (org_id, user_id),
            ).fetchone()
            return self._row_to_organization(row)

    def list_organizations_for_user(self, user_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            self._ensure_user_bootstrap(conn, user_id)
            rows = conn.execute(
                """
                SELECT o.*, om.role
                FROM organizations o
                JOIN organization_members om ON o.id = om.organization_id
                WHERE om.user_id = ?
                  AND om.deleted_at IS NULL
                  AND o.deleted_at IS NULL
                ORDER BY o.updated_at DESC, o.created_at DESC
                """,
                (user_id,),
            ).fetchall()
            return [self._row_to_organization(r) for r in rows]

    def get_organization_for_user(self, organization_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT o.*, om.role
                FROM organizations o
                JOIN organization_members om ON o.id = om.organization_id
                WHERE o.id = ?
                  AND om.user_id = ?
                  AND o.deleted_at IS NULL
                  AND om.deleted_at IS NULL
                """,
                (organization_id, user_id),
            ).fetchone()
            if not row:
                return None
            return self._row_to_organization(row)

    def add_organization_member(self, organization_id: str, actor_user_id: str, member_email: str, role: str) -> Dict[str, Any]:
        normalized_role = (role or "").strip().lower()
        if normalized_role not in VALID_ORG_ROLES:
            raise ValueError("invalid role")
        target_email = _normalize_email(member_email)
        if not target_email:
            raise ValueError("memberEmail is required")

        now = _now_ms()
        with self._connect() as conn:
            actor_role = self._get_org_role(conn, organization_id, actor_user_id)
            if actor_role not in MANAGE_ORG_MEMBER_ROLES:
                raise PermissionError("permission denied")

            target = conn.execute("SELECT * FROM users WHERE email = ?", (target_email,)).fetchone()
            if not target:
                raise ValueError("target user not found")

            conn.execute(
                """
                INSERT INTO organization_members (organization_id, user_id, role, invited_by, joined_at, updated_at, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(organization_id, user_id)
                DO UPDATE SET
                    role = excluded.role,
                    invited_by = excluded.invited_by,
                    updated_at = excluded.updated_at,
                    deleted_at = NULL
                """,
                (organization_id, target["id"], normalized_role, actor_user_id, now, now),
            )
            row = conn.execute(
                """
                SELECT u.id, u.email, u.display_name, om.role, om.joined_at, om.updated_at
                FROM organization_members om
                JOIN users u ON u.id = om.user_id
                WHERE om.organization_id = ? AND om.user_id = ?
                """,
                (organization_id, target["id"]),
            ).fetchone()
            return {
                "userId": row["id"],
                "email": row["email"],
                "displayName": row["display_name"] or "",
                "role": row["role"],
                "joinedAt": row["joined_at"],
                "updatedAt": row["updated_at"],
            }

    def update_organization_member_role(
        self,
        organization_id: str,
        actor_user_id: str,
        member_user_id: str,
        role: str,
    ) -> Dict[str, Any]:
        normalized_role = (role or "").strip().lower()
        if normalized_role not in VALID_ORG_ROLES:
            raise ValueError("invalid role")

        now = _now_ms()
        with self._connect() as conn:
            actor_role = self._get_org_role(conn, organization_id, actor_user_id)
            if actor_role not in MANAGE_ORG_MEMBER_ROLES:
                raise PermissionError("permission denied")

            existing = conn.execute(
                """
                SELECT 1 FROM organization_members
                WHERE organization_id = ? AND user_id = ? AND deleted_at IS NULL
                """,
                (organization_id, member_user_id),
            ).fetchone()
            if not existing:
                raise ValueError("member not found")

            conn.execute(
                """
                UPDATE organization_members
                SET role = ?, invited_by = ?, updated_at = ?
                WHERE organization_id = ? AND user_id = ?
                """,
                (normalized_role, actor_user_id, now, organization_id, member_user_id),
            )
            row = conn.execute(
                """
                SELECT u.id, u.email, u.display_name, om.role, om.joined_at, om.updated_at
                FROM organization_members om
                JOIN users u ON u.id = om.user_id
                WHERE om.organization_id = ? AND om.user_id = ?
                """,
                (organization_id, member_user_id),
            ).fetchone()
            return {
                "userId": row["id"],
                "email": row["email"],
                "displayName": row["display_name"] or "",
                "role": row["role"],
                "joinedAt": row["joined_at"],
                "updatedAt": row["updated_at"],
            }

    def list_organization_members(self, organization_id: str, user_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            role = self._get_org_role(conn, organization_id, user_id)
            if not role:
                raise PermissionError("permission denied")
            rows = conn.execute(
                """
                SELECT u.id, u.email, u.display_name, om.role, om.joined_at, om.updated_at
                FROM organization_members om
                JOIN users u ON u.id = om.user_id
                WHERE om.organization_id = ? AND om.deleted_at IS NULL
                ORDER BY om.joined_at ASC
                """,
                (organization_id,),
            ).fetchall()
            return [
                {
                    "userId": r["id"],
                    "email": r["email"],
                    "displayName": r["display_name"] or "",
                    "role": r["role"],
                    "joinedAt": r["joined_at"],
                    "updatedAt": r["updated_at"],
                }
                for r in rows
            ]

    # --- Project ---

    def _get_project_role(self, conn: sqlite3.Connection, project_id: str, user_id: str) -> Optional[str]:
        row = conn.execute(
            """
            SELECT pm.role
            FROM project_members pm
            JOIN projects p ON p.id = pm.project_id
            WHERE pm.project_id = ?
              AND pm.user_id = ?
              AND pm.deleted_at IS NULL
              AND p.deleted_at IS NULL
            """,
            (project_id, user_id),
        ).fetchone()
        return row["role"] if row else None

    def create_project(self, user_id: str, name: str, description: str = "", org_id: Optional[str] = None) -> Dict[str, Any]:
        clean_name = (name or "").strip()
        if not clean_name:
            raise ValueError("name is required")
        if len(clean_name) > 120:
            raise ValueError("name is too long")

        now = _now_ms()
        project_id = f"prj_{uuid.uuid4().hex[:12]}"
        with self._connect() as conn:
            self._ensure_user_bootstrap(conn, user_id)
            target_org_id = org_id or self._get_default_org_for_user(conn, user_id)
            if not target_org_id:
                raise ValueError("organization is required")

            org_role = self._get_org_role(conn, target_org_id, user_id)
            if org_role not in VALID_ORG_ROLES:
                raise PermissionError("permission denied")

            conn.execute(
                """
                INSERT INTO projects (id, org_id, name, description, created_by, archived, deleted_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?)
                """,
                (project_id, target_org_id, clean_name, (description or "").strip(), user_id, now, now),
            )
            conn.execute(
                """
                INSERT INTO project_members (project_id, user_id, role, added_by, created_at, updated_at, deleted_at)
                VALUES (?, ?, 'owner', ?, ?, ?, NULL)
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

    def search_projects_for_user(
        self,
        user_id: str,
        page: int = 1,
        page_size: int = 20,
        search: str = "",
        include_archived: bool = False,
    ) -> Dict[str, Any]:
        safe_page = max(int(page), 1)
        safe_size = max(1, min(int(page_size), 100))
        offset = (safe_page - 1) * safe_size
        search_term = (search or "").strip()

        filters = [
            "pm.user_id = ?",
            "pm.deleted_at IS NULL",
            "p.deleted_at IS NULL",
            "o.deleted_at IS NULL",
        ]
        params: List[Any] = [user_id]

        if not include_archived:
            filters.append("p.archived = 0")
        if search_term:
            filters.append("LOWER(p.name) LIKE ?")
            params.append(f"%{search_term.lower()}%")

        where_clause = " AND ".join(filters)

        with self._connect() as conn:
            self._ensure_user_bootstrap(conn, user_id)
            total = conn.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM projects p
                JOIN project_members pm ON p.id = pm.project_id
                JOIN organizations o ON o.id = p.org_id
                WHERE {where_clause}
                """,
                params,
            ).fetchone()["cnt"]

            rows = conn.execute(
                f"""
                SELECT p.*, pm.role
                FROM projects p
                JOIN project_members pm ON p.id = pm.project_id
                JOIN organizations o ON o.id = p.org_id
                WHERE {where_clause}
                ORDER BY p.updated_at DESC, p.created_at DESC
                LIMIT ? OFFSET ?
                """,
                params + [safe_size, offset],
            ).fetchall()

            items = [self._row_to_project(r) for r in rows]
            return {
                "items": items,
                "page": safe_page,
                "pageSize": safe_size,
                "total": int(total),
                "hasMore": (offset + len(items)) < int(total),
                "search": search_term,
            }

    def list_projects_for_user(self, user_id: str) -> List[Dict[str, Any]]:
        return self.search_projects_for_user(user_id=user_id, page=1, page_size=100, search="", include_archived=False)["items"]

    def get_project_for_user(self, project_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT p.*, pm.role
                FROM projects p
                JOIN project_members pm ON p.id = pm.project_id
                JOIN organizations o ON o.id = p.org_id
                WHERE p.id = ?
                  AND pm.user_id = ?
                  AND pm.deleted_at IS NULL
                  AND p.deleted_at IS NULL
                  AND o.deleted_at IS NULL
                """,
                (project_id, user_id),
            ).fetchone()
            if not row:
                return None
            return self._row_to_project(row)

    def archive_project(self, project_id: str, actor_user_id: str, archived: bool) -> Dict[str, Any]:
        now = _now_ms()
        with self._connect() as conn:
            role = self._get_project_role(conn, project_id, actor_user_id)
            if role not in MANAGE_PROJECT_MEMBER_ROLES:
                raise PermissionError("permission denied")
            conn.execute(
                "UPDATE projects SET archived = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
                (1 if archived else 0, now, project_id),
            )
        project = self.get_project_for_user(project_id, actor_user_id)
        if not project:
            raise ValueError("project not found")
        return project

    def soft_delete_project(self, project_id: str, actor_user_id: str) -> None:
        now = _now_ms()
        with self._connect() as conn:
            role = self._get_project_role(conn, project_id, actor_user_id)
            if role not in MANAGE_PROJECT_MEMBER_ROLES:
                raise PermissionError("permission denied")
            conn.execute(
                "UPDATE projects SET deleted_at = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
                (now, now, project_id),
            )

    def add_project_member(self, project_id: str, actor_user_id: str, member_email: str, role: str) -> Dict[str, Any]:
        normalized_role = (role or "").strip().lower()
        if normalized_role not in VALID_PROJECT_ROLES:
            raise ValueError("invalid role")
        target_email = _normalize_email(member_email)
        if not target_email:
            raise ValueError("memberEmail is required")

        now = _now_ms()
        with self._connect() as conn:
            actor_role = self._get_project_role(conn, project_id, actor_user_id)
            if actor_role not in MANAGE_PROJECT_MEMBER_ROLES:
                raise PermissionError("permission denied")

            project = conn.execute("SELECT * FROM projects WHERE id = ? AND deleted_at IS NULL", (project_id,)).fetchone()
            if not project:
                raise ValueError("project not found")

            target = conn.execute("SELECT * FROM users WHERE email = ?", (target_email,)).fetchone()
            if not target:
                raise ValueError("target user not found")

            # Ensure target is an org member before adding to project.
            org_member = conn.execute(
                """
                SELECT 1 FROM organization_members
                WHERE organization_id = ? AND user_id = ? AND deleted_at IS NULL
                """,
                (project["org_id"], target["id"]),
            ).fetchone()
            if not org_member:
                conn.execute(
                    """
                    INSERT INTO organization_members (organization_id, user_id, role, invited_by, joined_at, updated_at, deleted_at)
                    VALUES (?, ?, 'member', ?, ?, ?, NULL)
                    ON CONFLICT(organization_id, user_id)
                    DO UPDATE SET deleted_at = NULL, updated_at = excluded.updated_at
                    """,
                    (project["org_id"], target["id"], actor_user_id, now, now),
                )

            conn.execute(
                """
                INSERT INTO project_members (project_id, user_id, role, added_by, created_at, updated_at, deleted_at)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(project_id, user_id)
                DO UPDATE SET
                    role = excluded.role,
                    added_by = excluded.added_by,
                    updated_at = excluded.updated_at,
                    deleted_at = NULL
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
                WHERE pm.project_id = ? AND pm.deleted_at IS NULL
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
        if normalized_role not in VALID_PROJECT_ROLES:
            raise ValueError("invalid role")
        now = _now_ms()
        with self._connect() as conn:
            actor_role = self._get_project_role(conn, project_id, actor_user_id)
            if actor_role not in MANAGE_PROJECT_MEMBER_ROLES:
                raise PermissionError("permission denied")

            existing = conn.execute(
                """
                SELECT 1 FROM project_members
                WHERE project_id = ? AND user_id = ? AND deleted_at IS NULL
                """,
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

    # --- Project state / events ---

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

    def get_project_state_since(self, project_id: str, user_id: str, since_version: int = 0) -> Dict[str, Any]:
        state = self.get_project_state(project_id, user_id)
        current_version = int(state["version"])
        base_version = max(int(since_version or 0), 0)
        if base_version >= current_version:
            return {
                "projectId": project_id,
                "sinceVersion": base_version,
                "version": current_version,
                "changed": False,
                "events": [],
                "state": None,
            }
        events = self.list_project_events(project_id, user_id, from_version=base_version, limit=500)
        return {
            "projectId": project_id,
            "sinceVersion": base_version,
            "version": current_version,
            "changed": True,
            "events": events["events"],
            "state": state["state"],
            "updatedBy": state.get("updatedBy"),
            "updatedAt": state.get("updatedAt"),
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
                if role not in WRITE_PROJECT_ROLES:
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
                if next_version % STATE_SNAPSHOT_INTERVAL == 0:
                    snapshot_payload = json.dumps(
                        {
                            "version": next_version,
                            "state": next_state,
                        },
                        ensure_ascii=False,
                    )
                    conn.execute(
                        """
                        INSERT INTO project_events (project_id, version, client_op_id, event_type, payload_json, created_by, created_at)
                        VALUES (?, ?, ?, 'state_snapshot', ?, ?, ?)
                        """,
                        (project_id, next_version, None, snapshot_payload, user_id, now),
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
