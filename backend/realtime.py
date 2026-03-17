import asyncio
import contextlib
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from fastapi import WebSocket


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class RealtimeConnectionContext:
    websocket: WebSocket
    user: Dict[str, Any]
    project_id: str
    role: str
    connection_id: str = field(default_factory=lambda: f"ws_{uuid.uuid4().hex[:12]}")
    subscribed: bool = False
    session_id: Optional[str] = None
    last_seen_version: int = 0
    editing_node_id: Optional[str] = None
    last_pong_at: int = field(default_factory=_now_ms)
    heartbeat_task: Optional[asyncio.Task] = None


class ProjectRealtimeHub:
    def __init__(self, heartbeat_interval_ms: int = 15_000, heartbeat_timeout_ms: int = 45_000):
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.heartbeat_timeout_ms = heartbeat_timeout_ms
        self._connections: Dict[str, Dict[str, RealtimeConnectionContext]] = {}
        self._lock = threading.RLock()

    async def accept_connection(
        self,
        websocket: WebSocket,
        project_id: str,
        user: Dict[str, Any],
        role: str,
    ) -> RealtimeConnectionContext:
        await websocket.accept()
        ctx = RealtimeConnectionContext(websocket=websocket, user=user, project_id=project_id, role=role)
        with self._lock:
            self._connections.setdefault(project_id, {})[ctx.connection_id] = ctx
        ctx.heartbeat_task = asyncio.create_task(self._heartbeat_loop(ctx))
        return ctx

    async def subscribe(
        self,
        ctx: RealtimeConnectionContext,
        latest_version: int,
        replay_events: List[Dict[str, Any]],
    ) -> None:
        with self._lock:
            was_subscribed = ctx.subscribed
            ctx.subscribed = True
            presence = self._presence_for_project_locked(ctx.project_id)
            peers = [
                item
                for item in self._connections.get(ctx.project_id, {}).values()
                if item.subscribed and item.connection_id != ctx.connection_id
            ]

        await self._send_json(
            ctx,
            self._build_event(
                project_id=ctx.project_id,
                event_type="subscribed",
                version=latest_version,
                payload={
                    "connectionId": ctx.connection_id,
                    "projectId": ctx.project_id,
                    "latestVersion": latest_version,
                    "presence": presence,
                },
            ),
        )

        replayed_version = max(int(ctx.last_seen_version), 0)
        for event in replay_events:
            outbound = self._history_event_to_realtime(ctx.project_id, event)
            if not outbound:
                continue
            replayed_version = max(replayed_version, int(outbound.get("version") or 0))
            await self._send_json(ctx, outbound)

        ctx.last_seen_version = max(replayed_version, int(latest_version or 0))

        member = self._presence_entry(ctx)
        if not was_subscribed:
            await self._broadcast(
                peers,
                self._build_event(
                    project_id=ctx.project_id,
                    event_type="presence_join",
                    version=latest_version,
                    payload={"member": member},
                ),
            )

    async def update_presence(
        self,
        ctx: RealtimeConnectionContext,
        *,
        editing_node_id: Optional[str] = None,
        last_seen_version: Optional[int] = None,
    ) -> None:
        if editing_node_id is not None:
            ctx.editing_node_id = str(editing_node_id or "").strip() or None
        if last_seen_version is not None:
            ctx.last_seen_version = max(ctx.last_seen_version, int(last_seen_version))

        with self._lock:
            peers = [
                item
                for item in self._connections.get(ctx.project_id, {}).values()
                if item.subscribed and item.connection_id != ctx.connection_id
            ]

        await self._broadcast(
            peers,
            self._build_event(
                project_id=ctx.project_id,
                event_type="presence_update",
                version=ctx.last_seen_version,
                payload={"member": self._presence_entry(ctx)},
            ),
        )

    def mark_pong(self, ctx: RealtimeConnectionContext, last_seen_version: Optional[int] = None) -> None:
        ctx.last_pong_at = _now_ms()
        if last_seen_version is not None:
            ctx.last_seen_version = max(ctx.last_seen_version, int(last_seen_version))

    def acknowledge_version(self, ctx: RealtimeConnectionContext, version: int) -> None:
        ctx.last_seen_version = max(ctx.last_seen_version, int(version or 0))

    async def send_conflict_notice(
        self,
        ctx: RealtimeConnectionContext,
        latest_version: int,
        message: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        extra = dict(payload or {})
        extra.setdefault("latestVersion", int(latest_version or 0))
        extra.setdefault("message", message)
        await self._send_json(
            ctx,
            self._build_event(
                project_id=ctx.project_id,
                event_type="conflict_notice",
                version=int(latest_version or 0),
                payload=extra,
            ),
        )

    async def broadcast_state_committed(
        self,
        project_id: str,
        *,
        version: int,
        payload: Dict[str, Any],
    ) -> None:
        with self._lock:
            targets = [
                item for item in self._connections.get(project_id, {}).values() if item.subscribed
            ]

        await self._broadcast(
            targets,
            self._build_event(
                project_id=project_id,
                event_type="state_committed",
                version=version,
                payload=payload,
            ),
        )

    async def disconnect(self, ctx: RealtimeConnectionContext) -> None:
        peers: List[RealtimeConnectionContext] = []
        subscribed = False
        with self._lock:
            project_connections = self._connections.get(ctx.project_id, {})
            existing = project_connections.pop(ctx.connection_id, None)
            if not project_connections and ctx.project_id in self._connections:
                self._connections.pop(ctx.project_id, None)
            if existing and existing.subscribed:
                subscribed = True
                peers = [item for item in project_connections.values() if item.subscribed]

        if ctx.heartbeat_task:
            current_task = asyncio.current_task()
            ctx.heartbeat_task.cancel()
            if ctx.heartbeat_task is not current_task:
                with contextlib.suppress(Exception):
                    await ctx.heartbeat_task

        if subscribed:
            await self._broadcast(
                peers,
                self._build_event(
                    project_id=ctx.project_id,
                    event_type="presence_leave",
                    version=ctx.last_seen_version,
                    payload={
                        "connectionId": ctx.connection_id,
                        "userId": ctx.user.get("id"),
                    },
                ),
            )

    def reset(self) -> None:
        with self._lock:
            connections = [
                ctx
                for project_connections in self._connections.values()
                for ctx in project_connections.values()
            ]
            self._connections = {}
        for ctx in connections:
            if ctx.heartbeat_task:
                ctx.heartbeat_task.cancel()

    def _presence_for_project_locked(self, project_id: str) -> List[Dict[str, Any]]:
        return [
            self._presence_entry(item)
            for item in self._connections.get(project_id, {}).values()
            if item.subscribed
        ]

    def _presence_entry(self, ctx: RealtimeConnectionContext) -> Dict[str, Any]:
        return {
            "connectionId": ctx.connection_id,
            "projectId": ctx.project_id,
            "userId": ctx.user.get("id"),
            "displayName": ctx.user.get("displayName") or ctx.user.get("email") or "",
            "email": ctx.user.get("email") or "",
            "role": ctx.role,
            "sessionId": ctx.session_id,
            "lastSeenVersion": ctx.last_seen_version,
            "editingNodeId": ctx.editing_node_id,
        }

    def _build_event(
        self,
        *,
        project_id: str,
        event_type: str,
        version: int,
        payload: Dict[str, Any],
        event_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "eventId": event_id or f"evt_{uuid.uuid4().hex[:16]}",
            "projectId": project_id,
            "eventType": event_type,
            "version": int(version or 0),
            "serverTime": _now_ms(),
            "payload": payload,
        }

    def _history_event_to_realtime(
        self,
        project_id: str,
        event: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        event_type = str(event.get("eventType") or "").strip().lower()
        if event_type != "state_commit":
            return None
        payload = dict(event.get("payload") or {})
        payload.setdefault("clientOpId", event.get("clientOpId"))
        payload.setdefault("createdBy", event.get("createdBy"))
        payload.setdefault("createdAt", event.get("createdAt"))
        return self._build_event(
            project_id=project_id,
            event_type="state_committed",
            version=int(event.get("version") or 0),
            payload=payload,
            event_id=(
                f"evt_hist_{project_id}_{event.get('version')}_{event_type}_{event.get('clientOpId') or event.get('createdAt')}"
            ),
        )

    async def _send_json(self, ctx: RealtimeConnectionContext, payload: Dict[str, Any]) -> None:
        try:
            await ctx.websocket.send_text(json.dumps(payload, ensure_ascii=False))
        except Exception:
            await self.disconnect(ctx)

    async def _broadcast(
        self,
        targets: Iterable[RealtimeConnectionContext],
        payload: Dict[str, Any],
    ) -> None:
        if not targets:
            return
        await asyncio.gather(
            *(self._send_json(ctx, payload) for ctx in list(targets)),
            return_exceptions=True,
        )

    async def _heartbeat_loop(self, ctx: RealtimeConnectionContext) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval_ms / 1000)
                if _now_ms() - ctx.last_pong_at > self.heartbeat_timeout_ms:
                    with contextlib.suppress(Exception):
                        await ctx.websocket.close(code=1011, reason="heartbeat timeout")
                    return
                await self._send_json(
                    ctx,
                    self._build_event(
                        project_id=ctx.project_id,
                        event_type="heartbeat_ping",
                        version=ctx.last_seen_version,
                        payload={"connectionId": ctx.connection_id},
                    ),
                )
        except asyncio.CancelledError:
            raise


realtime_hub = ProjectRealtimeHub()
