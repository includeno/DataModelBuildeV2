import { applyPatches, ProjectPatch, ProjectState } from './collabSync';

export type RealtimeStatus = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'closed';

export type PresenceMember = {
  connectionId: string;
  projectId: string;
  userId: string;
  displayName?: string;
  email?: string;
  role?: string;
  sessionId?: string | null;
  lastSeenVersion?: number;
  editingNodeId?: string | null;
};

export type RealtimeServerEvent = {
  eventId?: string;
  projectId: string;
  eventType: string;
  version: number;
  serverTime?: number;
  payload?: Record<string, any>;
};

export interface RealtimeWebSocketLike {
  readyState: number;
  addEventListener(type: string, listener: (event: any) => void): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

type RealtimeCallbacks = {
  onStatusChange?: (status: RealtimeStatus) => void;
  onPresenceChange?: (presence: PresenceMember[]) => void;
  onStateChange?: (state: ProjectState, event: RealtimeServerEvent) => void;
  onConflictNotice?: (event: RealtimeServerEvent) => void;
  onEvent?: (event: RealtimeServerEvent) => void;
  onGapDetected?: (event: RealtimeServerEvent, expectedVersion: number) => void;
};

export type RealtimeProjectClientOptions = RealtimeCallbacks & {
  baseUrl: string;
  projectId: string;
  tokenProvider: () => string | null;
  initialState?: ProjectState;
  initialVersion?: number;
  sessionId?: string;
  reconnectBaseMs?: number;
  reconnectMaxMs?: number;
  webSocketFactory?: (url: string) => RealtimeWebSocketLike;
};

const OPEN = 1;
const cloneState = (value: ProjectState): ProjectState => JSON.parse(JSON.stringify(value || {}));

const normalizeBaseUrl = (value: string): string => {
  const trimmed = String(value || '').trim();
  if (!trimmed) return '';
  if (trimmed.startsWith('ws://') || trimmed.startsWith('wss://')) return trimmed;
  if (trimmed.startsWith('https://')) return `wss://${trimmed.slice('https://'.length)}`;
  if (trimmed.startsWith('http://')) return `ws://${trimmed.slice('http://'.length)}`;
  return trimmed;
};

export const buildProjectRealtimeUrl = (baseUrl: string, projectId: string, token?: string | null): string => {
  const url = new URL(`${normalizeBaseUrl(baseUrl).replace(/\/$/, '')}/ws/projects/${encodeURIComponent(projectId)}`);
  if (token) url.searchParams.set('token', token);
  return url.toString();
};

export class RealtimeProjectClient {
  private readonly options: RealtimeProjectClientOptions;
  private readonly webSocketFactory: (url: string) => RealtimeWebSocketLike;
  private readonly seenEventIds = new Set<string>();
  private readonly seenEventQueue: string[] = [];
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;
  private socket: RealtimeWebSocketLike | null = null;
  private manualClose = false;
  private status: RealtimeStatus = 'idle';
  private currentVersion: number;
  private currentState: ProjectState;
  private presence = new Map<string, PresenceMember>();

  constructor(options: RealtimeProjectClientOptions) {
    this.options = options;
    this.currentVersion = Math.max(Number(options.initialVersion || 0), 0);
    this.currentState = cloneState(options.initialState || {});
    this.webSocketFactory =
      options.webSocketFactory ||
      ((url: string) => new WebSocket(url) as unknown as RealtimeWebSocketLike);
  }

  connect() {
    this.manualClose = false;
    this.clearReconnectTimer();
    this.openSocket(false);
  }

  disconnect() {
    this.manualClose = true;
    this.clearReconnectTimer();
    const current = this.socket;
    this.socket = null;
    if (current) current.close(1000, 'client disconnect');
    this.setStatus('closed');
  }

  getStatus(): RealtimeStatus {
    return this.status;
  }

  getCurrentVersion(): number {
    return this.currentVersion;
  }

  getState(): ProjectState {
    return cloneState(this.currentState);
  }

  getPresence(): PresenceMember[] {
    return Array.from(this.presence.values()).map((item) => ({ ...item }));
  }

  sendPresenceUpdate(payload: { editingNodeId?: string | null; sessionId?: string | null }): boolean {
    return this.sendMessage({
      type: 'presence_update',
      sessionId: payload.sessionId ?? this.options.sessionId ?? null,
      editingNodeId: payload.editingNodeId ?? null,
      lastSeenVersion: this.currentVersion,
    });
  }

  private openSocket(isReconnect: boolean) {
    const token = this.options.tokenProvider();
    const url = buildProjectRealtimeUrl(this.options.baseUrl, this.options.projectId, token);
    const socket = this.webSocketFactory(url);
    this.socket = socket;
    this.setStatus(isReconnect ? 'reconnecting' : 'connecting');

    socket.addEventListener('open', () => {
      if (socket !== this.socket) return;
      this.reconnectAttempt = 0;
      this.setStatus('connected');
      this.sendMessage({
        type: 'subscribe',
        projectId: this.options.projectId,
        clientVersion: this.currentVersion,
        sessionId: this.options.sessionId ?? null,
      });
    });

    socket.addEventListener('message', (event: { data: string }) => {
      if (socket !== this.socket) return;
      this.handleMessage(event?.data);
    });

    socket.addEventListener('close', () => {
      if (socket !== this.socket) return;
      this.socket = null;
      if (this.manualClose) {
        this.setStatus('closed');
        return;
      }
      this.scheduleReconnect();
    });

    socket.addEventListener('error', () => {
      if (socket !== this.socket) return;
      if (!this.manualClose && socket.readyState !== OPEN) {
        this.scheduleReconnect();
      }
    });
  }

  private handleMessage(rawData: string) {
    let event: RealtimeServerEvent;
    try {
      event = JSON.parse(rawData || '{}');
    } catch {
      return;
    }

    if (!event || typeof event !== 'object') return;
    if (event.eventId && this.seenEventIds.has(event.eventId)) return;
    if (event.eventId) this.trackEventId(event.eventId);

    this.options.onEvent?.(event);

    switch (event.eventType) {
      case 'heartbeat_ping':
        this.sendMessage({ type: 'pong', lastSeenVersion: this.currentVersion });
        return;
      case 'subscribed':
        this.replacePresence((event.payload?.presence as PresenceMember[]) || []);
        return;
      case 'presence_join':
        if (event.payload?.member) this.upsertPresence(event.payload.member as PresenceMember);
        return;
      case 'presence_leave':
        this.removePresence(event.payload);
        return;
      case 'presence_update':
        if (event.payload?.member) this.upsertPresence(event.payload.member as PresenceMember);
        return;
      case 'conflict_notice':
        this.options.onConflictNotice?.(event);
        return;
      case 'state_committed':
        this.handleStateCommitted(event);
        return;
      default:
        return;
    }
  }

  private handleStateCommitted(event: RealtimeServerEvent) {
    const nextVersion = Number(event.version || 0);
    if (nextVersion <= this.currentVersion) return;
    if (nextVersion > this.currentVersion + 1) {
      this.options.onGapDetected?.(event, this.currentVersion + 1);
      this.forceReconnect();
      return;
    }

    const payload = event.payload || {};
    const patches = Array.isArray(payload.patches) ? (payload.patches as ProjectPatch[]) : [];

    if (patches.length > 0) {
      this.currentState = applyPatches(this.currentState, patches);
    } else if (payload.state && typeof payload.state === 'object') {
      this.currentState = cloneState(payload.state as ProjectState);
    }

    this.currentVersion = nextVersion;
    this.sendMessage({ type: 'ack_version', version: this.currentVersion });
    this.options.onStateChange?.(this.getState(), event);
  }

  private replacePresence(nextPresence: PresenceMember[]) {
    this.presence = new Map(
      (nextPresence || [])
        .filter((item) => item?.connectionId)
        .map((item) => [item.connectionId, { ...item }])
    );
    this.emitPresence();
  }

  private upsertPresence(member: PresenceMember) {
    if (!member?.connectionId) return;
    const prev = this.presence.get(member.connectionId) || {};
    this.presence.set(member.connectionId, { ...prev, ...member });
    this.emitPresence();
  }

  private removePresence(payload?: Record<string, any>) {
    const connectionId = String(payload?.connectionId || '').trim();
    if (connectionId) {
      this.presence.delete(connectionId);
      this.emitPresence();
      return;
    }
    const userId = String(payload?.userId || '').trim();
    if (!userId) return;
    for (const [key, value] of Array.from(this.presence.entries())) {
      if (value.userId === userId) this.presence.delete(key);
    }
    this.emitPresence();
  }

  private emitPresence() {
    this.options.onPresenceChange?.(this.getPresence());
  }

  private sendMessage(payload: Record<string, any>): boolean {
    if (!this.socket || this.socket.readyState !== OPEN) return false;
    this.socket.send(JSON.stringify(payload));
    return true;
  }

  private trackEventId(eventId: string) {
    this.seenEventIds.add(eventId);
    this.seenEventQueue.push(eventId);
    if (this.seenEventQueue.length <= 512) return;
    const removed = this.seenEventQueue.shift();
    if (removed) this.seenEventIds.delete(removed);
  }

  private forceReconnect() {
    if (this.manualClose) return;
    const current = this.socket;
    this.socket = null;
    if (current) {
      current.close(1012, 'state gap');
    }
    this.scheduleReconnect();
  }

  private scheduleReconnect() {
    if (this.manualClose || this.reconnectTimer) return;
    const delay = Math.min(
      (this.options.reconnectBaseMs ?? 500) * (2 ** this.reconnectAttempt),
      this.options.reconnectMaxMs ?? 8000
    );
    this.reconnectAttempt += 1;
    this.setStatus('reconnecting');
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.openSocket(true);
    }, delay);
  }

  private clearReconnectTimer() {
    if (!this.reconnectTimer) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  private setStatus(status: RealtimeStatus) {
    if (this.status === status) return;
    this.status = status;
    this.options.onStatusChange?.(status);
  }
}
