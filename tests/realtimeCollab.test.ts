import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { RealtimeProjectClient, buildProjectRealtimeUrl } from '../utils/realtimeCollab';


class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 3;

  readonly url: string;
  readyState = FakeWebSocket.CONNECTING;
  sent: string[] = [];
  private listeners = new Map<string, Array<(event: any) => void>>();

  constructor(url: string) {
    this.url = url;
  }

  addEventListener(type: string, listener: (event: any) => void) {
    const items = this.listeners.get(type) || [];
    items.push(listener);
    this.listeners.set(type, items);
  }

  send(data: string) {
    this.sent.push(data);
  }

  close(code?: number, reason?: string) {
    this.readyState = FakeWebSocket.CLOSED;
    this.emit('close', { code, reason });
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.emit('open', {});
  }

  serverSend(payload: Record<string, any>) {
    this.emit('message', { data: JSON.stringify(payload) });
  }

  private emit(type: string, event: any) {
    for (const listener of this.listeners.get(type) || []) {
      listener(event);
    }
  }
}


describe('realtime collaboration client', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('builds websocket url from http base url', () => {
    const url = buildProjectRealtimeUrl('http://localhost:8000', 'prj_123', 'token_1');
    expect(url).toBe('ws://localhost:8000/ws/projects/prj_123?token=token_1');
  });

  it('tracks presence, applies remote patches, deduplicates events and responds to heartbeat', () => {
    const sockets: FakeWebSocket[] = [];
    const states: Array<Record<string, any>> = [];
    const presences: Array<string[]> = [];

    const client = new RealtimeProjectClient({
      baseUrl: 'http://localhost:8000',
      projectId: 'prj_live',
      tokenProvider: () => 'token_abc',
      initialVersion: 1,
      initialState: { tree: { id: 'root', name: 'Local' } },
      sessionId: 'sess_local',
      webSocketFactory: (url) => {
        const socket = new FakeWebSocket(url);
        sockets.push(socket);
        return socket;
      },
      onStateChange: (state) => {
        states.push(state);
      },
      onPresenceChange: (presence) => {
        presences.push(presence.map((item) => item.userId));
      },
    });

    client.connect();
    expect(sockets).toHaveLength(1);
    expect(client.getStatus()).toBe('connecting');

    sockets[0].open();
    expect(client.getStatus()).toBe('connected');
    expect(JSON.parse(sockets[0].sent[0])).toMatchObject({
      type: 'subscribe',
      projectId: 'prj_live',
      clientVersion: 1,
      sessionId: 'sess_local',
    });

    sockets[0].serverSend({
      eventId: 'evt_sub_1',
      projectId: 'prj_live',
      eventType: 'subscribed',
      version: 2,
      payload: {
        latestVersion: 2,
        presence: [
          {
            connectionId: 'ws_a',
            projectId: 'prj_live',
            userId: 'usr_owner',
          },
        ],
      },
    });
    expect(client.getPresence()).toHaveLength(1);
    expect(presences[presences.length - 1]).toEqual(['usr_owner']);

    sockets[0].serverSend({
      eventId: 'evt_ping_1',
      projectId: 'prj_live',
      eventType: 'heartbeat_ping',
      version: 1,
      payload: {},
    });
    expect(JSON.parse(sockets[0].sent[1])).toEqual({ type: 'pong', lastSeenVersion: 1 });

    sockets[0].serverSend({
      eventId: 'evt_join_1',
      projectId: 'prj_live',
      eventType: 'presence_join',
      version: 1,
      payload: {
        member: {
          connectionId: 'ws_b',
          projectId: 'prj_live',
          userId: 'usr_editor',
        },
      },
    });
    expect(client.getPresence()).toHaveLength(2);

    sockets[0].serverSend({
      eventId: 'evt_state_2',
      projectId: 'prj_live',
      eventType: 'state_committed',
      version: 2,
      payload: {
        patches: [{ op: 'set_top_level', key: 'note', value: 'remote-edit' }],
      },
    });

    expect(client.getCurrentVersion()).toBe(2);
    expect(client.getState()).toEqual({
      tree: { id: 'root', name: 'Local' },
      note: 'remote-edit',
    });
    expect(states[0]).toEqual({
      tree: { id: 'root', name: 'Local' },
      note: 'remote-edit',
    });
    expect(JSON.parse(sockets[0].sent[2])).toEqual({ type: 'ack_version', version: 2 });

    sockets[0].serverSend({
      eventId: 'evt_state_2',
      projectId: 'prj_live',
      eventType: 'state_committed',
      version: 2,
      payload: {
        patches: [{ op: 'set_top_level', key: 'note', value: 'duplicate' }],
      },
    });
    sockets[0].serverSend({
      eventId: 'evt_old_1',
      projectId: 'prj_live',
      eventType: 'state_committed',
      version: 1,
      payload: {
        patches: [{ op: 'set_top_level', key: 'note', value: 'stale' }],
      },
    });

    expect(client.getCurrentVersion()).toBe(2);
    expect(client.getState().note).toBe('remote-edit');
    expect(states).toHaveLength(1);
  });

  it('reconnects with exponential backoff and resubscribes from the last applied version', () => {
    const sockets: FakeWebSocket[] = [];
    const gapEvents: number[] = [];

    const client = new RealtimeProjectClient({
      baseUrl: 'http://localhost:8000',
      projectId: 'prj_gap',
      tokenProvider: () => 'token_gap',
      initialVersion: 1,
      initialState: { tree: { id: 'root' } },
      reconnectBaseMs: 100,
      reconnectMaxMs: 1000,
      webSocketFactory: (url) => {
        const socket = new FakeWebSocket(url);
        sockets.push(socket);
        return socket;
      },
      onGapDetected: (_, expectedVersion) => {
        gapEvents.push(expectedVersion);
      },
    });

    client.connect();
    sockets[0].open();
    expect(JSON.parse(sockets[0].sent[0]).clientVersion).toBe(1);

    sockets[0].serverSend({
      eventId: 'evt_gap_3',
      projectId: 'prj_gap',
      eventType: 'state_committed',
      version: 3,
      payload: {
        patches: [{ op: 'set_top_level', key: 'note', value: 'missing-v2' }],
      },
    });

    expect(gapEvents).toEqual([2]);
    expect(client.getStatus()).toBe('reconnecting');

    vi.advanceTimersByTime(100);
    expect(sockets).toHaveLength(2);

    sockets[1].open();
    expect(client.getStatus()).toBe('connected');
    expect(JSON.parse(sockets[1].sent[0])).toMatchObject({
      type: 'subscribe',
      projectId: 'prj_gap',
      clientVersion: 1,
    });

    sockets[1].serverSend({
      eventId: 'evt_state_2',
      projectId: 'prj_gap',
      eventType: 'state_committed',
      version: 2,
      payload: {
        patches: [{ op: 'set_top_level', key: 'note', value: 'caught-up' }],
      },
    });
    sockets[1].serverSend({
      eventId: 'evt_state_3',
      projectId: 'prj_gap',
      eventType: 'state_committed',
      version: 3,
      payload: {
        patches: [{ op: 'set_top_level', key: 'ready', value: true }],
      },
    });

    expect(client.getCurrentVersion()).toBe(3);
    expect(client.getState()).toEqual({
      tree: { id: 'root' },
      note: 'caught-up',
      ready: true,
    });
  });
});
