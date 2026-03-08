import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import App from '../../App';
import { api } from '../../utils/api';

const flushPromises = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

describe('Backend status recovery (UI)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    vi.useFakeTimers();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it('transitions from offline to online after next ping', async () => {
    vi.spyOn(api, 'get').mockResolvedValue([] as any);
    vi.spyOn(api, 'ping')
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true);

    const fetchMock = vi.fn(async (input: RequestInfo) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/config/default_server')) {
        return Promise.resolve({ ok: true, json: async () => ({ server: 'http://localhost:8000' }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });

    // @ts-expect-error test override
    global.fetch = fetchMock;

    await act(async () => {
      root.render(<App />);
      await flushPromises();
    });

    await act(async () => {
      await flushPromises();
    });

    const badgeOffline = container.querySelector('[title^="Backend Status:"]') as HTMLElement | null;
    expect(badgeOffline).toBeTruthy();
    expect(badgeOffline!.getAttribute('title')).toContain('Offline');

    await act(async () => {
      vi.advanceTimersByTime(10000);
      await flushPromises();
    });

    const badgeOnline = container.querySelector('[title^="Backend Status:"]') as HTMLElement | null;
    expect(badgeOnline).toBeTruthy();
    expect(badgeOnline!.getAttribute('title')).toContain('Localhost');
  });

  it('shows mock status when default server is mockServer', async () => {
    vi.spyOn(api, 'get').mockResolvedValue([] as any);
    vi.spyOn(api, 'ping').mockResolvedValue(false);

    const fetchMock = vi.fn(async (input: RequestInfo) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/config/default_server')) {
        return Promise.resolve({ ok: true, json: async () => ({ server: 'mockServer' }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });

    // @ts-expect-error test override
    global.fetch = fetchMock;

    await act(async () => {
      root.render(<App />);
      await flushPromises();
    });

    await act(async () => {
      await flushPromises();
    });

    const badge = container.querySelector('[title^="Backend Status:"]') as HTMLElement | null;
    expect(badge).toBeTruthy();
    expect(badge!.getAttribute('title')).toContain('Mock Server');
  });
});
