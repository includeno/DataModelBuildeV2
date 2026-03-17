import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import App from '../../App';
import { api } from '../../utils/api';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const waitFor = async (check: () => boolean, attempts = 20) => {
  for (let i = 0; i < attempts; i += 1) {
    if (check()) return;
    await act(async () => { await flush(); });
  }
  throw new Error('Condition not met in time');
};

describe('No-auth server flow', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  it('skips login and does not call auth APIs when server auth is disabled', async () => {
    vi.spyOn(api, 'get').mockResolvedValue([] as any);
    vi.spyOn(api, 'ping').mockResolvedValue(true);
    const authMeSpy = vi.spyOn(api, 'authMe').mockResolvedValue({} as any);

    const fetchMock = vi.fn(async (input: RequestInfo) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/config/default_server')) {
        return Promise.resolve({ ok: true, json: async () => ({ server: 'http://localhost:8000', authEnabled: false }) });
      }
      if (url.includes('/config/auth')) {
        return Promise.resolve({ ok: true, json: async () => ({ authEnabled: false, mode: 'disabled' }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });

    // @ts-expect-error test override
    global.fetch = fetchMock;

    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });

    await waitFor(() => {
      const badge = container.querySelector('[title^="Connected Server:"]') as HTMLElement | null;
      return !!badge;
    });

    const statusBadge = container.querySelector('[title^="Connected Server:"]') as HTMLElement | null;
    expect(statusBadge).toBeTruthy();
    expect(statusBadge!.getAttribute('title')).toContain('免登录');
    expect(container.textContent).not.toContain('登录协作空间');
    expect(container.querySelector('button[title="Log out"]')).toBeNull();
    expect(authMeSpy).not.toHaveBeenCalled();
  });
});
