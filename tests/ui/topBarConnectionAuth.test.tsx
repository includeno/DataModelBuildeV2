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

describe('TopBar connection status and logout', () => {
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

  const setupRealServer = () => {
    vi.spyOn(api, 'get').mockResolvedValue([] as any);
    vi.spyOn(api, 'ping').mockResolvedValue(true);
    vi.spyOn(api, 'authMe').mockResolvedValue({ email: 'user@example.com' } as any);

    const fetchMock = vi.fn(async (input: RequestInfo) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/config/default_server')) {
        return Promise.resolve({ ok: true, json: async () => ({ server: 'http://localhost:8000' }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });

    // @ts-expect-error test override
    global.fetch = fetchMock;
  };

  it('shows connected server status in top bar', async () => {
    setupRealServer();

    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });

    await waitFor(() => {
      const badge = container.querySelector('[title^="Connected Server:"]') as HTMLElement | null;
      return !!badge;
    });

    const badge = container.querySelector('[title^="Connected Server:"]') as HTMLElement | null;
    expect(badge).toBeTruthy();
    expect(badge!.getAttribute('title')).toContain('http://localhost:8000');
    expect(badge!.getAttribute('title')).toContain('已连接');
  });

  it('shows logout button and returns to login page after logout', async () => {
    setupRealServer();
    const logoutSpy = vi.spyOn(api, 'authLogout').mockResolvedValue({ status: 'ok' } as any);

    await act(async () => {
      root.render(<App />);
      await flush();
      await flush();
    });

    await waitFor(() => {
      const btn = container.querySelector('button[title="Log out"]') as HTMLButtonElement | null;
      return !!btn;
    });

    const logoutBtn = container.querySelector('button[title="Log out"]') as HTMLButtonElement | null;
    expect(logoutBtn).toBeTruthy();

    await act(async () => {
      logoutBtn!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(logoutSpy).toHaveBeenCalledTimes(1);
    await waitFor(() => container.textContent?.includes('登录协作空间') ?? false);
    expect(container.textContent).toContain('登录协作空间');
  });
});
