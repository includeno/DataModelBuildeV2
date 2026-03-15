import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import App from '../../App';
import { api } from '../../utils/api';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const waitFor = async (check: () => boolean, attempts = 10) => {
  for (let i = 0; i < attempts; i += 1) {
    if (check()) return;
    await act(async () => { await flush(); });
  }
  throw new Error('Condition not met in time');
};

describe('Backend status (offline)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    vi.spyOn(api, 'get').mockResolvedValue([] as any);
    vi.spyOn(api, 'ping').mockResolvedValue(false);

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
      await flush();
      await flush();
    });
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.restoreAllMocks();
  });

  it('shows offline badge when ping fails', async () => {
    await waitFor(() => {
      const badge = container.querySelector('[title^="Backend Status:"]') as HTMLElement | null;
      return !!badge && (badge.getAttribute('title') || '').includes('Offline');
    });

    const badge = container.querySelector('[title^="Backend Status:"]') as HTMLElement | null;
    expect(badge).toBeTruthy();
    expect(badge!.getAttribute('title')).toContain('Offline');
  });
});
