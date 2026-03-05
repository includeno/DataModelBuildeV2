import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import App from '../../App';
import { api } from '../../utils/api';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

type SessionMeta = { sessionId: string; displayName?: string; createdAt: number };

const sessionsByStorage: Record<string, SessionMeta[]> = {
    sessions_a: [{ sessionId: 'sess_a1', displayName: 'Storage A Session', createdAt: 1 }],
    sessions_b: [{ sessionId: 'sess_b1', displayName: 'Storage B Session', createdAt: 2 }]
};

describe('Session storage switching (UI)', () => {
    let container: HTMLDivElement;
    let root: Root;
    let currentStorage = 'sessions_a';

    beforeEach(async () => {
        currentStorage = 'sessions_a';
        container = document.createElement('div');
        document.body.appendChild(container);
        root = createRoot(container);

        vi.spyOn(api, 'get').mockImplementation(async (_config: any, endpoint: string) => {
            if (endpoint === '/sessions') {
                return sessionsByStorage[currentStorage];
            }
            return {};
        });
        vi.spyOn(api, 'ping').mockResolvedValue(true);

        const fetchMock = vi.fn(async (input: RequestInfo, init?: RequestInit) => {
            const url = typeof input === 'string' ? input : input.toString();
            const ok = (data: any) => Promise.resolve({ ok: true, json: async () => data });

            if (url.includes('/config/default_server')) {
                return ok({ server: 'http://localhost:8000', isMock: false });
            }
            if (url.includes('/config/session_storage')) {
                if (url.includes('/select')) {
                    const body = init?.body ? JSON.parse(init.body.toString()) : {};
                    currentStorage = body.path || currentStorage;
                    return ok({ dataRoot: '/data', sessionsDir: `/data/${currentStorage}`, relative: currentStorage });
                }
                if (url.includes('/list')) {
                    return ok({
                        path: '',
                        folders: [
                            { name: 'sessions_a', path: 'sessions_a' },
                            { name: 'sessions_b', path: 'sessions_b' }
                        ]
                    });
                }
                return ok({ dataRoot: '/data', sessionsDir: `/data/${currentStorage}`, relative: currentStorage });
            }
            return ok({});
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

    const click = (el: Element | null) => {
        if (!el) throw new Error('Element not found');
        el.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    };

    const openSessionMenu = async () => {
        const btn = Array.from(document.querySelectorAll('button'))
            .find(b => (b.textContent || '').includes('Create Session') && (b.textContent || '').includes('Session'));
        click(btn || null);
        await act(async () => { await flush(); });
    };

    it('updates session list immediately after switching storage', async () => {
        await openSessionMenu();
        expect(document.body.textContent).toContain('Storage A Session');
        expect(document.body.textContent).not.toContain('Storage B Session');
        // Close session menu
        await openSessionMenu();

        // Open settings modal
        const settingsBtn = document.querySelector('button[title="Global Settings (Connection & Appearance)"]');
        click(settingsBtn);
        await act(async () => { await flush(); });

        // Select sessions_b in Session Storage list
        const target = Array.from(document.querySelectorAll('span'))
            .find(el => el.textContent === 'sessions_b');
        click(target || null);
        await act(async () => { await flush(); });

        // Open session menu again and verify list updated
        await openSessionMenu();
        expect(document.body.textContent).toContain('Storage B Session');
        expect(document.body.textContent).not.toContain('Storage A Session');
    });
});
