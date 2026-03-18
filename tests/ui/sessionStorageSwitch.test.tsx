import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import App from '../../App';
import { api } from '../../utils/api';
import { ProjectMetadata } from '../../types';
import { INITIAL_TREE } from '../../utils/projectStore';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

type ProjectMeta = ProjectMetadata;

const projectsByStorage: Record<string, ProjectMeta[]> = {
    sessions_a: [{ id: 'proj_a1', name: 'Storage A Project', role: 'owner', createdAt: 1, updatedAt: 1 }],
    sessions_b: [{ id: 'proj_b1', name: 'Storage B Project', role: 'owner', createdAt: 2, updatedAt: 2 }]
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
            if (endpoint === '/projects') {
                return projectsByStorage[currentStorage];
            }
            if (endpoint.startsWith('/projects/') && endpoint.endsWith('/state')) {
                return {
                    version: 1,
                    updatedAt: Date.now(),
                    state: {
                        tree: INITIAL_TREE,
                        sqlHistory: [],
                    },
                };
            }
            if (endpoint.startsWith('/projects/') && endpoint.endsWith('/metadata')) {
                const projectId = endpoint.split('/')[2];
                const project = Object.values(projectsByStorage).flat().find(item => item.id === projectId);
                return {
                    displayName: project?.name || projectId,
                    settings: {
                        cascadeDisable: true,
                        panelPosition: 'right',
                    },
                };
            }
            if (endpoint.startsWith('/projects/') && endpoint.endsWith('/datasets')) return [];
            if (endpoint.startsWith('/projects/') && endpoint.endsWith('/members')) return [];
            if (endpoint.startsWith('/projects/') && endpoint.endsWith('/jobs')) return [];
            return {} as any;
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
        const btn = document.querySelector('button[title="Project Switcher"]');
        click(btn || null);
        await act(async () => { await flush(); });
    };

    it('updates session list immediately after switching storage', async () => {
        await openSessionMenu();
        expect(document.body.textContent).toContain('Storage A Project');
        expect(document.body.textContent).not.toContain('Storage B Project');
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
        expect(document.body.textContent).toContain('Storage B Project');
        expect(document.body.textContent).not.toContain('Storage A Project');
    });
});
