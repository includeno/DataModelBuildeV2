import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../utils/api';

const REAL_FETCH = global.fetch;
const CONFIG = { baseUrl: 'http://localhost:8000', isMock: false };

const jsonResponse = (status: number, payload: any) =>
  ({
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => payload
  } as Response);

describe('api contract client', () => {
  beforeEach(() => {
    api.clearAuthTokens();
    api.setAuthStorageMode('local_storage');
    api.setAuthApiEnabled(true);
  });

  afterEach(() => {
    api.clearAuthTokens();
    api.setAuthApiEnabled(true);
    vi.restoreAllMocks();
    global.fetch = REAL_FETCH;
  });

  it('unwraps v2 success envelopes automatically', async () => {
    global.fetch = vi.fn().mockResolvedValue(
      jsonResponse(200, {
        data: [{ id: 'prj_1', name: 'Contract Project' }],
        error: null,
        meta: { api_version: 'v2' },
        request_id: 'req_123'
      })
    ) as unknown as typeof fetch;

    const body = await api.get(CONFIG, '/v2/projects');

    expect(Array.isArray(body)).toBe(true);
    expect(body[0].id).toBe('prj_1');
  });

  it('uses v2 error envelopes for thrown messages', async () => {
    global.fetch = vi.fn().mockResolvedValue(
      jsonResponse(409, {
        data: null,
        error: {
          code: 'PROJECT_STATE_CONFLICT',
          message: 'Version conflict',
          category: 'conflict'
        },
        meta: { api_version: 'v2', status_code: 409 },
        request_id: 'req_409'
      })
    ) as unknown as typeof fetch;

    await expect(
      api.post(CONFIG, '/v2/projects/prj_1/state/commit', { baseVersion: 0, state: {} })
    ).rejects.toThrow('Version conflict');
  });
});
