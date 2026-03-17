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

describe('api auth client', () => {
  beforeEach(() => {
    api.clearAuthTokens();
    api.setAuthStorageMode('local_storage');
  });

  afterEach(() => {
    api.clearAuthTokens();
    vi.restoreAllMocks();
    global.fetch = REAL_FETCH;
  });

  it('automatically attaches Authorization header', async () => {
    api.setAuthTokens({ accessToken: 'access_1', refreshToken: 'refresh_1', expiresAt: 1 });
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(200, { items: [] }));
    global.fetch = fetchMock as unknown as typeof fetch;

    await api.get(CONFIG, '/projects');

    const call = fetchMock.mock.calls[0];
    const headers = (call[1] as RequestInit).headers as Record<string, string>;
    expect(headers.Authorization).toBe('Bearer access_1');
  });

  it('refreshes expired access token and retries once', async () => {
    api.setAuthTokens({ accessToken: 'expired_access', refreshToken: 'refresh_1', expiresAt: 1 });
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(async () => jsonResponse(401, { detail: { code: 'AUTH_UNAUTHORIZED' } }))
      .mockImplementationOnce(async () => jsonResponse(200, { accessToken: 'new_access', refreshToken: 'refresh_1' }))
      .mockImplementationOnce(async () => jsonResponse(200, { items: [{ id: 'prj_1' }] }));
    global.fetch = fetchMock as unknown as typeof fetch;

    const body = await api.get(CONFIG, '/projects');

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(body.items[0].id).toBe('prj_1');
    const retryHeaders = (fetchMock.mock.calls[2][1] as RequestInit).headers as Record<string, string>;
    expect(retryHeaders.Authorization).toBe('Bearer new_access');
    expect(api.getAuthTokens()?.accessToken).toBe('new_access');
  });

  it('stores tokens after login', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, {
        accessToken: 'access_login',
        refreshToken: 'refresh_login',
        expiresAt: 123456,
        user: { id: 'usr_1', email: 'u@example.com' }
      })
    );
    global.fetch = fetchMock as unknown as typeof fetch;

    const body = await api.authLogin(CONFIG, { email: 'u@example.com', password: 'Passw0rd!' });
    const tokens = api.getAuthTokens();

    expect(body.user.id).toBe('usr_1');
    expect(tokens?.accessToken).toBe('access_login');
    expect(tokens?.refreshToken).toBe('refresh_login');
  });
});
