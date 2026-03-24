import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { api } from '../utils/api';

const REAL_FETCH = global.fetch;
const CONFIG = { baseUrl: 'http://localhost:8000', isMock: false };

const jsonResponse = (status: number, payload: any) =>
  ({
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => payload,
  } as Response);

const blobResponse = (status: number, body: string) =>
  ({
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    blob: async () => new Blob([body], { type: 'text/csv' }),
    json: async () => ({ detail: body }),
  } as unknown as Response);

describe('api transport helpers', () => {
  beforeEach(() => {
    api.clearAuthTokens();
    api.setAuthStorageMode('local_storage');
    api.setAuthApiEnabled(true);
    window.localStorage.clear();
  });

  afterEach(() => {
    api.clearAuthTokens();
    api.setAuthApiEnabled(true);
    vi.restoreAllMocks();
    global.fetch = REAL_FETCH;
    window.localStorage.clear();
  });

  it('registers users and surfaces envelope errors', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, {
          data: { user: { id: 'usr_new', email: 'new@example.com' } },
          error: null,
          meta: { api_version: 'v2' },
        })
      )
      .mockResolvedValueOnce(
        jsonResponse(422, {
          data: null,
          error: { code: 'AUTH_INVALID', message: 'Invalid password' },
          meta: { api_version: 'v2' },
        })
      );
    global.fetch = fetchMock as unknown as typeof fetch;

    const registered = await api.authRegister(CONFIG, { email: 'new@example.com', password: 'Passw0rd!' });
    expect(registered.user.id).toBe('usr_new');

    await expect(
      api.authRegister(CONFIG, { email: 'new@example.com', password: 'weak' })
    ).rejects.toThrow('Invalid password');
  });

  it('clears tokens when refresh fails and keeps original 401 response path', async () => {
    api.setAuthTokens({ accessToken: 'expired', refreshToken: 'refresh_1', expiresAt: 1 });
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(async () => jsonResponse(401, { detail: { code: 'AUTH_UNAUTHORIZED' } }))
      .mockImplementationOnce(async () => jsonResponse(401, { detail: 'refresh failed' }));
    global.fetch = fetchMock as unknown as typeof fetch;

    await expect(api.get(CONFIG, '/projects')).rejects.toThrow('API Error: 401');
    expect(api.getAuthTokens()).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('supports logout when auth is disabled and clears stored tokens', async () => {
    api.setAuthTokens({ accessToken: 'access_1', refreshToken: 'refresh_1', expiresAt: 10 });
    api.setAuthApiEnabled(false);

    const result = await api.authLogout(CONFIG);

    expect(result.status).toBe('skipped');
    expect(api.getAuthTokens()).toBeNull();
  });

  it('handles ping failures and request errors for CRUD helpers', async () => {
    global.fetch = vi
      .fn()
      .mockRejectedValueOnce(new Error('offline'))
      .mockResolvedValueOnce(jsonResponse(500, { detail: { message: 'broken get' } }))
      .mockResolvedValueOnce(jsonResponse(409, { detail: { message: 'broken post' } }))
      .mockResolvedValueOnce(jsonResponse(403, { detail: { message: 'broken patch' } }))
      .mockResolvedValueOnce(jsonResponse(404, { detail: { message: 'broken delete' } }))
      .mockResolvedValueOnce(jsonResponse(413, { detail: { message: 'broken upload' } })) as unknown as typeof fetch;

    await expect(api.ping(CONFIG, 1)).resolves.toBe(false);
    await expect(api.get(CONFIG, '/broken')).rejects.toThrow('API Error: 500');
    await expect(api.post(CONFIG, '/broken', {})).rejects.toThrow('broken post');
    await expect(api.patch(CONFIG, '/broken', {})).rejects.toThrow('broken patch');
    await expect(api.delete(CONFIG, '/broken')).rejects.toThrow('API Error: 404');

    const formData = new FormData();
    formData.set('file', new File(['a,b'], 'broken.csv', { type: 'text/csv' }));
    await expect(api.upload(CONFIG, '/broken', formData)).rejects.toThrow('Upload Error: 413');
  });

  it('exports csv blobs and unwraps upload success responses', async () => {
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
    const createObjectURL = vi.fn(() => 'blob:export');
    const revokeObjectURL = vi.fn();
    vi.stubGlobal('URL', {
      createObjectURL,
      revokeObjectURL,
    });

    global.fetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, {
          data: { id: 'ds_1', name: 'upload.csv' },
          error: null,
          meta: { api_version: 'v2' },
        })
      )
      .mockResolvedValueOnce(blobResponse(200, 'id,name\n1,Alice')) as unknown as typeof fetch;

    const formData = new FormData();
    formData.set('file', new File(['id,name\n1,Alice'], 'upload.csv', { type: 'text/csv' }));

    const uploaded = await api.upload(CONFIG, '/projects/proj_1/upload', formData);
    expect(uploaded.name).toBe('upload.csv');

    await api.export(CONFIG, '/projects/proj_1/export', { format: 'csv' });
    expect(createObjectURL).toHaveBeenCalled();
    expect(anchorClick).toHaveBeenCalled();
  });
});
