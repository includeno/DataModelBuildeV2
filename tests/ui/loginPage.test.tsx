import React from 'react';
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import { act, Simulate } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { LoginPage } from '../../components/LoginPage';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

describe('LoginPage', () => {
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

  const renderPage = async (props?: Partial<React.ComponentProps<typeof LoginPage>>) => {
    const onLogin = props?.onLogin || vi.fn(async () => {});
    await act(async () => {
      root.render(
        <LoginPage
          backendLabel="http://localhost:8000"
          onLogin={onLogin}
          loading={props?.loading}
          error={props?.error}
        />
      );
      await flush();
    });
    return onLogin;
  };

  it('submits email and password', async () => {
    const onLogin = (await renderPage()) as ReturnType<typeof vi.fn>;
    const email = container.querySelector('#dmb-login-email') as HTMLInputElement;
    const password = container.querySelector('#dmb-login-password') as HTMLInputElement;
    const form = container.querySelector('form') as HTMLFormElement;

    await act(async () => {
      Simulate.change(email, { target: { value: 'user@example.com' } });
      Simulate.change(password, { target: { value: 'Passw0rd!' } });
      await flush();
    });

    await act(async () => {
      Simulate.submit(form);
      await flush();
    });

    expect(onLogin).toHaveBeenCalledWith('user@example.com', 'Passw0rd!');
  });

  it('shows loading and error state', async () => {
    await renderPage({ loading: true, error: 'Invalid credentials' });
    expect(container.textContent).toContain('Invalid credentials');
    const submit = container.querySelector('button[type="submit"]') as HTMLButtonElement;
    expect(submit.disabled).toBe(true);
    expect(submit.textContent).toContain('登录中');
  });
});
