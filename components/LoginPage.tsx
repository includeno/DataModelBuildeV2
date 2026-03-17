import React, { useState } from 'react';

interface LoginPageProps {
  backendLabel: string;
  loading?: boolean;
  error?: string | null;
  onLogin: (email: string, password: string) => Promise<void> | void;
  onBack?: () => void;
}

export const LoginPage: React.FC<LoginPageProps> = ({
  backendLabel,
  loading = false,
  error = null,
  onLogin,
  onBack
}) => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    await onLogin(email.trim(), password);
  };

  return (
    <div className="min-h-screen w-screen bg-slate-100 text-slate-900 flex items-center justify-center p-4">
      <div className="w-full max-w-md rounded-2xl border border-slate-200 bg-white shadow-sm p-6">
        <h1 className="text-xl font-semibold">登录协作空间</h1>
        <p className="mt-2 text-sm text-slate-600">
          当前后端: <span className="font-medium text-slate-800">{backendLabel}</span>
        </p>

        <form className="mt-6 space-y-4" onSubmit={handleSubmit}>
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1" htmlFor="dmb-login-email">
              邮箱
            </label>
            <input
              id="dmb-login-email"
              className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-300"
              type="email"
              autoComplete="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1" htmlFor="dmb-login-password">
              密码
            </label>
            <input
              id="dmb-login-password"
              className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-300"
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>
          {error ? (
            <div className="rounded-lg bg-red-50 border border-red-200 px-3 py-2 text-sm text-red-700">
              {error}
            </div>
          ) : null}
          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-lg bg-blue-600 text-white text-sm font-medium py-2.5 disabled:opacity-60"
          >
            {loading ? '登录中...' : '登录'}
          </button>
          {onBack ? (
            <button
              type="button"
              className="w-full rounded-lg border border-slate-300 text-slate-700 text-sm font-medium py-2.5 hover:bg-slate-50"
              onClick={onBack}
            >
              返回首页
            </button>
          ) : null}
        </form>
      </div>
    </div>
  );
};
