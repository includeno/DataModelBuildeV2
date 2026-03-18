#!/usr/bin/env node

import fs from 'fs';
import fsPromises from 'fs/promises';
import path from 'path';
import process from 'process';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(SCRIPT_DIR, '..');
const BACKEND_CWD = path.join(REPO_ROOT, 'backend');
const DEFAULT_ARTIFACT_DIR = path.join(REPO_ROOT, 'output', 'playwright');
const DEFAULT_FRONTEND_HOST = '127.0.0.1';
const DEFAULT_FRONTEND_PORT = 1425;
const DEFAULT_BACKEND_HOST = '127.0.0.1';
const DEFAULT_BACKEND_PORT = 18001;
const DEFAULT_AUTH = 'enabled';
const DEFAULT_PURGE_RUNTIME_DIR = true;

const usage = `Usage:
  node scripts/run_cross_browser_collab_smoke.mjs [wrapper options] [smoke options]

Wrapper options:
  --frontend-url <url>       Reuse an existing frontend instead of starting one
  --backend-url <url>        Reuse an existing backend instead of starting one
  --frontend-port <port>     Frontend port when wrapper starts Vite. Default: ${DEFAULT_FRONTEND_PORT}
  --backend-port <port>      Backend port when wrapper starts uvicorn. Default: ${DEFAULT_BACKEND_PORT}
  --artifact-dir <path>      Artifact root for logs/results. Default: ${DEFAULT_ARTIFACT_DIR}
  --auth <enabled|disabled>  Auth mode for spawned backend. Default: ${DEFAULT_AUTH}
  --headless                 Run browsers headlessly instead of the default headed mode
  --keep-servers             Keep spawned frontend/backend running after smoke finishes
  --keep-runtime-dir         Keep the generated sqlite/sessions/log runtime dir after the run
  --help                     Show this help

Smoke options:
  Any remaining flags are forwarded to scripts/cross_browser_collab_smoke.mjs.
  Wrapper defaults to API project bootstrap for stability in fresh environments.
  Common examples: --headed --slow-mo 150 --dataset test_data/customers.csv

Examples:
  node scripts/run_cross_browser_collab_smoke.mjs --headed
  node scripts/run_cross_browser_collab_smoke.mjs --frontend-url http://127.0.0.1:1420 --backend-url http://127.0.0.1:8001 --headed
`;

const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const pythonCommand = process.platform === 'win32' ? 'python.exe' : 'python';

const parseArgs = (argv) => {
  const args = {
    frontendUrl: '',
    backendUrl: '',
    frontendPort: DEFAULT_FRONTEND_PORT,
    backendPort: DEFAULT_BACKEND_PORT,
    artifactDir: DEFAULT_ARTIFACT_DIR,
    auth: DEFAULT_AUTH,
    headed: true,
    keepServers: false,
    purgeRuntimeDir: DEFAULT_PURGE_RUNTIME_DIR,
    forwarded: [],
  };

  for (let i = 0; i < argv.length; i += 1) {
    const raw = argv[i];
    if (!raw.startsWith('--')) {
      args.forwarded.push(raw);
      continue;
    }

    const [flag, inlineValue] = raw.split('=', 2);
    const nextValue = inlineValue ?? argv[i + 1];
    const hasInline = inlineValue !== undefined;
    const consumeValue = () => {
      if (!hasInline) i += 1;
      return nextValue;
    };

    switch (flag) {
      case '--frontend-url':
        args.frontendUrl = consumeValue() || '';
        break;
      case '--backend-url':
        args.backendUrl = consumeValue() || '';
        break;
      case '--frontend-port':
        args.frontendPort = Number(consumeValue() || DEFAULT_FRONTEND_PORT);
        break;
      case '--backend-port':
        args.backendPort = Number(consumeValue() || DEFAULT_BACKEND_PORT);
        break;
      case '--artifact-dir':
        args.artifactDir = path.resolve(consumeValue() || DEFAULT_ARTIFACT_DIR);
        args.forwarded.push('--artifact-dir', args.artifactDir);
        break;
      case '--auth': {
        const value = String(consumeValue() || DEFAULT_AUTH).toLowerCase();
        args.auth = value;
        args.forwarded.push('--auth', value);
        break;
      }
      case '--headless':
        args.headed = false;
        break;
      case '--headed':
        args.headed = true;
        args.forwarded.push('--headed');
        break;
      case '--keep-servers':
        args.keepServers = true;
        break;
      case '--keep-runtime-dir':
        args.purgeRuntimeDir = false;
        break;
      case '--help':
        console.log(usage);
        process.exit(0);
        break;
      default:
        args.forwarded.push(raw);
        if (!hasInline && argv[i + 1] && !argv[i + 1].startsWith('--')) {
          args.forwarded.push(argv[i + 1]);
          i += 1;
        }
        break;
    }
  }

  if (!['enabled', 'disabled'].includes(args.auth)) {
    throw new Error(`Invalid --auth value: ${args.auth}`);
  }
  if (!Number.isFinite(args.frontendPort) || args.frontendPort <= 0) {
    throw new Error(`Invalid --frontend-port value: ${args.frontendPort}`);
  }
  if (!Number.isFinite(args.backendPort) || args.backendPort <= 0) {
    throw new Error(`Invalid --backend-port value: ${args.backendPort}`);
  }

  if (!args.forwarded.includes('--auth')) {
    args.forwarded.push('--auth', args.auth);
  }

  if (args.forwarded.includes('--keep-open')) {
    args.keepServers = true;
  }
  if (args.headed && !args.forwarded.includes('--headed')) {
    args.forwarded.push('--headed');
  }
  if (args.auth === 'enabled' && !args.forwarded.includes('--seed-auth')) {
    args.forwarded.push('--seed-auth');
  }
  if (!args.forwarded.includes('--project-bootstrap')) {
    args.forwarded.push('--project-bootstrap', 'api');
  }

  return args;
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const buildCorsOrigins = (frontendUrl) => {
  const defaults = [
    'http://127.0.0.1:5173',
    'http://localhost:5173',
    'http://127.0.0.1:1420',
    'http://localhost:1420',
    'http://127.0.0.1:4173',
    'http://localhost:4173',
  ];
  const origins = new Set(defaults);

  try {
    const parsed = new URL(frontendUrl);
    origins.add(parsed.origin);
    if (parsed.hostname === '127.0.0.1') {
      origins.add(`${parsed.protocol}//localhost${parsed.port ? `:${parsed.port}` : ''}`);
    }
    if (parsed.hostname === 'localhost') {
      origins.add(`${parsed.protocol}//127.0.0.1${parsed.port ? `:${parsed.port}` : ''}`);
    }
  } catch {
    // Keep defaults if the URL is malformed; the readiness check will fail later anyway.
  }

  return Array.from(origins).join(',');
};

const waitForUrl = async (url, label, timeoutMs = 45000) => {
  const startedAt = Date.now();
  let lastError = null;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url, { method: 'GET' });
      if (response.ok) return;
      lastError = new Error(`${label} responded with status ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await sleep(500);
  }
  throw new Error(`Timed out waiting for ${label}: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
};

const createLogStream = async (filePath) => {
  await fsPromises.mkdir(path.dirname(filePath), { recursive: true });
  return fs.createWriteStream(filePath, { flags: 'a' });
};

const spawnManagedProcess = async ({
  label,
  command,
  args,
  cwd,
  env = {},
  logFile,
}) => {
  const logStream = await createLogStream(logFile);
  const child = spawn(command, args, {
    cwd,
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: process.platform !== 'win32',
  });

  child.stdout.on('data', chunk => logStream.write(chunk));
  child.stderr.on('data', chunk => logStream.write(chunk));
  child.on('exit', (code, signal) => {
    logStream.write(`\n[${label}] exited code=${code ?? 'null'} signal=${signal ?? 'null'}\n`);
  });

  return { label, child, logFile, logStream };
};

const stopManagedProcess = async (proc) => {
  if (!proc) return;
  const { child, logStream } = proc;
  if (!child.pid || child.exitCode !== null) {
    logStream.end();
    return;
  }

  try {
    if (process.platform !== 'win32') {
      process.kill(-child.pid, 'SIGTERM');
    } else {
      child.kill('SIGTERM');
    }
  } catch {
    // ignore missing process group
  }

  const waited = await new Promise(resolve => {
    const timer = setTimeout(() => resolve(false), 5000);
    child.once('exit', () => {
      clearTimeout(timer);
      resolve(true);
    });
  });

  if (!waited) {
    try {
      if (process.platform !== 'win32') {
        process.kill(-child.pid, 'SIGKILL');
      } else {
        child.kill('SIGKILL');
      }
    } catch {
      // ignore
    }
  }

  logStream.end();
};

const removeRuntimeDir = async (dirPath) => {
  await fsPromises.rm(dirPath, { recursive: true, force: true });
};

const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const args = parseArgs(process.argv.slice(2));
const runtimeDir = path.join(args.artifactDir, `cross-browser-runtime-${runId}`);
await fsPromises.mkdir(runtimeDir, { recursive: true });

const spawnedProcesses = [];
let cleaningUp = false;
let runtimeDirPurged = false;

const cleanup = async () => {
  if (cleaningUp) return;
  cleaningUp = true;
  if (!args.keepServers) {
    await Promise.allSettled(spawnedProcesses.reverse().map(stopManagedProcess));
  }
  if (args.keepServers || !args.purgeRuntimeDir) return;
  await removeRuntimeDir(runtimeDir).catch(() => {});
  runtimeDirPurged = true;
};

process.on('SIGINT', async () => {
  await cleanup();
  process.exit(130);
});
process.on('SIGTERM', async () => {
  await cleanup();
  process.exit(143);
});

const resolvedBackendUrl = args.backendUrl || `http://${DEFAULT_BACKEND_HOST}:${args.backendPort}`;
const resolvedFrontendUrl = args.frontendUrl || `http://${DEFAULT_FRONTEND_HOST}:${args.frontendPort}`;
const backendCorsOrigins = buildCorsOrigins(resolvedFrontendUrl);

try {
  if (!args.backendUrl) {
    const backendLog = path.join(runtimeDir, 'backend.log');
    const backendProc = await spawnManagedProcess({
      label: 'backend',
      command: pythonCommand,
      args: ['-m', 'uvicorn', 'main:app', '--host', DEFAULT_BACKEND_HOST, '--port', String(args.backendPort)],
      cwd: BACKEND_CWD,
      env: {
        BACKEND_ENV: 'production',
        BACKEND_AUTH_ENABLED: args.auth === 'enabled' ? '1' : '0',
        BACKEND_CORS_ORIGINS: backendCorsOrigins,
        BACKEND_LOG_PATH: path.join(runtimeDir, 'backend-app.log'),
        COLLAB_DB_PATH: path.join(runtimeDir, 'collab.sqlite3'),
        SESSION_STORAGE_DIR: path.join(runtimeDir, 'sessions'),
        PYTHONUNBUFFERED: '1',
      },
      logFile: backendLog,
    });
    spawnedProcesses.push(backendProc);
    console.log(`[smoke-wrap] backend starting -> ${resolvedBackendUrl}`);
    console.log(`[smoke-wrap] backend log -> ${backendLog}`);
    await waitForUrl(`${resolvedBackendUrl}/config/auth`, 'backend /config/auth');
  } else {
    console.log(`[smoke-wrap] reusing backend -> ${resolvedBackendUrl}`);
    await waitForUrl(`${resolvedBackendUrl}/config/auth`, 'backend /config/auth');
  }

  if (!args.frontendUrl) {
    const frontendLog = path.join(runtimeDir, 'frontend.log');
    const frontendProc = await spawnManagedProcess({
      label: 'frontend',
      command: npmCommand,
      args: ['run', 'dev', '--', '--host', DEFAULT_FRONTEND_HOST, '--port', String(args.frontendPort)],
      cwd: REPO_ROOT,
      env: {
        BROWSER: 'none',
      },
      logFile: frontendLog,
    });
    spawnedProcesses.push(frontendProc);
    console.log(`[smoke-wrap] frontend starting -> ${resolvedFrontendUrl}`);
    console.log(`[smoke-wrap] frontend log -> ${frontendLog}`);
    await waitForUrl(resolvedFrontendUrl, 'frontend root');
  } else {
    console.log(`[smoke-wrap] reusing frontend -> ${resolvedFrontendUrl}`);
    await waitForUrl(resolvedFrontendUrl, 'frontend root');
  }

  const smokeArgs = [
    path.join(REPO_ROOT, 'scripts', 'cross_browser_collab_smoke.mjs'),
    '--backend-url', resolvedBackendUrl,
    '--frontend-url', resolvedFrontendUrl,
    ...args.forwarded,
  ];

  console.log(`[smoke-wrap] smoke run -> node ${smokeArgs.join(' ')}`);
  const smokeExitCode = await new Promise((resolve) => {
    const child = spawn(process.execPath, smokeArgs, {
      cwd: REPO_ROOT,
      stdio: 'inherit',
      env: process.env,
    });
    child.on('exit', (code, signal) => {
      if (signal) resolve(1);
      else resolve(code ?? 1);
    });
  });

  console.log(`[smoke-wrap] runtime dir -> ${runtimeDir}`);
  if (smokeExitCode !== 0) {
    process.exitCode = smokeExitCode;
  }
} finally {
  await cleanup();
  console.log(`[smoke-wrap] runtime dir cleanup -> ${runtimeDirPurged ? 'purged' : 'kept'}`);
}
