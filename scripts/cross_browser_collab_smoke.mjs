#!/usr/bin/env node

import path from 'path';
import { chromium } from '@playwright/test';

const DEFAULT_FRONTEND_URL = 'http://localhost:1420';
const DEFAULT_BACKEND_URL = 'http://127.0.0.1:8000';
const DEFAULT_DATASET = path.resolve('test_data/customers.csv');

const usage = `Usage:
  node scripts/cross_browser_collab_smoke.mjs [options]

Options:
  --frontend-url <url>         Frontend app URL. Default: ${DEFAULT_FRONTEND_URL}
  --backend-url <url>          Backend API URL. Default: ${DEFAULT_BACKEND_URL}
  --headed                     Launch visible Chrome + Edge windows
  --slow-mo <ms>               Slow down browser actions. Default: 0
  --auth <enabled|disabled>    Collaboration mode. Default: disabled
  --dataset <path>             Dataset file to upload. Default: ${DEFAULT_DATASET}
  --primary-channel <channel>  Primary browser channel. Default: chrome
  --secondary-channel <chan>   Secondary browser channel. Default: msedge
  --primary-email <email>      Primary user email
  --primary-password <pass>    Primary user password
  --secondary-email <email>    Secondary user email
  --secondary-password <pass>  Secondary user password
  --help                       Show this help

Examples:
  node scripts/cross_browser_collab_smoke.mjs --headed
  node scripts/cross_browser_collab_smoke.mjs --auth enabled --backend-url http://127.0.0.1:8001 --headed
`;

const parseArgs = (argv) => {
  const args = {
    frontendUrl: DEFAULT_FRONTEND_URL,
    backendUrl: DEFAULT_BACKEND_URL,
    headed: false,
    slowMo: 0,
    auth: 'disabled',
    dataset: DEFAULT_DATASET,
    primaryChannel: 'chrome',
    secondaryChannel: 'msedge',
    primaryEmail: '',
    primaryPassword: '',
    secondaryEmail: '',
    secondaryPassword: '',
  };

  for (let i = 0; i < argv.length; i += 1) {
    const raw = argv[i];
    if (!raw.startsWith('--')) continue;
    const [flag, inlineValue] = raw.split('=', 2);
    const nextValue = inlineValue ?? argv[i + 1];

    const consumeValue = () => {
      if (inlineValue === undefined) i += 1;
      return nextValue;
    };

    switch (flag) {
      case '--frontend-url':
        args.frontendUrl = consumeValue();
        break;
      case '--backend-url':
        args.backendUrl = consumeValue();
        break;
      case '--headed':
        args.headed = true;
        break;
      case '--slow-mo':
        args.slowMo = Number(consumeValue() || 0);
        break;
      case '--auth':
        args.auth = String(consumeValue() || 'disabled').toLowerCase();
        break;
      case '--dataset':
        args.dataset = path.resolve(consumeValue() || DEFAULT_DATASET);
        break;
      case '--primary-channel':
        args.primaryChannel = consumeValue() || 'chrome';
        break;
      case '--secondary-channel':
        args.secondaryChannel = consumeValue() || 'msedge';
        break;
      case '--primary-email':
        args.primaryEmail = consumeValue() || '';
        break;
      case '--primary-password':
        args.primaryPassword = consumeValue() || '';
        break;
      case '--secondary-email':
        args.secondaryEmail = consumeValue() || '';
        break;
      case '--secondary-password':
        args.secondaryPassword = consumeValue() || '';
        break;
      case '--help':
        console.log(usage);
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown option: ${flag}`);
    }
  }

  if (!['enabled', 'disabled'].includes(args.auth)) {
    throw new Error(`Invalid --auth value: ${args.auth}`);
  }

  return args;
};

const args = parseArgs(process.argv.slice(2));
const runId = Date.now().toString(36);
const passwords = {
  primary: args.primaryPassword || 'Passw0rd!',
  secondary: args.secondaryPassword || 'Passw0rd!',
};
const users = {
  primary: {
    email: args.primaryEmail || `chrome_${runId}@example.com`,
    password: passwords.primary,
    displayName: 'Chrome User',
  },
  secondary: {
    email: args.secondaryEmail || `edge_${runId}@example.com`,
    password: passwords.secondary,
    displayName: 'Edge User',
  },
};

const connectionState = {
  apiConfig: { baseUrl: args.backendUrl, isMock: false },
  knownServers: ['mockServer', 'http://localhost:8000', 'http://127.0.0.1:8000', args.backendUrl],
  savedAt: Date.now(),
};

const logStep = (step, detail = '') => {
  console.log(`[step] ${step}${detail ? ` :: ${detail}` : ''}`);
};

const requestJson = async (url, options = {}) => {
  const {
    method = 'GET',
    body,
    token,
    allowed = [200],
  } = options;

  const headers = {};
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (token) headers.Authorization = `Bearer ${token}`;

  const res = await fetch(url, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!allowed.includes(res.status)) {
    throw new Error(`${method} ${url} failed with ${res.status}: ${typeof data === 'string' ? data : JSON.stringify(data)}`);
  }

  return { status: res.status, data };
};

const waitFor = async (fn, label, timeoutMs = 20000, intervalMs = 500) => {
  const startedAt = Date.now();
  let lastError = null;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  if (lastError) throw lastError;
  throw new Error(`Timed out waiting for ${label}`);
};

const ensureAuthMode = async () => {
  const cfg = await requestJson(`${args.backendUrl}/config/auth`);
  const enabled = Boolean(cfg.data?.authEnabled);
  if (args.auth === 'enabled' && !enabled) {
    throw new Error(`Backend ${args.backendUrl} is not auth-enabled. Restart backend with auth enabled and retry.`);
  }
  if (args.auth === 'disabled' && enabled) {
    throw new Error(`Backend ${args.backendUrl} requires auth. Re-run with --auth enabled or point to a no-auth backend.`);
  }
  return cfg.data;
};

const registerUser = async (user) => {
  if (args.auth !== 'enabled') return null;
  try {
    await requestJson(`${args.backendUrl}/auth/register`, {
      method: 'POST',
      body: user,
    });
  } catch (error) {
    const message = String(error.message || '');
    if (!message.includes('email already exists')) throw error;
  }
  const login = await requestJson(`${args.backendUrl}/auth/login`, {
    method: 'POST',
    body: { email: user.email, password: user.password },
  });
  return login.data;
};

const listProjects = async (token) => {
  const res = await requestJson(`${args.backendUrl}/projects`, {
    token,
  });
  return Array.isArray(res.data) ? res.data : [];
};

const inviteProjectMember = async (projectId, token, email, role = 'editor') => {
  await requestJson(`${args.backendUrl}/projects/${projectId}/members`, {
    method: 'POST',
    token,
    body: {
      memberEmail: email,
      role,
    },
  });
};

const seedConnection = async (page, browserName) => {
  logStep(`${browserName} seedConnection:start`);
  await page.goto(args.frontendUrl);
  await page.evaluate((state) => {
    localStorage.clear();
    localStorage.setItem('dmb_connection_state_v1', JSON.stringify(state));
  }, connectionState);
  await page.reload();
  await page.locator('[title^="Connected Server:"]').waitFor({ timeout: 15000 });
  logStep(`${browserName} seedConnection:done`);
};

const waitForLoginOrShell = async (page) => {
  return waitFor(async () => {
    if (await page.getByRole('heading', { name: '登录协作空间' }).isVisible().catch(() => false)) return 'login';
    if (await page.locator('[title="Project Switcher"]').isVisible().catch(() => false)) return 'app';
    return null;
  }, 'login page or app shell');
};

const loginViaUi = async (page, user, browserName) => {
  logStep(`${browserName} login:start`, user.email);
  const state = await waitForLoginOrShell(page);
  if (state === 'app') {
    logStep(`${browserName} login:skipped`, 'already authenticated');
    return;
  }

  await page.locator('#dmb-login-email').fill(user.email);
  await page.locator('#dmb-login-password').fill(user.password);
  await page.getByRole('button', { name: '登录' }).click();
  await page.locator('[title="Project Switcher"]').waitFor({ timeout: 15000 });
  logStep(`${browserName} login:done`);
};

const createProjectViaUi = async (page, browserName, token) => {
  const before = await listProjects(token);
  logStep(`${browserName} createProject:start`, `before=${before.length}`);
  await page.locator('[title="Project Switcher"]').click();
  await page.getByRole('button', { name: 'Create New Project' }).click();

  const created = await waitFor(async () => {
    const after = await listProjects(token);
    return after.find((project) => !before.some((prev) => prev.id === project.id)) || null;
  }, 'new project in backend');

  logStep(`${browserName} createProject:done`, `${created.id}`);
  return created;
};

const importDatasetViaUi = async (page, datasetPath, browserName) => {
  logStep(`${browserName} importDataset:start`, path.basename(datasetPath));
  await page.getByRole('button', { name: 'Import Dataset' }).first().click();
  await page.getByRole('heading', { name: 'Import Data Source' }).waitFor({ timeout: 10000 });
  await page.locator('input[type="file"]').setInputFiles(datasetPath);
  await page.getByPlaceholder('Enter a name for this dataset').fill('customers');
  await page.getByRole('button', { name: 'Import Dataset' }).last().click();
  await page.getByText('customers', { exact: true }).waitFor({ timeout: 15000 });
  logStep(`${browserName} importDataset:done`);
};

const addWorkflowAndRun = async (page, browserName) => {
  logStep(`${browserName} workflow:start`);
  await page.getByRole('button', { name: 'Workflow' }).click();
  await page.getByRole('button', { name: 'Add Data Source' }).click();
  await page.getByText('-- Select Dataset --', { exact: true }).last().click();
  await page.getByRole('option', { name: /customers/ }).first().click();

  await page.getByRole('button', { name: 'Add Child' }).first().click();
  await page.getByText('Operation 1', { exact: true }).waitFor({ timeout: 10000 });
  await page.getByText('Operation 1', { exact: true }).click();
  await page.getByText('Add your first command', { exact: true }).click();
  await page.locator('select').nth(1).selectOption({ index: 1 });
  await page.getByRole('button', { name: 'Run this operation' }).click();
  await page.getByText('Execution Result', { exact: true }).waitFor({ timeout: 15000 });
  await page.getByText('5 Rows', { exact: true }).waitFor({ timeout: 15000 });
  logStep(`${browserName} workflow:done`);
};

const runSqlQuery = async (page, browserName) => {
  logStep(`${browserName} sql:start`);
  await page.getByRole('button', { name: 'SQL Studio' }).click();
  const editor = page.getByRole('textbox', { name: /Enter your SQL query/i });
  await editor.waitFor({ timeout: 10000 });
  await editor.fill('select count(*) as total_customers from customers;');
  await page.getByRole('button', { name: 'Run Query' }).click();
  await page.getByText('1 Rows', { exact: true }).waitFor({ timeout: 15000 });
  await page.getByText('5', { exact: true }).waitFor({ timeout: 15000 });
  logStep(`${browserName} sql:done`);
};

const openDataViewer = async (page, browserName) => {
  logStep(`${browserName} dataViewer:start`);
  await page.getByRole('button', { name: 'Data Viewer' }).click();
  await page.getByText('Raw Data Viewer', { exact: true }).waitFor({ timeout: 10000 });
  await page.getByText('customers', { exact: true }).waitFor({ timeout: 10000 });
  logStep(`${browserName} dataViewer:done`);
};

const ensureProjectLoaded = async (page, browserName, projectId) => {
  logStep(`${browserName} projectLoad:start`, projectId);
  await waitFor(async () => {
    const text = ((await page.locator('[title="Project Switcher"]').textContent().catch(() => '')) || '').trim();
    return text && !text.includes('Create Project') ? text : null;
  }, `${browserName} project auto-load`, 15000, 500).catch(async () => {
    await page.locator('[title="Project Switcher"]').click();
    await page.getByText(projectId, { exact: false }).waitFor({ timeout: 15000 });
    await page.getByText(projectId, { exact: false }).click();
  });
  await page.waitForTimeout(1000);
  logStep(`${browserName} projectLoad:done`);
};

const openOperationEditor = async (page, browserName) => {
  logStep(`${browserName} openOperation:start`);
  await page.getByRole('button', { name: 'Workflow' }).click();
  await page.getByText('Operation 1', { exact: true }).waitFor({ timeout: 15000 });
  await page.getByText('Operation 1', { exact: true }).click();
  await page.getByRole('textbox', { name: 'Operation Name' }).waitFor({ timeout: 10000 });
  logStep(`${browserName} openOperation:done`);
};

const waitForInputValue = async (locator, expected, label) => {
  await waitFor(async () => {
    const value = await locator.inputValue();
    return value === expected ? value : null;
  }, label, 15000, 400);
};

const launchBrowser = (channel) => chromium.launch({
  channel,
  headless: !args.headed,
  slowMo: args.slowMo,
});

const authInfo = await ensureAuthMode();
const primaryAuth = await registerUser(users.primary);
const secondaryAuth = await registerUser(users.secondary);

const primaryBrowser = await launchBrowser(args.primaryChannel);
const secondaryBrowser = await launchBrowser(args.secondaryChannel);
const primaryContext = await primaryBrowser.newContext();
const secondaryContext = await secondaryBrowser.newContext();
const primaryPage = await primaryContext.newPage();
const secondaryPage = await secondaryContext.newPage();

const result = {
  authMode: authInfo.mode,
  backendUrl: args.backendUrl,
  frontendUrl: args.frontendUrl,
  primaryBrowser: args.primaryChannel,
  secondaryBrowser: args.secondaryChannel,
  primaryUser: users.primary.email,
  secondaryUser: users.secondary.email,
  dataset: args.dataset,
};

try {
  await seedConnection(primaryPage, 'primary');
  if (args.auth === 'enabled') {
    await loginViaUi(primaryPage, users.primary, 'primary');
  }

  const project = await createProjectViaUi(primaryPage, 'primary', primaryAuth?.accessToken);
  result.projectId = project.id;
  result.projectName = project.name;

  if (args.auth === 'enabled') {
    await inviteProjectMember(project.id, primaryAuth.accessToken, users.secondary.email, 'editor');
    result.secondaryInvited = true;
  }

  await importDatasetViaUi(primaryPage, args.dataset, 'primary');
  await addWorkflowAndRun(primaryPage, 'primary');
  await runSqlQuery(primaryPage, 'primary');
  await openDataViewer(primaryPage, 'primary');

  await seedConnection(secondaryPage, 'secondary');
  if (args.auth === 'enabled') {
    await loginViaUi(secondaryPage, users.secondary, 'secondary');
  }
  await ensureProjectLoaded(secondaryPage, 'secondary', project.id);
  await openOperationEditor(primaryPage, 'primary');
  await openOperationEditor(secondaryPage, 'secondary');

  const primaryInput = primaryPage.getByRole('textbox', { name: 'Operation Name' });
  const secondaryInput = secondaryPage.getByRole('textbox', { name: 'Operation Name' });

  await primaryInput.fill('Shared Op Primary');
  await waitForInputValue(secondaryInput, 'Shared Op Primary', 'secondary to receive primary edit');
  result.secondarySawPrimaryEdit = await secondaryInput.inputValue();

  await secondaryInput.fill('Shared Op Secondary');
  await waitForInputValue(primaryInput, 'Shared Op Secondary', 'primary to receive secondary edit');
  result.primarySawSecondaryEdit = await primaryInput.inputValue();

  await runSqlQuery(secondaryPage, 'secondary');
  await openDataViewer(secondaryPage, 'secondary');

  result.primarySaveTitle = await primaryPage.locator('[title^="项目保存状态："]').getAttribute('title');
  result.secondarySaveTitle = await secondaryPage.locator('[title^="项目保存状态："]').getAttribute('title');
  result.primaryRealtimeTitle = await primaryPage.locator('[title^="实时协作状态："]').getAttribute('title');
  result.secondaryRealtimeTitle = await secondaryPage.locator('[title^="实时协作状态："]').getAttribute('title');

  console.log(JSON.stringify(result, null, 2));
} finally {
  logStep('cleanup:start');
  await Promise.allSettled([
    secondaryContext.close(),
    primaryContext.close(),
    secondaryBrowser.close(),
    primaryBrowser.close(),
  ]);
  logStep('cleanup:done');
}
