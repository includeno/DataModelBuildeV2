#!/usr/bin/env node

import fs from 'fs/promises';
import path from 'path';
import { chromium } from '@playwright/test';

const DEFAULT_FRONTEND_URL = 'http://localhost:1420';
const DEFAULT_BACKEND_URL = 'http://127.0.0.1:8000';
const DEFAULT_DATASET = path.resolve('test_data/customers.csv');
const DEFAULT_ARTIFACT_DIR = path.resolve('output/playwright');
const AUTH_STORAGE_KEY = 'dmb_auth_tokens_v1';

const usage = `Usage:
  node scripts/cross_browser_collab_smoke.mjs [options]

Options:
  --frontend-url <url>         Frontend app URL. Default: ${DEFAULT_FRONTEND_URL}
  --backend-url <url>          Backend API URL. Default: ${DEFAULT_BACKEND_URL}
  --headed                     Launch visible Chrome + Edge windows
  --slow-mo <ms>               Slow down browser actions. Default: 0
  --auth <enabled|disabled>    Collaboration mode. Default: disabled
  --dataset <path>             Dataset file to upload. Default: ${DEFAULT_DATASET}
  --dataset-name <name>        Dataset name used in UI. Default: customers
  --project-label <name>       Project label used in result output
  --project-bootstrap <mode>   Project creation mode: ui|api. Default: ui
  --primary-edit <name>        Operation name written by primary browser
  --secondary-edit <name>      Operation name written by secondary browser
  --invite-role <role>         Secondary member role. Default: editor
  --artifact-dir <path>        Directory for JSON/screenshots. Default: ${DEFAULT_ARTIFACT_DIR}
  --output <path>              Result JSON file path. Default: artifact dir auto file
  --seed-auth                  Seed browser auth tokens from API login for a deterministic auth-enabled smoke run
  --keep-open                  Keep browsers open after success/failure
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
    datasetName: 'customers',
    projectLabel: `Cross Browser Project ${runSafeTimestamp()}`,
    projectBootstrap: 'ui',
    primaryEdit: 'Shared Op Primary',
    secondaryEdit: 'Shared Op Secondary',
    inviteRole: 'editor',
    artifactDir: DEFAULT_ARTIFACT_DIR,
    outputPath: '',
    seedAuth: false,
    keepOpen: false,
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
      case '--dataset-name':
        args.datasetName = consumeValue() || 'customers';
        break;
      case '--project-label':
        args.projectLabel = consumeValue() || args.projectLabel;
        break;
      case '--project-bootstrap':
        args.projectBootstrap = String(consumeValue() || 'ui').toLowerCase();
        break;
      case '--primary-edit':
        args.primaryEdit = consumeValue() || 'Shared Op Primary';
        break;
      case '--secondary-edit':
        args.secondaryEdit = consumeValue() || 'Shared Op Secondary';
        break;
      case '--invite-role':
        args.inviteRole = consumeValue() || 'editor';
        break;
      case '--artifact-dir':
        args.artifactDir = path.resolve(consumeValue() || DEFAULT_ARTIFACT_DIR);
        break;
      case '--output':
        args.outputPath = path.resolve(consumeValue() || '');
        break;
      case '--seed-auth':
        args.seedAuth = true;
        break;
      case '--keep-open':
        args.keepOpen = true;
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
  if (!['ui', 'api'].includes(args.projectBootstrap)) {
    throw new Error(`Invalid --project-bootstrap value: ${args.projectBootstrap}`);
  }

  return args;
};

function runSafeTimestamp() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

const args = parseArgs(process.argv.slice(2));
const runId = Date.now().toString(36);
const artifactBaseName = `cross-browser-collab-${runSafeTimestamp()}-${runId}`;
const resultOutputPath = args.outputPath || path.join(args.artifactDir, `${artifactBaseName}.json`);
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

const withTimeout = async (promise, label, timeoutMs = 5000) => {
  let timer = null;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
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

const createProjectViaApi = async (token) => {
  logStep('primary createProjectApi:start', args.projectLabel);
  const res = await requestJson(`${args.backendUrl}/projects`, {
    method: 'POST',
    token,
    body: {
      name: args.projectLabel,
      description: 'Cross-browser collaboration smoke project',
    },
    allowed: [200, 201],
  });
  const created = res.data;
  logStep('primary createProjectApi:done', created.id);
  return created;
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

const listProjectMembers = async (projectId, token) => {
  const res = await requestJson(`${args.backendUrl}/projects/${projectId}/members`, {
    token,
  });
  return Array.isArray(res.data) ? res.data : [];
};

const ensureArtifactDir = async () => {
  await fs.mkdir(args.artifactDir, { recursive: true });
};

const writeResultFile = async (payload) => {
  await ensureArtifactDir();
  await fs.writeFile(resultOutputPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
};

const clickElement = async (locator) => {
  await locator.evaluate((element) => {
    element.scrollIntoView({ block: 'center', inline: 'center' });
    (element).click();
  });
};

const clickToolbarButton = async (page, name) => {
  await clickElement(page.getByRole('button', { name }).first());
  await page.waitForTimeout(150);
};

const dismissTransientUi = async (page) => {
  await page.keyboard.press('Escape').catch(() => {});
  const switchOrManage = page.getByText(/switch or manage/i).first();
  const createProjectButton = page.getByRole('button', { name: /Create New Project/i }).first();
  const projectMenuVisible =
    (await switchOrManage.isVisible().catch(() => false)) ||
    (await createProjectButton.isVisible().catch(() => false));
  if (projectMenuVisible) {
    await clickElement(page.locator('[title="Project Switcher"]').first()).catch(() => {});
  }
  await page.waitForTimeout(200);
};

const matchesExpectedBackend = async (page) => {
  return page.locator(`[title^="Connected Server: ${args.backendUrl}"]`).first().isVisible().catch(() => false);
};

const seedConnection = async (page, browserName, authTokens = null) => {
  logStep(`${browserName} seedConnection:start`);
  await page.addInitScript(({ state, storedAuth, authStorageKey }) => {
    window.localStorage.clear();
    window.sessionStorage.clear();
    window.localStorage.setItem('dmb_connection_state_v1', JSON.stringify(state));
    if (storedAuth?.accessToken) {
      window.localStorage.setItem(authStorageKey, JSON.stringify(storedAuth));
    }
  }, {
    state: connectionState,
    storedAuth: authTokens,
    authStorageKey: AUTH_STORAGE_KEY,
  });
  await page.goto(args.frontendUrl);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (attempt > 0) {
      await page.reload();
    }
    try {
      await waitFor(async () => {
        if (await matchesExpectedBackend(page)) return 'connected';
        if (args.auth === 'enabled' && await page.getByRole('heading', { name: '登录协作空间' }).isVisible().catch(() => false)) return 'login';
        return null;
      }, `${browserName} expected backend shell`, 15000, 300);
      logStep(`${browserName} seedConnection:done`);
      return;
    } catch (error) {
      if (attempt === 1) throw error;
    }
  }
  logStep(`${browserName} seedConnection:done`);
};

const waitForLoginOrShell = async (page) => {
  return waitFor(async () => {
    if (await page.getByRole('heading', { name: '登录协作空间' }).isVisible().catch(() => false)) return 'login';
    if (await page.locator('button[title="Log out"]').isVisible().catch(() => false)) return 'authenticated';
    if (await page.locator('[title="Project Switcher"]').isVisible().catch(() => false)) return 'shell';
    return null;
  }, 'login page or app shell');
};

const loginViaUi = async (page, user, browserName) => {
  logStep(`${browserName} login:start`, user.email);
  const state = await waitForLoginOrShell(page);
  if (state === 'authenticated') {
    logStep(`${browserName} login:skipped`, 'already authenticated');
    return;
  }
  if (state === 'shell') {
    await waitFor(async () => {
      if (await page.getByRole('heading', { name: '登录协作空间' }).isVisible().catch(() => false)) return 'login';
      if (await page.locator('button[title="Log out"]').isVisible().catch(() => false)) return 'authenticated';
      return null;
    }, `${browserName} auth gate`, 10000, 250);
    if (await page.locator('button[title="Log out"]').isVisible().catch(() => false)) {
      logStep(`${browserName} login:skipped`, 'already authenticated');
      return;
    }
  }

  await page.locator('#dmb-login-email').fill(user.email);
  await page.locator('#dmb-login-password').fill(user.password);
  await page.getByRole('button', { name: '登录' }).click();
  await page.locator('button[title="Log out"]').waitFor({ timeout: 15000 });
  logStep(`${browserName} login:done`);
};

const createProjectViaUi = async (page, browserName, token) => {
  const before = await listProjects(token);
  logStep(`${browserName} createProject:start`, `before=${before.length}`);
  await clickElement(page.locator('[title="Project Switcher"]').first());
  const createProjectButton = page.getByRole('button', { name: 'Create New Project' }).first();
  await createProjectButton.waitFor({ state: 'visible', timeout: 10000 });
  await createProjectButton.click();

  await waitFor(async () => {
    const currentText = ((await page.locator('[title="Project Switcher"]').textContent().catch(() => '')) || '').trim();
    return currentText && !currentText.includes('Create Project') ? currentText : null;
  }, `${browserName} project switcher text`);

  const created = await waitFor(async () => {
    const after = await listProjects(token);
    return after.find((project) => !before.some((prev) => prev.id === project.id)) || null;
  }, 'new project in backend');

  await dismissTransientUi(page);
  logStep(`${browserName} createProject:done`, `${created.id}`);
  return created;
};

const importDatasetViaUi = async (page, datasetPath, browserName) => {
  logStep(`${browserName} importDataset:start`, path.basename(datasetPath));
  await dismissTransientUi(page);
  await page.getByRole('button', { name: 'Import Dataset' }).first().click();
  const modal = page.locator('div.fixed.inset-0.z-50:has([aria-label="Close Import Data Source"])').first();
  await modal.getByRole('heading', { name: 'Import Data Source' }).waitFor({ timeout: 10000 });
  await modal.getByTestId('data-import-file-input').setInputFiles(datasetPath);
  await modal.getByPlaceholder('Enter a name for this dataset').fill(args.datasetName);

  const previewButton = modal.getByRole('button', { name: 'Next: Preview' });
  if (await previewButton.isVisible().catch(() => false)) {
    await clickElement(previewButton);
  }

  const importButton = modal.getByRole('button', { name: 'Import Dataset' });
  await importButton.waitFor({ state: 'visible', timeout: 10000 });
  await clickElement(importButton);

  await page.getByText(args.datasetName, { exact: true }).waitFor({ timeout: 15000 });
  await modal.waitFor({ state: 'detached', timeout: 10000 }).catch(() => {});
  await dismissTransientUi(page);
  logStep(`${browserName} importDataset:done`);
};

const addWorkflowAndRun = async (page, browserName) => {
  logStep(`${browserName} workflow:start`);
  await dismissTransientUi(page);
  await clickToolbarButton(page, 'Workflow');
  await page.getByRole('button', { name: 'Add Data Source' }).click();
  await page.getByText('-- Select Dataset --', { exact: true }).last().click();
  await page.getByRole('option', { name: new RegExp(args.datasetName) }).first().click();

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
  await dismissTransientUi(page);
  await clickToolbarButton(page, 'SQL Studio');
  const editor = page.getByRole('textbox', { name: /Enter your SQL query/i });
  await editor.waitFor({ timeout: 10000 });
  await editor.fill(`select count(*) as total_customers from ${args.datasetName};`);
  await page.getByRole('button', { name: 'Run Query' }).click();
  await page.getByText('1 Rows', { exact: true }).waitFor({ timeout: 15000 });
  await page.getByText('5', { exact: true }).waitFor({ timeout: 15000 });
  logStep(`${browserName} sql:done`);
};

const openDataViewer = async (page, browserName) => {
  logStep(`${browserName} dataViewer:start`);
  await dismissTransientUi(page);
  await clickToolbarButton(page, 'Data Viewer');
  await page.getByText('Raw Data Viewer', { exact: true }).waitFor({ timeout: 10000 });
  await page.getByTitle(args.datasetName).first().waitFor({ timeout: 10000 });
  logStep(`${browserName} dataViewer:done`);
};

const ensureProjectLoaded = async (page, browserName, project) => {
  const targetTexts = [project?.name, project?.id].filter(Boolean);
  const switcher = page.locator('[title="Project Switcher"]').first();

  const hasTargetProject = async () => {
    const text = ((await switcher.textContent().catch(() => '')) || '').trim();
    if (!text || text.includes('Create Project')) return null;
    return targetTexts.some(target => text.includes(target)) ? text : null;
  };

  const selectProjectFromMenu = async () => {
    await clickElement(switcher);
    for (const target of targetTexts) {
      const item = page.getByText(target, { exact: false }).first();
      const visible = await item.isVisible().catch(() => false);
      if (!visible) continue;
      await clickElement(item);
      return;
    }
    throw new Error(`Project ${project.id} was not visible in the project switcher`);
  };

  logStep(`${browserName} projectLoad:start`, project.id);
  try {
    await waitFor(hasTargetProject, `${browserName} project auto-load`, 10000, 400);
  } catch {
    await page.reload();
    await switcher.waitFor({ state: 'visible', timeout: 15000 });
    try {
      await waitFor(hasTargetProject, `${browserName} project auto-load after reload`, 8000, 400);
    } catch {
      await selectProjectFromMenu();
      await waitFor(hasTargetProject, `${browserName} selected project`, 15000, 400);
    }
  }
  await dismissTransientUi(page);
  await page.waitForTimeout(1000);
  logStep(`${browserName} projectLoad:done`);
};

const openOperationEditor = async (page, browserName) => {
  logStep(`${browserName} openOperation:start`);
  await dismissTransientUi(page);
  await clickToolbarButton(page, 'Workflow');
  const operationNode = page.locator('[title="Operation 1"]').first();
  await operationNode.waitFor({ timeout: 15000 });
  await clickElement(operationNode);
  await page.getByRole('textbox', { name: 'Operation Name' }).waitFor({ timeout: 10000 });
  logStep(`${browserName} openOperation:done`);
};

const waitForInputValue = async (locator, expected, label) => {
  await waitFor(async () => {
    const value = await locator.inputValue();
    return value === expected ? value : null;
  }, label, 15000, 400);
};

const waitForSavedState = async (page, browserName) => {
  return waitFor(async () => {
    const title = await page.locator('[title^="项目保存状态："]').getAttribute('title').catch(() => null);
    if (!title) return null;
    return title.includes('已保存') || title.includes('未修改') ? title : null;
  }, `${browserName} save status settled`, 15000, 400);
};

const launchBrowser = (channel) => chromium.launch({
  channel,
  headless: !args.headed,
  slowMo: args.slowMo,
});

const captureFailureArtifacts = async (primaryPage, secondaryPage) => {
  await ensureArtifactDir();
  const actions = [];
  if (primaryPage) {
    actions.push(primaryPage.screenshot({ path: path.join(args.artifactDir, `${artifactBaseName}-primary.png`), fullPage: true }).catch(() => {}));
  }
  if (secondaryPage) {
    actions.push(secondaryPage.screenshot({ path: path.join(args.artifactDir, `${artifactBaseName}-secondary.png`), fullPage: true }).catch(() => {}));
  }
  await Promise.all(actions);
};

const authInfo = await ensureAuthMode();
const primaryAuth = await registerUser(users.primary);
const secondaryAuth = await registerUser(users.secondary);
let bootstrappedProject = null;

if (args.projectBootstrap === 'api') {
  bootstrappedProject = await createProjectViaApi(primaryAuth?.accessToken);
}

const primaryBrowser = await launchBrowser(args.primaryChannel);
const secondaryBrowser = await launchBrowser(args.secondaryChannel);
const contextOptions = {
  viewport: { width: 1600, height: 1000 },
};
const primaryContext = await primaryBrowser.newContext(contextOptions);
const secondaryContext = await secondaryBrowser.newContext(contextOptions);
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
  datasetName: args.datasetName,
  projectLabel: args.projectLabel,
  artifactDir: args.artifactDir,
  resultFile: resultOutputPath,
};

try {
  await seedConnection(primaryPage, 'primary', args.seedAuth ? primaryAuth : null);
  if (args.auth === 'enabled') {
    await loginViaUi(primaryPage, users.primary, 'primary');
  }

  const project = bootstrappedProject || await createProjectViaUi(primaryPage, 'primary', primaryAuth?.accessToken);
  result.projectId = project.id;
  result.projectName = project.name;

  if (args.auth === 'enabled') {
    await inviteProjectMember(project.id, primaryAuth.accessToken, users.secondary.email, args.inviteRole);
    result.inviteRole = args.inviteRole;
    result.secondaryInvited = true;
    const projectMembers = await listProjectMembers(project.id, primaryAuth.accessToken);
    result.projectMembers = projectMembers.map((member) => ({
      email: member.email,
      role: member.role,
      displayName: member.displayName,
    }));
  }

  if (args.projectBootstrap === 'api') {
    await ensureProjectLoaded(primaryPage, 'primary', project);
  }

  await importDatasetViaUi(primaryPage, args.dataset, 'primary');
  await addWorkflowAndRun(primaryPage, 'primary');
  await runSqlQuery(primaryPage, 'primary');
  await openDataViewer(primaryPage, 'primary');

  await seedConnection(secondaryPage, 'secondary', args.seedAuth ? secondaryAuth : null);
  if (args.auth === 'enabled') {
    await loginViaUi(secondaryPage, users.secondary, 'secondary');
  }
  await ensureProjectLoaded(secondaryPage, 'secondary', project);
  await openOperationEditor(primaryPage, 'primary');
  await openOperationEditor(secondaryPage, 'secondary');

  const primaryInput = primaryPage.getByRole('textbox', { name: 'Operation Name' });
  const secondaryInput = secondaryPage.getByRole('textbox', { name: 'Operation Name' });

  await primaryInput.fill(args.primaryEdit);
  await waitForInputValue(secondaryInput, args.primaryEdit, 'secondary to receive primary edit');
  result.secondarySawPrimaryEdit = await secondaryInput.inputValue();

  await secondaryInput.fill(args.secondaryEdit);
  await waitForInputValue(primaryInput, args.secondaryEdit, 'primary to receive secondary edit');
  result.primarySawSecondaryEdit = await primaryInput.inputValue();

  await runSqlQuery(secondaryPage, 'secondary');
  await openDataViewer(secondaryPage, 'secondary');

  const [primarySaveTitle, secondarySaveTitle] = await Promise.all([
    waitForSavedState(primaryPage, 'primary'),
    waitForSavedState(secondaryPage, 'secondary'),
  ]);

  result.primarySaveTitle = primarySaveTitle;
  result.secondarySaveTitle = secondarySaveTitle;
  result.primaryRealtimeTitle = await primaryPage.locator('[title^="实时协作状态："]').getAttribute('title');
  result.secondaryRealtimeTitle = await secondaryPage.locator('[title^="实时协作状态："]').getAttribute('title');

  await writeResultFile(result);
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  result.error = error instanceof Error ? error.message : String(error);
  await captureFailureArtifacts(primaryPage, secondaryPage);
  await writeResultFile(result);
  console.error(JSON.stringify(result, null, 2));
  process.exitCode = 1;
} finally {
  logStep('cleanup:start');
  if (!args.keepOpen) {
    const cleanupTasks = [
      ['secondaryContext.close', () => secondaryContext.close()],
      ['primaryContext.close', () => primaryContext.close()],
      ['secondaryBrowser.close', () => secondaryBrowser.close()],
      ['primaryBrowser.close', () => primaryBrowser.close()],
    ];
    await Promise.allSettled(
      cleanupTasks.map(async ([label, action]) => {
        try {
          await withTimeout(action(), label, 5000);
        } catch (error) {
          logStep('cleanup:warn', `${label} :: ${error instanceof Error ? error.message : String(error)}`);
        }
      }),
    );
  }
  logStep('cleanup:done');
}
