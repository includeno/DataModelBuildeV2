import { test, expect, type BrowserContext, type Locator, type Page } from '@playwright/test';
import path from 'path';

const projectRoot = path.resolve(__dirname, '../..');
const testDataDir = path.join(projectRoot, 'test_data');

async function selectOptionByPartialText(select: Locator, partial: string) {
  const value = await select.evaluate((el, text) => {
    const selectEl = el as HTMLSelectElement;
    const option = Array.from(selectEl.options).find(opt =>
      (opt.textContent || '').includes(text)
    );
    return option?.value || '';
  }, partial);

  if (!value) {
    throw new Error(`Option not found (contains "${partial}")`);
  }

  await select.selectOption(value);
}

async function importDataset(page: Page, datasetName: string, filePath: string) {
  await page.getByRole('button', { name: 'Import Dataset' }).first().click();
  const modalHeading = page.getByRole('heading', { name: 'Import Data Source' });
  await expect(modalHeading).toBeVisible();
  const modal = modalHeading.locator('..').locator('..');

  const fileInput = modal.locator('input[type="file"]').first();
  await fileInput.setInputFiles(filePath);

  await modal.getByPlaceholder('Enter a name for this dataset').fill(datasetName);
  await modal.getByRole('button', { name: 'Import Dataset' }).click();

  await expect(page.getByText(datasetName, { exact: true })).toBeVisible();
}

async function addDataSource(page: Page, datasetLabelPartial: string, alias?: string) {
  await page.getByRole('button', { name: 'Add Data Source' }).click();
  const datasetSelect = page.locator('select').filter({ hasText: '-- Select Dataset --' }).last();
  await selectOptionByPartialText(datasetSelect, datasetLabelPartial);
  if (alias) {
    await page.getByPlaceholder('e.g. Users').last().fill(alias);
  }
}

function getStep(page: Page, index: number) {
  return page.locator('main').locator('div.relative.group.bg-white.border').nth(index);
}

async function setCommandType(step: Locator, type: string) {
  await step.locator('select').first().selectOption(type);
}

async function setStepDataset(step: Locator, datasetPartial: string) {
  const datasetSelect = step.getByText('Select Dataset:').locator('..').locator('select');
  await selectOptionByPartialText(datasetSelect, datasetPartial);
}

async function ensureBackendAndStorage(page: Page) {
  await page.getByRole('button', { name: /Global Settings/i }).click();
  await page.getByText('http://localhost:8000').click();
  await expect(page.getByText(/Backend Status: Localhost/)).toBeVisible();
  const storageOption = page.getByText('test_sessions', { exact: true });
  await expect(storageOption).toBeVisible();
  await storageOption.click();
  await expect(page.getByText('Current: test_sessions')).toBeVisible();
  await page.getByRole('button', { name: 'Done' }).click();
}

async function createNewSession(page: Page) {
  await page.getByRole('button', { name: /Session/i }).click();
  await page.getByRole('button', { name: 'Create New Session' }).click();
  await expect(page.getByText('Data Setup')).toBeVisible();
}

test.describe.serial('complete command flow (layered)', () => {
  let context: BrowserContext;
  let page: Page;
  const runId = Date.now().toString(36);
  const datasetNames = {
    customers: `customers_${runId}`,
    orders: `orders_${runId}`,
    items: `order_items_${runId}`,
  };

  test.beforeAll(async ({ browser }) => {
    context = await browser.newContext();
    page = await context.newPage();
    await page.goto('/');
    await ensureBackendAndStorage(page);
    await createNewSession(page);
  });

  test.afterAll(async () => {
    await context.close();
  });

  test('import datasets (test_data)', async () => {
    await importDataset(page, datasetNames.customers, path.join(testDataDir, 'customers.csv'));
    await importDataset(page, datasetNames.orders, path.join(testDataDir, 'orders.csv'));
    await importDataset(page, datasetNames.items, path.join(testDataDir, 'order_items.csv'));
  });

  test('configure data sources + variables', async () => {
    await addDataSource(page, datasetNames.customers, 'customers');
    await addDataSource(page, datasetNames.orders, 'orders');
    await addDataSource(page, datasetNames.items, 'order_items');

    await page.getByRole('button', { name: 'Add Variable' }).click();
    await page.getByPlaceholder('var_name').last().fill('status_filter');
    await page.getByPlaceholder('Enter value').last().fill('PAID');
  });

  test('build commands (all types)', async () => {
    await page.getByRole('button', { name: 'Add Child' }).first().click();
    await page.getByText('Add your first command').click();

    // Step 1: Filter
    const step1 = getStep(page, 0);
    await setCommandType(step1, 'filter');
    await setStepDataset(step1, 'orders to');
    await step1.getByRole('button', { name: 'Add Rule' }).click();
    await selectOptionByPartialText(step1.locator('select').filter({ hasText: 'Field...' }).first(), 'status');
    await selectOptionByPartialText(step1.locator('select').filter({ hasText: 'Raw' }).first(), 'Variable');
    await step1.getByPlaceholder('Variable Name').fill('status_filter');

    // Step 2: Join
    await page.getByRole('button', { name: 'Add Step' }).click();
    const step2 = getStep(page, 1);
    await setCommandType(step2, 'join');
    await selectOptionByPartialText(step2.locator('select').filter({ hasText: '-- Select Source --' }).first(), 'customers to');
    await step2.getByPlaceholder(/ON Condition/).fill('orders.customer_id = customers.customer_id');

    // Step 3: Mapping
    await page.getByRole('button', { name: 'Add Step' }).click();
    const step3 = getStep(page, 2);
    await setCommandType(step3, 'transform');
    await step3.getByPlaceholder('Expression').fill('amount * 1.1');
    await step3.getByPlaceholder('Output Field').fill('amount_with_tax');

    // Step 4: Sort
    await page.getByRole('button', { name: 'Add Step' }).click();
    const step4 = getStep(page, 3);
    await setCommandType(step4, 'sort');
    await setStepDataset(step4, 'orders to');
    await selectOptionByPartialText(step4.locator('select').filter({ hasText: 'Select Field...' }).first(), 'amount');
    await selectOptionByPartialText(step4.locator('select').filter({ hasText: 'Asc' }).first(), 'Desc');

    // Step 5: Group
    await page.getByRole('button', { name: 'Add Step' }).click();
    const step5 = getStep(page, 4);
    await setCommandType(step5, 'group');
    await setStepDataset(step5, 'orders to');
    await step5.getByRole('button', { name: 'Add Column' }).click();
    await selectOptionByPartialText(step5.locator('select').filter({ hasText: 'Select Field...' }).first(), 'customer_id');
    await step5.getByRole('button', { name: 'Add Metric' }).click();
    await selectOptionByPartialText(step5.locator('select').filter({ hasText: 'Count' }).first(), 'Sum');
    await selectOptionByPartialText(step5.locator('select').filter({ hasText: 'Field...' }).first(), 'amount');
    await step5.getByPlaceholder('As...').fill('sum_amount');
    await step5.getByPlaceholder('Output Table Name').fill('orders_by_customer');

    // Step 6: Save Variable
    await page.getByRole('button', { name: 'Add Step' }).click();
    const step6 = getStep(page, 5);
    await setCommandType(step6, 'save');
    await setStepDataset(step6, 'orders to');
    await selectOptionByPartialText(step6.locator('select').filter({ hasText: 'Select Field...' }).first(), 'customer_id');
    await step6.getByPlaceholder('var_name').fill('customer_ids');

    // Step 7: View
    await page.getByRole('button', { name: 'Add Step' }).click();
    const step7 = getStep(page, 6);
    await setCommandType(step7, 'view');
    const viewTableSelect = step7.locator('select').filter({ hasText: '-- Select Table --' }).first();
    await selectOptionByPartialText(viewTableSelect, 'orders to');
    await step7.getByRole('button', { name: 'Add Field' }).click();
    await selectOptionByPartialText(step7.locator('select').filter({ hasText: 'Select Field...' }).first(), 'order_id');
    await step7.getByRole('button', { name: 'Add Field' }).click();
    await selectOptionByPartialText(step7.locator('select').filter({ hasText: 'Select Field...' }).nth(1), 'amount');

    // Step 8: Complex View
    await page.getByRole('button', { name: 'Add Step' }).click();
    const step8 = getStep(page, 7);
    await setCommandType(step8, 'multi_table');
    await setStepDataset(step8, 'orders to');
    await step8.getByRole('button', { name: 'Add Sub-Table' }).click();
    await selectOptionByPartialText(step8.locator('select').filter({ hasText: 'Select Source...' }).first(), 'order_items to');
    await step8.getByPlaceholder('Tab Name').fill('Items');
    await step8.getByPlaceholder('main.id = sub.user_id').fill('main.order_id = sub.order_id');
  });

  test('run and verify complex view', async () => {
    await page.getByRole('button', { name: 'Run this operation' }).click();
    await expect(page.getByText('Complex Result')).toBeVisible();

    const mainTable = page.locator('main').getByRole('table').first();
    await expect(mainTable).toContainText('O1001');

    await mainTable.locator('tbody tr').first().locator('td').first().click();
    await expect(page.getByRole('button', { name: 'Items' })).toBeVisible();
    await expect(page.getByText(/Showing 1 related record/)).toBeVisible();
  });
});
