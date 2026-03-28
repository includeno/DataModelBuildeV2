import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import { ImportCleanPanel } from '../../components/ImportCleanPanel';
import type { ImportCleanConfig, CleanPreviewReport, FieldInfo } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const defaultConfig = (): ImportCleanConfig => ({
  dedup: { enabled: true, fields: 'all', keep: 'first' },
  fillMissing: {
    enabled: true,
    rules: [
      { field: '*number', strategy: 'median' },
      { field: '*string', strategy: 'constant', constantValue: '' },
    ],
  },
  outlier: { enabled: false, method: 'iqr', threshold: 1.5, action: 'flag', targetFields: 'numeric' },
  trimWhitespace: { enabled: true, fields: 'string' },
});

const defaultFields = ['id', 'name', 'salary'];
const defaultFieldTypes: Record<string, FieldInfo> = {
  id: { type: 'number' },
  name: { type: 'string' },
  salary: { type: 'number' },
};

const makeReport = (overrides: Partial<CleanPreviewReport> = {}): CleanPreviewReport => ({
  duplicateRowCount: 0,
  missingValueCounts: {},
  outlierCounts: {},
  whitespaceFieldCount: 0,
  ...overrides,
});

describe('ImportCleanPanel', () => {
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

  // ── Rendering ──────────────────────────────────────────────────────────

  it('renders the panel with all four sections', async () => {
    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={defaultConfig()}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    const panel = container.querySelector('[data-testid="import-clean-panel"]');
    expect(panel).not.toBeNull();
    expect(container.textContent).toContain('Deduplication');
    expect(container.textContent).toContain('Trim Whitespace');
    expect(container.textContent).toContain('Fill Missing Values');
    expect(container.textContent).toContain('Outlier Detection');
  });

  it('shows preview report badges when report is provided', async () => {
    const report = makeReport({
      duplicateRowCount: 5,
      missingValueCounts: { name: 3 },
      whitespaceFieldCount: 2,
      outlierCounts: { salary: 1 },
    });

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={defaultConfig()}
          onChange={vi.fn()}
          previewReport={report}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('5 duplicate rows detected');
    expect(container.textContent).toContain('2 fields with whitespace');
    expect(container.textContent).toContain('1 fields, 3 missing values');
  });

  it('shows "No cleaning enabled" summary when all steps are disabled', async () => {
    const config: ImportCleanConfig = {
      dedup: { enabled: false, fields: 'all', keep: 'first' },
      fillMissing: { enabled: false, rules: [] },
      outlier: { enabled: false, method: 'iqr', threshold: 1.5, action: 'flag', targetFields: 'numeric' },
      trimWhitespace: { enabled: false, fields: 'string' },
    };

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('No cleaning enabled');
  });

  it('shows active steps in summary', async () => {
    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={defaultConfig()}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    // default config: dedup + trim + fill enabled, outlier disabled
    expect(container.textContent).toContain('Dedup');
    expect(container.textContent).toContain('Trim');
    expect(container.textContent).toContain('Fill');
  });

  // ── Dedup section ─────────────────────────────────────────────────────

  it('toggles dedup section off via checkbox', async () => {
    const onChange = vi.fn();

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={defaultConfig()}
          onChange={onChange}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    const checkboxes = container.querySelectorAll('input[type="checkbox"]');
    // First checkbox is dedup
    const dedupCheckbox = checkboxes[0] as HTMLInputElement;
    expect(dedupCheckbox.checked).toBe(true);

    await act(async () => {
      dedupCheckbox.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onChange).toHaveBeenCalled();
    const updatedConfig = onChange.mock.calls[0][0] as ImportCleanConfig;
    expect(updatedConfig.dedup.enabled).toBe(false);
  });

  it('exposes dedup fields selector when dedup is enabled', async () => {
    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={defaultConfig()}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    // The fields select should be visible (dedup enabled by default)
    const selects = Array.from(container.querySelectorAll('select'));
    const fieldsSelect = selects.find((s) => s.textContent?.includes('All Fields'));
    expect(fieldsSelect).toBeTruthy();
  });

  // ── Fill Missing section ──────────────────────────────────────────────

  it('adds a fill rule when "+ Add Rule" is clicked', async () => {
    const onChange = vi.fn();
    const config = defaultConfig();
    config.fillMissing.rules = [];

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={onChange}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    const addButton = Array.from(container.querySelectorAll('button')).find((b) =>
      b.textContent?.includes('+ Add Rule')
    );
    expect(addButton).toBeTruthy();

    await act(async () => {
      addButton!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onChange).toHaveBeenCalled();
    const updated = onChange.mock.calls[0][0] as ImportCleanConfig;
    expect(updated.fillMissing.rules).toHaveLength(1);
    expect(updated.fillMissing.rules[0].field).toBe('id');
  });

  it('removes a fill rule when the trash button is clicked', async () => {
    const onChange = vi.fn();
    const config = defaultConfig();
    // keep only one rule
    config.fillMissing.rules = [{ field: '*number', strategy: 'median' }];

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={onChange}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    // find the small trash button inside the fill section
    const trashButtons = Array.from(container.querySelectorAll('button')).filter(
      (b) => b.querySelector('svg') && !b.textContent?.trim()
    );
    expect(trashButtons.length).toBeGreaterThan(0);

    await act(async () => {
      trashButtons[0].dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onChange).toHaveBeenCalled();
    const updated = onChange.mock.calls[0][0] as ImportCleanConfig;
    expect(updated.fillMissing.rules).toHaveLength(0);
  });

  it('shows constant value input when strategy is "constant"', async () => {
    const config = defaultConfig();
    config.fillMissing.rules = [{ field: 'name', strategy: 'constant', constantValue: 'N/A' }];

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    const constantInput = container.querySelector('input[placeholder="value"]') as HTMLInputElement | null;
    expect(constantInput).not.toBeNull();
    expect(constantInput!.value).toBe('N/A');
  });

  it('does not show constant value input when strategy is not "constant"', async () => {
    const config = defaultConfig();
    config.fillMissing.rules = [{ field: '*number', strategy: 'median' }];

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    const constantInput = container.querySelector('input[placeholder="value"]');
    expect(constantInput).toBeNull();
  });

  // ── Outlier section ───────────────────────────────────────────────────

  it('shows warning when outlier action is "remove"', async () => {
    const config = defaultConfig();
    config.outlier = { enabled: true, method: 'iqr', threshold: 1.5, action: 'remove', targetFields: 'numeric' };

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('permanently removed');
  });

  it('does not show warning when outlier action is "flag"', async () => {
    const config = defaultConfig();
    config.outlier = { enabled: true, method: 'iqr', threshold: 1.5, action: 'flag', targetFields: 'numeric' };

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    expect(container.textContent).not.toContain('permanently removed');
  });

  it('shows numeric field count in outlier section', async () => {
    const config = defaultConfig();
    config.outlier.enabled = true;

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    // id and salary are numbers → 2 numeric fields
    expect(container.textContent).toContain('2 numeric field');
  });

  // ── Reset & Skip buttons ──────────────────────────────────────────────

  it('calls onReset when "Reset Defaults" is clicked', async () => {
    const onReset = vi.fn();

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={defaultConfig()}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={onReset}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    const resetButton = Array.from(container.querySelectorAll('button')).find((b) =>
      b.textContent?.includes('Reset Defaults')
    );
    expect(resetButton).toBeTruthy();

    await act(async () => {
      resetButton!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onReset).toHaveBeenCalledTimes(1);
  });

  it('calls onSkip when "Skip Cleaning" is clicked', async () => {
    const onSkip = vi.fn();

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={defaultConfig()}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={onSkip}
        />
      );
      await flush();
    });

    const skipButton = Array.from(container.querySelectorAll('button')).find((b) =>
      b.textContent?.includes('Skip Cleaning')
    );
    expect(skipButton).toBeTruthy();

    await act(async () => {
      skipButton!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(onSkip).toHaveBeenCalledTimes(1);
  });

  // ── Section expand/collapse ───────────────────────────────────────────

  it('hides section body when section is toggled off', async () => {
    const onChange = vi.fn();
    const config = defaultConfig();
    config.dedup.enabled = false;

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    // Dedup body (Fields/Keep selects) should not be visible
    const selects = Array.from(container.querySelectorAll('select'));
    const fieldsSelect = selects.find((s) => s.textContent?.includes('All Fields'));
    expect(fieldsSelect).toBeFalsy();
  });

  it('shows dedup custom field checkboxes when "Custom" is selected', async () => {
    const onChange = vi.fn();
    const config = defaultConfig();
    config.dedup.fields = ['id'];  // custom mode

    await act(async () => {
      root.render(
        <ImportCleanPanel
          config={config}
          onChange={vi.fn()}
          previewReport={null}
          fields={defaultFields}
          fieldTypes={defaultFieldTypes}
          onReset={vi.fn()}
          onSkip={vi.fn()}
        />
      );
      await flush();
    });

    // Should show individual field checkboxes for id, name, salary
    const labels = Array.from(container.querySelectorAll('label'));
    const fieldLabels = labels.filter((l) => defaultFields.some((f) => l.textContent?.trim() === f));
    expect(fieldLabels.length).toBe(3);
  });
});
