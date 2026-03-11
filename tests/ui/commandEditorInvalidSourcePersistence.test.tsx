import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const commands: Command[] = [
  {
    id: 'cmd_filter_1',
    type: 'filter',
    order: 1,
    config: {
      dataSource: 'legacy_orders',
      filterRoot: {
        id: 'g1',
        type: 'group',
        logicalOperator: 'AND',
        conditions: []
      }
    }
  } as Command
];

describe('CommandEditor invalid source persistence', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="op_1"
          operationName="Process 1"
          operationType="process"
          commands={commands}
          datasets={[]}
          inputSchema={{}}
          onUpdateCommands={() => {}}
          onUpdateName={() => {}}
          onUpdateType={() => {}}
          onViewPath={() => {}}
          canRun={true}
        />
      );
      await flush();
    });
  });

  afterEach(() => {
    root.unmount();
    container.remove();
  });

  it('keeps unresolved source visible and disables run', () => {
    const sourceSelect = Array.from(container.querySelectorAll('select')).find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').includes('-- Select Source --'))
    ) as HTMLSelectElement | undefined;

    expect(sourceSelect).toBeDefined();
    expect(sourceSelect!.value).toBe('legacy_orders');
    expect(sourceSelect!.className).toContain('text-red-600');

    const unavailableOption = Array.from(sourceSelect!.options).find(opt => opt.value === 'legacy_orders');
    expect(unavailableOption).toBeDefined();
    expect(unavailableOption!.disabled).toBe(true);
    expect((unavailableOption!.textContent || '').trim()).toBe('legacy_orders');
    expect(container.textContent).not.toContain('(Unavailable)');

    const stepRunButton = container.querySelector('button[title*="unavailable"]') as HTMLButtonElement | null;
    expect(stepRunButton).not.toBeNull();
    expect(stepRunButton!.disabled).toBe(true);

    const operationRunButton = container.querySelector('button[title="Select a valid data source for each step before running"]') as HTMLButtonElement | null;
    expect(operationRunButton).not.toBeNull();
    expect(operationRunButton!.disabled).toBe(true);
  });
});
