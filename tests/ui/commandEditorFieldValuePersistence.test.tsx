import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const commands: Command[] = [
  {
    id: 'cmd_sort_1',
    type: 'sort',
    order: 1,
    config: {
      dataSource: 'legacy_orders',
      field: 'amount',
      ascending: true
    }
  } as Command
];

describe('CommandEditor field value persistence', () => {
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

  it('keeps selected field visible even when schema is unavailable', () => {
    const fieldSelect = Array.from(container.querySelectorAll('select')).find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').includes('Select Field...'))
    ) as HTMLSelectElement | undefined;

    expect(fieldSelect).toBeDefined();
    expect(fieldSelect!.value).toBe('amount');

    const currentFieldOption = Array.from(fieldSelect!.options).find(opt => opt.value === 'amount');
    expect(currentFieldOption).toBeDefined();
    expect((currentFieldOption!.textContent || '').trim()).toBe('amount');
  });
});
