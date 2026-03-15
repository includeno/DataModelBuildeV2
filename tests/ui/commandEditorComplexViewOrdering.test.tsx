import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const commands: Command[] = [
  {
    id: 'cmd_multi_1',
    type: 'multi_table',
    order: 1,
    config: {
      dataSource: 'stream',
      subTables: []
    }
  } as Command
];

describe('CommandEditor complex view ordering', () => {
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

  it('does not enforce complex view as final step in UI', () => {
    expect(container.textContent).not.toContain('Complex View must be the final step in this operation.');

    const addStepButton = Array.from(container.querySelectorAll('button')).find(
      btn => (btn.textContent || '').trim() === 'Add Step'
    );
    expect(addStepButton).toBeDefined();

    const commandTypeSelect = Array.from(container.querySelectorAll('select')).find((el) =>
      Array.from(el.options).some(opt => (opt.textContent || '').trim() === 'Filter')
    ) as HTMLSelectElement | undefined;
    expect(commandTypeSelect).toBeDefined();

    const optionTexts = Array.from(commandTypeSelect!.options).map(opt => (opt.textContent || '').trim());
    expect(optionTexts).toContain('Complex View');
    expect(optionTexts).not.toContain('Complex View (Final Step)');
  });
});

