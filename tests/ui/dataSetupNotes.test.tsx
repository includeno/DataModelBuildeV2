import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act, Simulate } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command, Dataset } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const datasets: Dataset[] = [
  { name: 'users', fields: ['id', 'name'], totalCount: 2 } as Dataset
];

const commands: Command[] = [
  {
    id: 'cmd_src_1',
    type: 'source',
    order: 1,
    config: { mainTable: 'users', alias: 'users', linkId: 'link_users' }
  } as Command,
  {
    id: 'cmd_var_1',
    type: 'define_variable',
    order: 2,
    config: { variableName: 'region', variableType: 'text', variableValue: 'APAC' }
  } as Command
];

describe('Data Setup notes (UI)', () => {
  let container: HTMLDivElement;
  let root: Root;
  const onUpdateCommands = vi.fn();

  beforeEach(async () => {
    onUpdateCommands.mockReset();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="op_setup"
          operationName="Setup"
          operationType="setup"
          commands={commands}
          datasets={datasets}
          inputSchema={{}}
          onUpdateCommands={onUpdateCommands}
          onUpdateName={() => {}}
          onUpdateType={() => {}}
          onViewPath={() => {}}
        />
      );
      await flush();
    });
  });

  afterEach(() => {
    root.unmount();
    container.remove();
  });

  it('writes source note into source config', async () => {
    const sourceNote = container.querySelector('textarea[placeholder="Describe this data source..."]') as HTMLTextAreaElement | null;
    expect(sourceNote).not.toBeNull();

    await act(async () => {
      Simulate.change(sourceNote!, { target: { value: 'Primary users dataset' } });
      await flush();
    });

    expect(onUpdateCommands).toHaveBeenCalled();
    const [, updatedCommands] = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1];
    const updatedSource = (updatedCommands as Command[]).find(c => c.id === 'cmd_src_1');
    expect(updatedSource?.config.note).toBe('Primary users dataset');
  });

  it('writes variable note into variable config', async () => {
    const variableNote = container.querySelector('textarea[placeholder="Describe this variable..."]') as HTMLTextAreaElement | null;
    expect(variableNote).not.toBeNull();

    await act(async () => {
      Simulate.change(variableNote!, { target: { value: 'Region filter default value' } });
      await flush();
    });

    expect(onUpdateCommands).toHaveBeenCalled();
    const [, updatedCommands] = onUpdateCommands.mock.calls[onUpdateCommands.mock.calls.length - 1];
    const updatedVariable = (updatedCommands as Command[]).find(c => c.id === 'cmd_var_1');
    expect(updatedVariable?.config.note).toBe('Region filter default value');
  });
});
