import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command, Dataset } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const datasets: Dataset[] = [
  { name: 'order', fields: ['id'], totalCount: 1 } as Dataset
];

const commands: Command[] = [
  {
    id: 'cmd_src_1',
    type: 'source',
    order: 1,
    config: { mainTable: 'order', alias: 'order', linkId: 'link_1' }
  } as Command
];

describe('Data Setup reserved names (UI)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
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
          onUpdateCommands={() => {}}
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

  it('shows reserved keyword warning and validation errors', () => {
    expect(container.textContent).toContain('Reserved keyword dataset detected');
    expect(container.textContent?.toLowerCase()).toContain('reserved keyword');
  });
});
