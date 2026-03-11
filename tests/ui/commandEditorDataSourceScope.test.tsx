import React from 'react';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { act } from 'react-dom/test-utils';
import { createRoot, Root } from 'react-dom/client';
import { CommandEditor } from '../../components/CommandEditor';
import { Command, Dataset, OperationNode } from '../../types';

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

const tree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Project',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'setup_a',
      type: 'operation',
      operationType: 'setup',
      name: 'Setup A',
      enabled: true,
      commands: [
        {
          id: 'src_a',
          type: 'source',
          order: 1,
          config: { mainTable: 'table_alpha', alias: 'shared', linkId: 'link_a' }
        } as Command
      ],
      children: [
        {
          id: 'op_a',
          type: 'operation',
          operationType: 'process',
          name: 'Op A',
          enabled: true,
          commands: [],
          children: []
        }
      ]
    },
    {
      id: 'setup_b',
      type: 'operation',
      operationType: 'setup',
      name: 'Setup B',
      enabled: true,
      commands: [
        {
          id: 'src_b',
          type: 'source',
          order: 1,
          config: { mainTable: 'table_beta', alias: 'shared', linkId: 'link_b' }
        } as Command
      ],
      children: [
        {
          id: 'op_b',
          type: 'operation',
          operationType: 'process',
          name: 'Op B',
          enabled: true,
          commands: [],
          children: []
        }
      ]
    }
  ]
};

const datasets: Dataset[] = [
  { id: 'table_alpha', name: 'table_alpha', fields: ['alpha_only'], rows: [], totalCount: 0 } as Dataset,
  { id: 'table_beta', name: 'table_beta', fields: ['beta_only'], rows: [], totalCount: 0 } as Dataset
];

const commands: Command[] = [
  {
    id: 'cmd_filter_1',
    type: 'filter',
    order: 1,
    config: {
      dataSource: 'shared',
      filterRoot: {
        id: 'g1',
        type: 'group',
        logicalOperator: 'AND',
        conditions: [
          {
            id: 'c1',
            type: 'condition',
            field: '',
            operator: '=',
            value: '',
            valueType: 'raw',
            dataType: 'string'
          }
        ]
      }
    }
  } as Command
];

describe('CommandEditor data source scope', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(async () => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);

    await act(async () => {
      root.render(
        <CommandEditor
          operationId="op_b"
          operationName="Op B"
          operationType="process"
          commands={commands}
          datasets={datasets}
          inputSchema={{}}
          tree={tree}
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

  it('resolves alias from its own setup instead of another setup', () => {
    expect(container.textContent).toContain('beta_only');
    expect(container.textContent).not.toContain('alpha_only');
  });
});
