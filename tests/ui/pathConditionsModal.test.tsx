import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { PathConditionsModal } from '../../components/PathConditionsModal';
import { api } from '../../utils/api';
import type { ApiConfig, OperationNode } from '../../types';

const flush = () => Promise.resolve();

const apiConfig: ApiConfig = {
  currentServer: 'http://localhost:8000',
  servers: ['http://localhost:8000'],
  isMock: false,
};

const tree: OperationNode = {
  id: 'root',
  type: 'operation',
  operationType: 'root',
  name: 'Root',
  enabled: true,
  commands: [],
  children: [
    {
      id: 'node_source',
      type: 'operation',
      operationType: 'dataset',
      name: 'Load Customers',
      enabled: true,
      commands: [
        {
          id: 'cmd_source',
          type: 'source',
          order: 1,
          config: {
            mainTable: 'customers',
            alias: 'cust',
          },
        },
      ],
      children: [
        {
          id: 'node_filter',
          type: 'operation',
          operationType: 'process',
          name: 'Filter Active',
          enabled: true,
          commands: [
            {
              id: 'cmd_filter',
              type: 'filter',
              order: 1,
              config: {
                filterRoot: {
                  id: 'group_1',
                  type: 'group',
                  logicalOperator: 'AND',
                  conditions: [
                    {
                      id: 'cond_1',
                      type: 'condition',
                      field: 'status',
                      operator: '=',
                      value: 'active',
                    },
                    {
                      id: 'group_2',
                      type: 'group',
                      logicalOperator: 'OR',
                      conditions: [
                        {
                          id: 'cond_2',
                          type: 'condition',
                          field: 'country',
                          operator: '=',
                          value: 'CN',
                        },
                      ],
                    },
                  ],
                },
              },
            },
          ],
          children: [
            {
              id: 'node_target',
              type: 'operation',
              operationType: 'process',
              name: 'Analyze Revenue',
              enabled: true,
              commands: [
                {
                  id: 'cmd_join',
                  type: 'join',
                  order: 1,
                  config: {
                    dataSource: 'cust',
                    joinType: 'INNER',
                    joinTable: 'orders',
                    on: 'cust.id = orders.customer_id',
                  },
                },
                {
                  id: 'cmd_transform',
                  type: 'transform',
                  order: 2,
                  config: {
                    dataSource: 'cust',
                    outputField: 'revenue_score',
                    expression: 'amount * 2',
                  },
                },
                {
                  id: 'cmd_sort',
                  type: 'sort',
                  order: 3,
                  config: {
                    field: 'created_at',
                    ascending: false,
                  },
                },
                {
                  id: 'cmd_group',
                  type: 'group',
                  order: 4,
                  config: {
                    aggFunc: 'sum',
                    field: 'amount',
                    groupBy: ['country'],
                  },
                },
                {
                  id: 'cmd_custom',
                  type: 'custom',
                  order: 5,
                  config: {
                    formula: 'noop',
                  },
                },
              ],
            },
          ],
        },
      ],
    },
  ],
};

describe('PathConditionsModal', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-03-19T00:00:00Z'));
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    root.unmount();
    container.remove();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('renders path synthesis, trims target commands, and refreshes step counts', async () => {
    const onClose = vi.fn();
    const postSpy = vi.spyOn(api, 'post').mockResolvedValue({ totalCount: 42 } as any);

    await act(async () => {
      root.render(
        <PathConditionsModal
          isOpen={true}
          onClose={onClose}
          tree={tree}
          targetNodeId="node_target"
          targetCommandId="cmd_transform"
          projectId="prj_cov"
          apiConfig={apiConfig}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Path Logic Synthesis');
    expect(container.textContent).toContain('Showing 3 steps in execution path');
    expect(container.textContent).toContain('Load Customers');
    expect(container.textContent).toContain('Filter Active');
    expect(container.textContent).toContain('Analyze Revenue');
    expect(container.textContent).toContain('Load Table:');
    expect(container.textContent).toContain('Filter Active');
    expect(container.textContent).toContain('INNER JOIN');
    expect(container.textContent).toContain('revenue_score');
    expect(container.textContent).not.toContain('Sort by');
    expect(container.textContent).not.toContain('SUM');

    const countButtons = Array.from(container.querySelectorAll('button')).filter((button) => button.textContent?.includes('Count'));
    await act(async () => {
      countButtons[0]?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(postSpy).toHaveBeenCalledWith(
      apiConfig,
      '/projects/prj_cov/execute',
      expect.objectContaining({
        projectId: 'prj_cov',
        tree,
        targetNodeId: 'node_source',
        page: 1,
        pageSize: 1,
      })
    );
    expect(container.textContent).toContain('42 rows');

    await act(async () => {
      vi.advanceTimersByTime(31_000);
      await flush();
    });

    expect(Array.from(container.querySelectorAll('button')).some((button) => button.textContent?.includes('Count'))).toBe(true);

    const closeButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Close'));
    await act(async () => {
      closeButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('renders all command summaries, supports session fallback, and surfaces count failures', async () => {
    const alertSpy = vi.spyOn(window, 'alert').mockImplementation(() => {});
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const postSpy = vi.spyOn(api, 'post').mockRejectedValue(new Error('broken'));

    await act(async () => {
      root.render(
        <PathConditionsModal
          isOpen={true}
          onClose={() => {}}
          tree={tree}
          targetNodeId="node_target"
          sessionId="sess_legacy"
          apiConfig={apiConfig}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Sort by');
    expect(container.textContent).toContain('DESC');
    expect(container.textContent).toContain('sum');
    expect(container.textContent).toContain('country');
    expect(container.textContent).toContain('{"formula":"noop"}');

    const countButton = Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes('Count'));
    await act(async () => {
      countButton?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await flush();
    });

    expect(postSpy).toHaveBeenCalledWith(
      apiConfig,
      '/projects/sess_legacy/execute',
      expect.objectContaining({ projectId: 'sess_legacy' })
    );
    expect(errorSpy).toHaveBeenCalled();
    expect(alertSpy).toHaveBeenCalledWith('Failed to calculate count: broken');

    await act(async () => {
      root.render(
        <PathConditionsModal
          isOpen={true}
          onClose={() => {}}
          tree={tree}
          targetNodeId="root"
          apiConfig={apiConfig}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('Node not found or is root.');
  });
});
