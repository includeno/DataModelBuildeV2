import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';

import { SettingsModal } from '../../components/SettingsModal';
import type { AppearanceConfig } from '../../types';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

const setInputValue = (element: HTMLInputElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('input', { bubbles: true }));
};

const setColorValue = (element: HTMLInputElement, value: string) => {
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
  setter?.call(element, value);
  element.dispatchEvent(new Event('change', { bubbles: true }));
};

const click = (node: Element | null) => {
  node?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
};

const findPanelButton = (container: HTMLDivElement, text: string) =>
  Array.from(container.querySelectorAll('button')).find((button) => button.textContent?.includes(text));

const findToggleButton = (container: HTMLDivElement, label: string) =>
  Array.from(container.querySelectorAll('span'))
    .find((node) => node.textContent === label)
    ?.parentElement
    ?.querySelector('button') ?? null;

const findClickableRowByText = (container: HTMLDivElement, text: string) =>
  Array.from(container.querySelectorAll('span'))
    .find((node) => node.textContent === text)
    ?.closest('.cursor-pointer');

describe('SettingsModal', () => {
  let container: HTMLDivElement;
  let root: Root;

  const appearance: AppearanceConfig = {
    textSize: 12,
    textColor: '#111111',
    showGuideLines: true,
    guideLineColor: '#222222',
    showNodeIds: false,
    showOperationIds: false,
    showCommandIds: false,
    showDatasetIds: false,
  };

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

  it('manages server list and session storage actions', async () => {
    const onSelectServer = vi.fn();
    const onAddServer = vi.fn();
    const onRemoveServer = vi.fn();
    const onRefreshSessionStorage = vi.fn();
    const onSelectSessionStorage = vi.fn();
    const onCreateSessionStorage = vi.fn();

    await act(async () => {
      root.render(
        <SettingsModal
          isOpen={true}
          onClose={() => {}}
          servers={['mockServer', 'http://localhost:8000', 'http://remote-server:9000']}
          currentServer="mockServer"
          onSelectServer={onSelectServer}
          onAddServer={onAddServer}
          onRemoveServer={onRemoveServer}
          appearance={appearance}
          onUpdateAppearance={vi.fn()}
          sessionStorageInfo={{ dataRoot: '/data', sessionsDir: '/data/sessions_a', relative: 'sessions_a' }}
          sessionStorageFolders={[
            { name: 'sessions_a', path: 'sessions_a' },
            { name: 'sessions_b', path: 'sessions_b' },
          ]}
          onRefreshSessionStorage={onRefreshSessionStorage}
          onSelectSessionStorage={onSelectSessionStorage}
          onCreateSessionStorage={onCreateSessionStorage}
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('App Settings');
    expect(container.textContent).toContain('Mock Server (Offline)');
    expect(container.textContent).toContain('Current: sessions_a');

    const customServerRow = findClickableRowByText(container, 'http://remote-server:9000');
    click(customServerRow);
    expect(onSelectServer).toHaveBeenCalledWith('http://remote-server:9000');

    const removeButton = customServerRow?.querySelector('button') ?? null;
    click(removeButton);
    expect(onRemoveServer).toHaveBeenCalledWith('http://remote-server:9000');

    const serverInput = container.querySelector('input[placeholder="http://192.168.1.10:8000"]') as HTMLInputElement | null;
    await act(async () => {
      if (serverInput) {
        setInputValue(serverInput, 'http://new-server:9100');
      }
      await flush();
    });
    click(serverInput?.parentElement?.querySelector('button') ?? null);
    expect(onAddServer).toHaveBeenCalledWith('http://new-server:9100');

    click(findPanelButton(container, 'Refresh') ?? null);
    expect(onRefreshSessionStorage).toHaveBeenCalledTimes(1);

    const storageRow = findClickableRowByText(container, 'sessions_b');
    click(storageRow ?? null);
    expect(onSelectSessionStorage).toHaveBeenCalledWith('sessions_b');

    const storageInput = container.querySelector('input[placeholder="sessions_test"]') as HTMLInputElement | null;
    await act(async () => {
      if (storageInput) {
        setInputValue(storageInput, 'sessions_new');
      }
      await flush();
    });
    await act(async () => {
      click(findPanelButton(container, 'Create') ?? null);
      await flush();
    });
    expect(onCreateSessionStorage).toHaveBeenCalledWith('sessions_new');
    expect(storageInput?.value).toBe('');
  });

  it('updates appearance settings, disabled storage hints, and closes from done button', async () => {
    const onClose = vi.fn();
    const onUpdateAppearance = vi.fn();

    await act(async () => {
      root.render(
        <SettingsModal
          isOpen={true}
          onClose={onClose}
          servers={['mockServer']}
          currentServer="mockServer"
          onSelectServer={vi.fn()}
          onAddServer={vi.fn()}
          onRemoveServer={vi.fn()}
          appearance={appearance}
          onUpdateAppearance={onUpdateAppearance}
          sessionStorageInfo={null}
          sessionStorageFolders={[]}
          sessionStorageDisabled={true}
          sessionStorageError="backend unavailable"
        />
      );
      await flush();
    });

    expect(container.textContent).toContain('backend unavailable');
    expect(container.textContent).toContain('Switch to a real backend server');
    expect(container.textContent).toContain('No folders found.');

    click(findPanelButton(container, 'Appearance') ?? null);
    await act(async () => {
      await flush();
    });

    click(findPanelButton(container, '14px') ?? null);
    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ textSize: 14 }));

    const colorInputs = Array.from(container.querySelectorAll('input[type="color"]')) as HTMLInputElement[];
    await act(async () => {
      setColorValue(colorInputs[0], '#abcdef');
      setColorValue(colorInputs[1], '#123456');
      await flush();
    });
    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ textColor: '#abcdef' }));
    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ guideLineColor: '#123456' }));

    click(findToggleButton(container, 'Show Indentation Lines'));
    click(findToggleButton(container, 'Show Node IDs (Tree)'));
    click(findToggleButton(container, 'Show Operation IDs (Editor)'));
    click(findToggleButton(container, 'Show Command IDs (Steps)'));
    click(findToggleButton(container, 'Show Dataset IDs (Sidebar)'));

    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ showGuideLines: false }));
    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ showNodeIds: true }));
    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ showOperationIds: true }));
    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ showCommandIds: true }));
    expect(onUpdateAppearance).toHaveBeenCalledWith(expect.objectContaining({ showDatasetIds: true }));

    click(findPanelButton(container, 'Done') ?? null);
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
