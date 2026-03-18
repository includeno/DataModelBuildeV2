import React, { useMemo, useState } from 'react';
import {
  AlertTriangle,
  Check,
  ChevronDown,
  Clock,
  Cloud,
  CloudOff,
  GitBranch,
  Layers,
  Loader2,
  LogOut,
  Menu,
  PanelRight,
  Play,
  Plus,
  RefreshCw,
  Settings,
  Table as TableIcon,
  Terminal,
  Trash2,
  Users,
} from 'lucide-react';
import { ApiConfig, ProjectMetadata, ProjectSaveStatus } from '../types';
import { RealtimeStatus } from '../utils/realtimeCollab';
import { Button } from './Button';

interface TopBarProps {
  projectId: string;
  projectName?: string;
  projects: ProjectMetadata[];
  currentView: 'workflow' | 'sql' | 'data';
  apiConfig: ApiConfig;
  isRightPanelOpen: boolean;
  backendStatus: 'mock' | 'checking' | 'online' | 'offline';
  realtimeStatus: RealtimeStatus;
  saveStatus: ProjectSaveStatus;
  lastSavedAt?: number | null;
  onlineMembersCount?: number;
  remoteEditingLabel?: string | null;
  syncing?: boolean;
  onProjectSelect: (id: string) => void;
  onProjectCreate: () => void;
  onProjectDelete: (e: React.MouseEvent, id: string) => void;
  onViewChange: (view: 'workflow' | 'sql' | 'data') => void;
  onSettingsOpen: () => void;
  onProjectSettingsOpen: () => void;
  onProjectDiagnostics: () => void;
  onProjectMembersOpen: () => void;
  onManualSync: () => void;
  onRunSql: () => void;
  onToggleRightPanel: () => void;
  onToggleMobileSidebar: () => void;
  authEnabled?: boolean;
  isAuthenticated?: boolean;
  authChecking?: boolean;
  authError?: string | null;
  onLogout?: () => void | Promise<void>;
  canExecute?: boolean;
}

const SAVE_STATUS_STYLES: Record<ProjectSaveStatus, string> = {
  idle: 'bg-gray-50 text-gray-600 border-gray-200',
  dirty: 'bg-amber-50 text-amber-700 border-amber-200',
  saving: 'bg-blue-50 text-blue-700 border-blue-200',
  saved: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  conflict: 'bg-red-50 text-red-700 border-red-200',
  error: 'bg-red-50 text-red-700 border-red-200',
};

const SAVE_STATUS_LABELS: Record<ProjectSaveStatus, string> = {
  idle: '未修改',
  dirty: '待保存',
  saving: '保存中',
  saved: '已保存',
  conflict: '冲突',
  error: '失败',
};

const REALTIME_STATUS_LABELS: Record<RealtimeStatus, string> = {
  idle: '未连接',
  connecting: '连接中',
  connected: '在线',
  reconnecting: '重连中',
  closed: '离线',
};

const REALTIME_STATUS_STYLES: Record<RealtimeStatus, string> = {
  idle: 'bg-gray-50 text-gray-600 border-gray-200',
  connecting: 'bg-blue-50 text-blue-700 border-blue-200',
  connected: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  reconnecting: 'bg-amber-50 text-amber-700 border-amber-200',
  closed: 'bg-red-50 text-red-700 border-red-200',
};

export const TopBar: React.FC<TopBarProps> = ({
  projectId,
  projectName,
  projects,
  currentView,
  apiConfig,
  isRightPanelOpen,
  backendStatus,
  realtimeStatus,
  saveStatus,
  lastSavedAt = null,
  onlineMembersCount = 0,
  remoteEditingLabel = null,
  syncing = false,
  onProjectSelect,
  onProjectCreate,
  onProjectDelete,
  onViewChange,
  onSettingsOpen,
  onProjectSettingsOpen,
  onProjectDiagnostics,
  onProjectMembersOpen,
  onManualSync,
  onRunSql,
  onToggleRightPanel,
  onToggleMobileSidebar,
  authEnabled = true,
  isAuthenticated = false,
  authChecking = false,
  authError = null,
  onLogout,
  canExecute = true,
}) => {
  const [isProjectMenuOpen, setIsProjectMenuOpen] = useState(false);

  const connectionLabel = apiConfig.isMock
    ? 'Mock Mode'
    : backendStatus === 'online'
      ? '已连接'
      : backendStatus === 'offline'
        ? '已断开'
        : '连接中';

  const authLabel = apiConfig.isMock || !authEnabled
    ? '免登录'
    : authChecking
      ? '认证检查中'
      : isAuthenticated
        ? '已登录'
        : '未登录';

  const canShowLogout = !apiConfig.isMock && authEnabled && isAuthenticated && typeof onLogout === 'function';
  const connectedServerTitle = `Connected Server: ${apiConfig.baseUrl || 'N/A'} (${connectionLabel}, ${authLabel}${authError ? `, ${authError}` : ''})`;
  const saveStatusTitle = useMemo(() => {
    const timeLabel = lastSavedAt ? `，${new Date(lastSavedAt).toLocaleTimeString()}` : '';
    return `项目保存状态：${SAVE_STATUS_LABELS[saveStatus]}${timeLabel}`;
  }, [lastSavedAt, saveStatus]);
  const realtimeTitle = useMemo(() => {
    const editor = remoteEditingLabel ? `，${remoteEditingLabel}` : '';
    return `实时协作状态：${REALTIME_STATUS_LABELS[realtimeStatus]}，在线 ${onlineMembersCount} 人${editor}`;
  }, [onlineMembersCount, realtimeStatus, remoteEditingLabel]);

  return (
    <>
      {isProjectMenuOpen && (
        <div className="fixed inset-0 z-30" onClick={() => setIsProjectMenuOpen(false)} />
      )}

      <header className="h-14 bg-white border-b border-gray-200 flex items-center justify-between px-3 md:px-4 shrink-0 shadow-sm z-40 relative">
        <div className="flex items-center space-x-2 md:space-x-3 min-w-0">
          <button
            onClick={onToggleMobileSidebar}
            className="md:hidden p-2 -ml-2 text-gray-600 hover:bg-gray-100 rounded-md"
          >
            <Menu className="w-5 h-5" />
          </button>

          <div className="bg-blue-600 p-1.5 rounded-lg hidden md:block">
            <GitBranch className="w-5 h-5 text-white" />
          </div>
          <h1 className="font-bold text-gray-800 tracking-tight hidden lg:block">DataFlow Engine</h1>
          <span className="text-gray-300 text-xl font-light hidden lg:block">|</span>

          <div className="relative flex items-center space-x-1 min-w-0">
            <button
              onClick={() => setIsProjectMenuOpen(!isProjectMenuOpen)}
              title="Project Switcher"
              className={`flex items-center justify-between space-x-2 bg-white border hover:bg-gray-50 text-gray-900 px-2 md:px-3 py-1.5 rounded-md shadow-sm transition-all text-sm min-w-[180px] md:min-w-[260px] max-w-[360px] ${
                !projectId ? 'border-blue-400 ring-1 ring-blue-100' : 'border-gray-300'
              }`}
            >
              <div className="flex items-center overflow-hidden">
                <span className="text-gray-400 mr-2 text-xs uppercase font-semibold hidden md:inline">Project</span>
                <span className={`font-medium truncate ${!projectId ? 'text-blue-600' : ''}`}>
                  {projectName || projectId || 'Create Project'}
                </span>
              </div>
              <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform ${isProjectMenuOpen ? 'rotate-180' : ''}`} />
            </button>

            {isProjectMenuOpen && (
              <div className="absolute top-full left-0 mt-1 w-80 bg-white border border-gray-200 rounded-lg shadow-xl z-50 flex flex-col animate-in fade-in zoom-in-95 duration-100 origin-top-left">
                <div className="px-4 py-2 border-b border-gray-100 bg-gray-50 rounded-t-lg">
                  <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Switch or Manage</span>
                </div>

                <div className="max-h-[300px] overflow-y-auto p-1">
                  {projects.map(project => (
                    <div
                      key={project.id}
                      onClick={() => {
                        onProjectSelect(project.id);
                        setIsProjectMenuOpen(false);
                      }}
                      className={`group flex items-center justify-between px-3 py-2.5 rounded-md cursor-pointer transition-colors ${project.id === projectId ? 'bg-blue-50' : 'hover:bg-gray-100'}`}
                    >
                      <div className="flex items-center min-w-0">
                        <div className={`w-1.5 h-1.5 rounded-full mr-3 ${project.id === projectId ? 'bg-blue-500' : 'bg-gray-300'}`} />
                        <div className="flex flex-col min-w-0">
                          <span className={`text-sm font-medium truncate ${project.id === projectId ? 'text-blue-900' : 'text-gray-700'}`}>
                            {project.name || project.id}
                          </span>
                          <span className="text-[10px] text-gray-400 truncate">{project.role} · {project.id}</span>
                          <div className="flex items-center text-[10px] text-gray-400 mt-0.5">
                            <Clock className="w-3 h-3 mr-1" />
                            {new Date(project.updatedAt || project.createdAt).toLocaleTimeString()}
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center">
                        {project.id === projectId && <Check className="w-4 h-4 text-blue-500 mr-2" />}

                        {project.id === projectId && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              onProjectSettingsOpen();
                              setIsProjectMenuOpen(false);
                            }}
                            className="p-1.5 rounded-md text-gray-400 hover:text-blue-600 hover:bg-blue-100 transition-all mr-1"
                            title="Project Settings"
                          >
                            <Settings className="w-4 h-4" />
                          </button>
                        )}

                        {project.id === projectId && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              onProjectDiagnostics();
                              setIsProjectMenuOpen(false);
                            }}
                            className="p-1.5 rounded-md text-gray-400 hover:text-emerald-600 hover:bg-emerald-100 transition-all mr-1"
                            title="Project Diagnostics"
                          >
                            <Cloud className="w-4 h-4" />
                          </button>
                        )}

                        <button
                          onClick={(e) => onProjectDelete(e, project.id)}
                          className={`p-1.5 rounded-md text-gray-400 hover:text-red-600 hover:bg-red-50 transition-all ${projects.length === 1 ? 'hidden' : 'opacity-0 group-hover:opacity-100'}`}
                          title="Delete Project"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </div>
                  ))}
                  {projects.length === 0 && (
                    <div className="p-3 text-center text-sm text-gray-400 italic">No active projects found.<br />Create one to start.</div>
                  )}
                </div>

                <div className="p-2 border-t border-gray-100 bg-gray-50/50 rounded-b-lg">
                  <button
                    onClick={() => {
                      onProjectCreate();
                      setIsProjectMenuOpen(false);
                    }}
                    className="w-full flex items-center justify-center space-x-2 bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 hover:border-gray-400 text-sm font-medium py-2 rounded-md transition-colors shadow-sm"
                  >
                    <Plus className="w-4 h-4 text-green-600" />
                    <span>Create New Project</span>
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>

        <div className="hidden md:flex items-center bg-gray-100 p-1 rounded-lg">
          <button
            onClick={() => onViewChange('workflow')}
            className={`flex items-center px-3 py-1.5 text-sm font-medium rounded-md transition-all ${currentView === 'workflow' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            <Layers className="w-4 h-4 mr-2" /> Workflow
          </button>
          <button
            onClick={() => onViewChange('sql')}
            className={`flex items-center px-3 py-1.5 text-sm font-medium rounded-md transition-all ${currentView === 'sql' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            <Terminal className="w-4 h-4 mr-2" /> SQL Studio
          </button>
          <button
            onClick={() => onViewChange('data')}
            className={`flex items-center px-3 py-1.5 text-sm font-medium rounded-md transition-all ${currentView === 'data' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
          >
            <TableIcon className="w-4 h-4 mr-2" /> Data Viewer
          </button>
        </div>

        <div className="flex md:hidden items-center bg-gray-100 p-1 rounded-lg mr-2">
          <button onClick={() => onViewChange('workflow')} className={`p-1.5 rounded-md ${currentView === 'workflow' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500'}`}>
            <Layers className="w-4 h-4" />
          </button>
          <button onClick={() => onViewChange('sql')} className={`p-1.5 rounded-md ${currentView === 'sql' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500'}`}>
            <Terminal className="w-4 h-4" />
          </button>
          <button onClick={() => onViewChange('data')} className={`p-1.5 rounded-md ${currentView === 'data' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500'}`}>
            <TableIcon className="w-4 h-4" />
          </button>
        </div>

        <div className="flex items-center space-x-2 shrink-0">
          <div
            className={`hidden md:flex items-center max-w-[320px] px-3 py-1.5 text-xs font-medium rounded-full border cursor-default select-none ${
              backendStatus === 'online'
                ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                : backendStatus === 'offline'
                  ? 'bg-red-50 text-red-700 border-red-200'
                  : backendStatus === 'checking'
                    ? 'bg-gray-50 text-gray-600 border-gray-200'
                    : 'bg-yellow-50 text-yellow-700 border-yellow-200'
            }`}
            title={connectedServerTitle}
          >
            <span className="truncate max-w-[170px]">{apiConfig.baseUrl}</span>
            <span className="mx-1 text-gray-400">|</span>
            <span className="whitespace-nowrap">{connectionLabel}</span>
          </div>

          <div className={`hidden lg:flex items-center px-3 py-1.5 text-xs font-medium rounded-full border ${SAVE_STATUS_STYLES[saveStatus]}`} title={saveStatusTitle}>
            {saveStatus === 'saving' ? <Loader2 className="w-3.5 h-3.5 mr-1 animate-spin" /> : <Cloud className="w-3.5 h-3.5 mr-1" />}
            <span>{SAVE_STATUS_LABELS[saveStatus]}</span>
          </div>

          <div className={`hidden lg:flex items-center px-3 py-1.5 text-xs font-medium rounded-full border ${REALTIME_STATUS_STYLES[realtimeStatus]}`} title={realtimeTitle}>
            {realtimeStatus === 'connected' ? <Cloud className="w-3.5 h-3.5 mr-1" /> : <CloudOff className="w-3.5 h-3.5 mr-1" />}
            <span>{REALTIME_STATUS_LABELS[realtimeStatus]}</span>
            <span className="mx-1 text-current/40">|</span>
            <span>{onlineMembersCount} 人</span>
          </div>

          {remoteEditingLabel && (
            <div className="hidden xl:flex items-center px-3 py-1.5 text-xs font-medium rounded-full border bg-violet-50 text-violet-700 border-violet-200 max-w-[240px]" title={remoteEditingLabel}>
              <AlertTriangle className="w-3.5 h-3.5 mr-1 shrink-0" />
              <span className="truncate">{remoteEditingLabel}</span>
            </div>
          )}

          {projectId && (
            <button
              onClick={onManualSync}
              className="inline-flex items-center px-2.5 py-1.5 text-xs font-medium rounded-md border border-gray-200 text-gray-600 hover:bg-gray-50 transition-colors"
              title="Manual sync"
              disabled={syncing}
            >
              <RefreshCw className={`w-3.5 h-3.5 md:mr-1 ${syncing ? 'animate-spin' : ''}`} />
              <span className="hidden md:inline">同步</span>
            </button>
          )}

          {projectId && (
            <button
              onClick={onProjectMembersOpen}
              className="inline-flex items-center px-2.5 py-1.5 text-xs font-medium rounded-md border border-gray-200 text-gray-600 hover:bg-gray-50 transition-colors"
              title="Project members"
            >
              <Users className="w-3.5 h-3.5 md:mr-1" />
              <span className="hidden md:inline">成员</span>
            </button>
          )}

          {canShowLogout && (
            <button
              onClick={onLogout}
              className="inline-flex items-center px-2.5 py-1.5 text-xs font-medium rounded-md border border-red-200 text-red-600 hover:bg-red-50 transition-colors"
              title="Log out"
            >
              <LogOut className="w-3.5 h-3.5 md:mr-1" />
              <span className="hidden md:inline">登出</span>
            </button>
          )}

          {currentView === 'workflow' && (
            <>
              <div className="h-6 w-px bg-gray-300 mx-1 hidden sm:block" />
              <button
                onClick={onToggleRightPanel}
                className={`p-2 rounded-md transition-colors ${isRightPanelOpen ? 'bg-blue-100 text-blue-700' : 'text-gray-500 hover:bg-gray-100'}`}
                title={isRightPanelOpen ? 'Hide Preview' : 'Show Preview'}
              >
                <PanelRight className="w-5 h-5" />
              </button>
            </>
          )}

          {currentView === 'sql' && (
            <>
              <div className="h-6 w-px bg-gray-300 mx-1 hidden sm:block" />
              <Button
                variant="primary"
                size="sm"
                icon={<Play className="w-4 h-4" />}
                onClick={onRunSql}
                disabled={!projectId || !canExecute}
                className={`px-2 md:px-4 ${!projectId || !canExecute ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                <span className="hidden md:inline">Run</span>
              </Button>
            </>
          )}

          <div className="h-6 w-px bg-gray-300 mx-1" />
          <button
            onClick={onSettingsOpen}
            className="p-2 text-gray-500 hover:bg-gray-100 hover:text-gray-700 rounded-md transition-colors"
            title="Global Settings (Connection & Appearance)"
          >
            <Settings className="w-5 h-5" />
          </button>
        </div>
      </header>
    </>
  );
};
