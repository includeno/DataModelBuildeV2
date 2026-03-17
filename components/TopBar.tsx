
import React, { useState } from 'react';
import { GitBranch, ChevronDown, Clock, Check, Trash2, Plus, Layers, Terminal, Play, PanelRight, Settings, Menu, SlidersHorizontal, Activity, Table as TableIcon, LogOut } from 'lucide-react';
import { SessionMetadata, ApiConfig } from '../types';
import { Button } from './Button';

interface TopBarProps {
  sessionId: string;
  sessionName?: string; 
  sessions: SessionMetadata[];
  currentView: 'workflow' | 'sql' | 'data';
  apiConfig: ApiConfig;
  isRightPanelOpen: boolean;
  backendStatus: 'mock' | 'checking' | 'online' | 'offline';
  onSessionSelect: (id: string) => void;
  onSessionCreate: () => void;
  onSessionDelete: (e: React.MouseEvent, id: string) => void;
  onViewChange: (view: 'workflow' | 'sql' | 'data') => void;
  onSettingsOpen: () => void;
  onSessionSettingsOpen: () => void;
  onSessionDiagnostics: () => void;
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

export const TopBar: React.FC<TopBarProps> = ({
  sessionId,
  sessionName,
  sessions,
  currentView,
  apiConfig,
  isRightPanelOpen,
  backendStatus,
  onSessionSelect,
  onSessionCreate,
  onSessionDelete,
  onViewChange,
  onSettingsOpen,
  onSessionSettingsOpen,
  onSessionDiagnostics,
  onRunSql,
  onToggleRightPanel,
  onToggleMobileSidebar,
  authEnabled = true,
  isAuthenticated = false,
  authChecking = false,
  authError = null,
  onLogout,
  canExecute = true
}) => {
  const [isSessionMenuOpen, setIsSessionMenuOpen] = useState(false);
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

  return (
    <>
      {/* Click outside handler for session menu */}
      {isSessionMenuOpen && (
          <div className="fixed inset-0 z-30" onClick={() => setIsSessionMenuOpen(false)} />
      )}

      <header className="h-14 bg-white border-b border-gray-200 flex items-center justify-between px-3 md:px-4 shrink-0 shadow-sm z-40 relative">
        <div className="flex items-center space-x-2 md:space-x-3">
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
          
          {/* SESSION MANAGER */}
          <div className="relative flex items-center space-x-1">
              <button 
                  onClick={() => setIsSessionMenuOpen(!isSessionMenuOpen)}
                  className={`flex items-center justify-between space-x-2 bg-white border hover:bg-gray-50 text-gray-900 px-2 md:px-3 py-1.5 rounded-md shadow-sm transition-all text-sm min-w-[160px] md:min-w-[240px] max-w-[320px] ${
                    !sessionId ? 'border-blue-400 ring-1 ring-blue-100' : 'border-gray-300'
                  }`}
              >
                  <div className="flex items-center overflow-hidden">
                      <span className="text-gray-400 mr-2 text-xs uppercase font-semibold hidden md:inline">Session</span>
                      <span className={`font-medium truncate ${!sessionId ? 'text-blue-600' : ''}`}>
                        {sessionName || sessionId || 'Create Session'}
                      </span>
                  </div>
                  <ChevronDown className={`w-4 h-4 text-gray-500 transition-transform ${isSessionMenuOpen ? 'rotate-180' : ''}`} />
              </button>
              
              {isSessionMenuOpen && (
                  <div className="absolute top-full left-0 mt-1 w-72 bg-white border border-gray-200 rounded-lg shadow-xl z-50 flex flex-col animate-in fade-in zoom-in-95 duration-100 origin-top-left">
                      <div className="px-4 py-2 border-b border-gray-100 bg-gray-50 rounded-t-lg">
                          <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Switch or Manage</span>
                      </div>
                      
                      <div className="max-h-[300px] overflow-y-auto p-1">
                          {sessions.map(s => (
                              <div 
                                  key={s.sessionId}
                                  onClick={() => {
                                      onSessionSelect(s.sessionId);
                                      setIsSessionMenuOpen(false);
                                  }}
                                  className={`group flex items-center justify-between px-3 py-2.5 rounded-md cursor-pointer transition-colors ${s.sessionId === sessionId ? 'bg-blue-50' : 'hover:bg-gray-100'}`}
                              >
                                  <div className="flex items-center min-w-0">
                                      <div className={`w-1.5 h-1.5 rounded-full mr-3 ${s.sessionId === sessionId ? 'bg-blue-500' : 'bg-gray-300'}`} />
                                      <div className="flex flex-col min-w-0">
                                          <span className={`text-sm font-medium truncate ${s.sessionId === sessionId ? 'text-blue-900' : 'text-gray-700'}`}>
                                              {s.displayName || s.sessionId}
                                          </span>
                                          {s.displayName && <span className="text-[10px] text-gray-400 truncate">{s.sessionId}</span>}
                                          <div className="flex items-center text-[10px] text-gray-400 mt-0.5">
                                              <Clock className="w-3 h-3 mr-1" />
                                              {new Date(s.createdAt).toLocaleTimeString()}
                                          </div>
                                      </div>
                                  </div>
                                  
                                  <div className="flex items-center">
                                      {s.sessionId === sessionId && <Check className="w-4 h-4 text-blue-500 mr-2" />}
                                      
                                      {/* Settings Button (Only for active session) */}
                                      {s.sessionId === sessionId && (
                                          <button
                                              onClick={(e) => {
                                                  e.stopPropagation();
                                                  onSessionSettingsOpen();
                                                  setIsSessionMenuOpen(false);
                                              }}
                                              className="p-1.5 rounded-md text-gray-400 hover:text-blue-600 hover:bg-blue-100 transition-all mr-1"
                                              title="Session Settings"
                                          >
                                              <SlidersHorizontal className="w-4 h-4" />
                                          </button>
                                      )}

                                      {s.sessionId === sessionId && (
                                          <button
                                              onClick={(e) => {
                                                  e.stopPropagation();
                                                  onSessionDiagnostics();
                                                  setIsSessionMenuOpen(false);
                                              }}
                                              className="p-1.5 rounded-md text-gray-400 hover:text-emerald-600 hover:bg-emerald-100 transition-all mr-1"
                                              title="Session Diagnostics"
                                          >
                                              <Activity className="w-4 h-4" />
                                          </button>
                                      )}

                                      {/* Only allow deleting if NOT mock mode */}
                                      {!apiConfig.isMock && (
                                          <button 
                                              onClick={(e) => onSessionDelete(e, s.sessionId)}
                                              className={`p-1.5 rounded-md text-gray-400 hover:text-red-600 hover:bg-red-50 transition-all ${sessions.length === 1 ? 'hidden' : 'opacity-0 group-hover:opacity-100'}`}
                                              title="Delete Session"
                                          >
                                              <Trash2 className="w-4 h-4" />
                                          </button>
                                      )}
                                  </div>
                              </div>
                          ))}
                          {sessions.length === 0 && (
                             <div className="p-3 text-center text-sm text-gray-400 italic">No active sessions found.<br/>Create one to start.</div>
                          )}
                      </div>
                      
                      {/* Create Button Footer - Hidden in Mock Mode */}
                      {!apiConfig.isMock && (
                          <div className="p-2 border-t border-gray-100 bg-gray-50/50 rounded-b-lg">
                              <button 
                                  onClick={() => {
                                      onSessionCreate();
                                      setIsSessionMenuOpen(false);
                                  }}
                                  className="w-full flex items-center justify-center space-x-2 bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 hover:border-gray-400 text-sm font-medium py-2 rounded-md transition-colors shadow-sm"
                              >
                                  <Plus className="w-4 h-4 text-green-600" />
                                  <span>Create New Session</span>
                              </button>
                          </div>
                      )}
                  </div>
              )}
          </div>
        </div>

        {/* VIEW SWITCHER TABS */}
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
        
        {/* Mobile View Switcher */}
        <div className="flex md:hidden items-center bg-gray-100 p-1 rounded-lg mr-2">
             <button 
                onClick={() => onViewChange('workflow')}
                className={`p-1.5 rounded-md ${currentView === 'workflow' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500'}`}
             >
                <Layers className="w-4 h-4" />
             </button>
             <button 
                onClick={() => onViewChange('sql')}
                className={`p-1.5 rounded-md ${currentView === 'sql' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500'}`}
             >
                <Terminal className="w-4 h-4" />
             </button>
             <button 
                onClick={() => onViewChange('data')}
                className={`p-1.5 rounded-md ${currentView === 'data' ? 'bg-white text-blue-700 shadow-sm' : 'text-gray-500'}`}
             >
                <TableIcon className="w-4 h-4" />
             </button>
        </div>

        <div className="flex items-center space-x-2">
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
                        title={isRightPanelOpen ? "Hide Preview" : "Show Preview"}
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
                        disabled={!sessionId || !canExecute} 
                        className={`px-2 md:px-4 ${!sessionId || !canExecute ? 'opacity-50 cursor-not-allowed' : ''}`}
                    >
                        <span className="hidden md:inline">Run</span>
                    </Button>
                </>
            )}

             {/* Global Settings Button */}
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
