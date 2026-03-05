
import React, { useState, useEffect, useRef } from 'react';
import { Play, Plus, X, Clock, CheckCircle, XCircle, Table as TableIcon, History, Terminal, Copy } from 'lucide-react';
import { Button } from './Button';
import { ExecutionResult, ApiConfig, SqlHistoryItem, Dataset } from '../types';
import { api } from '../utils/api';
import { DataPreview } from './DataPreview';

interface SqlEditorProps {
  sessionId: string;
  apiConfig: ApiConfig;
  datasets: Dataset[];
  targetTable?: string | null;
  onClearTarget?: () => void;
  runRequestId?: number;
  onRunStateChange?: (state: { canRun: boolean; running: boolean }) => void;
  history?: SqlHistoryItem[];
  onUpdateHistory?: (history: SqlHistoryItem[]) => void;
}

interface SqlTab {
  id: string;
  title: string;
  query: string;
  dataSource?: string; // Track which table this query is related to
  result: ExecutionResult & { columns?: string[] } | null;
  loading: boolean;
  error: string | null;
  executionTime?: number;
}

export const SqlEditor: React.FC<SqlEditorProps> = ({ 
    sessionId, 
    apiConfig, 
    datasets,
    targetTable, 
    onClearTarget,
    runRequestId,
    onRunStateChange,
    history = [],
    onUpdateHistory
}) => {
  // --- STATE ---
  const [tabs, setTabs] = useState<SqlTab[]>([
    { id: '1', title: 'Query 1', query: '', result: null, loading: false, error: null }
  ]);
  const [activeTabId, setActiveTabId] = useState<string>('1');
  const [showHistory, setShowHistory] = useState(false);
  const lastRunRequestId = useRef<number | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(0);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const runSeqRef = useRef(0);
  const lastRunByTabRef = useRef<Record<string, number>>({});

  // Derived state
  const activeTab = tabs.find(t => t.id === activeTabId) || tabs[0];

  // --- EFFECTS ---
  
  // Handle external request to open a specific table
  useEffect(() => {
      if (targetTable && onClearTarget) {
          handleOpenTable(targetTable);
          onClearTarget();
      }
  }, [targetTable, onClearTarget]);

  useEffect(() => {
      if (runRequestId === undefined) return;
      if (lastRunRequestId.current === runRequestId) return;
      lastRunRequestId.current = runRequestId;
      const current = getCurrentQuery();
      if (!current || activeTab.loading) return;
      executeQuery(1, current);
  }, [runRequestId, activeTab.query, activeTab.loading]);

  useEffect(() => {
      if (!onRunStateChange) return;
      const canRun = !!activeTab.query.trim();
      onRunStateChange({ canRun, running: activeTab.loading });
  }, [activeTab.query, activeTab.loading, onRunStateChange]);

  useEffect(() => {
      setSelectedSuggestionIndex(0);
  }, [activeTabId]);

  const handleOpenTable = (tableName: string) => {
      const newQuery = `SELECT * FROM ${tableName}`; // Removed LIMIT to rely on pagination
      
      // Always update the active tab with the new table context and query
      // Instead of creating a new tab.
      setTabs(prev => prev.map(t => {
          if (t.id === activeTabId) {
              return { 
                  ...t, 
                  query: newQuery,
                  dataSource: tableName,
                  error: null,
                  result: null,
                  loading: false
              };
          }
          return t;
      }));
  };

  const SQL_KEYWORDS = [
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL JOIN',
      'ON', 'GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'AND', 'OR', 'AS', 'DISTINCT',
      'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
  ];

  const datasetNames = datasets.map(d => d.name);
  const datasetFieldMap: Record<string, string[]> = datasets.reduce((acc, d) => {
      acc[d.name] = d.fieldTypes ? Object.keys(d.fieldTypes) : (d.fields || []);
      return acc;
  }, {} as Record<string, string[]>);

  const getTokenAtCursor = (text: string, cursor: number) => {
      let start = cursor - 1;
      while (start >= 0 && /[A-Za-z0-9_.]/.test(text[start])) start -= 1;
      start += 1;
      const token = text.slice(start, cursor);
      return { token, start };
  };

  const computeSuggestions = (text: string, cursor: number): string[] => {
      const { token } = getTokenAtCursor(text, cursor);
      const trimmed = token.trim();
      if (!trimmed) return [];

      const lower = trimmed.toLowerCase();
      if (trimmed.includes('.')) {
          const [tablePart, fieldPart = ''] = trimmed.split('.');
          const tableMatch = datasetNames.find(t => t.toLowerCase() === tablePart.toLowerCase());
          if (!tableMatch) return [];
          const fields = datasetFieldMap[tableMatch] || [];
          return fields
              .filter(f => f.toLowerCase().startsWith(fieldPart.toLowerCase()))
              .slice(0, 12)
              .map(f => `${tableMatch}.${f}`);
      }

      const keywordMatches = SQL_KEYWORDS.filter(k => k.toLowerCase().startsWith(lower));
      const tableMatches = datasetNames.filter(t => t.toLowerCase().startsWith(lower));
      const fieldMatches = Object.values(datasetFieldMap)
          .flat()
          .filter((f, i, arr) => arr.indexOf(f) === i)
          .filter(f => f.toLowerCase().startsWith(lower))
          .slice(0, 10);

      return [...keywordMatches, ...tableMatches, ...fieldMatches].slice(0, 12);
  };

  const applySuggestion = (suggestion: string) => {
      const el = textareaRef.current;
      if (!el) return;
      const cursor = el.selectionStart || 0;
      const { token, start } = getTokenAtCursor(activeTab.query, cursor);
      const before = activeTab.query.slice(0, start);
      const after = activeTab.query.slice(start + token.length);
      const trailingSpace = suggestion.endsWith(' ') ? '' : ' ';
      const next = `${before}${suggestion}${trailingSpace}${after}`;
      updateActiveTab({ query: next });
      const nextCursor = before.length + suggestion.length + 1;
      requestAnimationFrame(() => {
          if (textareaRef.current) {
              textareaRef.current.selectionStart = nextCursor;
              textareaRef.current.selectionEnd = nextCursor;
          }
      });
      setShowSuggestions(false);
  };

  // --- ACTIONS ---

  const handleAddTab = () => {
    const newId = String(Date.now());
    const newTab: SqlTab = {
      id: newId,
      title: `Query ${tabs.length + 1}`,
      query: '',
      result: null,
      loading: false,
      error: null
    };
    setTabs([...tabs, newTab]);
    setActiveTabId(newId);
  };

  const handleCloseTab = (e: React.MouseEvent, id: string) => {
    e.stopPropagation();
    if (tabs.length === 1) return; // Don't close last tab
    
    const newTabs = tabs.filter(t => t.id !== id);
    setTabs(newTabs);
    
    if (activeTabId === id) {
      setActiveTabId(newTabs[newTabs.length - 1].id);
    }
  };

  const updateActiveTab = (updates: Partial<SqlTab>) => {
    setTabs(prev => prev.map(t => t.id === activeTabId ? { ...t, ...updates } : t));
  };

  const updateTabById = (tabId: string, updates: Partial<SqlTab>) => {
    setTabs(prev => prev.map(t => t.id === tabId ? { ...t, ...updates } : t));
  };

  const getCurrentQuery = () => {
      const live = textareaRef.current?.value;
      return (live !== undefined ? live : activeTab.query).trim();
  };

  const executeQuery = async (page: number = 1, overrideQuery?: string) => {
    const tabId = activeTabId;
    const queryText = (overrideQuery ?? getCurrentQuery()).trim();
    if (!queryText) return;
    
    const runId = ++runSeqRef.current;
    lastRunByTabRef.current[tabId] = runId;

    updateTabById(tabId, { loading: true, error: null, ...(page === 1 ? { result: null } : {}) });
    const startTime = performance.now();
    
    try {
        const data = await api.post(apiConfig, '/query', { 
            sessionId, 
            query: queryText,
            page: page,
            pageSize: 50
        });
        const duration = Math.round(performance.now() - startTime);
        
        if (lastRunByTabRef.current[tabId] !== runId) return;

        updateTabById(tabId, { 
            loading: false, 
            result: data, 
            executionTime: duration 
        });

        // Add to history only on first page execution to avoid clutter
        if (page === 1) {
            addToHistory(queryText, 'success', duration, data.totalCount);
        }

    } catch (err: any) {
        const duration = Math.round(performance.now() - startTime);
        if (lastRunByTabRef.current[tabId] !== runId) return;

        updateTabById(tabId, { 
            loading: false, 
            error: err.message 
        });
        
        if (page === 1) {
            addToHistory(queryText, 'error', duration, undefined, err.message);
        }
    }
  };

  const addToHistory = (query: string, status: 'success' | 'error', durationMs: number, rowCount?: number, errorMessage?: string) => {
      const newItem: SqlHistoryItem = {
          id: String(Date.now()),
          timestamp: Date.now(),
          query,
          status,
          durationMs,
          rowCount,
          errorMessage
      };
      if (onUpdateHistory) {
          onUpdateHistory([newItem, ...history]);
      }
  };

  const clearHistory = () => {
      if (onUpdateHistory) {
          onUpdateHistory([]);
      }
  };

  const restoreFromHistory = (query: string) => {
      updateActiveTab({ query });
  };

  // --- RENDER HELPERS ---
  
  // Format timestamp
  const formatTime = (ms: number) => new Date(ms).toLocaleTimeString();

  const activeTable = activeTab.dataSource;
  const headerText = activeTable || (apiConfig.isMock ? 'Mock DB' : 'DuckDB');

  return (
    <div className="flex flex-col h-full bg-gray-100 overflow-hidden">
        
        {/* TOP BAR: Tabs & Toolbar */}
        <div className="bg-gray-50 border-b border-gray-200 flex flex-col shrink-0">
            {/* Tab Bar */}
            <div className="flex items-center px-1 pt-1 space-x-1 overflow-x-auto no-scrollbar">
                {tabs.map(tab => (
                    <div 
                        key={tab.id}
                        onClick={() => setActiveTabId(tab.id)}
                        className={`
                            group flex items-center space-x-2 px-4 py-2 text-xs font-medium border-t-2 rounded-t-md cursor-pointer select-none transition-colors min-w-[120px] max-w-[200px]
                            ${activeTabId === tab.id 
                                ? 'bg-white border-blue-500 text-blue-700 shadow-[0_-1px_2px_rgba(0,0,0,0.05)]' 
                                : 'bg-gray-100 border-transparent text-gray-500 hover:bg-gray-200 hover:text-gray-700'
                            }
                        `}
                    >
                        <span className="truncate flex-1">{tab.title}</span>
                        {tabs.length > 1 && (
                            <button 
                                onClick={(e) => handleCloseTab(e, tab.id)}
                                className="opacity-0 group-hover:opacity-100 p-0.5 rounded-full hover:bg-gray-300 text-gray-500"
                                aria-label={`Close ${tab.title}`}
                                title="Close Tab"
                            >
                                <X className="w-3 h-3" />
                            </button>
                        )}
                    </div>
                ))}
                <button 
                    onClick={handleAddTab}
                    className="p-1.5 ml-1 rounded hover:bg-gray-200 text-gray-500 transition-colors"
                    title="New Query Tab"
                >
                    <Plus className="w-4 h-4" />
                </button>
            </div>

            {/* Action Toolbar */}
            <div className="flex items-center justify-between px-4 py-2 bg-white border-t border-gray-100">
                <div className="flex items-center space-x-2 text-xs text-gray-500">
                     <Terminal className={`w-4 h-4 ${activeTable ? 'text-blue-600' : 'text-gray-400'}`} />
                     <span className={`font-mono ${activeTable ? 'font-semibold text-gray-800' : 'text-gray-500'}`}>
                        {headerText}
                     </span>
                     {activeTab.executionTime !== undefined && !activeTab.loading && (
                         <span className="ml-4 flex items-center text-green-600 bg-green-50 px-2 py-0.5 rounded-full">
                             <Clock className="w-3 h-3 mr-1" />
                             {activeTab.executionTime}ms
                         </span>
                     )}
                </div>
                <div className="flex items-center space-x-3">
                    <button 
                        onClick={() => setShowHistory(!showHistory)}
                        className={`flex items-center px-3 py-1.5 text-xs font-medium rounded transition-colors ${showHistory ? 'bg-gray-200 text-gray-900' : 'text-gray-600 hover:bg-gray-100'}`}
                    >
                        <History className="w-3.5 h-3.5 mr-1.5" />
                        Log
                    </button>
                    <div className="h-4 w-px bg-gray-300 mx-1" />
                    <Button 
                        variant="primary" 
                        size="sm" 
                        icon={<Play className="w-3 h-3" />}
                        onClick={() => executeQuery(1)}
                        disabled={activeTab.loading || !activeTab.query}
                        className={activeTab.loading ? 'opacity-80' : ''}
                    >
                        {activeTab.loading ? 'Running...' : 'Run Query'}
                    </Button>
                </div>
            </div>
        </div>

        {/* MAIN CONTENT AREA */}
        <div className="flex-1 flex overflow-hidden">
            
            {/* Editor & Results Split */}
            <div className="flex-1 flex flex-col min-w-0">
                {/* DARK MODE EDITOR */}
                <div className="h-[40%] bg-[#1e1e1e] flex flex-col shrink-0 relative border-b border-gray-700">
                    <textarea
                        className="flex-1 w-full h-full p-4 font-mono text-sm resize-none focus:outline-none bg-[#1e1e1e] text-[#d4d4d4] leading-relaxed selection:bg-[#264f78]"
                        placeholder="-- Enter your SQL query here&#10;SELECT * FROM table_name;"
                        value={activeTab.query}
                        ref={textareaRef}
                        onChange={(e) => {
                            const next = e.target.value;
                            updateActiveTab({ query: next });
                            const cursor = e.target.selectionStart || 0;
                            const nextSuggestions = computeSuggestions(next, cursor);
                            setSuggestions(nextSuggestions);
                            setShowSuggestions(nextSuggestions.length > 0);
                        }}
                        onKeyDown={(e) => {
                            if (!showSuggestions || suggestions.length === 0) return;
                            if (e.key === 'ArrowDown') {
                                e.preventDefault();
                                setSelectedSuggestionIndex((i) => Math.min(i + 1, suggestions.length - 1));
                            } else if (e.key === 'ArrowUp') {
                                e.preventDefault();
                                setSelectedSuggestionIndex((i) => Math.max(i - 1, 0));
                            } else if (e.key === 'Tab' || e.key === 'Enter') {
                                e.preventDefault();
                                applySuggestion(suggestions[selectedSuggestionIndex]);
                            } else if (e.key === 'Escape') {
                                setShowSuggestions(false);
                            }
                        }}
                        onKeyUp={(e) => {
                            if (e.key === 'ArrowUp' || e.key === 'ArrowDown' || e.key === 'Enter' || e.key === 'Tab') return;
                            const el = textareaRef.current;
                            if (!el) return;
                            const cursor = el.selectionStart || 0;
                            const nextSuggestions = computeSuggestions(el.value, cursor);
                            setSuggestions(nextSuggestions);
                            setShowSuggestions(nextSuggestions.length > 0);
                        }}
                        onBlur={() => setShowSuggestions(false)}
                        spellCheck={false}
                    />
                    {showSuggestions && suggestions.length > 0 && (
                        <div className="absolute left-4 bottom-4 bg-[#1f2937] border border-[#374151] rounded-md shadow-lg w-80 max-h-56 overflow-y-auto z-20">
                            {suggestions.map((s, i) => (
                                <button
                                    key={`${s}-${i}`}
                                    className={`w-full text-left px-3 py-1.5 text-xs font-mono ${
                                        i === selectedSuggestionIndex ? 'bg-[#2563eb] text-white' : 'text-[#d1d5db] hover:bg-[#374151]'
                                    }`}
                                    onMouseDown={(e) => {
                                        e.preventDefault();
                                        applySuggestion(s);
                                    }}
                                >
                                    {s}
                                </button>
                            ))}
                            <div className="px-3 py-1 text-[10px] text-gray-400 border-t border-[#374151]">
                                Tab/Enter to accept, Esc to close
                            </div>
                        </div>
                    )}
                    {/* Error Banner Overlay */}
                    {activeTab.error && (
                        <div className="absolute bottom-0 left-0 right-0 bg-red-900/90 text-red-100 text-xs p-2 backdrop-blur-sm border-t border-red-700 flex justify-between items-center">
                            <span className="font-mono">{activeTab.error}</span>
                            <button onClick={() => updateActiveTab({ error: null })}><X className="w-4 h-4" /></button>
                        </div>
                    )}
                </div>

                {/* RESULTS TABLE */}
                <div className="flex-1 overflow-auto bg-gray-50 relative">
                    {!activeTab.loading && !activeTab.result && !activeTab.error && (
                        <div className="flex flex-col items-center justify-center h-full text-gray-400">
                            <TableIcon className="w-12 h-12 mb-3 opacity-20" />
                            <p className="text-sm">Results will appear here</p>
                        </div>
                    )}
                    
                    {/* Reusing DataPreview logic for pagination and rendering */}
                    {(activeTab.result || activeTab.loading) && (
                        <div className="h-full">
                            <DataPreview 
                                data={activeTab.result}
                                loading={activeTab.loading}
                                onRefresh={() => executeQuery(activeTab.result?.page || 1)}
                                onPageChange={(newPage) => executeQuery(newPage)}
                            />
                        </div>
                    )}
                </div>
            </div>

            {/* HISTORY SIDE PANEL (Collapsible) */}
            {showHistory && (
                <div className="w-72 bg-white border-l border-gray-200 flex flex-col shrink-0 animate-in slide-in-from-right-10 duration-200">
                    <div className="p-3 border-b border-gray-100 bg-gray-50 flex justify-between items-center">
                        <span className="text-xs font-bold text-gray-600 uppercase tracking-wider">Execution Log</span>
                        <button onClick={clearHistory} className="text-[10px] text-gray-400 hover:text-red-500">Clear</button>
                    </div>
                    <div className="flex-1 overflow-y-auto p-2 space-y-2">
                        {history.length === 0 ? (
                            <div className="text-center text-gray-400 text-xs py-8 italic">No history yet</div>
                        ) : (
                            history.map(item => (
                                <div 
                                    key={item.id} 
                                    className={`group p-3 rounded-lg border text-xs cursor-pointer hover:shadow-md transition-all ${item.status === 'success' ? 'bg-white border-gray-200' : 'bg-red-50 border-red-100'}`}
                                    onClick={() => restoreFromHistory(item.query)}
                                >
                                    <div className="flex justify-between items-start mb-1">
                                        <span className={`flex items-center font-bold ${item.status === 'success' ? 'text-green-600' : 'text-red-600'}`}>
                                            {item.status === 'success' ? <CheckCircle className="w-3 h-3 mr-1" /> : <XCircle className="w-3 h-3 mr-1" />}
                                            {item.status === 'success' ? 'Success' : 'Error'}
                                        </span>
                                        <span className="text-gray-400 font-mono text-[10px]">{formatTime(item.timestamp)}</span>
                                    </div>
                                    <div className="font-mono text-gray-700 line-clamp-2 mb-2 bg-gray-50 p-1.5 rounded text-[10px] break-all border border-gray-100">
                                        {item.query}
                                    </div>
                                    <div className="flex justify-between items-center text-[10px] text-gray-500">
                                        <span>{item.durationMs}ms</span>
                                        {item.rowCount !== undefined && <span>{item.rowCount} rows</span>}
                                    </div>
                                    
                                    {/* Restore Button overlay */}
                                    <div className="hidden group-hover:flex justify-end mt-2 pt-2 border-t border-dashed border-gray-200">
                                         <span className="text-blue-600 flex items-center font-medium">
                                            <Copy className="w-3 h-3 mr-1" /> Use Query
                                         </span>
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>
            )}

        </div>
    </div>
  );
};
