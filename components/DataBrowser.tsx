import React, { useEffect, useMemo, useState } from 'react';
import { RefreshCw, Database, Filter, List, Table as TableIcon } from 'lucide-react';
import { ApiConfig, Dataset, ImportHistoryItem } from '../types';
import { api } from '../utils/api';
import { Button } from './Button';

interface DataBrowserProps {
  sessionId: string;
  apiConfig: ApiConfig;
  datasets: Dataset[];
  selectedTable?: string | null;
  onSelectTable?: (name: string) => void;
}

const FILTER_COLUMNS_ALL = '__all__';

export const DataBrowser: React.FC<DataBrowserProps> = ({
  sessionId,
  apiConfig,
  datasets,
  selectedTable,
  onSelectTable
}) => {
  const [activeTable, setActiveTable] = useState<string>('');
  const [limit, setLimit] = useState<number>(200);
  const [rows, setRows] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  const [filterText, setFilterText] = useState('');
  const [filterColumn, setFilterColumn] = useState(FILTER_COLUMNS_ALL);
  const [filterMode, setFilterMode] = useState<'contains' | 'equals' | 'starts' | 'ends'>('contains');
  const [caseSensitive, setCaseSensitive] = useState(false);

  const [imports, setImports] = useState<ImportHistoryItem[]>([]);
  const [importsLoading, setImportsLoading] = useState(false);
  const [importsError, setImportsError] = useState<string | null>(null);

  useEffect(() => {
      if (selectedTable && selectedTable !== activeTable) {
          setActiveTable(selectedTable);
      }
  }, [selectedTable, activeTable]);

  useEffect(() => {
      if (!activeTable && datasets.length > 0) {
          setActiveTable(datasets[0].name);
      }
  }, [datasets, activeTable]);

  useEffect(() => {
      let cancelled = false;
      const loadImports = async () => {
          setImportsLoading(true);
          setImportsError(null);
          try {
              const data = await api.get(apiConfig, `/sessions/${sessionId}/imports`) as ImportHistoryItem[];
              if (!cancelled) setImports(data || []);
          } catch (e: any) {
              if (!cancelled) setImportsError(e.message || 'Failed to load import history.');
          } finally {
              if (!cancelled) setImportsLoading(false);
          }
      };
      loadImports();
      return () => { cancelled = true; };
  }, [sessionId, apiConfig]);

  useEffect(() => {
      let cancelled = false;
      const loadPreview = async () => {
          if (!activeTable) {
              setRows([]);
              setColumns([]);
              return;
          }
          setLoading(true);
          setError(null);
          try {
              const res = await api.get(
                  apiConfig,
                  `/sessions/${sessionId}/datasets/${encodeURIComponent(activeTable)}/preview?limit=${limit}`
              ) as { rows: any[]; totalCount?: number };
              if (cancelled) return;
              const previewRows = res?.rows || [];
              setRows(previewRows);
              const ds = datasets.find(d => d.name === activeTable);
              const cols = ds?.fields && ds.fields.length > 0
                  ? ds.fields
                  : previewRows[0] ? Object.keys(previewRows[0]) : [];
              setColumns(cols);
          } catch (e: any) {
              if (!cancelled) setError(e.message || 'Failed to load preview data.');
          } finally {
              if (!cancelled) setLoading(false);
          }
      };
      loadPreview();
      return () => { cancelled = true; };
  }, [activeTable, limit, refreshKey, sessionId, apiConfig, datasets]);

  const filteredRows = useMemo(() => {
      const text = filterText.trim();
      if (!text) return rows;
      const normalized = caseSensitive ? text : text.toLowerCase();
      const match = (val: unknown) => {
          if (val === null || val === undefined) return false;
          const raw = String(val);
          const target = caseSensitive ? raw : raw.toLowerCase();
          if (filterMode === 'equals') return target === normalized;
          if (filterMode === 'starts') return target.startsWith(normalized);
          if (filterMode === 'ends') return target.endsWith(normalized);
          return target.includes(normalized);
      };
      if (filterColumn === FILTER_COLUMNS_ALL) {
          return rows.filter(row => columns.some(col => match(row[col])));
      }
      return rows.filter(row => match(row[filterColumn]));
  }, [rows, columns, filterText, filterColumn, filterMode, caseSensitive]);

  const activeDataset = datasets.find(d => d.name === activeTable);
  const totalCount = activeDataset?.totalCount;
  const previewCount = rows.length;

  const sortedImports = useMemo(() => {
      return [...imports].sort((a, b) => a.timestamp - b.timestamp);
  }, [imports]);

  const handleSelectTable = (name: string) => {
      setActiveTable(name);
      if (onSelectTable) onSelectTable(name);
  };

  return (
    <div className="flex flex-1 min-w-0 h-full bg-gray-50/50">
        <div className="flex flex-1 min-w-0 h-full">
            <div className="flex flex-col flex-1 min-w-0">
                <div className="px-6 py-4 border-b border-gray-200 bg-white flex items-center justify-between">
                    <div className="flex items-center space-x-3">
                        <Database className="w-5 h-5 text-blue-600" />
                        <div>
                            <div className="text-sm font-semibold text-gray-800">Raw Data Viewer</div>
                            <div className="text-xs text-gray-500">Browse imported datasets and filter raw rows.</div>
                        </div>
                    </div>
                    <div className="flex items-center space-x-2">
                        <Button
                            variant="secondary"
                            size="sm"
                            onClick={() => setRefreshKey(v => v + 1)}
                            icon={<RefreshCw className="w-3.5 h-3.5" />}
                        >
                            Refresh
                        </Button>
                    </div>
                </div>

                <div className="px-6 py-4 bg-white border-b border-gray-200">
                    <div className="grid grid-cols-12 gap-4 items-end">
                        <div className="col-span-12 md:col-span-4">
                            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Dataset</label>
                            <select
                                value={activeTable}
                                onChange={(e) => handleSelectTable(e.target.value)}
                                className="mt-1 w-full text-sm border-gray-200 rounded-md py-2 px-2 bg-white"
                            >
                                <option value="">-- Select Dataset --</option>
                                {datasets.map(ds => (
                                    <option key={ds.id} value={ds.name}>{ds.name}</option>
                                ))}
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-3">
                            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Preview Limit</label>
                            <select
                                value={limit}
                                onChange={(e) => setLimit(Number(e.target.value))}
                                className="mt-1 w-full text-sm border-gray-200 rounded-md py-2 px-2 bg-white"
                            >
                                <option value={50}>50 rows</option>
                                <option value={200}>200 rows</option>
                                <option value={500}>500 rows</option>
                                <option value={1000}>1000 rows</option>
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-5">
                            <div className="flex items-center space-x-2">
                                <div className="px-3 py-2 bg-blue-50 border border-blue-100 rounded-md text-xs text-blue-700">
                                    Preview: {previewCount}
                                </div>
                                <div className="px-3 py-2 bg-gray-50 border border-gray-100 rounded-md text-xs text-gray-600">
                                    Total: {totalCount ?? 'Unknown'}
                                </div>
                                {activeDataset?.id && (
                                    <div className="px-3 py-2 bg-gray-50 border border-gray-100 rounded-md text-xs text-gray-600 font-mono">
                                        {activeDataset.id}
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>

                    <div className="mt-4 grid grid-cols-12 gap-3 items-end">
                        <div className="col-span-12 md:col-span-3">
                            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Filter Column</label>
                            <select
                                value={filterColumn}
                                onChange={(e) => setFilterColumn(e.target.value)}
                                className="mt-1 w-full text-sm border-gray-200 rounded-md py-2 px-2 bg-white"
                            >
                                <option value={FILTER_COLUMNS_ALL}>All Columns</option>
                                {columns.map(col => (
                                    <option key={col} value={col}>{col}</option>
                                ))}
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-2">
                            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Match</label>
                            <select
                                value={filterMode}
                                onChange={(e) => setFilterMode(e.target.value as any)}
                                className="mt-1 w-full text-sm border-gray-200 rounded-md py-2 px-2 bg-white"
                            >
                                <option value="contains">Contains</option>
                                <option value="equals">Equals</option>
                                <option value="starts">Starts With</option>
                                <option value="ends">Ends With</option>
                            </select>
                        </div>
                        <div className="col-span-12 md:col-span-5">
                            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Filter Text</label>
                            <div className="mt-1 relative">
                                <Filter className="w-4 h-4 text-gray-400 absolute left-2 top-1/2 -translate-y-1/2" />
                                <input
                                    value={filterText}
                                    onChange={(e) => setFilterText(e.target.value)}
                                    placeholder="Type to filter rows..."
                                    className="w-full text-sm border-gray-200 rounded-md py-2 pl-8 pr-2 bg-white"
                                />
                            </div>
                        </div>
                        <div className="col-span-12 md:col-span-2 flex items-center space-x-2">
                            <label className="flex items-center space-x-2 text-xs text-gray-600">
                                <input
                                    type="checkbox"
                                    checked={caseSensitive}
                                    onChange={(e) => setCaseSensitive(e.target.checked)}
                                />
                                <span>Case Sensitive</span>
                            </label>
                            <Button variant="secondary" size="sm" onClick={() => setFilterText('')}>
                                Clear
                            </Button>
                        </div>
                    </div>
                </div>

                <div className="flex-1 min-h-0 overflow-hidden bg-white">
                    {error && (
                        <div className="p-4 text-sm text-red-600 bg-red-50 border-b border-red-100">{error}</div>
                    )}
                    {loading ? (
                        <div className="flex items-center justify-center h-full text-sm text-gray-500">
                            <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                            Loading preview...
                        </div>
                    ) : !activeTable ? (
                        <div className="flex flex-col items-center justify-center h-full text-gray-400">
                            <TableIcon className="w-10 h-10 mb-2 opacity-40" />
                            <div className="text-sm font-medium">Select a dataset to preview.</div>
                        </div>
                    ) : filteredRows.length === 0 ? (
                        <div className="flex flex-col items-center justify-center h-full text-gray-400">
                            <TableIcon className="w-10 h-10 mb-2 opacity-40" />
                            <div className="text-sm font-medium">No matching rows.</div>
                        </div>
                    ) : (
                        <div className="h-full overflow-auto">
                            <table className="min-w-full divide-y divide-gray-200 text-xs">
                                <thead className="bg-gray-50 sticky top-0 z-10">
                                    <tr>
                                        {columns.map(col => (
                                            <th key={col} className="px-3 py-2 text-left font-semibold text-gray-600 whitespace-nowrap">
                                                {col}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                    {filteredRows.map((row, idx) => (
                                        <tr key={idx} className="hover:bg-blue-50/40">
                                            {columns.map(col => (
                                                <td key={col} className="px-3 py-1.5 text-gray-700 whitespace-nowrap">
                                                    {row[col] === null || row[col] === undefined ? '' : String(row[col])}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>
            </div>

            <div className="hidden lg:flex w-80 border-l border-gray-200 bg-white flex-col">
                <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 flex items-center justify-between">
                    <div className="flex items-center space-x-2">
                        <List className="w-4 h-4 text-gray-500" />
                        <span className="text-xs font-bold text-gray-600 uppercase tracking-wider">Import History</span>
                    </div>
                </div>
                <div className="flex-1 overflow-y-auto">
                    {importsLoading && (
                        <div className="p-4 text-xs text-gray-500">Loading imports...</div>
                    )}
                    {importsError && (
                        <div className="p-4 text-xs text-red-600">{importsError}</div>
                    )}
                    {!importsLoading && !importsError && sortedImports.length === 0 && (
                        <div className="p-4 text-xs text-gray-400 italic">No imports recorded.</div>
                    )}
                    {!importsLoading && !importsError && sortedImports.length > 0 && (
                        <div className="p-3 space-y-2">
                            {sortedImports.map((item, index) => (
                                <button
                                    key={`${item.tableName}-${item.timestamp}-${index}`}
                                    onClick={() => handleSelectTable(item.tableName)}
                                    className={`w-full text-left border rounded-md p-2 text-xs transition-colors ${
                                        item.tableName === activeTable ? 'border-blue-300 bg-blue-50' : 'border-gray-200 hover:bg-gray-50'
                                    }`}
                                >
                                    <div className="flex items-center justify-between">
                                        <span className="font-semibold text-gray-700">#{index + 1} {item.datasetName || item.tableName}</span>
                                        <span className="text-[10px] text-gray-400">{new Date(item.timestamp).toLocaleString()}</span>
                                    </div>
                                    <div className="text-[10px] text-gray-500 mt-1">File: {item.originalFileName || '-'}</div>
                                    <div className="text-[10px] text-gray-500">Rows: {item.rows}</div>
                                </button>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    </div>
  );
};
