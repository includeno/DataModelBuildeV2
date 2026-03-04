

import React, { useState, useEffect, useRef } from 'react';
import { Download, RefreshCw, Table as TableIcon, ChevronLeft, ChevronRight, FileDown, List, Columns, Check } from 'lucide-react';
import { Button } from './Button';
import { ExecutionResult } from '../types';

interface DataPreviewProps {
  data: ExecutionResult | null;
  loading: boolean;
  pageSize?: number;
  onRefresh: () => void;
  onPageChange?: (newPage: number) => void;
  onUpdatePageSize?: (size: number) => void;
  onExportFull?: () => void;
  sourceId?: string;
}

export const DataPreview: React.FC<DataPreviewProps> = ({ 
  data, 
  loading, 
  pageSize, 
  onRefresh, 
  onPageChange,
  onUpdatePageSize,
  onExportFull,
  sourceId
}) => {
  const [isExportMenuOpen, setIsExportMenuOpen] = useState(false);
  const [isColumnMenuOpen, setIsColumnMenuOpen] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState<string[] | null>(null);
  const prevColumnsRef = useRef<string[]>([]);
  const prevSourceIdRef = useRef<string | undefined>(undefined);

  // Initialize visible columns when data changes
  useEffect(() => {
      if (data) {
          const cols = data.columns || Object.keys(data.rows[0] || {});
          const prevCols = prevColumnsRef.current;
          const prevSourceId = prevSourceIdRef.current;
          
          // Check if columns have changed OR sourceId has changed
          const isSameColumns = cols.length === prevCols.length && cols.every((col, i) => col === prevCols[i]);
          const isSameSource = sourceId === prevSourceId;
          
          if (!isSameColumns || !isSameSource) {
              setVisibleColumns(cols);
              prevColumnsRef.current = cols;
              prevSourceIdRef.current = sourceId;
          }
      }
  }, [data, sourceId]);

  const columns = data?.columns || (data?.rows?.[0] ? Object.keys(data.rows[0]) : []);
  const activeCols = visibleColumns === null ? columns : visibleColumns;

  const toggleColumn = (col: string) => {
      if (activeCols.includes(col)) {
          setVisibleColumns(activeCols.filter(c => c !== col));
      } else {
          setVisibleColumns([...activeCols, col]);
      }
  };

  const handleExportCsvPage = () => {
    if (!data || !data.rows.length) return;

    // Get headers (use visible columns)
    const headers = activeCols;
    
    // Convert rows to CSV format
    const csvContent = [
      headers.join(','), // Header row
      ...data.rows.map(row => {
        return headers.map(fieldName => {
          let val = row[fieldName];
          if (val === null || val === undefined) return '';
          // Escape quotes and wrap in quotes if contains comma, quote or newline
          val = String(val).replace(/"/g, '""');
          if (val.search(/("|,|\n)/g) >= 0) {
            val = `"${val}"`;
          }
          return val;
        }).join(',');
      })
    ].join('\n');

    // Create download link
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `export_page_${Date.now()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    setIsExportMenuOpen(false);
  };

  const handleExportFull = () => {
      if(onExportFull) onExportFull();
      setIsExportMenuOpen(false);
  };

  if (loading) {
    return (
        <div className="flex-1 flex flex-col items-center justify-center bg-white h-full border-t border-gray-200">
            <RefreshCw className="w-8 h-8 text-blue-500 animate-spin mb-4" />
            <span className="text-gray-500 font-medium">Processing Data...</span>
        </div>
    );
  }

  if (!data || data.rows.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-gray-50/50 h-full border-t border-gray-200">
        <div className="bg-white p-4 rounded-full shadow-sm mb-4">
            <TableIcon className="w-10 h-10 text-gray-300" />
        </div>
        <p className="text-gray-900 font-medium">No Data Available</p>
        <p className="text-gray-500 text-sm mt-1">Run the analysis or select a table.</p>
      </div>
    );
  }

  const page = data.page || 1;
  const currentPageSize = pageSize || data.pageSize || 50;
  const totalPages = Math.ceil(data.totalCount / currentPageSize);
  const formatCellValue = (value: unknown) => {
      if (value === null || value === undefined) return '';
      if (typeof value === 'boolean') return value ? 'true' : 'false';
      if (typeof value === 'object') return JSON.stringify(value);
      return value as string | number;
  };

  return (
    <div className="flex flex-col h-full bg-white relative">
      {(isExportMenuOpen || isColumnMenuOpen) && (
          <div className="fixed inset-0 z-20" onClick={() => { setIsExportMenuOpen(false); setIsColumnMenuOpen(false); }} />
      )}
      
      <div className="px-5 py-2 border-b border-gray-200 flex justify-between items-center bg-white sticky top-0 z-40">
        <div className="flex items-center space-x-3">
            <span className="text-sm font-bold text-gray-800">Preview</span>
            <span className="px-2.5 py-0.5 rounded-full bg-blue-50 text-blue-700 text-[11px] font-semibold border border-blue-100">
                {data.totalCount} Rows
            </span>
            
            {onUpdatePageSize && (
                <div className="flex items-center space-x-1 ml-4 border-l pl-4 border-gray-200">
                    <span className="text-xs text-gray-500">Page Size:</span>
                    <select 
                        value={currentPageSize}
                        onChange={(e) => onUpdatePageSize(Number(e.target.value))}
                        className="text-xs border-gray-200 rounded-md py-0.5 pl-1.5 pr-6 focus:ring-blue-500 focus:border-blue-500 bg-white"
                    >
                        <option value="20">20</option>
                        <option value="50">50</option>
                        <option value="100">100</option>
                        <option value="500">500</option>
                    </select>
                </div>
            )}
        </div>
        <div className="flex space-x-2 relative">
            <button 
                onClick={onRefresh}
                className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors"
                title="Refresh Data"
            >
                <RefreshCw className="w-4 h-4" />
            </button>
            
            <div className="relative">
                <Button 
                    variant="secondary" 
                    size="sm" 
                    icon={<Columns className="w-3.5 h-3.5"/>}
                    onClick={() => setIsColumnMenuOpen(!isColumnMenuOpen)}
                >
                    Columns
                </Button>
                
                {isColumnMenuOpen && (
                    <div className="absolute right-0 top-full mt-1 w-64 bg-white border border-gray-200 rounded-lg shadow-lg z-50 flex flex-col max-h-80 overflow-hidden">
                        <div className="p-2 border-b border-gray-100 flex justify-between items-center bg-gray-50">
                            <span className="text-xs font-semibold text-gray-600">Visible Columns</span>
                            <button 
                                onClick={() => setVisibleColumns(columns)}
                                className="text-[10px] text-blue-600 hover:text-blue-800 font-medium"
                            >
                                Reset All
                            </button>
                        </div>
                        <div className="overflow-y-auto p-1">
                            {columns.map(col => (
                                <label 
                                    key={col} 
                                    className="flex items-center space-x-2 px-2 py-1.5 hover:bg-gray-50 rounded cursor-pointer select-none transition-colors"
                                    onClick={(e) => {
                                        e.preventDefault();
                                        toggleColumn(col);
                                    }}
                                >
                                    <div className={`w-3.5 h-3.5 rounded border flex items-center justify-center transition-colors ${activeCols.includes(col) ? 'bg-blue-500 border-blue-500' : 'border-gray-300 bg-white'}`}>
                                        {activeCols.includes(col) && <Check className="w-2.5 h-2.5 text-white" strokeWidth={3} />}
                                    </div>
                                    <span className="text-xs text-gray-700 truncate flex-1" title={col}>{col}</span>
                                </label>
                            ))}
                        </div>
                    </div>
                )}
            </div>

            <div className="relative">
                <Button 
                    variant="secondary" 
                    size="sm" 
                    icon={<Download className="w-3.5 h-3.5"/>}
                    onClick={() => setIsExportMenuOpen(!isExportMenuOpen)}
                >
                    Export
                </Button>
                
                {isExportMenuOpen && (
                    <div className="absolute right-0 top-full mt-1 w-56 bg-white border border-gray-200 rounded-lg shadow-lg z-50 flex flex-col">
                         <button 
                            onClick={handleExportCsvPage}
                            className="text-left px-4 py-3 text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2 transition-colors"
                         >
                             <List className="w-4 h-4 text-gray-400" />
                             <span className="whitespace-nowrap">Export Current Page</span>
                         </button>
                         {onExportFull && (
                            <button 
                                onClick={handleExportFull}
                                className="text-left px-4 py-3 text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2 border-t border-gray-100 transition-colors"
                            >
                                <FileDown className="w-4 h-4 text-blue-500" />
                                <span className="whitespace-nowrap">Export All Rows (Full)</span>
                            </button>
                         )}
                    </div>
                )}
            </div>
        </div>
      </div>
      
      <div className="flex-1 overflow-auto bg-gray-50/30">
        <table className="min-w-full divide-y divide-gray-200 border-separate border-spacing-0">
          <thead className="bg-gray-50 sticky top-0 z-10">
            <tr>
              <th className="sticky left-0 bg-gray-50 z-20 px-4 py-3 text-center text-[10px] font-bold text-gray-400 tracking-wider w-12 border-b border-gray-200">
                #
              </th>
              {columns.filter(col => activeCols.includes(col)).map((col) => (
                <th key={col} className="px-6 py-3 text-left text-[10px] font-bold text-gray-500 tracking-wider whitespace-nowrap border-b border-gray-200">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-100">
            {data.rows.map((row, idx) => (
              <tr key={idx} className="hover:bg-blue-50/40 transition-colors group">
                <td className="sticky left-0 bg-white group-hover:bg-blue-50/40 px-4 py-2.5 text-center text-xs text-gray-400 border-r border-gray-100 font-mono">
                  {(page - 1) * currentPageSize + idx + 1}
                </td>
                {columns.filter(col => activeCols.includes(col)).map((col) => (
                  <td key={`${idx}-${col}`} className="px-6 py-2.5 text-sm text-gray-700 whitespace-nowrap font-medium">
                    {formatCellValue(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {/* Pagination Footer */}
      <div className="px-5 py-2 border-t border-gray-200 bg-white text-xs text-gray-500 flex justify-between items-center shrink-0">
         <div className="flex items-center space-x-4">
             <span>Page {page} of {totalPages || 1}</span>
             <span className="text-gray-300">|</span>
             <span>{Math.min((page - 1) * currentPageSize + 1, data.totalCount)} - {Math.min(page * currentPageSize, data.totalCount)} displayed</span>
         </div>
         
         {onPageChange && totalPages > 1 && (
            <div className="flex items-center space-x-1">
                <button 
                    onClick={() => onPageChange(page - 1)}
                    disabled={page <= 1}
                    className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                    <ChevronLeft className="w-4 h-4" />
                </button>
                <button 
                    onClick={() => onPageChange(page + 1)}
                    disabled={page >= totalPages}
                    className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                    <ChevronRight className="w-4 h-4" />
                </button>
            </div>
         )}
      </div>
    </div>
  );
};
