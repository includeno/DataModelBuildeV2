
import React, { useState, useEffect, useMemo } from 'react';
import { RefreshCw, LayoutDashboard, ChevronDown, ChevronRight, Database, Table as TableIcon, Download, List, FileDown } from 'lucide-react';
import { ExecutionResult, OperationNode, SubTableConfig } from '../types';
import { Button } from './Button';

interface ComplexDataPreviewProps {
  initialResult: ExecutionResult | null;
  selectedNode: OperationNode;
  loading: boolean;
  onRefreshView: (viewId: string, page: number, pageSize: number) => Promise<ExecutionResult>;
  onExportFull: () => void;
  mainSourceName?: string;
}

interface SubTableViewState {
    data: ExecutionResult | null;
    loading: boolean;
}

export const ComplexDataPreview: React.FC<ComplexDataPreviewProps> = ({
  initialResult,
  selectedNode,
  loading: mainLoading,
  onRefreshView,
  onExportFull,
  mainSourceName
}) => {
  const multiCmd = selectedNode.commands.find(c => c.type === 'multi_table');
  const subTables = multiCmd?.config.subTables || [];

  const [mainPageSize, setMainPageSize] = useState(50);
  const [subTableStates, setSubTableStates] = useState<Record<string, SubTableViewState>>({});
  const [expandedRowIndices, setExpandedRowIndices] = useState<Set<number>>(new Set());
  const [activeSubTab, setActiveSubTab] = useState<string>('');
  const [isExportMenuOpen, setIsExportMenuOpen] = useState(false);

  // Auto-select first tab if active tab is invalid or empty
  useEffect(() => {
      if (subTables.length > 0) {
          const activeExists = subTables.find(s => s.id === activeSubTab);
          if (!activeSubTab || !activeExists) {
              setActiveSubTab(subTables[0].id);
          }
      } else {
          setActiveSubTab('');
      }
  }, [subTables, activeSubTab]);

  // Initial Fetch of all sub-tables to have data ready for the "preview"
  useEffect(() => {
      const fetchAllSubTables = async () => {
          const newStates: Record<string, SubTableViewState> = {};
          
          await Promise.all(subTables.map(async (sub) => {
              newStates[sub.id] = { data: null, loading: true };
              try {
                  // Fetch a larger chunk for preview purposes to cover most visible main rows
                  const res = await onRefreshView(sub.id, 1, 200);
                  newStates[sub.id] = { data: res, loading: false };
              } catch (e) {
                  console.error(`Failed to fetch sub table ${sub.id}`, e);
                  newStates[sub.id] = { data: null, loading: false };
              }
          }));
          
          setSubTableStates(newStates);
      };

      if (subTables.length > 0) {
          fetchAllSubTables();
      }
  }, [selectedNode.id, subTables.length]); // Re-fetch if node or config changes

  const toggleRow = (index: number) => {
      const newSet = new Set(expandedRowIndices);
      if (newSet.has(index)) {
          newSet.delete(index);
      } else {
          newSet.add(index);
      }
      setExpandedRowIndices(newSet);
  };

  const getMatchedSubRowsInfo = (mainRow: any, subConfig: SubTableConfig) => {
      const subState = subTableStates[subConfig.id];
      if (!subState || !subState.data || !subState.data.rows) return { rows: [], info: "No data loaded" };

      // Parse Join Condition (Simple equality support for preview: main.id = sub.uid)
      const condition = subConfig.on || "";
      if (!condition.includes('=')) return { rows: subState.data.rows, info: "No condition" }; // Return all if no condition (fallback)

      const parts = condition.split('=').map(s => s.trim());
      // Attempt to identify keys. Heuristic: parts starting with 'main.' vs 'sub.'
      let mainKey = 'id';
      let subKey = 'uid';
      
      parts.forEach(p => {
          if (p.startsWith('main.')) mainKey = p.replace('main.', '');
          else if (p.startsWith('sub.')) subKey = p.replace('sub.', '');
          else if (p.includes('.')) {
               // Fallback for named tables e.g. "employees.id"
               if(p.startsWith(selectedNode.name)) mainKey = p.split('.')[1];
               else subKey = p.split('.')[1]; 
          } else {
              // Blind guess if no prefixes
              if (mainRow[p] !== undefined) mainKey = p;
              else subKey = p;
          }
      });

      const mainVal = mainRow[mainKey];
      
      // Filter sub rows
      const filtered = subState.data.rows.filter((subRow: any) => {
          // Loose comparison for string vs number issues
          return String(subRow[subKey]) === String(mainVal);
      });

      return { 
          rows: filtered, 
          info: `Match: main.${mainKey}[${mainVal}] == sub.${subKey}`,
          matchCount: filtered.length
      };
  };

  const handleExportCsvPage = () => {
     if (!initialResult || !initialResult.rows.length) return;
     const headers = initialResult.columns || Object.keys(initialResult.rows[0]);
     const csvContent = [
       headers.join(','),
       ...initialResult.rows.map(row => headers.map(fieldName => {
         let val = row[fieldName];
         if (val === null || val === undefined) return '';
         val = String(val).replace(/"/g, '""');
         if (val.search(/("|,|\n)/g) >= 0) val = `"${val}"`;
         return val;
       }).join(','))
     ].join('\n');
 
     const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
     const url = URL.createObjectURL(blob);
     const link = document.createElement('a');
     link.href = url;
     link.setAttribute('download', `export_complex_page_${Date.now()}.csv`);
     document.body.appendChild(link);
     link.click();
     document.body.removeChild(link);
     setIsExportMenuOpen(false);
  };

  if (mainLoading) {
    return (
        <div className="flex-1 flex flex-col items-center justify-center bg-white h-full">
            <RefreshCw className="w-8 h-8 text-blue-500 animate-spin mb-4" />
            <span className="text-gray-500 font-medium">Loading Complex View...</span>
        </div>
    );
  }

  if (!initialResult || initialResult.rows.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-gray-50/50 h-full">
        <div className="bg-white p-4 rounded-full shadow-sm mb-4">
            <LayoutDashboard className="w-10 h-10 text-gray-300" />
        </div>
        <p className="text-gray-900 font-medium">No Main Stream Data</p>
        <p className="text-gray-500 text-sm mt-1">Adjust upstream filters or check source data.</p>
      </div>
    );
  }

  const columns = initialResult.columns || Object.keys(initialResult.rows[0]);
  const page = initialResult.page || 1;
  const currentPageSize = initialResult.pageSize || 50;
  const totalPages = Math.ceil(initialResult.totalCount / currentPageSize);

  return (
    <div className="flex flex-col h-full bg-white relative">
        {isExportMenuOpen && <div className="fixed inset-0 z-20" onClick={() => setIsExportMenuOpen(false)} />}
        
        {/* Header Bar */}
        <div className="px-5 py-2 border-b border-gray-200 flex justify-between items-center bg-white sticky top-0 z-10 shadow-sm">
            <div className="flex items-center space-x-3">
                <div className="flex items-center space-x-2">
                    <div className="p-1 bg-blue-100 rounded text-blue-600">
                        <LayoutDashboard className="w-4 h-4" />
                    </div>
                    <div>
                        <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider leading-none">Main Stream</div>
                        <div className="text-sm font-bold text-gray-800 leading-none mt-0.5">{selectedNode.name}</div>
                    </div>
                </div>
                {mainSourceName && (
                    <span className="text-[10px] bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full border border-gray-200 font-mono">
                        {mainSourceName}
                    </span>
                )}
                <span className="h-4 w-px bg-gray-300 mx-2"></span>
                <span className="px-2.5 py-0.5 rounded-full bg-blue-50 text-blue-700 text-[11px] font-semibold border border-blue-100">
                    {initialResult.totalCount} Rows
                </span>
            </div>

            <div className="flex space-x-2 relative">
                 <Button 
                     variant="secondary" 
                     size="sm" 
                     icon={<Download className="w-3.5 h-3.5"/>}
                     onClick={() => setIsExportMenuOpen(!isExportMenuOpen)}
                 >
                     Export
                 </Button>
                 {isExportMenuOpen && (
                     <div className="absolute right-0 top-full mt-1 w-48 bg-white border border-gray-200 rounded-lg shadow-xl z-30 flex flex-col animate-in fade-in zoom-in-95 duration-100">
                          <button onClick={handleExportCsvPage} className="text-left px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 flex items-center">
                              <List className="w-4 h-4 mr-2 text-gray-400" /> Export Current Page
                          </button>
                          {onExportFull && (
                             <button onClick={() => { onExportFull(); setIsExportMenuOpen(false); }} className="text-left px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 flex items-center border-t border-gray-100">
                                 <FileDown className="w-4 h-4 mr-2 text-blue-500" /> Export All Rows (Full)
                             </button>
                          )}
                     </div>
                 )}
                 <button 
                     onClick={() => onRefreshView('main', page, mainPageSize)}
                     className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors"
                     title="Refresh Data"
                 >
                     <RefreshCw className="w-4 h-4" />
                 </button>
            </div>
        </div>

        {/* Master-Detail Table */}
        <div className="flex-1 overflow-auto bg-gray-50/30">
            <table className="min-w-full divide-y divide-gray-200 border-separate border-spacing-0">
                <thead className="bg-gray-50 sticky top-0 z-10">
                    <tr>
                        <th className="sticky left-0 bg-gray-50 z-20 px-4 py-3 text-center w-10 border-b border-gray-200">
                            {/* Toggle Header */}
                        </th>
                        <th className="px-4 py-3 text-left text-[10px] font-bold text-gray-400 uppercase tracking-wider border-b border-gray-200">#</th>
                        {columns.map((col) => (
                            <th key={col} className="px-6 py-3 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap border-b border-gray-200">
                                {col}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-100">
                    {initialResult.rows.map((row, idx) => {
                        const isExpanded = expandedRowIndices.has(idx);
                        const rowNum = (page - 1) * mainPageSize + idx + 1;
                        
                        return (
                            <React.Fragment key={idx}>
                                <tr 
                                    className={`transition-colors hover:bg-blue-50/30 ${isExpanded ? 'bg-blue-50/50' : ''}`}
                                    onClick={() => toggleRow(idx)}
                                >
                                    <td className="sticky left-0 bg-inherit px-2 py-3 text-center border-r border-gray-100 cursor-pointer">
                                        {isExpanded ? <ChevronDown className="w-4 h-4 text-blue-600 mx-auto" /> : <ChevronRight className="w-4 h-4 text-gray-400 mx-auto" />}
                                    </td>
                                    <td className="px-4 py-3 text-xs text-gray-400 font-mono text-center w-12">{rowNum}</td>
                                    {columns.map((col) => (
                                        <td key={`${idx}-${col}`} className="px-6 py-3 text-sm text-gray-700 whitespace-nowrap font-medium">
                                            {typeof row[col] === 'object' ? JSON.stringify(row[col]) : row[col]}
                                        </td>
                                    ))}
                                </tr>
                                
                                {isExpanded && (
                                    <tr>
                                        <td colSpan={columns.length + 2} className="p-0 border-b border-gray-200 bg-gray-50 shadow-inner">
                                            <div className="pl-12 pr-4 py-4">
                                                {/* Sub Table Tabs */}
                                                <div className="flex items-center space-x-1 mb-0 border-b border-gray-200 overflow-x-auto">
                                                    {subTables.length === 0 ? (
                                                        <div className="px-3 py-2 text-xs text-gray-400 italic">No sub-tables configured.</div>
                                                    ) : subTables.map(sub => (
                                                        <button
                                                            key={sub.id}
                                                            onClick={(e) => { e.stopPropagation(); setActiveSubTab(sub.id); }}
                                                            className={`px-3 py-1.5 text-xs font-semibold rounded-t-md border-t border-x border-b-0 transition-colors flex items-center space-x-2 shrink-0 ${
                                                                activeSubTab === sub.id 
                                                                ? 'bg-white border-gray-200 text-blue-700 shadow-sm relative top-[1px]' 
                                                                : 'bg-transparent border-transparent text-gray-500 hover:bg-gray-200/50'
                                                            }`}
                                                        >
                                                            <Database className="w-3 h-3 opacity-70" />
                                                            <span>{sub.label || sub.table}</span>
                                                        </button>
                                                    ))}
                                                </div>

                                                {/* Sub Table Content */}
                                                <div className="bg-white border border-gray-200 rounded-b-md rounded-tr-md p-4 shadow-sm min-h-[100px]">
                                                    {(() => {
                                                        const activeConfig = subTables.find(s => s.id === activeSubTab);
                                                        
                                                        // Case: No Sub Tables
                                                        if (!activeConfig) {
                                                            if (subTables.length > 0) return <div className="text-gray-400 text-xs italic">Select a sub-table tab above.</div>;
                                                            return <div className="text-gray-400 text-xs italic">Add a sub-table in the configuration panel to see details here.</div>;
                                                        }
                                                        
                                                        // Case: Loading
                                                        const subState = subTableStates[activeConfig.id];
                                                        if (subState?.loading) return (
                                                            <div className="flex items-center justify-center py-8 text-gray-400 text-xs">
                                                                <RefreshCw className="w-4 h-4 animate-spin mr-2" /> Loading related records...
                                                            </div>
                                                        );

                                                        // Case: Matching
                                                        const { rows: matchedRows, info } = getMatchedSubRowsInfo(row, activeConfig);
                                                        
                                                        // Case: No Matches
                                                        if (matchedRows.length === 0) return (
                                                            <div className="flex flex-col items-center justify-center py-6 text-gray-400">
                                                                <TableIcon className="w-8 h-8 opacity-20 mb-2" />
                                                                <span className="text-xs italic">No related records found.</span>
                                                                <span className="text-[10px] text-gray-300 mt-1 font-mono">{info}</span>
                                                            </div>
                                                        );

                                                        const subCols = Object.keys(matchedRows[0]);
                                                        
                                                        return (
                                                            <div className="overflow-x-auto">
                                                                <table className="min-w-full divide-y divide-gray-100">
                                                                    <thead className="bg-gray-50">
                                                                        <tr>
                                                                            {subCols.map(c => (
                                                                                <th key={c} className="px-3 py-2 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider">{c}</th>
                                                                            ))}
                                                                        </tr>
                                                                    </thead>
                                                                    <tbody className="divide-y divide-gray-50">
                                                                        {matchedRows.map((subRow: any, sIdx: number) => (
                                                                            <tr key={sIdx} className="hover:bg-gray-50">
                                                                                {subCols.map(c => (
                                                                                    <td key={c} className="px-3 py-1.5 text-xs text-gray-700 whitespace-nowrap font-mono">{String(subRow[c])}</td>
                                                                                ))}
                                                                            </tr>
                                                                        ))}
                                                                    </tbody>
                                                                </table>
                                                                <div className="mt-2 flex justify-between items-center px-2">
                                                                    <span className="text-[10px] text-gray-300 font-mono">{info}</span>
                                                                    <div className="text-[10px] text-gray-400 text-right">
                                                                        Showing {matchedRows.length} related record(s)
                                                                    </div>
                                                                </div>
                                                            </div>
                                                        );
                                                    })()}
                                                </div>
                                            </div>
                                        </td>
                                    </tr>
                                )}
                            </React.Fragment>
                        );
                    })}
                </tbody>
            </table>
        </div>
        
        {/* Pagination Footer */}
        <div className="px-5 py-2 border-t border-gray-200 bg-white text-xs text-gray-500 flex justify-between items-center shrink-0">
             <div className="flex items-center space-x-4">
                 <span>Page {page} of {totalPages || 1}</span>
                 <span className="text-gray-300">|</span>
                 <span>{Math.min((page - 1) * currentPageSize + 1, initialResult.totalCount)} - {Math.min(page * currentPageSize, initialResult.totalCount)} displayed</span>
             </div>
             <div className="flex items-center space-x-1">
                <button 
                    onClick={() => onRefreshView('main', page - 1, mainPageSize)}
                    disabled={page <= 1}
                    className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                    <ChevronDown className="w-4 h-4 rotate-90" />
                </button>
                <button 
                    onClick={() => onRefreshView('main', page + 1, mainPageSize)}
                    disabled={page >= totalPages}
                    className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                    <ChevronRight className="w-4 h-4" />
                </button>
             </div>
        </div>
    </div>
  );
};
