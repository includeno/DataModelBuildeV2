import React from 'react';
import { Download, RefreshCw, Table as TableIcon } from 'lucide-react';
import { Button } from './Button';
import { ExecutionResult } from '../types';

interface DataPreviewProps {
  data: ExecutionResult | null;
  loading: boolean;
  onRefresh: () => void;
}

export const DataPreview: React.FC<DataPreviewProps> = ({ data, loading, onRefresh }) => {
  const handleExportCsv = () => {
    if (!data || !data.rows.length) return;

    // Get headers
    const headers = data.columns || Object.keys(data.rows[0]);
    
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
    link.setAttribute('download', `export_${Date.now()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
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
        <p className="text-gray-500 text-sm mt-1">Run the analysis or check your filters.</p>
      </div>
    );
  }

  const columns = data.columns || Object.keys(data.rows[0]);

  return (
    <div className="flex flex-col h-full bg-white border-t border-gray-200">
      <div className="px-5 py-3 border-b border-gray-200 flex justify-between items-center bg-white">
        <div className="flex items-center space-x-3">
            <span className="text-sm font-bold text-gray-800">Result Preview</span>
            <span className="px-2.5 py-0.5 rounded-full bg-blue-50 text-blue-700 text-[11px] font-semibold border border-blue-100">
                {data.totalCount} Rows
            </span>
        </div>
        <div className="flex space-x-2">
            <button 
                onClick={onRefresh}
                className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors"
                title="Refresh Data"
            >
                <RefreshCw className="w-4 h-4" />
            </button>
            <Button 
                variant="secondary" 
                size="sm" 
                icon={<Download className="w-3.5 h-3.5"/>}
                onClick={handleExportCsv}
            >
                Export CSV
            </Button>
        </div>
      </div>
      
      <div className="flex-1 overflow-auto bg-gray-50/30">
        <table className="min-w-full divide-y divide-gray-200 border-separate border-spacing-0">
          <thead className="bg-gray-50 sticky top-0 z-10">
            <tr>
              <th className="sticky left-0 bg-gray-50 z-20 px-4 py-3 text-center text-[10px] font-bold text-gray-400 uppercase tracking-wider w-12 border-b border-gray-200">
                #
              </th>
              {columns.map((col) => (
                <th key={col} className="px-6 py-3 text-left text-[10px] font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap border-b border-gray-200">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-100">
            {data.rows.map((row, idx) => (
              <tr key={idx} className="hover:bg-blue-50/40 transition-colors group">
                <td className="sticky left-0 bg-white group-hover:bg-blue-50/40 px-4 py-2.5 text-center text-xs text-gray-400 border-r border-gray-100 font-mono">
                  {idx + 1}
                </td>
                {columns.map((col) => (
                  <td key={`${idx}-${col}`} className="px-6 py-2.5 text-sm text-gray-700 whitespace-nowrap font-medium">
                    {typeof row[col] === 'object' ? JSON.stringify(row[col]) : row[col]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      <div className="px-5 py-2 border-t border-gray-200 bg-white text-xs text-gray-400 flex justify-between items-center">
         <span>Displaying first {Math.min(data.rows.length, 100)} rows</span>
         <span className="font-mono">Page 1</span>
      </div>
    </div>
  );
};
