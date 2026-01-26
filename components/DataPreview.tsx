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
  if (loading) {
    return (
        <div className="flex-1 flex flex-col items-center justify-center bg-gray-50 h-full border-t border-gray-200">
            <RefreshCw className="w-8 h-8 text-blue-500 animate-spin mb-4" />
            <span className="text-gray-500 font-medium">Processing Data...</span>
        </div>
    );
  }

  if (!data || data.rows.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-gray-50 h-full border-t border-gray-200">
        <TableIcon className="w-12 h-12 text-gray-300 mb-4" />
        <span className="text-gray-500">No data available. Run the operation to see results.</span>
      </div>
    );
  }

  const columns = Object.keys(data.rows[0]);

  return (
    <div className="flex flex-col h-full bg-white border-t border-gray-200">
      <div className="px-4 py-2 border-b border-gray-200 bg-gray-50 flex justify-between items-center">
        <div className="flex items-center space-x-2">
            <span className="text-sm font-semibold text-gray-700">Result Preview</span>
            <span className="px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 text-xs font-medium">
                {data.totalCount} rows
            </span>
        </div>
        <div className="flex space-x-2">
            <button 
                onClick={onRefresh}
                className="p-1.5 text-gray-500 hover:text-blue-600 hover:bg-white rounded transition-colors"
                title="Refresh"
            >
                <RefreshCw className="w-4 h-4" />
            </button>
            <Button variant="secondary" size="sm" icon={<Download className="w-3 h-3"/>}>
                Export CSV
            </Button>
        </div>
      </div>
      
      <div className="flex-1 overflow-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50 sticky top-0 z-10">
            <tr>
              {columns.map((col) => (
                <th key={col} className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {data.rows.map((row, idx) => (
              <tr key={idx} className="hover:bg-blue-50 transition-colors">
                {columns.map((col) => (
                  <td key={`${idx}-${col}`} className="px-6 py-3 text-sm text-gray-700 whitespace-nowrap">
                    {typeof row[col] === 'object' ? JSON.stringify(row[col]) : row[col]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {/* Footer / Pagination Placeholder */}
      <div className="px-4 py-2 border-t border-gray-200 bg-gray-50 text-xs text-gray-500 flex justify-between">
         <span>Showing top 50 rows</span>
         <span>Page 1 of 1</span>
      </div>
    </div>
  );
};