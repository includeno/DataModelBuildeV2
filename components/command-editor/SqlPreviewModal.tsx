import React from 'react';
import { Database, Loader2, Copy, X } from 'lucide-react';
import { Button } from '../Button';

interface SqlPreviewModalProps {
    isOpen: boolean;
    onClose: () => void;
    sql: string;
    loading: boolean;
}

export const SqlPreviewModal: React.FC<SqlPreviewModalProps> = ({ isOpen, onClose, sql, loading }) => {
    if (!isOpen) return null;
    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
            <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl flex flex-col max-h-[80vh] animate-in fade-in zoom-in duration-200">
                <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50 rounded-t-xl">
                    <div className="flex items-center space-x-2">
                        <div className="p-2 bg-blue-100 rounded-lg">
                            <Database className="w-5 h-5 text-blue-600" />
                        </div>
                        <h3 className="text-lg font-bold text-gray-900">Generated SQL</h3>
                    </div>
                    <button onClick={onClose} className="text-gray-400 hover:text-gray-600 p-2 rounded-full hover:bg-gray-200 transition-all">
                        <X className="w-5 h-5" />
                    </button>
                </div>
                <div className="p-6 overflow-y-auto bg-[#1e1e1e] min-h-[200px]">
                    {loading ? (
                        <div className="flex flex-col items-center justify-center h-full text-gray-400 py-8">
                            <Loader2 className="w-8 h-8 animate-spin mb-3" />
                            <span className="text-sm">Generating SQL from logic...</span>
                        </div>
                    ) : (
                        <pre className="text-sm font-mono text-blue-300 whitespace-pre-wrap leading-relaxed">
                            {sql || "-- No SQL generated"}
                        </pre>
                    )}
                </div>
                <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3 rounded-b-xl">
                    <Button variant="secondary" onClick={onClose}>Close</Button>
                    <Button variant="primary" onClick={() => navigator.clipboard.writeText(sql)} icon={<Copy className="w-4 h-4" />}>Copy SQL</Button>
                </div>
            </div>
        </div>
    );
};
