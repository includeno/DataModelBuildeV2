import React, { useState } from 'react';
import { Upload, FileText, CheckCircle, X, AlertTriangle } from 'lucide-react';
import { Dataset } from '../types';

interface DataImportModalProps {
  isOpen: boolean;
  onClose: () => void;
  onImport: (dataset: Dataset) => void;
  sessionId: string;
}

export const DataImportModal: React.FC<DataImportModalProps> = ({ isOpen, onClose, onImport, sessionId }) => {
  const [dragActive, setDragActive] = useState(false);
  const [fileName, setFileName] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const parseRows = (rows: unknown): any[] => {
    if (typeof rows === 'string') {
      try {
        const parsed = JSON.parse(rows);
        return Array.isArray(parsed) ? parsed : [];
      } catch {
        return [];
      }
    }
    return Array.isArray(rows) ? rows : [];
  };

  if (!isOpen) return null;

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      processFile(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      processFile(e.target.files[0]);
    }
  };

  const processFile = async (file: File) => {
    setFileName(file.name);
    setUploading(true);
    setError(null);
    
    const formData = new FormData();
    formData.append('file', file);
    formData.append('session_id', sessionId);

    try {
        // Assuming backend is running locally on port 8000
        const response = await fetch('http://localhost:8000/upload', {
            method: 'POST',
            body: formData,
        });

        if (!response.ok) {
            throw new Error(`Upload failed: ${response.statusText}`);
        }

        const data = await response.json();
        
        if (data.error) {
            throw new Error(data.error);
        }

        const newDataset: Dataset = {
            id: data.id,
            name: data.name,
            fields: data.fields,
            rows: parseRows(data.rows) // Backend returns preview rows
        };
        
        onImport(newDataset);
        setFileName(null);
        onClose();
    } catch (err: any) {
        console.error("Upload error:", err);
        setError(err.message || "Failed to upload file");
    } finally {
        setUploading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg overflow-hidden animate-in fade-in zoom-in duration-200">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
            <h3 className="text-lg font-semibold text-gray-900 flex items-center">
                <Upload className="w-5 h-5 mr-2 text-blue-600" /> Import Data Source
            </h3>
            <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
                <X className="w-5 h-5" />
            </button>
        </div>
        
        <div className="p-6">
            <div 
                className={`relative flex flex-col items-center justify-center w-full h-48 border-2 border-dashed rounded-xl transition-all duration-200 ${
                    dragActive 
                    ? "border-blue-500 bg-blue-50 scale-[1.02]" 
                    : "border-gray-300 bg-gray-50 hover:bg-gray-100 hover:border-gray-400"
                }`}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
            >
                <input 
                    type="file" 
                    className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                    onChange={handleChange}
                    accept=".csv"
                    disabled={uploading}
                />
                
                {uploading ? (
                     <div className="flex flex-col items-center text-blue-600 animate-pulse">
                        <Upload className="w-8 h-8 mb-2" />
                        <span className="text-sm font-semibold">Uploading to Backend...</span>
                     </div>
                ) : fileName ? (
                    <div className="flex flex-col items-center text-green-600">
                        <div className="bg-green-100 p-3 rounded-full mb-3">
                            <CheckCircle className="w-8 h-8" />
                        </div>
                        <span className="text-base font-semibold text-gray-900">{fileName}</span>
                        <span className="text-sm text-green-600 mt-1">Processed successfully</span>
                    </div>
                ) : (
                    <div className="flex flex-col items-center text-gray-500">
                        <div className="bg-white p-3 rounded-full shadow-sm mb-3">
                            <FileText className="w-8 h-8 text-blue-600" />
                        </div>
                        <span className="text-sm font-medium text-gray-900">Click to upload</span>
                        <span className="text-xs text-gray-500 mt-1">or drag and drop CSV</span>
                    </div>
                )}
            </div>

            {error && (
                <div className="mt-4 p-3 bg-red-50 text-red-700 text-sm rounded-lg flex items-center">
                    <AlertTriangle className="w-4 h-4 mr-2" />
                    {error}
                </div>
            )}
            
            <div className="mt-6 flex justify-end">
                <button 
                    onClick={onClose} 
                    className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50"
                    disabled={uploading}
                >
                    Cancel
                </button>
            </div>
        </div>
      </div>
    </div>
  );
};
