import React, { useState, useRef } from 'react';
import { Upload, FileText, CheckCircle, X, AlertTriangle, FileSpreadsheet } from 'lucide-react';
import { ApiConfig, Dataset } from '../types';
import { api } from '../utils/api';
import { Button } from './Button';

interface DataImportModalProps {
  isOpen: boolean;
  onClose: () => void;
  onImport: (dataset: Dataset) => void;
  sessionId: string;
  apiConfig: ApiConfig;
}

export const DataImportModal: React.FC<DataImportModalProps> = ({ isOpen, onClose, onImport, sessionId, apiConfig }) => {
  const [dragActive, setDragActive] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [customName, setCustomName] = useState('');
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const inputRef = useRef<HTMLInputElement>(null);

  if (!isOpen) return null;

  const resetState = () => {
    setSelectedFile(null);
    setCustomName('');
    setError(null);
    setUploading(false);
  };

  const handleClose = () => {
      resetState();
      onClose();
  };

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
      processFileSelection(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      processFileSelection(e.target.files[0]);
    }
  };

  const processFileSelection = (file: File) => {
      setSelectedFile(file);
      // Default name: remove extension
      const nameWithoutExt = file.name.replace(/\.[^/.]+$/, "");
      setCustomName(nameWithoutExt);
      setError(null);
  };

  const handleUpload = async () => {
    if (!selectedFile) return;

    setUploading(true);
    setError(null);
    
    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('sessionId', sessionId);
    formData.append('name', customName);

    try {
        const data = await api.upload(apiConfig, '/upload', formData);
        
        if (data.error) {
            throw new Error(data.error);
        }

        const newDataset: Dataset = {
            id: data.id,
            name: data.name,
            fields: data.fields,
            rows: data.rows,
            totalCount: data.totalCount
        };
        
        onImport(newDataset);
        handleClose();
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
            <button onClick={handleClose} className="text-gray-400 hover:text-gray-600 transition-colors">
                <X className="w-5 h-5" />
            </button>
        </div>
        
        <div className="p-6">
            <div className="mb-4 text-xs text-center text-gray-400">
                Server: {apiConfig.isMock ? 'Mock Mode' : apiConfig.baseUrl}
            </div>

            {!selectedFile ? (
                <div 
                    className={`relative flex flex-col items-center justify-center w-full h-48 border-2 border-dashed rounded-xl transition-all duration-200 cursor-pointer ${
                        dragActive 
                        ? "border-blue-500 bg-blue-50 scale-[1.02]" 
                        : "border-gray-300 bg-gray-50 hover:bg-gray-100 hover:border-gray-400"
                    }`}
                    onDragEnter={handleDrag}
                    onDragLeave={handleDrag}
                    onDragOver={handleDrag}
                    onDrop={handleDrop}
                    onClick={() => inputRef.current?.click()}
                >
                    <input 
                        ref={inputRef}
                        type="file" 
                        className="hidden"
                        onChange={handleChange}
                        accept=".csv,.xlsx,.xls"
                    />
                    
                    <div className="flex flex-col items-center text-gray-500">
                        <div className="bg-white p-3 rounded-full shadow-sm mb-3">
                            <FileSpreadsheet className="w-8 h-8 text-blue-600" />
                        </div>
                        <span className="text-sm font-medium text-gray-900">Click to upload</span>
                        <span className="text-sm text-gray-500 mt-1">Supports CSV and Excel (.xlsx)</span>
                    </div>
                </div>
            ) : (
                <div className="space-y-4">
                    <div className="flex items-start p-4 bg-blue-50 border border-blue-100 rounded-lg">
                        <div className="bg-white p-2 rounded-md shadow-sm mr-3">
                            <FileSpreadsheet className="w-6 h-6 text-green-600" />
                        </div>
                        <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-blue-900 truncate">{selectedFile.name}</p>
                            <p className="text-xs text-blue-700 mt-0.5">{(selectedFile.size / 1024).toFixed(1)} KB</p>
                        </div>
                        <button 
                            onClick={resetState}
                            className="text-blue-400 hover:text-blue-600 p-1"
                            disabled={uploading}
                        >
                            <X className="w-4 h-4" />
                        </button>
                    </div>

                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Dataset Name</label>
                        <input 
                            type="text" 
                            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                            value={customName}
                            onChange={(e) => setCustomName(e.target.value)}
                            placeholder="Enter a name for this dataset"
                            disabled={uploading}
                        />
                        <p className="mt-1 text-xs text-gray-500">This name will be used in SQL queries and operations.</p>
                    </div>
                </div>
            )}

            {error && (
                <div className="mt-4 p-3 bg-red-50 text-red-700 text-sm rounded-lg flex items-center">
                    <AlertTriangle className="w-4 h-4 mr-2" />
                    {error}
                </div>
            )}
            
            <div className="mt-6 flex justify-end space-x-3">
                <Button variant="secondary" onClick={handleClose} disabled={uploading}>
                    Cancel
                </Button>
                {selectedFile && (
                    <Button 
                        variant="primary" 
                        onClick={handleUpload} 
                        disabled={uploading || !customName.trim()}
                        icon={uploading ? <Upload className="w-4 h-4 animate-bounce" /> : <CheckCircle className="w-4 h-4" />}
                    >
                        {uploading ? 'Importing...' : 'Import Dataset'}
                    </Button>
                )}
            </div>
        </div>
      </div>
    </div>
  );
};