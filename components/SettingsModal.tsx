import React, { useState } from 'react';
import { X, Server, Plus, Trash2, Check } from 'lucide-react';
import { Button } from './Button';

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  servers: string[];
  currentServer: string;
  onSelectServer: (url: string) => void;
  onAddServer: (url: string) => void;
  onRemoveServer: (url: string) => void;
}

export const SettingsModal: React.FC<SettingsModalProps> = ({
  isOpen,
  onClose,
  servers,
  currentServer,
  onSelectServer,
  onAddServer,
  onRemoveServer
}) => {
  const [newUrl, setNewUrl] = useState('');

  if (!isOpen) return null;

  const handleAdd = () => {
    if (newUrl && !servers.includes(newUrl)) {
      onAddServer(newUrl);
      setNewUrl('');
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md animate-in fade-in zoom-in duration-200 overflow-hidden">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50">
          <div className="flex items-center space-x-2">
            <Server className="w-5 h-5 text-blue-600" />
            <h3 className="text-lg font-bold text-gray-900">Server Configuration</h3>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6">
            <p className="text-sm text-gray-600 mb-4">
                Select the backend server to connect to. Use <b>Mock Server</b> for offline frontend testing.
            </p>

            <div className="space-y-3 mb-6">
                {servers.map((server) => {
                    const isSelected = server === currentServer;
                    const isSystem = server === 'mockServer' || server === 'http://localhost:8000';
                    const displayLabel = server === 'mockServer' ? 'Mock Server (Offline)' : server;

                    return (
                        <div 
                            key={server}
                            onClick={() => onSelectServer(server)}
                            className={`flex items-center justify-between p-3 rounded-lg border cursor-pointer transition-all ${
                                isSelected 
                                ? 'border-blue-500 bg-blue-50 shadow-sm' 
                                : 'border-gray-200 hover:border-blue-300 hover:bg-gray-50'
                            }`}
                        >
                            <div className="flex items-center space-x-3">
                                <div className={`w-4 h-4 rounded-full border flex items-center justify-center ${isSelected ? 'border-blue-600' : 'border-gray-400'}`}>
                                    {isSelected && <div className="w-2 h-2 rounded-full bg-blue-600" />}
                                </div>
                                <span className={`text-sm font-medium ${isSelected ? 'text-blue-900' : 'text-gray-700'}`}>
                                    {displayLabel}
                                </span>
                            </div>

                            {!isSystem && (
                                <button 
                                    onClick={(e) => { e.stopPropagation(); onRemoveServer(server); }}
                                    className="p-1.5 text-gray-400 hover:text-red-500 rounded-md hover:bg-red-50 transition-colors"
                                >
                                    <Trash2 className="w-4 h-4" />
                                </button>
                            )}
                        </div>
                    );
                })}
            </div>

            <div className="pt-4 border-t border-gray-100">
                <label className="block text-xs font-semibold text-gray-500 uppercase mb-2">Add Custom Server</label>
                <div className="flex space-x-2">
                    <input 
                        type="text" 
                        value={newUrl}
                        onChange={(e) => setNewUrl(e.target.value)}
                        placeholder="http://192.168.1.10:8000"
                        className="flex-1 text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 bg-white"
                    />
                    <Button variant="secondary" size="sm" onClick={handleAdd} disabled={!newUrl}>
                        <Plus className="w-4 h-4" />
                    </Button>
                </div>
            </div>
        </div>
        
        <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-end">
            <Button variant="primary" onClick={onClose}>
                Done
            </Button>
        </div>
      </div>
    </div>
  );
};
