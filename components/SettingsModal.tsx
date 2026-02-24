
import React, { useState } from 'react';
import { X, Server, Plus, Trash2, Palette } from 'lucide-react';
import { Button } from './Button';
import { AppearanceConfig } from '../types';

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  servers: string[];
  currentServer: string;
  onSelectServer: (url: string) => void;
  onAddServer: (url: string) => void;
  onRemoveServer: (url: string) => void;
  appearance: AppearanceConfig;
  onUpdateAppearance: (config: AppearanceConfig) => void;
}

export const SettingsModal: React.FC<SettingsModalProps> = ({
  isOpen,
  onClose,
  servers,
  currentServer,
  onSelectServer,
  onAddServer,
  onRemoveServer,
  appearance,
  onUpdateAppearance
}) => {
  const [activeTab, setActiveTab] = useState<'server' | 'appearance'>('server');
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
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md animate-in fade-in zoom-in duration-200 overflow-hidden flex flex-col max-h-[90vh]">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50 shrink-0">
          <div className="flex items-center space-x-2">
            <h3 className="text-lg font-bold text-gray-900">App Settings</h3>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="flex border-b border-gray-100 shrink-0">
            <button 
                className={`flex-1 py-3 text-sm font-medium text-center border-b-2 transition-colors ${activeTab === 'server' ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                onClick={() => setActiveTab('server')}
            >
                <div className="flex items-center justify-center space-x-2">
                    <Server className="w-4 h-4" />
                    <span>Connection</span>
                </div>
            </button>
            <button 
                className={`flex-1 py-3 text-sm font-medium text-center border-b-2 transition-colors ${activeTab === 'appearance' ? 'border-blue-500 text-blue-600' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
                onClick={() => setActiveTab('appearance')}
            >
                <div className="flex items-center justify-center space-x-2">
                    <Palette className="w-4 h-4" />
                    <span>Appearance</span>
                </div>
            </button>
        </div>

        <div className="p-6 overflow-y-auto">
            {activeTab === 'server' && (
                <div>
                    <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-3">Backend Server</h4>
                    <p className="text-sm text-gray-600 mb-4">
                        Select the backend server to connect to. Use <b>Mock Server</b> for offline testing.
                    </p>

                    <div className="space-y-3 mb-4">
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
            )}

            {activeTab === 'appearance' && (
                <div className="space-y-6">
                    <div>
                        <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-4">Global UI Settings</h4>
                        
                        {/* Text Size */}
                        <div className="mb-4">
                            <label className="block text-sm font-medium text-gray-700 mb-2">Sidebar Text Size</label>
                            <div className="flex items-center space-x-2 bg-gray-50 p-1 rounded-lg border border-gray-200 w-fit">
                                {[11, 12, 13, 14].map(size => (
                                    <button
                                        key={size}
                                        onClick={() => onUpdateAppearance({ ...appearance, textSize: size })}
                                        className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all ${
                                            appearance.textSize === size 
                                            ? 'bg-white text-blue-600 shadow-sm border border-gray-100' 
                                            : 'text-gray-500 hover:text-gray-700'
                                        }`}
                                    >
                                        {size}px
                                    </button>
                                ))}
                            </div>
                        </div>

                        {/* Text Color */}
                        <div className="mb-4">
                             <label className="block text-sm font-medium text-gray-700 mb-2">Sidebar Text Color</label>
                             <div className="flex items-center space-x-2">
                                <input 
                                    type="color" 
                                    value={appearance.textColor}
                                    onChange={(e) => onUpdateAppearance({ ...appearance, textColor: e.target.value })}
                                    className="h-8 w-14 p-0 border-0 rounded overflow-hidden cursor-pointer"
                                />
                                <span className="text-xs font-mono text-gray-500 bg-gray-100 px-2 py-1 rounded border border-gray-200">
                                    {appearance.textColor}
                                </span>
                             </div>
                        </div>

                        {/* Guide Lines */}
                        <div className="mb-4">
                            <label className="block text-sm font-medium text-gray-700 mb-2">Tree Guide Lines</label>
                            <div className="flex items-center justify-between p-3 border border-gray-200 rounded-lg">
                                <span className="text-sm text-gray-600">Show Indentation Lines</span>
                                <button
                                    onClick={() => onUpdateAppearance({ ...appearance, showGuideLines: !appearance.showGuideLines })}
                                    className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-600 focus-visible:ring-offset-2 ${appearance.showGuideLines ? 'bg-blue-600' : 'bg-gray-200'}`}
                                >
                                    <span
                                        aria-hidden="true"
                                        className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${appearance.showGuideLines ? 'translate-x-4' : 'translate-x-0'}`}
                                    />
                                </button>
                            </div>
                            
                            {appearance.showGuideLines && (
                                <div className="mt-3">
                                    <label className="text-xs text-gray-500 block mb-1">Line Color</label>
                                    <div className="flex items-center space-x-2">
                                        <input 
                                            type="color" 
                                            value={appearance.guideLineColor}
                                            onChange={(e) => onUpdateAppearance({ ...appearance, guideLineColor: e.target.value })}
                                            className="h-6 w-10 p-0 border-0 rounded overflow-hidden cursor-pointer"
                                        />
                                        <span className="text-xs font-mono text-gray-500 bg-gray-100 px-2 py-1 rounded border border-gray-200">
                                            {appearance.guideLineColor}
                                        </span>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
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
