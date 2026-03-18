
import React, { useState, useEffect } from 'react';
import { X, Settings, Save, Type, Check, ShieldAlert, Loader2, LayoutPanelLeft, LayoutPanelTop, PanelRight, PanelBottom } from 'lucide-react';
import { Button } from './Button';
import { SessionConfig } from '../types';

interface SessionSettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  projectId?: string;
  sessionId?: string;
  initialDisplayName: string;
  initialSettings: SessionConfig;
  onSave: (displayName: string, settings: SessionConfig) => Promise<void>;
}

export const SessionSettingsModal: React.FC<SessionSettingsModalProps> = ({
  isOpen,
  onClose,
  projectId,
  sessionId,
  initialDisplayName,
  initialSettings,
  onSave
}) => {
  const [displayName, setDisplayName] = useState(initialDisplayName);
  const [settings, setSettings] = useState<SessionConfig>(initialSettings);
  const [saving, setSaving] = useState(false);
  const activeProjectId = projectId || sessionId || '';

  useEffect(() => {
    if (isOpen) {
        setDisplayName(initialDisplayName || '');
        setSettings(initialSettings || { cascadeDisable: false, panelPosition: 'right' });
    }
  }, [isOpen, initialDisplayName, initialSettings]);

  if (!isOpen) return null;

  const handleSave = async () => {
      setSaving(true);
      try {
          await onSave(displayName, settings);
          onClose();
      } catch (e) {
          alert("Failed to save settings");
      } finally {
          setSaving(false);
      }
  };

  const handleNameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
      const val = e.target.value;
      if (val.length <= 30) {
          setDisplayName(val);
      }
  };

  const positions = [
      { id: 'left', label: 'Left', icon: LayoutPanelLeft },
      { id: 'right', label: 'Right', icon: PanelRight },
      { id: 'top', label: 'Top', icon: LayoutPanelTop },
      { id: 'bottom', label: 'Bottom', icon: PanelBottom },
  ];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md animate-in fade-in zoom-in duration-200 overflow-hidden">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50">
          <div className="flex items-center space-x-2">
            <Settings className="w-5 h-5 text-blue-600" />
            <div>
                <h3 className="text-lg font-bold text-gray-900">Project Settings</h3>
                <p className="text-xs text-gray-500 font-mono">{activeProjectId}</p>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 space-y-6">
            {/* Display Name */}
            <div>
                <label className="block text-sm font-medium text-gray-700 mb-1 flex items-center">
                    <Type className="w-4 h-4 mr-1.5 text-gray-400" />
                    Display Name
                </label>
                <input 
                    type="text" 
                    value={displayName}
                    onChange={handleNameChange}
                    placeholder="My Analysis Project"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                />
                <div className="flex justify-between mt-1">
                    <span className="text-xs text-gray-500">Custom name for easier identification</span>
                    <span className={`text-xs ${displayName.length >= 30 ? 'text-red-500 font-bold' : 'text-gray-400'}`}>
                        {displayName.length}/30
                    </span>
                </div>
            </div>

            <div className="border-t border-gray-100 pt-4">
                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-3 flex items-center">
                    <ShieldAlert className="w-4 h-4 mr-1.5" /> Behavior & Layout
                </h4>
                
                {/* Cascade Disable */}
                <div 
                    className="flex items-start space-x-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer transition-colors mb-4"
                    onClick={() => setSettings({ ...settings, cascadeDisable: !settings.cascadeDisable })}
                >
                    <div className={`mt-0.5 w-5 h-5 rounded border flex items-center justify-center shrink-0 transition-colors ${settings.cascadeDisable ? 'bg-blue-600 border-blue-600' : 'bg-white border-gray-300'}`}>
                        {settings.cascadeDisable && <Check className="w-3.5 h-3.5 text-white" />}
                    </div>
                    <div>
                        <span className="text-sm font-medium text-gray-900 block">Cascade Disable</span>
                        <span className="text-xs text-gray-500 block mt-0.5">
                            Automatically disable child operations when parent is disabled.
                        </span>
                    </div>
                </div>

                {/* Panel Position */}
                <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">
                        Result Panel Position
                    </label>
                    <div className="grid grid-cols-4 gap-2">
                        {positions.map(pos => (
                            <button
                                key={pos.id}
                                onClick={() => setSettings({ ...settings, panelPosition: pos.id as any })}
                                className={`flex flex-col items-center justify-center p-2 rounded-lg border transition-all ${
                                    settings.panelPosition === pos.id 
                                    ? 'bg-blue-50 border-blue-500 text-blue-700 shadow-sm' 
                                    : 'bg-white border-gray-200 text-gray-500 hover:bg-gray-50'
                                }`}
                            >
                                <pos.icon className="w-5 h-5 mb-1" />
                                <span className="text-[10px] font-medium">{pos.label}</span>
                            </button>
                        ))}
                    </div>
                </div>
            </div>
        </div>
        
        <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3">
             <Button variant="secondary" onClick={onClose} disabled={saving}>
                Cancel
            </Button>
            <Button variant="primary" onClick={handleSave} disabled={saving} icon={saving ? <Loader2 className="w-4 h-4 animate-spin"/> : <Save className="w-4 h-4" />}>
                {saving ? 'Saving...' : 'Save Settings'}
            </Button>
        </div>
      </div>
    </div>
  );
};
