import React from 'react';
import { AlertTriangle, RefreshCw, X } from 'lucide-react';
import { ProjectConflictInfo } from '../types';
import { Button } from './Button';

interface ConflictNoticeModalProps {
  isOpen: boolean;
  conflict: ProjectConflictInfo | null;
  onClose: () => void;
  onSyncNow: () => void;
}

export const ConflictNoticeModal: React.FC<ConflictNoticeModalProps> = ({
  isOpen,
  conflict,
  onClose,
  onSyncNow,
}) => {
  if (!isOpen || !conflict) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg overflow-hidden animate-in fade-in zoom-in duration-200">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-red-50">
          <div className="flex items-center space-x-2">
            <AlertTriangle className="w-5 h-5 text-red-600" />
            <div>
              <h3 className="text-lg font-bold text-gray-900">Sync conflict notice</h3>
              <p className="text-xs text-gray-500">remote v{conflict.latestVersion}</p>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 space-y-4 text-sm text-gray-600">
          <p>{conflict.message}</p>
          <div className="grid grid-cols-2 gap-3 text-xs">
            <div className="bg-gray-50 border border-gray-100 rounded-md p-3">
              <div className="font-semibold text-gray-700">Remote version</div>
              <div className="mt-1 font-mono">v{conflict.latestVersion}</div>
            </div>
            <div className="bg-gray-50 border border-gray-100 rounded-md p-3">
              <div className="font-semibold text-gray-700">Pending local changes</div>
              <div className="mt-1 font-mono">{conflict.pendingPatchesCount}</div>
            </div>
          </div>
          <p className="text-xs text-gray-500">
            Local edits have already been rebased in memory. You can sync now to push them against the latest remote state.
          </p>
        </div>

        <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3">
          <Button variant="secondary" onClick={onClose}>稍后处理</Button>
          <Button variant="primary" onClick={onSyncNow} icon={<RefreshCw className="w-4 h-4" />}>立即同步</Button>
        </div>
      </div>
    </div>
  );
};
