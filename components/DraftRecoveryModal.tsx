import React from 'react';
import { Clock3, RotateCcw, Trash2 } from 'lucide-react';
import { ProjectDraft } from '../utils/projectStore';
import { Button } from './Button';

interface DraftRecoveryModalProps {
  isOpen: boolean;
  draft: ProjectDraft | null;
  onRestore: () => void;
  onDiscard: () => void;
}

export const DraftRecoveryModal: React.FC<DraftRecoveryModalProps> = ({
  isOpen,
  draft,
  onRestore,
  onDiscard,
}) => {
  if (!isOpen || !draft) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg overflow-hidden animate-in fade-in zoom-in duration-200">
        <div className="px-6 py-4 border-b border-gray-100 bg-amber-50">
          <div className="flex items-center space-x-2">
            <Clock3 className="w-5 h-5 text-amber-600" />
            <div>
              <h3 className="text-lg font-bold text-gray-900">Recover local draft</h3>
              <p className="text-xs text-gray-500">{new Date(draft.savedAt).toLocaleString()}</p>
            </div>
          </div>
        </div>

        <div className="p-6 space-y-4 text-sm text-gray-600">
          <p>
            We found a newer local draft for this project. This usually happens after a disconnect or closing the page before sync finishes.
          </p>
          <div className="bg-gray-50 border border-gray-100 rounded-md p-3 text-xs">
            <div>Draft version base: v{draft.version}</div>
            <div>Saved at: {new Date(draft.savedAt).toLocaleTimeString()}</div>
          </div>
        </div>

        <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-end space-x-3">
          <Button variant="secondary" onClick={onDiscard} icon={<Trash2 className="w-4 h-4" />}>放弃草稿</Button>
          <Button variant="primary" onClick={onRestore} icon={<RotateCcw className="w-4 h-4" />}>恢复草稿</Button>
        </div>
      </div>
    </div>
  );
};
