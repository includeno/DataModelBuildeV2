import React, { useState, useEffect } from 'react';
import { Settings } from 'lucide-react';
import { CommandEditor } from './CommandEditor';
import { SqlEditor } from './SqlEditor';
import { DataPreview } from './DataPreview';
import { OperationNode, Dataset, Command, ExecutionResult, ApiConfig } from '../types';

interface WorkspaceProps {
  currentView: 'workflow' | 'sql';
  sessionId: string;
  apiConfig: ApiConfig;
  targetSqlTable: string | null;
  onClearTargetSqlTable: () => void;
  selectedNode: OperationNode | null;
  datasets: Dataset[];
  inheritedDataSource?: string;
  onUpdateCommands: (opId: string, newCommands: Command[]) => void;
  onUpdateName: (name: string) => void;
  onViewPath: () => void;
  isRightPanelOpen: boolean;
  rightPanelWidth: number;
  onRightPanelResizeStart: () => void;
  previewData: ExecutionResult | null;
  loading: boolean;
  onRefreshPreview: () => void;
}

export const Workspace: React.FC<WorkspaceProps> = ({
  currentView,
  sessionId,
  apiConfig,
  targetSqlTable,
  onClearTargetSqlTable,
  selectedNode,
  datasets,
  inheritedDataSource,
  onUpdateCommands,
  onUpdateName,
  onViewPath,
  isRightPanelOpen,
  rightPanelWidth,
  onRightPanelResizeStart,
  previewData,
  loading,
  onRefreshPreview
}) => {
  const [isResizingRight, setIsResizingRight] = useState(false);

  return (
    <main className="flex-1 flex min-w-0">
        {currentView === 'sql' ? (
            // SQL VIEW
            <div className="flex-1 h-full overflow-hidden">
                <SqlEditor 
                    sessionId={sessionId} 
                    apiConfig={apiConfig} 
                    targetTable={targetSqlTable}
                    onClearTarget={onClearTargetSqlTable}
                />
            </div>
        ) : (
            // WORKFLOW VIEW
            <>
                {/* MIDDLE: COMMAND EDITOR (FLEX-1) */}
                <div className="flex-1 flex flex-col bg-gray-50/50 min-w-[300px]">
                    {selectedNode && selectedNode.id !== 'root' ? (
                        <CommandEditor 
                            operationId={selectedNode.id}
                            operationName={selectedNode.name}
                            commands={selectedNode.commands} 
                            datasets={datasets}
                            inheritedDataSource={inheritedDataSource}
                            onUpdateCommands={onUpdateCommands}
                            onUpdateName={onUpdateName}
                            onViewPath={onViewPath}
                        />
                    ) : (
                        <div className="flex-1 flex flex-col items-center justify-center text-gray-400 bg-gray-50">
                            <Settings className="w-12 h-12 mb-4 opacity-20" />
                            <p className="font-medium">Configuration Panel</p>
                            <p className="text-sm mt-2 opacity-70">Select an operation to edit commands.</p>
                        </div>
                    )}
                </div>

                 {/* RESIZER HANDLER (RIGHT) */}
                 {isRightPanelOpen && (
                     <div 
                        className={`w-1 hover:w-1.5 bg-gray-200 hover:bg-blue-400 cursor-col-resize z-20 flex flex-col justify-center items-center transition-all ${isResizingRight ? 'bg-blue-500 w-1.5' : ''}`}
                        onMouseDown={onRightPanelResizeStart}
                    >
                        <div className="h-8 w-0.5 bg-gray-400 rounded-full" />
                    </div>
                )}

                {/* RIGHT: DATA PREVIEW (RESIZABLE) */}
                {isRightPanelOpen && (
                    <div 
                        className="flex flex-col bg-white shrink-0"
                        style={{ width: rightPanelWidth }}
                    >
                        <DataPreview 
                            data={previewData} 
                            loading={loading}
                            onRefresh={onRefreshPreview}
                        />
                    </div>
                )}
            </>
        )}
    </main>
  );
};
