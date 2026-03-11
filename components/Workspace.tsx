
import React, { useState, useEffect } from 'react';
import { Settings, Play, ChevronLeft, ChevronRight, ChevronUp, ChevronDown, X, Table as TableIcon } from 'lucide-react';
import { CommandEditor } from './CommandEditor';
import { SqlEditor } from './SqlEditor';
import { DataPreview } from './DataPreview';
import { ComplexDataPreview } from './ComplexDataPreview';
import { DataBrowser } from './DataBrowser';
import { OperationNode, Dataset, Command, ExecutionResult, ApiConfig, OperationType, DataType, SqlHistoryItem, AppearanceConfig } from '../types';
import { api } from '../utils/api';

interface WorkspaceProps {
  currentView: 'workflow' | 'sql' | 'data';
  sessionId: string;
  apiConfig: ApiConfig;
  targetSqlTable: string | null;
  targetDataTable?: string | null;
  onSelectDataTable?: (name: string) => void;
  onClearTargetSqlTable: () => void;
  sqlRunRequestId?: number;
  onSqlRunStateChange?: (state: { canRun: boolean; running: boolean }) => void;
  selectedNode: OperationNode | null;
  datasets: Dataset[];
  appearance?: AppearanceConfig;
  inputFields: string[]; // Deprecated, kept for compat if needed but unused in new logic
  inputSchema: Record<string, DataType>; // New Schema
  onUpdateCommands: (opId: string, newCommands: Command[]) => void;
  onUpdateName: (name: string) => void;
  onUpdateType: (opId: string, type: OperationType) => void;
  onViewPath: (commandId?: string) => void;
  isRightPanelOpen: boolean;
  onCloseRightPanel: () => void;
  rightPanelWidth: number;
  onRightPanelResizeStart: () => void;
  previewData: ExecutionResult | null;
  onClearPreview?: () => void;
  loading: boolean;
  onRefreshPreview: (page?: number, commandId?: string) => void;
  canRunOperation?: boolean;
  onUpdatePageSize: (size: number) => void;
  onExportFull: () => void;
  isMobile: boolean;
  tree?: OperationNode;
  panelPosition?: 'right' | 'left' | 'top' | 'bottom';
  sqlHistory?: SqlHistoryItem[];
  onUpdateSqlHistory?: (history: SqlHistoryItem[]) => void;
}

export const Workspace: React.FC<WorkspaceProps> = ({
  currentView,
  sessionId,
  apiConfig,
  targetSqlTable,
  targetDataTable,
  onSelectDataTable,
  onClearTargetSqlTable,
  sqlRunRequestId,
  onSqlRunStateChange,
  selectedNode,
  datasets,
  appearance,
  // inputFields,
  inputSchema,
  onUpdateCommands,
  onUpdateName,
  onUpdateType,
  onViewPath,
  isRightPanelOpen,
  onCloseRightPanel,
  rightPanelWidth, // Acts as panelSize (Width or Height)
  onRightPanelResizeStart,
  previewData,
  onClearPreview,
  loading,
  onRefreshPreview,
  canRunOperation = true,
  onUpdatePageSize,
  onExportFull,
  isMobile,
  tree,
  panelPosition = 'right',
  sqlHistory,
  onUpdateSqlHistory
}) => {
  const [activeTab, setActiveTab] = useState<string>('');
  const [lastRunCommandId, setLastRunCommandId] = useState<string | undefined>(undefined);

  // --- Tab Management Effects ---

  useEffect(() => {
    if (previewData) {
        setActiveTab('output');
    }
  }, [previewData]);

  useEffect(() => {
      if (!previewData && activeTab === 'output') {
          setActiveTab('');
      }
  }, [previewData, activeTab]);

  const showExecutionTab = !!previewData;
  const isMultiTableMode = selectedNode?.commands.some(c => c.type === 'multi_table') ?? false;
  const firstComplexIndex = selectedNode?.commands.findIndex(c => c.type === 'multi_table') ?? -1;
  const lastRunIndex = lastRunCommandId && selectedNode
      ? selectedNode.commands.findIndex(c => c.id === lastRunCommandId)
      : -1;
  const shouldUseComplexResult = isMultiTableMode && (
      lastRunCommandId === undefined || firstComplexIndex < 0 || lastRunIndex >= firstComplexIndex
  );
  const allowResultPanel = selectedNode?.operationType !== 'setup';
  const showRightPanel = isRightPanelOpen && allowResultPanel;

  useEffect(() => {
      // Reset run-step context when switching operation.
      setLastRunCommandId(undefined);
  }, [selectedNode?.id]);

  useEffect(() => {
      if (!previewData) {
          setLastRunCommandId(undefined);
      }
  }, [previewData]);

  const runPreview = (
      page: number = 1,
      commandId?: string,
      options: { preserveLastCommandId?: boolean } = {}
  ) => {
      const shouldReuseLast = page > 1 || options.preserveLastCommandId === true;
      const effectiveCommandId = commandId === undefined
          ? (shouldReuseLast ? lastRunCommandId : undefined)
          : commandId;
      if (page === 1) {
          setLastRunCommandId(effectiveCommandId);
      }
      onRefreshPreview(page, effectiveCommandId);
  };

  const handleRefreshView = async (viewId: string, page: number, pageSize: number): Promise<ExecutionResult> => {
      if (!selectedNode || !tree) throw new Error("No context");
      const res = await api.post(apiConfig, '/execute', {
          sessionId,
          tree,
          targetNodeId: selectedNode.id,
          page,
          pageSize,
          viewId // Pass the viewId (main or subTableId)
      });
      return res;
  };
  
  // Layout Logic
  const isVertical = panelPosition === 'top' || panelPosition === 'bottom';
  const isPanelFirst = panelPosition === 'top' || panelPosition === 'left';
  
  // Helper to find the source table name for the current context
  const findMainSourceName = (node: OperationNode | null, currentTree: OperationNode | undefined): string | undefined => {
      if (!node || !currentTree) return undefined;
      
      const findPath = (root: OperationNode, targetId: string): OperationNode[] | null => {
          if (root.id === targetId) return [root];
          if (root.children) {
              for (const child of root.children) {
                  const path = findPath(child, targetId);
                  if (path) return [root, ...path];
              }
          }
          return null;
      };

      const path = findPath(currentTree, node.id);
      if (!path) return undefined;

      // Traverse path to find the last explicit source setting
      let sourceName = "Unknown Source";
      path.forEach(n => {
          n.commands.forEach(c => {
              if (c.type === 'source' && c.config.mainTable) sourceName = c.config.mainTable;
              else if (c.config.dataSource && c.config.dataSource !== 'stream') sourceName = c.config.dataSource;
          });
      });
      return sourceName;
  };

  const mainSourceName = findMainSourceName(selectedNode, tree);

  const handleGenerateSql = async (commandId: string): Promise<string> => {
      if (!selectedNode || !tree) return "-- No context";
      try {
          const res = await api.post(apiConfig, '/generate_sql', {
              sessionId,
              tree,
              targetNodeId: selectedNode.id,
              targetCommandId: commandId,
              includeCommandMeta: true
          });
          return res.sql;
      } catch (e: any) {
          return `-- Error: ${e.message || e}`;
      }
  };

  const mainContent = (
      <div className="flex-1 flex flex-col bg-gray-50/50 min-w-0 w-full h-full overflow-hidden">
        {selectedNode && selectedNode.id !== 'root' ? (
            <CommandEditor 
                operationId={selectedNode.id}
                operationName={selectedNode.name}
                operationType={selectedNode.operationType}
                commands={selectedNode.commands} 
                datasets={datasets}
                appearance={appearance}
                inputSchema={inputSchema}
                onUpdateCommands={onUpdateCommands}
                onUpdateName={onUpdateName}
                onUpdateType={onUpdateType}
                onViewPath={onViewPath}
                onRun={(cmdId) => runPreview(1, cmdId)}
                canRun={canRunOperation}
                onGenerateSql={handleGenerateSql}
                tree={tree} 
            />
        ) : (
            <div className="flex-1 flex flex-col items-center justify-center text-gray-400 bg-gray-50">
                <Settings className="w-12 h-12 mb-4 opacity-20" />
                <p className="font-medium">Configuration Panel</p>
                <p className="text-sm mt-2 opacity-70">Select an operation to edit commands.</p>
            </div>
        )}
      </div>
  );

  const panelContent = (
      <div 
          className={`flex flex-col bg-white shrink-0 transition-all duration-200 shadow-sm z-20 ${
              isVertical 
                ? 'w-full border-y border-gray-200' 
                : 'h-full border-x border-gray-200'
          } ${isMobile ? 'absolute inset-0 z-30 w-full shadow-2xl h-full' : ''}`}
          style={!isMobile ? { 
              [isVertical ? 'height' : 'width']: rightPanelWidth 
          } : {}}
      >
          {/* Panel Header */}
          <div className="flex items-center justify-between px-2 bg-gray-50 border-b border-gray-200 h-10 shrink-0">
              <div className="flex items-center overflow-x-auto no-scrollbar h-full flex-1 min-w-0">
                  {isMobile && (
                      <button 
                          onClick={onCloseRightPanel}
                          className="mr-2 p-1.5 text-gray-500 hover:text-gray-800 hover:bg-gray-200 rounded-md"
                      >
                          <ChevronLeft className="w-5 h-5" />
                      </button>
                  )}
                  {showExecutionTab ? (
                      <div
                          onClick={() => setActiveTab('output')}
                          className={`flex items-center px-4 py-2 text-xs font-medium border-t-2 border-x border-b-0 rounded-t-md transition-colors whitespace-nowrap h-9 mt-1 mr-1 cursor-pointer group shrink-0 ${
                              activeTab === 'output' 
                              ? 'border-gray-200 border-t-blue-500 bg-white text-blue-700' 
                              : 'border-transparent bg-transparent text-gray-500 hover:text-gray-700 hover:bg-gray-100'
                          }`}
                      >
                          <Play className="w-3 h-3 mr-2" />
                          <span>{shouldUseComplexResult ? 'Complex Result' : 'Execution Result'}</span>
                          <button 
                              onClick={(e) => {
                                  e.stopPropagation();
                                  if(onClearPreview) onClearPreview();
                              }}
                              className="ml-2 p-0.5 rounded-full hover:bg-red-100 hover:text-red-600 text-gray-400 transition-all"
                              title="Close Result"
                          >
                              <X className="w-3 h-3" />
                          </button>
                      </div>
                  ) : (
                      <span className="text-xs text-gray-400 italic px-2">Ready</span>
                  )}
              </div>
              <button
                  onClick={onCloseRightPanel}
                  className="ml-2 p-1.5 text-gray-400 hover:text-gray-700 hover:bg-gray-200 rounded-md shrink-0"
                  title="Close Panel"
              >
                  {isMobile ? <X className="w-4 h-4"/> : 
                     (isVertical ? (panelPosition === 'top' ? <ChevronUp className="w-4 h-4"/> : <ChevronDown className="w-4 h-4"/>) 
                     : (panelPosition === 'left' ? <ChevronLeft className="w-4 h-4"/> : <ChevronRight className="w-4 h-4"/>))
                  }
              </button>
          </div>

          <div className="flex-1 overflow-hidden relative">
              {activeTab === 'output' && showExecutionTab ? (
                  shouldUseComplexResult && selectedNode ? (
                      <ComplexDataPreview 
                          initialResult={previewData}
                          selectedNode={selectedNode}
                          loading={loading}
                          onRefreshView={handleRefreshView}
                          onExportFull={onExportFull}
                          mainSourceName={mainSourceName}
                      />
                  ) : (
                      <DataPreview 
                          data={previewData} 
                          loading={loading}
                          pageSize={selectedNode?.pageSize}
                          onRefresh={() => runPreview(previewData?.page || 1, undefined, { preserveLastCommandId: true })}
                          onPageChange={(page) => runPreview(page)}
                          onUpdatePageSize={onUpdatePageSize}
                          onExportFull={onExportFull}
                          sourceId={selectedNode?.id}
                      />
                  )
              ) : (
                  <div className="flex flex-col items-center justify-center h-full text-gray-400 p-6 text-center">
                      <TableIcon className="w-12 h-12 mb-3 opacity-20" />
                      <p className="text-sm font-medium">No Result</p>
                      <p className="text-xs mt-1">Configure operations and click Run to see data.</p>
                  </div>
              )}
          </div>
      </div>
  );

  const resizer = !isMobile && showRightPanel && (
      <div 
         className={`z-20 flex justify-center items-center transition-all ${
             isVertical 
             ? 'h-1 hover:h-1.5 w-full cursor-row-resize flex-row bg-gray-200 hover:bg-blue-400' 
             : 'w-1 hover:w-1.5 h-full cursor-col-resize flex-col bg-gray-200 hover:bg-blue-400'
         }`}
         onMouseDown={onRightPanelResizeStart}
     >
         <div className={`${isVertical ? 'w-8 h-0.5' : 'h-8 w-0.5'} bg-gray-400 rounded-full`} />
     </div>
  );

  return (
    <main className="flex-1 flex min-w-0 relative h-full overflow-hidden">
        {currentView === 'sql' ? (
            <div className="flex-1 h-full overflow-hidden">
                <SqlEditor 
                    sessionId={sessionId} 
                    apiConfig={apiConfig} 
                    datasets={datasets}
                    targetTable={targetSqlTable}
                    onClearTarget={onClearTargetSqlTable}
                    runRequestId={sqlRunRequestId}
                    onRunStateChange={onSqlRunStateChange}
                    history={sqlHistory}
                    onUpdateHistory={onUpdateSqlHistory}
                />
            </div>
        ) : currentView === 'data' ? (
            <div className="flex-1 h-full overflow-hidden">
                <DataBrowser
                    sessionId={sessionId}
                    apiConfig={apiConfig}
                    datasets={datasets}
                    selectedTable={targetDataTable}
                    onSelectTable={onSelectDataTable}
                />
            </div>
        ) : (
            <div className={`flex flex-1 min-w-0 h-full w-full ${isVertical ? 'flex-col' : 'flex-row'}`}>
                
                {/* Panel First (Left or Top) */}
                {showRightPanel && isPanelFirst && (
                    <>
                        {panelContent}
                        {resizer}
                    </>
                )}
                
                {/* Main Content */}
                {mainContent}

                {/* Panel Last (Right or Bottom) */}
                {showRightPanel && !isPanelFirst && (
                    <>
                        {resizer}
                        {panelContent}
                    </>
                )}
            </div>
        )}
    </main>
  );
};
