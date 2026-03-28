import React, { useMemo, useState, useEffect } from 'react';
import { X, GitBranch, ChevronRight, Lock, Table as TableIcon } from 'lucide-react';
import { LineageMap, FieldLineage, LineageStep } from '../types';

interface LineagePanelProps {
  lineage: LineageMap;
  nodeId: string;
  commandId?: string;
  /** When set (command-level), the table selector is locked to this value */
  lockedTable?: string;
  onClose: () => void;
}

const COMMAND_TYPE_COLOR: Record<string, string> = {
  source:    'bg-blue-100 text-blue-800 border-blue-200',
  join:      'bg-purple-100 text-purple-800 border-purple-200',
  transform: 'bg-amber-100 text-amber-800 border-amber-200',
  group:     'bg-green-100 text-green-800 border-green-200',
  view:      'bg-gray-100 text-gray-700 border-gray-200',
  filter:    'bg-red-100 text-red-800 border-red-200',
  validate:  'bg-orange-100 text-orange-800 border-orange-200',
};

const stepColor = (type: string) =>
  COMMAND_TYPE_COLOR[type] ?? 'bg-gray-100 text-gray-700 border-gray-200';

const ORIGIN_DOT: Record<string, string> = {
  computed: 'bg-amber-400',
  unknown:  'bg-gray-400',
};

// Deterministic active-tab background colour per table name
const TABLE_PALETTE = ['#2563eb','#7c3aed','#059669','#d97706','#dc2626','#0891b2','#db2777','#65a30d'];
const tableColorCache: Record<string, string> = {};
const getTableColor = (name: string): string => {
  if (!tableColorCache[name]) {
    let hash = 0;
    for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) >>> 0;
    tableColorCache[name] = TABLE_PALETTE[hash % TABLE_PALETTE.length];
  }
  return tableColorCache[name];
};
const originDot = (table: string) => ORIGIN_DOT[table] ?? 'bg-blue-500';

// ── SVG DAG for a single field's transformation chain ──────────────────────

interface ChainDiagramProps {
  field: FieldLineage;
}

const STEP_W = 140;
const STEP_H = 52;
const H_GAP = 24;
const V_CENTER = 36;

const ChainDiagram: React.FC<ChainDiagramProps> = ({ field }) => {
  const steps = field.transformations;
  const totalW = steps.length * (STEP_W + H_GAP);
  const svgH = STEP_H + 20;

  return (
    <svg
      width={totalW || 20}
      height={svgH}
      viewBox={`0 0 ${totalW || 20} ${svgH}`}
      className="overflow-visible"
    >
      {steps.map((step, i) => {
        const x = i * (STEP_W + H_GAP);
        const colors = COMMAND_TYPE_COLOR[step.commandType] ?? 'bg-gray-100 text-gray-700 border-gray-200';
        // extract tailwind bg color as SVG fill (approx)
        const fillMap: Record<string, string> = {
          source: '#dbeafe', join: '#f3e8ff', transform: '#fef3c7',
          group: '#dcfce7', view: '#f3f4f6', filter: '#fee2e2', validate: '#ffedd5',
        };
        const fill = fillMap[step.commandType] ?? '#f9fafb';
        const strokeMap: Record<string, string> = {
          source: '#93c5fd', join: '#d8b4fe', transform: '#fcd34d',
          group: '#86efac', view: '#d1d5db', filter: '#fca5a5', validate: '#fdba74',
        };
        const stroke = strokeMap[step.commandType] ?? '#e5e7eb';

        return (
          <g key={i}>
            {/* connector arrow */}
            {i > 0 && (
              <line
                x1={x - H_GAP}
                y1={V_CENTER}
                x2={x}
                y2={V_CENTER}
                stroke="#9ca3af"
                strokeWidth={1.5}
                markerEnd="url(#arrow)"
              />
            )}
            {/* step box */}
            <rect
              x={x}
              y={V_CENTER - 20}
              width={STEP_W}
              height={STEP_H - 4}
              rx={6}
              fill={fill}
              stroke={stroke}
              strokeWidth={1.5}
            />
            <text
              x={x + STEP_W / 2}
              y={V_CENTER - 4}
              textAnchor="middle"
              fontSize={9}
              fontWeight="600"
              fill="#374151"
              fontFamily="system-ui, sans-serif"
            >
              {step.commandType.toUpperCase()}
            </text>
            <text
              x={x + STEP_W / 2}
              y={V_CENTER + 8}
              textAnchor="middle"
              fontSize={8}
              fill="#6b7280"
              fontFamily="monospace"
            >
              {step.commandId.length > 16 ? step.commandId.slice(0, 15) + '…' : step.commandId}
            </text>
            {step.expression && (
              <text
                x={x + STEP_W / 2}
                y={V_CENTER + 20}
                textAnchor="middle"
                fontSize={7.5}
                fill="#9ca3af"
                fontFamily="monospace"
              >
                {step.expression.length > 20 ? step.expression.slice(0, 19) + '…' : step.expression}
              </text>
            )}
          </g>
        );
      })}
      <defs>
        <marker id="arrow" markerWidth="6" markerHeight="6" refX="3" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill="#9ca3af" />
        </marker>
      </defs>
    </svg>
  );
};

// ── Main panel ────────────────────────────────────────────────────────────────

export const LineagePanel: React.FC<LineagePanelProps> = ({ lineage, nodeId, commandId, lockedTable, onClose }) => {
  const [selected, setSelected] = useState<string | null>(null);

  // All unique origin tables present in the lineage
  const availableTables = useMemo(() => {
    const tables = new Set(Object.values(lineage).map(fl => fl.originTable));
    return Array.from(tables).sort();
  }, [lineage]);

  // Active table filter: locked (command-level) or user-selected (node-level, 'all' = show everything)
  const [activeTable, setActiveTable] = useState<string>(() =>
    lockedTable ?? (availableTables.length === 1 ? availableTables[0] : 'all')
  );

  // Reset selection when table filter changes
  useEffect(() => { setSelected(null); }, [activeTable]);

  // Sync lockedTable if it changes
  useEffect(() => {
    if (lockedTable) setActiveTable(lockedTable);
  }, [lockedTable]);

  const fields = useMemo(() => {
    const all = Object.keys(lineage).sort();
    if (activeTable === 'all') return all;
    return all.filter(f => lineage[f].originTable === activeTable);
  }, [lineage, activeTable]);

  const selectedLineage = selected ? lineage[selected] : null;

  if (Object.keys(lineage).length === 0) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
        <div className="bg-white rounded-xl shadow-2xl w-[480px] p-8 text-center">
          <GitBranch className="w-10 h-10 text-gray-300 mx-auto mb-3" />
          <p className="text-gray-600 font-medium">No lineage data available for this node.</p>
          <p className="text-gray-400 text-sm mt-1">Run the node first or add a source command.</p>
          <button onClick={onClose} className="mt-6 px-4 py-2 text-sm bg-gray-100 hover:bg-gray-200 rounded-lg">Close</button>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-12 bg-black/40">
      <div className="bg-white rounded-xl shadow-2xl flex flex-col w-[900px] max-h-[80vh] overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200 bg-gray-50 shrink-0">
          <div className="flex items-center gap-2">
            <GitBranch className="w-4 h-4 text-blue-600" />
            <span className="font-semibold text-gray-800 text-sm">数据血缘</span>
            <span className="text-[11px] text-gray-400 font-mono bg-gray-100 px-2 py-0.5 rounded">{nodeId}</span>
            {commandId && (
              <>
                <span className="text-gray-300 text-xs">→</span>
                <span className="text-[11px] font-mono bg-blue-50 text-blue-600 border border-blue-100 px-2 py-0.5 rounded">截止 {commandId}</span>
              </>
            )}
            {!commandId && (
              <span className="text-[10px] text-gray-400 bg-gray-50 border border-gray-200 px-2 py-0.5 rounded">完整节点</span>
            )}
          </div>
          <button onClick={onClose} className="p-1 rounded hover:bg-gray-200 text-gray-500">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Table filter bar */}
        <div className="px-4 py-2 border-b border-gray-100 bg-white shrink-0 flex items-center gap-2 flex-wrap">
          <TableIcon className="w-3.5 h-3.5 text-gray-400 shrink-0" />
          {lockedTable ? (
            // Command level — locked to a single table
            <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-blue-50 border border-blue-200 text-blue-700 text-xs font-medium">
              <Lock className="w-3 h-3" />
              <span>{lockedTable}</span>
            </div>
          ) : (
            // Node level — free tab switcher
            <>
              {availableTables.length > 1 && (
                <button
                  onClick={() => setActiveTable('all')}
                  className={`px-2.5 py-1 rounded-md text-xs font-medium border transition-colors ${
                    activeTable === 'all'
                      ? 'bg-gray-800 text-white border-gray-800'
                      : 'text-gray-500 border-gray-200 hover:bg-gray-50'
                  }`}
                >
                  全部
                </button>
              )}
              {availableTables.map(t => {
                const isActive = activeTable === t;
                const color = getTableColor(t);
                return (
                  <button
                    key={t}
                    onClick={() => setActiveTable(t)}
                    className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium border transition-colors ${
                      isActive ? 'text-white border-transparent' : 'text-gray-600 border-gray-200 hover:bg-gray-50'
                    }`}
                    style={isActive ? { background: color } : undefined}
                  >
                    <span
                      className="w-1.5 h-1.5 rounded-full"
                      style={{ background: isActive ? 'rgba(255,255,255,0.6)' : color }}
                    />
                    {t}
                  </button>
                );
              })}
            </>
          )}
          <span className="ml-auto text-[11px] text-gray-400">{fields.length} 个字段</span>
        </div>

        <div className="flex flex-1 overflow-hidden">
          {/* Field list */}
          <div className="w-52 shrink-0 border-r border-gray-200 overflow-y-auto py-2">
            {fields.length === 0 ? (
              <div className="px-3 py-6 text-xs text-gray-400 text-center">该表无字段</div>
            ) : fields.map(f => {
              const fl = lineage[f];
              const isSelected = selected === f;
              return (
                <button
                  key={f}
                  onClick={() => setSelected(f)}
                  className={`w-full text-left px-3 py-2 flex items-center gap-2 transition-colors ${isSelected ? 'bg-blue-50 text-blue-700' : 'hover:bg-gray-50 text-gray-700'}`}
                >
                  <span
                    className="w-2 h-2 rounded-full shrink-0"
                    style={{ background: getTableColor(fl.originTable) }}
                  />
                  <span className="text-xs font-medium truncate">{f}</span>
                  <span className="text-[10px] text-gray-300 shrink-0 hidden group-hover:inline">{fl.originTable}</span>
                  {isSelected && <ChevronRight className="w-3 h-3 ml-auto shrink-0 text-blue-500" />}
                </button>
              );
            })}
          </div>

          {/* Detail panel */}
          <div className="flex-1 overflow-y-auto p-5">
            {!selectedLineage ? (
              <div className="h-full flex items-center justify-center text-gray-400 text-sm">
                ← 选择一个字段查看血缘路径
              </div>
            ) : (
              <div className="space-y-5">
                {/* Origin */}
                <div>
                  <p className="text-[11px] font-semibold text-gray-400 uppercase tracking-wider mb-2">原始来源</p>
                  <div className="flex items-center gap-2">
                    <span
                      className="w-2.5 h-2.5 rounded-full"
                      style={{ background: getTableColor(selectedLineage.originTable) }}
                    />
                    <span className="text-sm font-mono text-gray-800">{selectedLineage.originTable}</span>
                    <span className="text-gray-400 text-xs">→</span>
                    <span className="text-sm font-mono text-gray-600">{selectedLineage.originField}</span>
                  </div>
                </div>

                {/* Chain diagram */}
                {selectedLineage.transformations.length > 0 && (
                  <div>
                    <p className="text-[11px] font-semibold text-gray-400 uppercase tracking-wider mb-3">变换链路</p>
                    <div className="overflow-x-auto pb-2">
                      <ChainDiagram field={selectedLineage} />
                    </div>
                  </div>
                )}

                {/* Step list */}
                <div>
                  <p className="text-[11px] font-semibold text-gray-400 uppercase tracking-wider mb-2">步骤详情</p>
                  <div className="space-y-1.5">
                    {selectedLineage.transformations.map((step, i) => (
                      <div key={i} className={`flex items-start gap-2 rounded-md border px-3 py-2 text-xs ${stepColor(step.commandType)}`}>
                        <span className="font-semibold shrink-0 uppercase">{step.commandType}</span>
                        <span className="font-mono text-[11px] text-gray-500 shrink-0">{step.commandId}</span>
                        {step.expression && (
                          <span className="font-mono text-[11px] italic truncate">{step.expression}</span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
