import React from 'react';
import { X, Activity, Database, ListChecks } from 'lucide-react';
import { SessionDiagnosticsReport } from '../types';
import { Button } from './Button';

interface SessionDiagnosticsModalProps {
  isOpen: boolean;
  onClose: () => void;
  report: SessionDiagnosticsReport | null;
  loading?: boolean;
  error?: string | null;
}

export const SessionDiagnosticsModal: React.FC<SessionDiagnosticsModalProps> = ({
  isOpen,
  onClose,
  report,
  loading = false,
  error = null
}) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[80vh] overflow-hidden animate-in fade-in zoom-in duration-200">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50">
          <div className="flex items-center space-x-2">
            <Activity className="w-5 h-5 text-blue-600" />
            <div>
              <h3 className="text-lg font-bold text-gray-900">Session Diagnostics</h3>
              <p className="text-xs text-gray-500 font-mono">
                {report?.sessionId || 'No session loaded'}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 space-y-5 overflow-y-auto max-h-[60vh]">
          {loading && (
            <div className="text-sm text-gray-500">Loading diagnostics...</div>
          )}

          {error && (
            <div className="text-sm text-red-600 bg-red-50 border border-red-100 rounded-md px-3 py-2">
              {error}
            </div>
          )}

          {!loading && !error && report && (
            <>
              <div className="grid grid-cols-3 gap-3 text-xs text-gray-500">
                <div className="bg-gray-50 border border-gray-100 rounded-md p-3">
                  <div className="font-semibold text-gray-700">Generated</div>
                  <div className="font-mono mt-1">{new Date(report.generatedAt).toLocaleString()}</div>
                </div>
                <div className="bg-gray-50 border border-gray-100 rounded-md p-3">
                  <div className="font-semibold text-gray-700">Sources</div>
                  <div className="mt-1">{report.sources.length}</div>
                </div>
                <div className="bg-gray-50 border border-gray-100 rounded-md p-3">
                  <div className="font-semibold text-gray-700">Datasets</div>
                  <div className="mt-1">{report.datasets.length}</div>
                </div>
              </div>

              {report.warnings.length > 0 && (
                <div className="bg-yellow-50 border border-yellow-100 rounded-md p-3 text-sm text-yellow-900">
                  <div className="font-semibold mb-1">Warnings</div>
                  <ul className="list-disc list-inside text-xs">
                    {report.warnings
                      .flatMap(w => w.split('\n').map(line => line.trim()).filter(Boolean))
                      .map((w, i) => (
                        <li key={i}>{w}</li>
                      ))}
                  </ul>
                </div>
              )}

              <div>
                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2 flex items-center">
                  <Database className="w-4 h-4 mr-1.5" /> Sources
                </h4>
                <div className="space-y-2 text-sm">
                  {report.sources.map(s => (
                    <div key={s.id} className="border border-gray-100 rounded-md px-3 py-2 bg-white">
                      <div className="font-mono text-xs text-gray-500">{s.id}</div>
                      <div className="flex flex-wrap gap-2 text-xs mt-1">
                        <span className="bg-gray-50 border border-gray-200 rounded px-2 py-0.5">table: {s.mainTable || '-'}</span>
                        <span className="bg-gray-50 border border-gray-200 rounded px-2 py-0.5">alias: {s.alias || '-'}</span>
                        <span className="bg-gray-50 border border-gray-200 rounded px-2 py-0.5">linkId: {s.linkId || '-'}</span>
                        <span className="bg-gray-50 border border-gray-200 rounded px-2 py-0.5">note: {s.note || '-'}</span>
                      </div>
                    </div>
                  ))}
                  {report.sources.length === 0 && (
                    <div className="text-xs text-gray-400 italic">No sources found.</div>
                  )}
                </div>
              </div>

              <div>
                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2 flex items-center">
                  <ListChecks className="w-4 h-4 mr-1.5" /> DataSource Resolution
                </h4>
                <div className="space-y-1 text-xs">
                  {report.dataSourceResolution.map(r => (
                    <div key={r.commandId} className="flex items-center justify-between border border-gray-100 rounded-md px-3 py-2 bg-white">
                      <div className="font-mono text-[11px] text-gray-500">{r.commandId}</div>
                      <div className="text-gray-600">{r.dataSource} → {r.resolved}</div>
                      <div className={`text-xs font-semibold ${r.status === 'ok' ? 'text-green-600' : 'text-red-600'}`}>
                        {r.status.toUpperCase()}
                      </div>
                    </div>
                  ))}
                  {report.dataSourceResolution.length === 0 && (
                    <div className="text-xs text-gray-400 italic">No dataSource overrides found.</div>
                  )}
                </div>
              </div>

              <div>
                <h4 className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-2">Operations</h4>
                <div className="space-y-2 text-sm">
                  {report.operations.map(op => (
                    <div key={op.id} className="border border-gray-100 rounded-md px-3 py-2 bg-white">
                      <div className="font-semibold text-gray-800">{op.name || op.id}</div>
                      <div className="text-[11px] text-gray-400 font-mono">{op.operationType || 'process'}</div>
                      <div className="mt-2 space-y-1">
                        {op.commands.map(cmd => (
                          <div key={cmd.id} className="flex items-center justify-between text-xs bg-gray-50 border border-gray-100 rounded px-2 py-1">
                            <span className="font-mono text-gray-500">{cmd.id}</span>
                            <span className="text-gray-700">{cmd.type}</span>
                            <span className="text-gray-400">order {cmd.order}</span>
                            <span className="text-gray-400">dataSource {cmd.dataSource || 'stream'}</span>
                          </div>
                        ))}
                        {op.commands.length === 0 && (
                          <div className="text-xs text-gray-400 italic">No commands.</div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>

        <div className="p-4 bg-gray-50 border-t border-gray-200 flex justify-end">
          <Button variant="secondary" onClick={onClose}>
            Close
          </Button>
        </div>
      </div>
    </div>
  );
};
