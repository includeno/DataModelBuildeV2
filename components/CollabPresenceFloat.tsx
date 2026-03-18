import React, { useMemo, useState } from 'react';
import { ChevronDown, Cloud, CloudOff, Users } from 'lucide-react';
import { RealtimeStatus } from '../utils/realtimeCollab';

type CollabPresenceItem = {
  connectionId: string;
  label: string;
  email?: string;
  role?: string;
  editingNodeName?: string | null;
};

interface CollabPresenceFloatProps {
  visible: boolean;
  projectName?: string;
  realtimeStatus: RealtimeStatus;
  onlineMembersCount: number;
  remoteEditingLabel?: string | null;
  members: CollabPresenceItem[];
  onOpenMembers?: () => void;
}

const STATUS_LABELS: Record<RealtimeStatus, string> = {
  idle: '未连接',
  connecting: '连接中',
  connected: '在线',
  reconnecting: '重连中',
  closed: '离线',
};

const STATUS_STYLES: Record<RealtimeStatus, string> = {
  idle: 'border-slate-200 bg-slate-100 text-slate-600',
  connecting: 'border-sky-200 bg-sky-100 text-sky-700',
  connected: 'border-emerald-200 bg-emerald-100 text-emerald-700',
  reconnecting: 'border-amber-200 bg-amber-100 text-amber-700',
  closed: 'border-rose-200 bg-rose-100 text-rose-700',
};

export const CollabPresenceFloat: React.FC<CollabPresenceFloatProps> = ({
  visible,
  projectName,
  realtimeStatus,
  onlineMembersCount,
  remoteEditingLabel = null,
  members,
  onOpenMembers,
}) => {
  const [collapsed, setCollapsed] = useState(false);

  const summaryText = useMemo(() => {
    if (remoteEditingLabel) return remoteEditingLabel;
    if (onlineMembersCount > 0) return `${onlineMembersCount} 位协作者在线`;
    if (realtimeStatus === 'connected') return '实时协作已连接';
    return '等待协作连接';
  }, [onlineMembersCount, realtimeStatus, remoteEditingLabel]);

  if (!visible) return null;

  return (
    <div className="pointer-events-none fixed bottom-4 right-4 z-50 flex max-w-[calc(100vw-2rem)] justify-end sm:bottom-5 sm:right-5">
      <aside
        data-testid="collab-float"
        className="pointer-events-auto w-[min(360px,calc(100vw-2rem))] overflow-hidden rounded-2xl border border-slate-200 bg-white/95 shadow-[0_20px_50px_rgba(15,23,42,0.18)] backdrop-blur"
      >
        <div className="border-b border-slate-200 bg-[radial-gradient(circle_at_top_left,_rgba(59,130,246,0.16),_transparent_55%),linear-gradient(135deg,_rgba(255,255,255,0.96),_rgba(248,250,252,0.98))] px-4 py-3">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                <Users className="h-3.5 w-3.5" />
                协作浮窗
              </div>
              <div className="mt-2 text-sm font-semibold text-slate-900 truncate">{projectName || '当前项目'}</div>
              <div data-testid="collab-float-summary" className="mt-1 text-xs text-slate-600">
                {summaryText}
              </div>
            </div>
            <button
              type="button"
              data-testid="collab-float-toggle"
              onClick={() => setCollapsed(value => !value)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-white/70 bg-white/80 text-slate-500 transition hover:border-slate-200 hover:text-slate-700"
              title={collapsed ? '展开协作浮窗' : '收起协作浮窗'}
            >
              <ChevronDown className={`h-4 w-4 transition-transform ${collapsed ? '-rotate-90' : 'rotate-0'}`} />
            </button>
          </div>

          <div className="mt-3 flex flex-wrap items-center gap-2">
            <div className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-xs font-medium ${STATUS_STYLES[realtimeStatus]}`}>
              {realtimeStatus === 'connected' ? <Cloud className="h-3.5 w-3.5" /> : <CloudOff className="h-3.5 w-3.5" />}
              <span>{STATUS_LABELS[realtimeStatus]}</span>
            </div>
            <div className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs font-medium text-slate-700">
              <Users className="h-3.5 w-3.5" />
              <span>{onlineMembersCount} 人在线</span>
            </div>
          </div>
        </div>

        {!collapsed && (
          <div data-testid="collab-float-body" className="space-y-3 px-4 py-3">
            {remoteEditingLabel && (
              <div className="rounded-xl border border-violet-200 bg-violet-50 px-3 py-2 text-xs font-medium text-violet-700">
                {remoteEditingLabel}
              </div>
            )}

            <div>
              <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">
                在线成员
              </div>
              {members.length === 0 ? (
                <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-3 py-3 text-xs text-slate-500">
                  暂无其他在线成员。邀请协作者后，这里会显示在线状态和编辑位置。
                </div>
              ) : (
                <div className="space-y-2">
                  {members.map(member => (
                    <div key={member.connectionId} className="rounded-xl border border-slate-200 bg-white px-3 py-2.5">
                      <div className="flex items-center justify-between gap-3">
                        <div className="min-w-0">
                          <div className="truncate text-sm font-medium text-slate-900">{member.label}</div>
                          <div className="mt-1 truncate text-[11px] text-slate-500">
                            {member.role || 'collaborator'}
                            {member.email ? ` · ${member.email}` : ''}
                          </div>
                        </div>
                        <span className={`inline-flex shrink-0 items-center rounded-full px-2 py-1 text-[10px] font-semibold ${member.editingNodeName ? 'bg-violet-100 text-violet-700' : 'bg-slate-100 text-slate-600'}`}>
                          {member.editingNodeName ? '编辑中' : '在线'}
                        </span>
                      </div>
                      <div className="mt-2 text-xs text-slate-600">
                        {member.editingNodeName ? `正在编辑 ${member.editingNodeName}` : '当前未选中编辑节点'}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {typeof onOpenMembers === 'function' && (
              <button
                type="button"
                onClick={onOpenMembers}
                className="w-full rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:bg-slate-100"
              >
                打开成员管理
              </button>
            )}
          </div>
        )}
      </aside>
    </div>
  );
};

export type { CollabPresenceItem };
