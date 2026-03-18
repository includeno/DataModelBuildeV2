import React, { useEffect, useState } from 'react';
import { Loader2, Trash2, UserPlus, Users, X } from 'lucide-react';
import { ProjectMember, ProjectRole } from '../types';
import { Button } from './Button';

interface ProjectMembersModalProps {
  isOpen: boolean;
  onClose: () => void;
  projectName?: string;
  members: ProjectMember[];
  loading?: boolean;
  error?: string | null;
  canManage?: boolean;
  onInvite: (email: string, role: ProjectRole) => Promise<void>;
  onUpdateRole: (member: ProjectMember, role: ProjectRole) => Promise<void>;
  onRemoveMember: (member: ProjectMember) => Promise<void>;
}

const ROLE_OPTIONS: ProjectRole[] = ['viewer', 'editor', 'admin'];

export const ProjectMembersModal: React.FC<ProjectMembersModalProps> = ({
  isOpen,
  onClose,
  projectName,
  members,
  loading = false,
  error = null,
  canManage = false,
  onInvite,
  onUpdateRole,
  onRemoveMember,
}) => {
  const [inviteEmail, setInviteEmail] = useState('');
  const [inviteRole, setInviteRole] = useState<ProjectRole>('viewer');
  const [submitting, setSubmitting] = useState(false);
  const [busyMemberId, setBusyMemberId] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen) return;
    setInviteEmail('');
    setInviteRole('viewer');
    setSubmitting(false);
    setBusyMemberId(null);
  }, [isOpen]);

  if (!isOpen) return null;

  const handleInvite = async () => {
    if (!inviteEmail.trim()) return;
    setSubmitting(true);
    try {
      await onInvite(inviteEmail.trim(), inviteRole);
      setInviteEmail('');
      setInviteRole('viewer');
    } finally {
      setSubmitting(false);
    }
  };

  const handleRoleChange = async (member: ProjectMember, role: ProjectRole) => {
    if (member.role === role) return;
    setBusyMemberId(member.userId);
    try {
      await onUpdateRole(member, role);
    } finally {
      setBusyMemberId(null);
    }
  };

  const handleRemove = async (member: ProjectMember) => {
    if (!confirm(`Remove ${member.email} from this project?`)) return;
    setBusyMemberId(member.userId);
    try {
      await onRemoveMember(member);
    } finally {
      setBusyMemberId(null);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[85vh] overflow-hidden animate-in fade-in zoom-in duration-200">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 bg-gray-50">
          <div className="flex items-center space-x-2">
            <Users className="w-5 h-5 text-blue-600" />
            <div>
              <h3 className="text-lg font-bold text-gray-900">Project Members</h3>
              <p className="text-xs text-gray-500">{projectName || 'Current project'}</p>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 transition-colors">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 space-y-5 overflow-y-auto max-h-[70vh]">
          {canManage && (
            <div className="border border-gray-200 rounded-xl p-4 bg-gray-50">
              <div className="text-sm font-semibold text-gray-800 mb-3">Invite member</div>
              <div className="grid grid-cols-1 md:grid-cols-[1fr,140px,auto] gap-3">
                <input
                  type="email"
                  value={inviteEmail}
                  onChange={(e) => setInviteEmail(e.target.value)}
                  placeholder="teammate@example.com"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm"
                />
                <select
                  value={inviteRole}
                  onChange={(e) => setInviteRole(e.target.value as ProjectRole)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm bg-white"
                >
                  {ROLE_OPTIONS.map(role => (
                    <option key={role} value={role}>{role}</option>
                  ))}
                </select>
                <Button
                  variant="primary"
                  onClick={handleInvite}
                  disabled={submitting || !inviteEmail.trim()}
                  icon={submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <UserPlus className="w-4 h-4" />}
                >
                  邀请
                </Button>
              </div>
            </div>
          )}

          {error && (
            <div className="text-sm text-red-600 bg-red-50 border border-red-100 rounded-md px-3 py-2">
              {error}
            </div>
          )}

          <div className="border border-gray-200 rounded-xl overflow-hidden">
            <div className="grid grid-cols-[1.5fr,1.2fr,140px,90px] gap-3 px-4 py-3 bg-gray-50 text-xs font-semibold uppercase tracking-wider text-gray-500">
              <div>Member</div>
              <div>Email</div>
              <div>Role</div>
              <div>Action</div>
            </div>
            <div className="divide-y divide-gray-100">
              {members.map((member) => {
                const isBusy = busyMemberId === member.userId;
                return (
                  <div key={member.userId} className="grid grid-cols-[1.5fr,1.2fr,140px,90px] gap-3 px-4 py-3 items-center text-sm">
                    <div>
                      <div className="font-medium text-gray-900">{member.displayName || member.email}</div>
                      <div className="text-xs text-gray-400">joined {new Date(member.createdAt).toLocaleDateString()}</div>
                    </div>
                    <div className="text-gray-600 break-all">{member.email}</div>
                    <div>
                      <select
                        value={member.role}
                        onChange={(e) => handleRoleChange(member, e.target.value as ProjectRole)}
                        disabled={!canManage || member.role === 'owner' || isBusy}
                        className="w-full px-2 py-2 border border-gray-300 rounded-md text-sm bg-white disabled:bg-gray-50"
                      >
                        <option value="owner">owner</option>
                        <option value="admin">admin</option>
                        <option value="editor">editor</option>
                        <option value="viewer">viewer</option>
                      </select>
                    </div>
                    <div>
                      {canManage && member.role !== 'owner' ? (
                        <button
                          onClick={() => handleRemove(member)}
                          disabled={isBusy}
                          className="inline-flex items-center px-2.5 py-2 text-xs font-medium rounded-md border border-red-200 text-red-600 hover:bg-red-50 disabled:opacity-60"
                          title="Remove member"
                        >
                          {isBusy ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Trash2 className="w-3.5 h-3.5" />}
                        </button>
                      ) : (
                        <span className="text-xs text-gray-400">-</span>
                      )}
                    </div>
                  </div>
                );
              })}
              {members.length === 0 && !loading && (
                <div className="px-4 py-6 text-sm text-gray-400 text-center">No members found.</div>
              )}
            </div>
          </div>

          {loading && (
            <div className="flex items-center justify-center text-sm text-gray-500">
              <Loader2 className="w-4 h-4 animate-spin mr-2" /> Loading members...
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
