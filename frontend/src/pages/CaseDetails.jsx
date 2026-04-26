import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'
import { hasPermission } from '../services/permissions'

const CASE_TRANSITIONS = {
  open: ['under_review', 'escalated', 'closed'],
  under_review: ['escalated', 'sar_filed', 'closed'],
  escalated: ['under_review', 'sar_filed', 'closed'],
  sar_filed: ['closed'],
  closed: [],
}

function normalizeCaseStatus(status) {
  const raw = String(status || '').trim().toLowerCase()
  if (!raw) return 'open'
  if (raw === 'in_review' || raw === 'investigating') return 'under_review'
  if (raw === 'assigned') return 'open'
  return raw
}

function labelForStatus(status) {
  return String(status || '')
    .split('_')
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(' ')
}

export function CaseDetails() {
  const { id } = useParams()
  const { user } = useAuth()
  const [data, setData] = useState(null)
  const [status, setStatus] = useState('open')
  const [error, setError] = useState('')

  const canEditCase = hasPermission(user, 'work_cases')
  const canRecordSarFiling = hasPermission(user, 'manager_approval')

  const load = async () => {
    try {
      const res = await api.getInvestigationCase(id)
      setData(res)
      setStatus(normalizeCaseStatus(res?.case?.status || res?.case?.case_status))
      setError('')
    } catch (err) {
      setError(err.message || 'Failed to load case')
    }
  }

  useEffect(() => {
    load()
  }, [id])

  const saveStatus = async (nextStatus = status) => {
    try {
      await api.updateInvestigationCaseStatus(id, nextStatus)
      setStatus(nextStatus)
      await load()
    } catch (err) {
      setError(err.message || 'Failed to update case status')
    }
  }

  const timeline = data?.timeline || []
  const currentCase = data?.case || {}
  const displayStatus = normalizeCaseStatus(currentCase.case_status || currentCase.status)
  const allowedTransitions = useMemo(() => {
    const candidates = CASE_TRANSITIONS[displayStatus] || []
    return candidates.filter((nextStatus) => {
      if (nextStatus === 'sar_filed') return canEditCase && canRecordSarFiling
      return canEditCase
    })
  }, [canRecordSarFiling, canEditCase, displayStatus])
  const availableStatuses = useMemo(() => {
    const ordered = [displayStatus, ...allowedTransitions]
    return Array.from(new Set(ordered))
  }, [allowedTransitions, displayStatus])
  const canEscalate = allowedTransitions.includes('escalated')
  const canMoveToReview = allowedTransitions.includes('under_review')
  const canApproveSarNow = allowedTransitions.includes('sar_filed')
  const canClose = allowedTransitions.includes('closed')
  const selectedStatusAllowed = availableStatuses.includes(status)
  const statusUpdateDisabled = !canEditCase || !selectedStatusAllowed || status === displayStatus

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Case {id}</h1>
          <p className="text-sm text-slate-500">Escalation support and human-reviewed SAR/STR filing records are preserved on this case record.</p>
        </div>
        <Link to="/investigation/dashboard" className="px-3 py-1 border rounded">Back</Link>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}

      <div className="grid gap-4 md:grid-cols-3">
        <div className="border rounded p-4 bg-white space-y-2">
          <div className="text-xs uppercase text-slate-500">Status</div>
          <div className="text-xl font-semibold">{displayStatus || '-'}</div>
          <p>Alert: <Link className="text-blue-600" to={`/investigation/alerts/${currentCase.alert_id}`}>{currentCase.alert_id}</Link></p>
          <p>Created by: {currentCase.created_by}</p>
          <p>Created at: {currentCase.created_at}</p>
        </div>
        <div className="border rounded p-4 bg-white space-y-2">
          <div className="text-xs uppercase text-slate-500">Escalation</div>
          <p className="text-sm text-slate-600">Use escalation controls to move the case through review. SAR/STR filing can only be recorded after human compliance review by authorized manager/admin roles.</p>
          <div className="flex flex-wrap gap-2">
            {canMoveToReview ? (
              <button className="px-3 py-1 border rounded" disabled={!canEditCase} onClick={() => saveStatus('under_review')}>Move to Review</button>
            ) : null}
            {canEscalate ? (
              <button className="px-3 py-1 border rounded" disabled={!canEditCase} onClick={() => saveStatus('escalated')}>Escalate</button>
            ) : null}
            {canApproveSarNow ? (
              <button className="px-3 py-1 border rounded" disabled={!canEditCase || !canRecordSarFiling} onClick={() => saveStatus('sar_filed')}>Record SAR/STR Filing</button>
            ) : null}
            {canClose ? (
              <button className="px-3 py-1 border rounded" disabled={!canEditCase} onClick={() => saveStatus('closed')}>Close</button>
            ) : null}
          </div>
          {!canApproveSarNow && canRecordSarFiling ? (
            <p className="text-xs text-slate-500">SAR/STR filing record controls become available after the case moves to review or escalation.</p>
          ) : null}
          {!allowedTransitions.length ? (
            <p className="text-xs text-slate-500">No additional workflow transitions are available from the current case status.</p>
          ) : null}
        </div>
        <div className="border rounded p-4 bg-white space-y-2">
          <div className="text-xs uppercase text-slate-500">Timeline Health</div>
          <div className="text-xl font-semibold">{timeline.length}</div>
          <p className="text-sm text-slate-600">Immutable case events captured for audit, escalation history, and closure decisions.</p>
        </div>
      </div>

      <div className="border rounded p-4 bg-white space-y-3">
        <h2 className="font-semibold">Manager Approval Interface</h2>
        {canEditCase ? (
          <div className="flex gap-2 items-center flex-wrap">
            <select className="border rounded px-2 py-1" value={status} onChange={(e) => setStatus(e.target.value)}>
              {availableStatuses.map((s) => <option key={s} value={s}>{labelForStatus(s)}</option>)}
            </select>
            <button className="px-3 py-1 border rounded" disabled={statusUpdateDisabled} onClick={() => saveStatus(status)}>Update status</button>
            {!canRecordSarFiling ? <span className="text-xs text-slate-500">Recording SAR/STR filing requires manager or admin human review authority.</span> : null}
            {canRecordSarFiling && !canApproveSarNow ? <span className="text-xs text-slate-500">SAR/STR filing cannot be recorded from {labelForStatus(displayStatus)}.</span> : null}
          </div>
        ) : (
          <p className="text-xs text-slate-500">Your role is read-only for case status updates.</p>
        )}
      </div>

      <div className="border rounded p-4 bg-white">
        <h2 className="font-semibold mb-2">Timeline</h2>
        {timeline.length === 0 ? <p className="text-sm text-slate-500">No events recorded.</p> : null}
        {timeline.map((log) => (
          <div className="text-sm border-t py-2" key={log.id || `${log.timestamp}-${log.action}`}>
            <div>{log.action}</div>
            <div className="text-xs text-slate-500">{log.performed_by} | {log.timestamp}</div>
          </div>
        ))}
      </div>
    </div>
  )
}
