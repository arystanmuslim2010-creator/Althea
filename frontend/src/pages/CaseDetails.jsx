import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'

const BASE_STATUSES = ['open', 'under_review', 'escalated', 'closed']

export function CaseDetails() {
  const { id } = useParams()
  const { user } = useAuth()
  const [data, setData] = useState(null)
  const [status, setStatus] = useState('open')
  const [error, setError] = useState('')

  const canEditCase = user?.role === 'analyst' || user?.role === 'lead' || user?.role === 'manager' || user?.role === 'admin'
  const canApproveSar = user?.role === 'manager' || user?.role === 'admin'

  const availableStatuses = useMemo(() => {
    if (canApproveSar) {
      return [...BASE_STATUSES, 'sar_filed']
    }
    return BASE_STATUSES
  }, [canApproveSar])

  const load = async () => {
    try {
      const res = await api.getInvestigationCase(id)
      setData(res)
      setStatus(res?.case?.status || 'open')
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

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Case {id}</h1>
          <p className="text-sm text-slate-500">Escalation, manager approval, and SAR workflow are preserved on this case record.</p>
        </div>
        <Link to="/investigation/dashboard" className="px-3 py-1 border rounded">Back</Link>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}

      <div className="grid gap-4 md:grid-cols-3">
        <div className="border rounded p-4 bg-white space-y-2">
          <div className="text-xs uppercase text-slate-500">Status</div>
          <div className="text-xl font-semibold">{currentCase.status || '-'}</div>
          <p>Alert: <Link className="text-blue-600" to={`/investigation/alerts/${currentCase.alert_id}`}>{currentCase.alert_id}</Link></p>
          <p>Created by: {currentCase.created_by}</p>
          <p>Created at: {currentCase.created_at}</p>
        </div>
        <div className="border rounded p-4 bg-white space-y-2">
          <div className="text-xs uppercase text-slate-500">Escalation</div>
          <p className="text-sm text-slate-600">Use `escalated` to push analyst review to lead/manager workflow. Manager and admin roles can approve SAR filing.</p>
          <div className="flex flex-wrap gap-2">
            <button className="px-3 py-1 border rounded" disabled={!canEditCase} onClick={() => saveStatus('escalated')}>Escalate</button>
            <button className="px-3 py-1 border rounded" disabled={!canApproveSar} onClick={() => saveStatus('sar_filed')}>Approve SAR</button>
            <button className="px-3 py-1 border rounded" disabled={!canEditCase} onClick={() => saveStatus('closed')}>Close</button>
          </div>
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
              {availableStatuses.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
            <button className="px-3 py-1 border rounded" onClick={() => saveStatus(status)}>Update status</button>
            {!canApproveSar ? <span className="text-xs text-slate-500">SAR filing requires manager or admin.</span> : null}
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
