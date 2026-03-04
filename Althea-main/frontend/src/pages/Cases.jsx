import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../services/api'

const STATUS_OPTIONS = ['OPEN', 'IN_PROGRESS', 'CLOSED_TP', 'CLOSED_FP']

function _fmtDate(s) {
  if (!s) return '-'
  try {
    const d = new Date(s)
    return isNaN(d.getTime()) ? s : d.toLocaleString()
  } catch {
    return s
  }
}

export function Cases() {
  const [cases, setCases] = useState({})
  const [selected, setSelected] = useState(null)
  const [auditEvents, setAuditEvents] = useState([])
  const [loading, setLoading] = useState(true)
  const [editStatus, setEditStatus] = useState('')
  const [editAssigned, setEditAssigned] = useState('')
  const [editNotes, setEditNotes] = useState('')
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState(null)

  const loadCases = () => {
    api.getCases().then((r) => {
      setCases(r.cases || {})
      const ids = Object.keys(r.cases || {})
      if (!selected || !ids.includes(selected)) setSelected(ids[0] || null)
    }).catch(() => {})
  }

  useEffect(() => {
    setLoading(true)
    api.getCases().then((r) => {
      setCases(r.cases || {})
      setSelected(Object.keys(r.cases || {})[0] || null)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [])

  useEffect(() => {
    if (selected && cases[selected]) {
      const c = cases[selected]
      setEditStatus(c.status || c.state || 'OPEN')
      setEditAssigned(c.assigned_to || c.owner || '')
      setEditNotes(c.notes || '')
    }
  }, [selected, cases])

  useEffect(() => {
    if (selected) {
      api.getCaseAudit(selected).then((r) => setAuditEvents(r.events || [])).catch(() => setAuditEvents([]))
    } else {
      setAuditEvents([])
    }
  }, [selected])

  const handleUpdate = async () => {
    if (!selected) return
    setSaving(true)
    setMsg(null)
    try {
      await api.updateCase(selected, {
        status: editStatus,
        assigned_to: editAssigned || undefined,
        notes: editNotes || undefined,
      })
      setMsg('Case updated.')
      loadCases()
      api.getCaseAudit(selected).then((r) => setAuditEvents(r.events || [])).catch(() => {})
    } catch (e) {
      setMsg(`Error: ${e.message}`)
    } finally {
      setSaving(false)
    }
  }

  const handleDelete = async () => {
    if (!selected) return
    if (!confirm(`Delete case ${selected}?`)) return
    const deletedId = selected
    setSaving(true)
    setMsg(null)
    try {
      await api.deleteCase(deletedId)
      const nextCases = { ...cases }
      delete nextCases[deletedId]
      const ids = Object.keys(nextCases)
      setCases(nextCases)
      setSelected(ids[0] || null)
      setMsg('Case deleted.')
      loadCases()
    } catch (e) {
      setMsg(`Error: ${e.message}`)
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <div className="max-w-[1200px] mx-auto"><div className="py-10 text-center text-[var(--muted)] text-[0.9375rem]">Loading...</div></div>

  const caseIds = Object.keys(cases)

  return (
    <div className="max-w-[1200px] mx-auto">
      <h1 className="text-[1.375rem] font-medium mb-5 text-[var(--text)]">Case Management</h1>
      {caseIds.length === 0 ? (
        <div className="p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)] text-sm leading-relaxed">
          No cases created yet. Select an alert in the <Link to="/alert-queue" className="text-blue-600 dark:text-blue-400 no-underline hover:underline">Alert Queue</Link> and click <strong>Create Case</strong>.
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-[200px_1fr] gap-6">
          <div className="flex flex-col gap-1">
            <h3 className="mt-0 text-sm font-semibold">Cases</h3>
            {caseIds.map((cid) => (
              <button
                key={cid}
                type="button"
                className={`px-3 py-2 text-sm text-left rounded-md border transition-all ${
                  selected === cid
                    ? 'bg-[var(--accent2)] text-white border-[var(--accent2)] dark:bg-white dark:text-[#0c0c0c] dark:border-white'
                    : 'bg-[var(--surface)] border-[var(--border)] text-[var(--text)] hover:bg-[var(--accent2)] hover:text-white hover:border-[var(--accent2)]'
                }`}
                onClick={() => setSelected(cid)}
              >
                {cid}
              </button>
            ))}
          </div>
          <div>
            {selected && cases[selected] && (
              <>
                <h3 className="mt-0 mb-4 text-lg font-semibold">Case: {selected}</h3>

                <section className="mt-6 p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-3">
                  <h4 className="mt-0 mb-3 text-sm text-[var(--muted)] uppercase tracking-wider">Metadata</h4>
                  <div className="flex flex-wrap gap-5 my-3">
                    <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Created</span><span className="font-semibold text-[0.9375rem]">{_fmtDate(cases[selected].created_at)}</span></div>
                    <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Updated</span><span className="font-semibold text-[0.9375rem]">{_fmtDate(cases[selected].updated_at)}</span></div>
                    <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Alerts</span><span className="font-semibold text-[0.9375rem]">{(cases[selected].alert_ids || []).length}</span></div>
                  </div>
                </section>

                <section className="mt-6 p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-3">
                  <h4 className="mt-0 mb-3 text-sm text-[var(--muted)] uppercase tracking-wider">Edit Case</h4>
                  <div className="mt-2">
                    <div className="mb-3">
                      <label className="block text-[0.8125rem] text-[var(--muted)] mb-1">Status</label>
                      <select className="w-full max-w-[320px] py-2 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md text-[var(--text)]" value={editStatus} onChange={(e) => setEditStatus(e.target.value)}>
                        {STATUS_OPTIONS.map((s) => (
                          <option key={s} value={s}>{s}</option>
                        ))}
                      </select>
                    </div>
                    <div className="mb-3">
                      <label className="block text-[0.8125rem] text-[var(--muted)] mb-1">Assigned to</label>
                      <input
                        type="text"
                        className="w-full max-w-[320px] py-2 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md text-[var(--text)]"
                        value={editAssigned}
                        onChange={(e) => setEditAssigned(e.target.value)}
                        placeholder="Analyst_1, Analyst_2, Manager..."
                      />
                    </div>
                    <div className="mb-3">
                      <label className="block text-[0.8125rem] text-[var(--muted)] mb-1">Notes</label>
                      <textarea
                        className="w-full max-w-[320px] py-2 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md text-[var(--text)] min-h-[60px] resize-y"
                        value={editNotes}
                        onChange={(e) => setEditNotes(e.target.value)}
                        placeholder="Case notes..."
                        rows={3}
                      />
                    </div>
                    <div className="flex gap-3 mt-4">
                      <button className="px-4 py-2 text-sm font-medium rounded-md bg-[var(--accent2)] text-white dark:bg-white dark:text-[#0c0c0c] disabled:opacity-60" onClick={handleUpdate} disabled={saving}>
                        {saving ? 'Saving...' : 'Update Case'}
                      </button>
                      <button type="button" className="px-4 py-2 text-sm font-medium rounded-md border border-red-500/40 text-red-600 dark:text-red-400 bg-transparent hover:bg-red-500/10 disabled:opacity-60" onClick={handleDelete} disabled={saving}>
                        Delete Case
                      </button>
                    </div>
                  </div>
                  {msg && <div className="mt-4 p-3 rounded-md border border-[var(--border)] bg-[var(--surface)] text-sm text-[var(--text)]">{msg}</div>}
                </section>

                <section className="mt-6 p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-3">
                  <h4 className="mt-0 mb-3 text-sm text-[var(--muted)] uppercase tracking-wider">Associated Alerts</h4>
                  <ul className="m-2 mt-2 pl-5 [&>li]:my-1.5 text-sm text-[var(--muted)]">
                    {(cases[selected].alert_ids || []).map((aid) => (
                      <li key={aid}>
                        <Link to={`/alert-queue?highlight=${aid}`} className="text-blue-600 dark:text-blue-400 no-underline hover:underline">{aid}</Link>
                      </li>
                    ))}
                  </ul>
                </section>

                <section className="mt-6 p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-3">
                  <h4 className="mt-0 mb-3 text-sm text-[var(--muted)] uppercase tracking-wider">Audit Log</h4>
                  {auditEvents.length === 0 ? (
                    <p className="text-xs text-[var(--muted)] m-0">No audit events yet.</p>
                  ) : (
                    <div className="flex flex-col gap-2 max-h-[200px] overflow-y-auto">
                      {auditEvents.map((e, i) => (
                        <div key={e.event_id || i} className="flex flex-wrap gap-2 items-baseline py-2 px-3 rounded-md bg-[var(--surface2)] text-[0.8125rem]">
                          <span className="text-[var(--muted)] whitespace-nowrap">{_fmtDate(e.ts)}</span>
                          <span className="font-semibold text-[var(--text)]">{e.actor}</span>
                          <span className="text-blue-600 dark:text-blue-400">{e.action}</span>
                          {e.payload && Object.keys(e.payload).length > 0 && (
                            <span className="flex-1 min-w-0 text-[var(--muted)] text-xs overflow-hidden text-ellipsis">{JSON.stringify(e.payload)}</span>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </section>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
