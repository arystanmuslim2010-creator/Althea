import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../services/api'

const STATUS_OPTIONS = ['OPEN', 'ASSIGNED', 'IN_PROGRESS', 'ESCALATED', 'MANAGER_REVIEW', 'SAR_FILED', 'CLOSED_TP', 'CLOSED_FP']
const TAB_IDS = [
  { id: 'overview', label: 'Alert Overview' },
  { id: 'why', label: 'Why This Alert' },
  { id: 'governance', label: 'Governance Decision' },
  { id: 'evidence', label: 'Evidence' },
  { id: 'audit', label: 'Audit & History' },
  { id: 'ai', label: 'AI Summary' },
]

function _fmtDate(s) {
  if (!s) return '-'
  try {
    const d = new Date(s)
    return isNaN(d.getTime()) ? s : d.toLocaleString()
  } catch {
    return s
  }
}

function _parseJson(val) {
  if (!val) return null
  if (typeof val === 'object') return val
  try {
    return typeof val === 'string' ? JSON.parse(val) : null
  } catch {
    return null
  }
}

function badgeClass(riskBand) {
  const b = (riskBand || '').toLowerCase()
  const base = 'inline-block px-2 py-0.5 text-[0.68rem] font-semibold rounded uppercase tracking-wide'
  if (b === 'critical') return `${base} bg-red-500/20 text-red-500`
  if (b === 'high') return `${base} bg-orange-500/20 text-orange-500`
  if (b === 'medium') return `${base} bg-orange-500/10 text-orange-500`
  if (b === 'low') return `${base} bg-green-500/20 text-green-500`
  return `${base} bg-[var(--surface2)] text-[var(--muted)]`
}

function govBadgeClass(status) {
  const s = (status || '').toLowerCase().replace(/[^a-z0-9]/g, '-')
  const base = 'inline-block px-2 py-0.5 text-[0.68rem] font-semibold rounded uppercase tracking-wide'
  if (s === 'eligible') return `${base} bg-green-500/15 text-green-600 dark:text-green-400`
  if (s === 'mandatory-review') return `${base} bg-orange-500/15 text-orange-600 dark:text-orange-400`
  if (s === 'suppressed') return `${base} bg-slate-500/20 text-[var(--muted)]`
  return `${base} bg-[var(--surface2)] text-[var(--muted)]`
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
  const [selectedAlertId, setSelectedAlertId] = useState(null)
  const [alertDetail, setAlertDetail] = useState(null)
  const [alertExplain, setAlertExplain] = useState(null)
  const [activeTab, setActiveTab] = useState('overview')
  const [aiSummary, setAiSummary] = useState(null)
  const [aiSummaryLoading, setAiSummaryLoading] = useState(false)

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
      const firstAlertId = (c.alert_ids || [])[0] || null
      setSelectedAlertId(firstAlertId)
      setActiveTab('overview')
    }
  }, [selected, cases])

  useEffect(() => {
    if (selected) {
      api.getCaseAudit(selected).then((r) => setAuditEvents(r.events || [])).catch(() => setAuditEvents([]))
    } else {
      setAuditEvents([])
    }
  }, [selected])

  useEffect(() => {
    const loadAlertData = async () => {
      if (!selectedAlertId) {
        setAlertDetail(null)
        setAlertExplain(null)
        setAiSummary(null)
        return
      }
      try {
        const [detail, explain, summaryRes] = await Promise.all([
          api.getAlert(selectedAlertId),
          api.getAlertExplain(selectedAlertId).catch(() => null),
          api.getAiSummary(selectedAlertId).catch(() => ({ summary: null })),
        ])
        setAlertDetail(detail || null)
        setAlertExplain(explain || null)
        setAiSummary(summaryRes?.summary || null)
      } catch {
        setAlertDetail(null)
        setAlertExplain(null)
        setAiSummary(null)
      }
    }
    loadAlertData()
  }, [selectedAlertId])

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

  const handleGenerateSummary = async () => {
    if (!selectedAlertId) return
    setAiSummaryLoading(true)
    try {
      const res = await api.generateAiSummary(selectedAlertId)
      setAiSummary(res.summary || null)
    } catch (e) {
      setMsg(`Error: ${e.message}`)
    } finally {
      setAiSummaryLoading(false)
    }
  }

  const handleClearSummary = async () => {
    if (!selectedAlertId) return
    setAiSummaryLoading(true)
    try {
      await api.clearAiSummary(selectedAlertId)
      setAiSummary(null)
    } catch (e) {
      setMsg(`Error: ${e.message}`)
    } finally {
      setAiSummaryLoading(false)
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
                        <button
                          type="button"
                          className={`mr-2 px-2 py-0.5 rounded border text-xs ${
                            selectedAlertId === aid
                              ? 'bg-blue-500/15 border-blue-500 text-blue-600 dark:text-blue-400'
                              : 'bg-[var(--surface2)] border-[var(--border)] text-[var(--muted)]'
                          }`}
                          onClick={() => setSelectedAlertId(aid)}
                        >
                          Inspect
                        </button>
                        <Link to={`/alert-queue?highlight=${aid}`} className="text-blue-600 dark:text-blue-400 no-underline hover:underline">{aid}</Link>
                      </li>
                    ))}
                  </ul>
                </section>

                <section className="mt-6 p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-3">
                  <h4 className="mt-0 mb-3 text-sm text-[var(--muted)] uppercase tracking-wider">Case Alert Investigation</h4>
                  {!selectedAlertId || !alertDetail ? (
                    <p className="text-xs text-[var(--muted)] m-0">Select an associated alert to view details.</p>
                  ) : (
                    <>
                      <div className="flex items-center justify-between flex-wrap gap-2 pb-4 border-b border-[var(--border)]">
                        <h3 className="m-0 text-base font-semibold text-[var(--text)]">Alert {alertDetail.alert_id}</h3>
                        <div className="flex gap-2">
                          <span className={badgeClass(alertDetail.risk_band)}>{alertDetail.risk_band || '-'}</span>
                          <span className={govBadgeClass(alertDetail.governance_status)}>{alertDetail.governance_status || '-'}</span>
                        </div>
                      </div>

                      <div className="flex flex-wrap gap-1 py-2 border-b border-[var(--border)] mb-4">
                        {TAB_IDS.map((t) => (
                          <button
                            key={t.id}
                            type="button"
                            className={`px-2.5 py-1.5 text-[0.75rem] font-medium rounded transition-colors ${
                              activeTab === t.id
                                ? 'text-blue-600 dark:text-blue-300 bg-blue-500/10 dark:bg-blue-400/15 font-semibold'
                                : 'text-[var(--muted)] bg-transparent hover:text-[var(--text)] hover:bg-[var(--surface2)]'
                            }`}
                            onClick={() => setActiveTab(t.id)}
                          >
                            {t.label}
                          </button>
                        ))}
                      </div>

                      {activeTab === 'overview' && (
                        <div className="py-2">
                          <div className="flex flex-wrap gap-5 my-3">
                            <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">RISK SCORE</span><span className="font-semibold text-[0.9375rem]">{alertDetail.risk_score != null ? Number(alertDetail.risk_score).toFixed(1) : '-'}</span></div>
                            <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">RISK PROB</span><span className="font-semibold text-[0.9375rem]">{alertDetail.risk_prob != null ? Number(alertDetail.risk_prob).toFixed(3) : '-'}</span></div>
                            <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">RANK</span><span className="font-semibold text-[0.9375rem]">{alertDetail.risk_score_rank ?? alertDetail.queue_rank ?? '-'}</span></div>
                          </div>
                          <div className="[&>p]:m-0.5 [&>p]:text-sm [&>p]:text-[var(--text)]">
                            <p>Segment: {alertDetail.segment ?? '-'}</p>
                            <p>Typology: {alertDetail.typology ?? '-'}</p>
                            <p>User ID: {alertDetail.user_id ?? '-'}</p>
                            <p>Tx Ref: {alertDetail.tx_ref ?? '-'}</p>
                          </div>
                        </div>
                      )}

                      {activeTab === 'why' && (
                        <div className="py-2">
                          <div className="bg-blue-500/5 border-l-4 border-blue-500 p-4 rounded-md mb-4">
                            <h5 className="m-0 mb-2 text-[0.8125rem]">Why This Alert Is Prioritized / Suppressed</h5>
                            {alertExplain ? (
                              <pre className="m-0 text-xs overflow-x-auto whitespace-pre-wrap break-words">{JSON.stringify(alertExplain, null, 2)}</pre>
                            ) : (
                              <p className="m-0 text-sm text-[var(--muted)]">No explanation payload available.</p>
                            )}
                          </div>
                        </div>
                      )}

                      {activeTab === 'governance' && (
                        <div className="py-2">
                          <h5 className="m-0 mb-2 text-[0.8125rem]">Governance Decision</h5>
                          <div className="[&>p]:my-2 [&>p]:text-sm">
                            <p>Status: <span className={govBadgeClass(alertDetail.governance_status)}>{alertDetail.governance_status ?? '-'}</span></p>
                            <p>In Queue: {alertDetail.in_queue ? 'Yes' : 'No'}</p>
                            <p>Suppression Code: {alertDetail.suppression_code || '-'}</p>
                            <p>Suppression Reason: {alertDetail.suppression_reason || 'None'}</p>
                            <p>Policy Version: {alertDetail.policy_version ?? '-'}</p>
                          </div>
                        </div>
                      )}

                      {activeTab === 'evidence' && (
                        <div className="py-2">
                          <h5 className="m-0 mb-2 text-[0.8125rem]">Evidence</h5>
                          <div className="space-y-3">
                            <div>
                              <p className="text-xs text-[var(--muted)] m-0 mb-1">Feature contributions</p>
                              <pre className="m-0 text-xs overflow-x-auto whitespace-pre-wrap break-words bg-[var(--surface2)] p-2 rounded">{JSON.stringify(_parseJson(alertDetail.top_feature_contributions_json) || alertDetail.top_features || [], null, 2)}</pre>
                            </div>
                            <div>
                              <p className="text-xs text-[var(--muted)] m-0 mb-1">Rule signals</p>
                              <pre className="m-0 text-xs overflow-x-auto whitespace-pre-wrap break-words bg-[var(--surface2)] p-2 rounded">{JSON.stringify(_parseJson(alertDetail.rules_json) || [], null, 2)}</pre>
                            </div>
                            <div>
                              <p className="text-xs text-[var(--muted)] m-0 mb-1">Rule evidence</p>
                              <pre className="m-0 text-xs overflow-x-auto whitespace-pre-wrap break-words bg-[var(--surface2)] p-2 rounded">{JSON.stringify(_parseJson(alertDetail.rule_evidence_json) || {}, null, 2)}</pre>
                            </div>
                          </div>
                        </div>
                      )}

                      {activeTab === 'audit' && (
                        <div className="py-2">
                          <h5 className="m-0 mb-2 text-[0.8125rem]">Audit & History</h5>
                          {auditEvents.length === 0 ? (
                            <p className="text-xs text-[var(--muted)] m-0">No audit events yet.</p>
                          ) : (
                            <div className="flex flex-col gap-2 max-h-[220px] overflow-y-auto">
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
                        </div>
                      )}

                      {activeTab === 'ai' && (
                        <div className="py-2">
                          <div className="flex gap-2 mb-4">
                            <button type="button" className="px-4 py-2 text-sm font-medium rounded-md bg-[var(--accent2)] text-white dark:bg-white dark:text-[#0c0c0c] disabled:opacity-60" onClick={handleGenerateSummary} disabled={aiSummaryLoading}>
                              {aiSummaryLoading ? 'Generating...' : 'Generate Summary'}
                            </button>
                            <button type="button" className="px-4 py-2 text-sm font-medium rounded-md border border-[var(--border)] bg-transparent disabled:opacity-60" onClick={handleClearSummary} disabled={aiSummaryLoading || !aiSummary}>
                              Clear Summary
                            </button>
                          </div>
                          {aiSummary ? (
                            <div className="p-4 rounded-md bg-blue-500/10 border-l-4 border-blue-500">
                              <pre className="m-0 whitespace-pre-wrap font-sans text-sm leading-relaxed">{aiSummary}</pre>
                            </div>
                          ) : (
                            <div className="mt-4 p-4 rounded-md bg-blue-500/10 border-l-4 border-blue-500 text-sm">
                              No AI summary yet. Click Generate Summary to create one.
                            </div>
                          )}
                        </div>
                      )}
                    </>
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
