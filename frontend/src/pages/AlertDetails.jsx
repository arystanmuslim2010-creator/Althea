import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'
import { InvestigationGraph } from '../components/InvestigationGraph'
import { normalizeExplanationPayload } from '../services/contracts'
import { hasPermission } from '../services/permissions'

function tryParseJson(value, fallback) {
  if (value == null) return fallback
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value)
  } catch {
    return fallback
  }
}

export function AlertDetails() {
  const { id } = useParams()
  const navigate = useNavigate()
  const { user } = useAuth()
  const [alert, setAlert] = useState(null)
  const [explain, setExplain] = useState(null)
  const [notes, setNotes] = useState([])
  const [noteText, setNoteText] = useState('')
  const [queueItem, setQueueItem] = useState(null)
  const [caseInfo, setCaseInfo] = useState(null)
  const [context, setContext] = useState(null)
  const [networkGraph, setNetworkGraph] = useState(null)
  const [graphLoading, setGraphLoading] = useState(false)
  const [graphError, setGraphError] = useState('')
  const [narrativeDraft, setNarrativeDraft] = useState(null)
  const [narrativeLoading, setNarrativeLoading] = useState(false)
  const [narrativeError, setNarrativeError] = useState('')
  const [copyStatus, setCopyStatus] = useState('')
  const [outcome, setOutcome] = useState(null)
  const [outcomeReason, setOutcomeReason] = useState('')
  const [actionBusy, setActionBusy] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const canAddNotes = hasPermission(user, 'add_investigation_notes')
  const canCreateCase = hasPermission(user, 'work_cases')
  const canAssign = hasPermission(user, 'reassign_alerts')
  const canChangeStatus = hasPermission(user, 'change_alert_status')
  const canRecordOutcome = canChangeStatus

  const loadGraph = async (alertId) => {
    setGraphLoading(true)
    setGraphError('')
    try {
      const graph = await api.getNetworkGraph(alertId)
      setNetworkGraph(graph)
    } catch (err) {
      setGraphError(err.message || 'Failed to load network graph')
    } finally {
      setGraphLoading(false)
    }
  }

  const loadNarrativeDraft = async (alertId) => {
    setNarrativeLoading(true)
    setNarrativeError('')
    try {
      const draft = await api.getNarrativeDraft(alertId)
      setNarrativeDraft(draft)
    } catch (err) {
      setNarrativeError(err.message || 'Failed to load narrative draft')
    } finally {
      setNarrativeLoading(false)
    }
  }

  const copyNarrative = async () => {
    const text = narrativeDraft?.narrative || ''
    if (!text) return
    try {
      await navigator.clipboard.writeText(text)
      setCopyStatus('Copied')
    } catch {
      setCopyStatus('Copy failed')
    } finally {
      setTimeout(() => setCopyStatus(''), 1600)
    }
  }

  const load = async () => {
    setLoading(true)
    try {
      const [alertResult, explainResult, notesResult, queueResult, contextResult, outcomeResult] = await Promise.allSettled([
        api.getAlert(id),
        api.getAlertExplain(id),
        api.getAlertNotes(id),
        api.getWorkQueue({ limit: 200 }),
        api.getInvestigationContext(id),
        api.getAlertOutcome(id),
      ])

      if (alertResult.status !== 'fulfilled') {
        throw alertResult.reason
      }

      const alertPayload = alertResult.value
      const explainPayload = explainResult.status === 'fulfilled' ? explainResult.value : null
      const notesPayload = notesResult.status === 'fulfilled' ? notesResult.value : { notes: [] }
      const queuePayload = queueResult.status === 'fulfilled' ? queueResult.value : { queue: [] }
      const contextPayload = contextResult.status === 'fulfilled' ? contextResult.value : null
      const outcomePayload = outcomeResult.status === 'fulfilled' ? outcomeResult.value : null

      setAlert(alertPayload)
      setExplain(explainPayload)
      setNotes(notesPayload.notes || [])
      setContext(contextPayload)
      setNetworkGraph(contextPayload?.network_graph || null)
      setNarrativeDraft(contextPayload?.narrative_draft || null)
      setOutcome(outcomePayload)
      const matched = (queuePayload.queue || []).find((item) => String(item.alert_id) === String(id)) || null
      setQueueItem(matched)
      const resolvedCase = contextPayload?.case_status || (matched?.case_id ? { case_id: matched.case_id, status: matched.case_status || matched.status } : null)
      setCaseInfo(resolvedCase)

      const hasSidecarFailure = [explainResult, notesResult, queueResult, contextResult, outcomeResult].some(
        (result) => result.status === 'rejected',
      )
      setError(hasSidecarFailure ? 'Some investigation context is temporarily unavailable.' : '')
    } catch (err) {
      setError(err.message || 'Failed to load alert')
    } finally {
      setLoading(false)
      void loadGraph(id)
      void loadNarrativeDraft(id)
    }
  }

  useEffect(() => {
    load()
  }, [id])

  const riskExplain = useMemo(() => tryParseJson(alert?.risk_explain_json, {}), [alert])
  const featuresJson = useMemo(() => tryParseJson(alert?.top_feature_contributions_json, []), [alert])
  const rulesJson = useMemo(() => tryParseJson(alert?.rules_json, []), [alert])
  const normalizedExplanation = useMemo(
    () => normalizeExplanationPayload(explain?.risk_explanation || riskExplain || {}),
    [explain, riskExplain],
  )

  const addNote = async () => {
    const text = noteText.trim()
    if (!text) return
    try {
      await api.addAlertNote(id, text)
      setNoteText('')
      await load()
    } catch (err) {
      setError(err.message || 'Failed to add note')
    }
  }

  const createCase = async () => {
    try {
      setActionBusy(true)
      const c = await api.createInvestigationCase(id)
      setCaseInfo(c)
      if (c?.case_id) {
        navigate(`/investigation/alerts/${id}`, {
          state: {
            created_case_id: c.case_id,
            source_alert_id: id,
          },
        })
        return
      }
      await load()
    } catch (err) {
      setError(err.message || 'Failed to create case')
    } finally {
      setActionBusy(false)
    }
  }

  const assignToMe = async () => {
    if (!user?.user_id) return
    try {
      setActionBusy(true)
      await api.workflowAssignAlert(id, user.user_id)
      await load()
    } catch (err) {
      setError(err.message || 'Failed to assign alert')
    } finally {
      setActionBusy(false)
    }
  }

  const escalateAlert = async () => {
    if (!user?.user_id) return
    try {
      setActionBusy(true)
      await api.workflowEscalateAlert(id, 'manual_escalation')
      await load()
    } catch (err) {
      setError(err.message || 'Failed to escalate alert')
    } finally {
      setActionBusy(false)
    }
  }

  const closeAlert = async () => {
    if (!user?.user_id) return
    try {
      setActionBusy(true)
      await api.workflowCloseAlert(id, 'manual_close')
      await load()
    } catch (err) {
      setError(err.message || 'Failed to close alert')
    } finally {
      setActionBusy(false)
    }
  }

  const saveOutcome = async (decision) => {
    if (!decision) return
    try {
      setActionBusy(true)
      const payload = {
        analyst_decision: decision,
        decision_reason: outcomeReason || null,
        model_version: context?.model_metadata?.model_version || alert?.model_version || null,
        risk_score_at_decision: Number(alert?.risk_score ?? 0),
      }
      const res = await api.recordAlertOutcome(id, payload)
      setOutcome(res)
      setOutcomeReason('')
    } catch (err) {
      setError(err.message || 'Failed to record outcome')
    } finally {
      setActionBusy(false)
    }
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between gap-4 flex-wrap">
        <h1 className="text-2xl font-semibold">Alert Details: {id}</h1>
        <div className="flex gap-2">
          <button onClick={() => navigate(-1)} className="px-3 py-1 border rounded">Back</button>
          <Link to="/investigation/dashboard" className="px-3 py-1 border rounded">Dashboard</Link>
        </div>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      {loading && <p className="text-sm text-slate-500">Loading investigation context...</p>}

      <div className="grid md:grid-cols-2 gap-4">
        <div className="border rounded p-4 bg-white space-y-1">
          <h2 className="font-semibold">Alert Information</h2>
          <p>Alert ID: {alert?.alert_id || id}</p>
          <p>Priority: {alert?.priority || alert?.risk_band || '-'}</p>
          <p>Risk Score: {alert?.risk_score ?? '-'}</p>
          <p>Assigned To: {queueItem?.assigned_to || 'Unassigned'}</p>
          <p>Status: {queueItem?.status || 'open'}</p>
          <p>Alert Age: {queueItem?.alert_age_hours != null ? `${queueItem.alert_age_hours}h` : '-'}</p>
          <p>Overdue Review: {queueItem?.overdue_review ? 'Yes' : 'No'}</p>
        </div>
        <div className="border rounded p-4 bg-white space-y-2">
          <h2 className="font-semibold">Case Status</h2>
          <p>Case ID: {caseInfo?.case_id || 'No case'}</p>
          <p>Case Status: {caseInfo?.status || queueItem?.case_status || '-'}</p>
          {caseInfo?.case_id ? (
            <Link className="text-blue-600" to={`/investigation/cases/${caseInfo.case_id}`}>Open Case</Link>
          ) : canCreateCase ? (
            <button className="px-3 py-1 border rounded" onClick={createCase} disabled={actionBusy}>Create Case</button>
          ) : (
            <span className="text-xs text-slate-500">Not permitted for your role.</span>
          )}
        </div>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Workflow Actions</h2>
        {canAssign || canChangeStatus ? (
          <div className="flex gap-2 flex-wrap">
            {canAssign ? <button className="px-3 py-1 border rounded" onClick={assignToMe} disabled={actionBusy}>Assign To Me</button> : null}
            {canChangeStatus ? <button className="px-3 py-1 border rounded" onClick={escalateAlert} disabled={actionBusy}>Escalate</button> : null}
            {canChangeStatus ? <button className="px-3 py-1 border rounded" onClick={closeAlert} disabled={actionBusy}>Close</button> : null}
          </div>
        ) : (
          <p className="text-xs text-slate-500">Workflow actions require backend permissions from your current session.</p>
        )}
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Risk Explanation</h2>
        <p className="text-sm">
          Method: <strong>{normalizedExplanation.explanation_method || 'unknown'}</strong> | Status: <strong>{normalizedExplanation.explanation_status || 'unknown'}</strong>
        </p>
        {(normalizedExplanation.explanation_method === 'shap' || normalizedExplanation.explanation_method === 'tree_shap') && (
          <p className="text-xs text-blue-700">Model attribution available (SHAP-based).</p>
        )}
        {normalizedExplanation.is_fallback && (
          <p className="text-xs text-amber-700">
            Heuristic feature highlights; not model contribution attribution.
            {normalizedExplanation.explanation_warning ? ` ${normalizedExplanation.explanation_warning}` : ''}
          </p>
        )}
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(normalizedExplanation, null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Feature Contributions</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(featuresJson || alert?.top_features || [], null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Rule Signals</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(rulesJson, null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Investigation Intelligence</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(context?.investigation_summary || {}, null, 2)}</pre>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(context?.risk_explanation || {}, null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Network Graph</h2>
        <p className="text-sm">
          Nodes: {networkGraph?.summary?.node_count ?? networkGraph?.node_count ?? 0} | Edges: {networkGraph?.summary?.edge_count ?? networkGraph?.edge_count ?? 0}
        </p>
        {graphLoading && <p className="text-xs text-slate-500">Loading graph...</p>}
        {graphError && <p className="text-xs text-amber-600">{graphError}</p>}
        <InvestigationGraph graph={networkGraph} />
        <details>
          <summary className="cursor-pointer text-xs text-slate-500">Raw graph payload</summary>
          <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(networkGraph || {}, null, 2)}</pre>
        </details>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Investigation Narrative Draft</h2>
        {narrativeLoading && <p className="text-xs text-slate-500">Loading narrative draft...</p>}
        {narrativeError && <p className="text-xs text-amber-600">{narrativeError}</p>}
        <p className="text-sm font-medium">{narrativeDraft?.title || 'Investigation Narrative Draft'}</p>
        <p className="text-xs text-slate-500">Draft for analyst review. Validate all statements before finalizing case notes.</p>
        <pre className="text-xs whitespace-pre-wrap">{narrativeDraft?.narrative || 'Draft unavailable.'}</pre>
        <div className="grid md:grid-cols-3 gap-3 text-xs">
          <div>
            <h3 className="font-semibold mb-1">Activity Summary</h3>
            <p>{narrativeDraft?.sections?.activity_summary || '-'}</p>
          </div>
          <div>
            <h3 className="font-semibold mb-1">Risk Indicators</h3>
            <ul className="list-disc pl-4">
              {(narrativeDraft?.sections?.risk_indicators || []).map((item, idx) => <li key={`risk-${idx}`}>{item}</li>)}
            </ul>
          </div>
          <div>
            <h3 className="font-semibold mb-1">Recommended Follow-up</h3>
            <ul className="list-disc pl-4">
              {(narrativeDraft?.sections?.recommended_follow_up || []).map((item, idx) => <li key={`follow-${idx}`}>{item}</li>)}
            </ul>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button className="px-3 py-1 border rounded" onClick={copyNarrative} disabled={!narrativeDraft?.narrative}>Copy Narrative</button>
          {copyStatus && <span className="text-xs text-slate-500">{copyStatus}</span>}
        </div>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Guidance and SAR Draft</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(context?.investigation_steps || {}, null, 2)}</pre>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(context?.sar_draft || {}, null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Cross-Tenant Signals and Model Metadata</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(context?.global_signals || [], null, 2)}</pre>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(context?.model_metadata || {}, null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Outcome Feedback</h2>
        {canRecordOutcome ? (
          <>
            <div className="flex gap-2 flex-wrap">
              <button className="px-3 py-1 border rounded" onClick={() => saveOutcome('true_positive')} disabled={actionBusy}>Mark TP</button>
              <button className="px-3 py-1 border rounded" onClick={() => saveOutcome('false_positive')} disabled={actionBusy}>Mark FP</button>
              <button className="px-3 py-1 border rounded" onClick={() => saveOutcome('escalated')} disabled={actionBusy}>Mark Escalated</button>
              <button className="px-3 py-1 border rounded" onClick={() => saveOutcome('sar_filed')} disabled={actionBusy}>Mark SAR Filed</button>
            </div>
            <input
              className="w-full border rounded px-3 py-2 text-sm"
              value={outcomeReason}
              onChange={(e) => setOutcomeReason(e.target.value)}
              placeholder="Outcome reason (optional)"
            />
          </>
        ) : (
          <p className="text-xs text-slate-500">Outcome recording is disabled for your current permission set.</p>
        )}
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(outcome || context?.outcome || {}, null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-3">
        <h2 className="font-semibold">Investigation Notes</h2>
        {canAddNotes ? (
          <div className="flex gap-2">
            <input
              className="flex-1 border rounded px-3 py-2"
              value={noteText}
              onChange={(e) => setNoteText(e.target.value)}
              placeholder="Add note"
            />
            <button className="px-3 py-2 border rounded" onClick={addNote}>Save</button>
          </div>
        ) : (
          <p className="text-xs text-slate-500">Your role cannot add notes.</p>
        )}
        {notes.map((n) => (
          <div key={n.id} className="text-sm border-t pt-2">
            <p>{n.note_text}</p>
            <p className="text-xs text-slate-500">{n.user_id} | {n.created_at}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
