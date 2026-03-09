import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'

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
  const [error, setError] = useState('')

  const canAddNotes = user?.role === 'analyst' || user?.role === 'investigator' || user?.role === 'lead' || user?.role === 'admin'
  const canCreateCase = canAddNotes

  const load = async () => {
    try {
      const [a, e, n, q] = await Promise.all([
        api.getAlert(id),
        api.getAlertExplain(id),
        api.getAlertNotes(id),
        api.getWorkQueue(),
      ])
      setAlert(a)
      setExplain(e)
      setNotes(n.notes || [])
      const matched = (q.queue || []).find((item) => String(item.alert_id) === String(id)) || null
      setQueueItem(matched)
      setCaseInfo(matched?.case_id ? { case_id: matched.case_id, status: matched.case_status } : null)
      setError('')
    } catch (err) {
      setError(err.message || 'Failed to load alert')
    }
  }

  useEffect(() => {
    load()
  }, [id])

  const riskExplain = useMemo(() => tryParseJson(alert?.risk_explain_json, {}), [alert])
  const featuresJson = useMemo(() => tryParseJson(alert?.top_feature_contributions_json, []), [alert])
  const rulesJson = useMemo(() => tryParseJson(alert?.rules_json, []), [alert])

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
      const c = await api.createInvestigationCase(id)
      setCaseInfo(c)
      await load()
    } catch (err) {
      setError(err.message || 'Failed to create case')
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

      <div className="grid md:grid-cols-2 gap-4">
        <div className="border rounded p-4 bg-white space-y-1">
          <h2 className="font-semibold">Alert Information</h2>
          <p>Alert ID: {alert?.alert_id || id}</p>
          <p>Priority: {alert?.priority || alert?.risk_band || '-'}</p>
          <p>Risk Score: {alert?.risk_score ?? '-'}</p>
          <p>Assigned To: {queueItem?.assigned_to || 'Unassigned'}</p>
          <p>Status: {queueItem?.status || 'open'}</p>
        </div>
        <div className="border rounded p-4 bg-white space-y-2">
          <h2 className="font-semibold">Case Status</h2>
          <p>Case ID: {caseInfo?.case_id || 'No case'}</p>
          <p>Case Status: {caseInfo?.status || queueItem?.case_status || '-'}</p>
          {caseInfo?.case_id ? (
            <Link className="text-blue-600" to={`/investigation/cases/${caseInfo.case_id}`}>Open Case</Link>
          ) : canCreateCase ? (
            <button className="px-3 py-1 border rounded" onClick={createCase}>Create Case</button>
          ) : (
            <span className="text-xs text-slate-500">Not permitted for your role.</span>
          )}
        </div>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Risk Explanation</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(explain || riskExplain || {}, null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Feature Contributions</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(featuresJson || alert?.top_features || [], null, 2)}</pre>
      </div>

      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Rule Signals</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(rulesJson, null, 2)}</pre>
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
