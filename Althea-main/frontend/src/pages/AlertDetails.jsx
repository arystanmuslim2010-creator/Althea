import { useEffect, useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../services/api'

export function AlertDetails() {
  const { id } = useParams()
  const [alert, setAlert] = useState(null)
  const [explain, setExplain] = useState(null)
  const [notes, setNotes] = useState([])
  const [noteText, setNoteText] = useState('')
  const [caseInfo, setCaseInfo] = useState(null)
  const [error, setError] = useState('')

  const load = async () => {
    try {
      const [a, e, n] = await Promise.all([
        api.getAlert(id),
        api.getAlertExplain(id),
        api.getAlertNotes(id),
      ])
      setAlert(a)
      setExplain(e)
      setNotes(n.notes || [])
    } catch (err) {
      setError(err.message || 'Failed to load alert')
    }
  }

  useEffect(() => {
    load()
  }, [id])

  const riskExplain = useMemo(() => {
    if (!alert) return null
    try {
      return typeof alert.risk_explain_json === 'string' ? JSON.parse(alert.risk_explain_json) : alert.risk_explain_json
    } catch {
      return null
    }
  }, [alert])

  const addNote = async () => {
    if (!noteText.trim()) return
    await api.addAlertNote(id, noteText.trim())
    setNoteText('')
    await load()
  }

  const createCase = async () => {
    const c = await api.createInvestigationCase(id)
    setCaseInfo(c)
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between">
        <h1 className="text-2xl font-semibold">Alert Details: {id}</h1>
        <Link to="/investigation/dashboard" className="px-3 py-1 border rounded">Back</Link>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      <div className="grid md:grid-cols-2 gap-4">
        <div className="border rounded p-4 bg-white space-y-1">
          <h2 className="font-semibold">Alert Data</h2>
          <p>Priority: {alert?.priority || '-'}</p>
          <p>Risk Score: {alert?.risk_score ?? '-'}</p>
          <p>Status: {alert?.governance_status || '-'}</p>
        </div>
        <div className="border rounded p-4 bg-white space-y-1">
          <h2 className="font-semibold">Case</h2>
          <button className="px-3 py-1 border rounded" onClick={createCase}>Create Case</button>
          {caseInfo?.case_id && <Link className="ml-3 text-blue-600" to={`/investigation/cases/${caseInfo.case_id}`}>{caseInfo.case_id}</Link>}
        </div>
      </div>
      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Risk Explanation</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(explain || riskExplain || {}, null, 2)}</pre>
      </div>
      <div className="border rounded p-4 bg-white space-y-2">
        <h2 className="font-semibold">Feature Contributions / Rule Hits</h2>
        <pre className="text-xs whitespace-pre-wrap">{JSON.stringify({ top_features: alert?.top_features, rules: alert?.rules_json }, null, 2)}</pre>
      </div>
      <div className="border rounded p-4 bg-white space-y-3">
        <h2 className="font-semibold">Investigation Notes</h2>
        <div className="flex gap-2">
          <input className="flex-1 border rounded px-3 py-2" value={noteText} onChange={(e) => setNoteText(e.target.value)} placeholder="Add note" />
          <button className="px-3 py-2 border rounded" onClick={addNote}>Save</button>
        </div>
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
