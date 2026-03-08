import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api } from '../services/api'

const STATUSES = ['open', 'under_review', 'escalated', 'sar_filed', 'closed']

export function CaseDetails() {
  const { id } = useParams()
  const [data, setData] = useState(null)
  const [status, setStatus] = useState('open')
  const [error, setError] = useState('')

  const load = async () => {
    try {
      const res = await api.getInvestigationCase(id)
      setData(res)
      setStatus(res?.case?.status || 'open')
    } catch (err) {
      setError(err.message || 'Failed to load case')
    }
  }

  useEffect(() => {
    load()
  }, [id])

  const saveStatus = async () => {
    await api.updateInvestigationCaseStatus(id, status)
    await load()
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex justify-between">
        <h1 className="text-2xl font-semibold">Case {id}</h1>
        <Link to="/investigation/dashboard" className="px-3 py-1 border rounded">Back</Link>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      <div className="border rounded p-4 bg-white space-y-2">
        <p>Alert: <Link className="text-blue-600" to={`/investigation/alerts/${data?.case?.alert_id}`}>{data?.case?.alert_id}</Link></p>
        <p>Created by: {data?.case?.created_by}</p>
        <p>Created at: {data?.case?.created_at}</p>
        <p>Current status: {data?.case?.status}</p>
        <div className="flex gap-2 items-center">
          <select className="border rounded px-2 py-1" value={status} onChange={(e) => setStatus(e.target.value)}>
            {STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
          <button className="px-3 py-1 border rounded" onClick={saveStatus}>Update status</button>
        </div>
      </div>
      <div className="border rounded p-4 bg-white">
        <h2 className="font-semibold mb-2">Timeline</h2>
        {(data?.timeline || []).map((log) => (
          <div className="text-sm border-t py-2" key={log.id}>
            <div>{log.action}</div>
            <div className="text-xs text-slate-500">{log.performed_by} | {log.timestamp}</div>
          </div>
        ))}
      </div>
    </div>
  )
}
