import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api } from '../services/api'

export function AnalystDashboard() {
  const navigate = useNavigate()
  const [queue, setQueue] = useState([])
  const [me, setMe] = useState(null)
  const [error, setError] = useState('')

  const load = async () => {
    try {
      const me = await api.me()
      setMe(me)
      const res = await api.getWorkQueue()
      const normalized = (res.queue || []).map((q) => ({ ...q, currentUserId: me.user_id }))
      setQueue(normalized)
    } catch (err) {
      setError(err.message || 'Failed to load queue')
      if ((err.message || '').toLowerCase().includes('unauthorized')) {
        api.clearToken()
        navigate('/login')
      }
    }
  }

  useEffect(() => {
    load()
  }, [])

  const assignToMe = async (alertId, userId) => {
    await api.assignAlert(alertId, userId)
    await load()
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Analyst Dashboard</h1>
        <button className="px-3 py-1 border rounded" onClick={() => { api.clearToken(); navigate('/login') }}>Logout</button>
      </div>
      {error && <p className="text-red-600 text-sm">{error}</p>}
      <div className="overflow-x-auto bg-white border rounded">
        <table className="w-full text-sm">
          <thead className="bg-slate-100">
            <tr>
              <th className="text-left p-2">Alert ID</th>
              <th className="text-left p-2">Priority</th>
              <th className="text-left p-2">Risk Score</th>
              <th className="text-left p-2">Assigned To</th>
              <th className="text-left p-2">Status</th>
              <th className="text-left p-2">Action</th>
            </tr>
          </thead>
          <tbody>
            {queue.map((item) => (
              <tr key={item.alert_id} className="border-t">
                <td className="p-2"><Link className="text-blue-600" to={`/investigation/alerts/${item.alert_id}`}>{item.alert_id}</Link></td>
                <td className="p-2">{item.priority}</td>
                <td className="p-2">{Number(item.risk_score || 0).toFixed(2)}</td>
                <td className="p-2">{item.assigned_to || 'Unassigned'}</td>
                <td className="p-2">{item.status}</td>
                <td className="p-2 flex gap-2">
                  {(me?.role === 'lead' || me?.role === 'admin') ? (
                    <button className="px-2 py-1 border rounded" onClick={() => assignToMe(item.alert_id, item.currentUserId)}>Assign to me</button>
                  ) : null}
                  {item.case_id ? <Link className="px-2 py-1 border rounded" to={`/investigation/cases/${item.case_id}`}>Case</Link> : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
