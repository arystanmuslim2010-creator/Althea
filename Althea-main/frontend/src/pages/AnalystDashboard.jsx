import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'

const ALERT_STATUSES = ['open', 'in_review', 'escalated', 'closed']

function canReassign(role) {
  return role === 'lead' || role === 'admin'
}

function canChangeStatus(role) {
  return role === 'analyst' || role === 'lead' || role === 'admin'
}

export function AnalystDashboard() {
  const navigate = useNavigate()
  const { user, logout } = useAuth()
  const [queue, setQueue] = useState([])
  const [error, setError] = useState('')
  const [statusEdits, setStatusEdits] = useState({})

  const load = async () => {
    try {
      const res = await api.getWorkQueue()
      setQueue(res.queue || [])
      const draft = {}
      ;(res.queue || []).forEach((item) => {
        draft[item.alert_id] = item.status || 'open'
      })
      setStatusEdits(draft)
      setError('')
    } catch (err) {
      const msg = err.message || 'Failed to load queue'
      setError(msg)
      if (msg.toLowerCase().includes('unauthorized')) {
        logout()
        navigate('/login')
      }
    }
  }

  useEffect(() => {
    load()
  }, [])

  const assignToMe = async (alertId) => {
    if (!user?.user_id) return
    await api.assignAlert(alertId, user.user_id)
    await load()
  }

  const saveStatus = async (alertId) => {
    const newStatus = statusEdits[alertId] || 'open'
    await api.updateAlertStatus(alertId, newStatus)
    await load()
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold">Investigation Dashboard</h1>
          <p className="text-sm text-slate-600">Role: {user?.role || '-'} | Team: {user?.team || '-'}</p>
        </div>
        <div className="flex gap-2">
          {(user?.role === 'analyst' || user?.role === 'manager' || user?.role === 'admin') ? (
            <Link className="px-3 py-1 border rounded" to="/ops">View Dashboards</Link>
          ) : null}
          {user?.role === 'admin' ? (
            <Link className="px-3 py-1 border rounded" to="/investigation/admin/users">Manage Users</Link>
          ) : null}
          <button className="px-3 py-1 border rounded" onClick={() => { logout(); navigate('/login') }}>Logout</button>
        </div>
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
                <td className="p-2">
                  <Link className="text-blue-600" to={`/investigation/alerts/${item.alert_id}`}>{item.alert_id}</Link>
                </td>
                <td className="p-2">{item.priority}</td>
                <td className="p-2">{Number(item.risk_score || 0).toFixed(2)}</td>
                <td className="p-2">{item.assigned_to || 'Unassigned'}</td>
                <td className="p-2">
                  {canChangeStatus(user?.role) ? (
                    <div className="flex items-center gap-2">
                      <select
                        className="border rounded px-2 py-1"
                        value={statusEdits[item.alert_id] || item.status || 'open'}
                        onChange={(e) => setStatusEdits((prev) => ({ ...prev, [item.alert_id]: e.target.value }))}
                      >
                        {ALERT_STATUSES.map((s) => (
                          <option key={s} value={s}>{s}</option>
                        ))}
                      </select>
                      <button className="px-2 py-1 border rounded" onClick={() => saveStatus(item.alert_id)}>Save</button>
                    </div>
                  ) : (
                    item.status
                  )}
                </td>
                <td className="p-2 flex gap-2">
                  {canReassign(user?.role) ? (
                    <button className="px-2 py-1 border rounded" onClick={() => assignToMe(item.alert_id)}>Assign to me</button>
                  ) : null}
                  {item.case_id ? (
                    <Link className="px-2 py-1 border rounded" to={`/investigation/cases/${item.case_id}`}>Case</Link>
                  ) : (
                    <span className="text-xs text-slate-500">No case</span>
                  )}
                </td>
              </tr>
            ))}
            {queue.length === 0 ? (
              <tr>
                <td className="p-3 text-slate-500" colSpan={6}>No alerts in queue for this role/team.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  )
}
