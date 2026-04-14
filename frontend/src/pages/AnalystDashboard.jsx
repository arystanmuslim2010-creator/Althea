import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'
import { hasPermission } from '../services/permissions'

const ALERT_STATUSES = ['open', 'in_review', 'escalated', 'closed']

export function AnalystDashboard() {
  const navigate = useNavigate()
  const { user, logout } = useAuth()
  const [queue, setQueue] = useState([])
  const [error, setError] = useState('')
  const [statusEdits, setStatusEdits] = useState({})
  const [selectedIds, setSelectedIds] = useState([])
  const [filters, setFilters] = useState({ search: '', status: 'all', assigned: 'all' })
  const [bulkStatus, setBulkStatus] = useState('in_review')
  const [actionBusy, setActionBusy] = useState(false)
  const canReassign = hasPermission(user, 'reassign_alerts')
  const canChangeStatus = hasPermission(user, 'change_alert_status')
  const canViewDashboards = hasPermission(user, 'view_dashboards')
  const canManageUsers = hasPermission(user, 'manage_users')

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

  const filteredQueue = useMemo(() => {
    return queue.filter((item) => {
      if (filters.status !== 'all' && (item.status || 'open') !== filters.status) return false
      if (filters.assigned === 'mine' && item.assigned_to !== user?.user_id) return false
      if (filters.assigned === 'unassigned' && item.assigned_to) return false
      if (filters.search) {
        const term = filters.search.toLowerCase()
        return `${item.alert_id} ${item.priority}`.toLowerCase().includes(term)
      }
      return true
    })
  }, [filters, queue, user?.user_id])

  const dashboardMetrics = useMemo(() => {
    const openCount = filteredQueue.filter((item) => item.status === 'open').length
    const escalatedCount = filteredQueue.filter((item) => item.status === 'escalated').length
    const avgRisk = filteredQueue.length
      ? filteredQueue.reduce((sum, item) => sum + Number(item.risk_score || 0), 0) / filteredQueue.length
      : 0
    const slaRisk = filteredQueue.filter((item) => Number(item.risk_score || 0) >= 80 && item.status !== 'closed').length
    return { openCount, escalatedCount, avgRisk, slaRisk }
  }, [filteredQueue])

  const assignToMe = async (alertId) => {
    if (!user?.user_id) return
    setActionBusy(true)
    try {
      await api.assignAlert(alertId, user.user_id)
      await load()
    } finally {
      setActionBusy(false)
    }
  }

  const saveStatus = async (alertId) => {
    const newStatus = statusEdits[alertId] || 'open'
    setActionBusy(true)
    try {
      await api.updateAlertStatus(alertId, newStatus)
      await load()
    } finally {
      setActionBusy(false)
    }
  }

  const toggleSelected = (alertId) => {
    setSelectedIds((current) => current.includes(alertId) ? current.filter((id) => id !== alertId) : [...current, alertId])
  }

  const toggleAll = () => {
    if (selectedIds.length === filteredQueue.length) {
      setSelectedIds([])
    } else {
      setSelectedIds(filteredQueue.map((item) => item.alert_id))
    }
  }

  const bulkAssignToMe = async () => {
    if (!user?.user_id || !selectedIds.length) return
    setActionBusy(true)
    try {
      await api.bulkAssignAlerts(selectedIds, user.user_id)
      setSelectedIds([])
      await load()
    } catch {
      await Promise.all(selectedIds.map((alertId) => api.assignAlert(alertId, user.user_id)))
      setSelectedIds([])
      await load()
    } finally {
      setActionBusy(false)
    }
  }

  const bulkUpdateStatus = async () => {
    if (!selectedIds.length) return
    setActionBusy(true)
    try {
      await api.bulkUpdateAlertStatus(selectedIds, bulkStatus)
      setSelectedIds([])
      await load()
    } catch {
      await Promise.all(selectedIds.map((alertId) => api.updateAlertStatus(alertId, bulkStatus)))
      setSelectedIds([])
      await load()
    } finally {
      setActionBusy(false)
    }
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h1 className="text-2xl font-semibold">Investigation Dashboard</h1>
          <p className="text-sm text-slate-600">Role: {user?.role || '-'} | Team: {user?.team || '-'}</p>
        </div>
        <div className="flex gap-2">
          {canViewDashboards ? (
            <Link className="px-3 py-1 border rounded" to="/ops">View Dashboards</Link>
          ) : null}
          {canManageUsers ? (
            <Link className="px-3 py-1 border rounded" to="/investigation/admin/users">Manage Users</Link>
          ) : null}
          <button className="px-3 py-1 border rounded" onClick={() => { logout(); navigate('/login') }}>Logout</button>
        </div>
      </div>

      {error && <p className="text-red-600 text-sm">{error}</p>}

      <div className="grid gap-3 md:grid-cols-4">
        <div className="rounded border bg-white p-4">
          <div className="text-xs uppercase text-slate-500">Open Queue</div>
          <div className="mt-2 text-2xl font-semibold">{dashboardMetrics.openCount}</div>
        </div>
        <div className="rounded border bg-white p-4">
          <div className="text-xs uppercase text-slate-500">Escalated</div>
          <div className="mt-2 text-2xl font-semibold">{dashboardMetrics.escalatedCount}</div>
        </div>
        <div className="rounded border bg-white p-4">
          <div className="text-xs uppercase text-slate-500">Average Risk</div>
          <div className="mt-2 text-2xl font-semibold">{dashboardMetrics.avgRisk.toFixed(1)}</div>
        </div>
        <div className="rounded border bg-white p-4">
          <div className="text-xs uppercase text-slate-500">SLA Attention</div>
          <div className="mt-2 text-2xl font-semibold">{dashboardMetrics.slaRisk}</div>
        </div>
      </div>

      <div className="rounded border bg-white p-4 space-y-3">
        <div className="flex flex-wrap gap-3">
          <input
            className="border rounded px-3 py-2 text-sm min-w-[220px]"
            placeholder="Search alert or priority"
            value={filters.search}
            onChange={(e) => setFilters((current) => ({ ...current, search: e.target.value }))}
          />
          <select className="border rounded px-3 py-2 text-sm" value={filters.status} onChange={(e) => setFilters((current) => ({ ...current, status: e.target.value }))}>
            <option value="all">All statuses</option>
            {ALERT_STATUSES.map((status) => <option key={status} value={status}>{status}</option>)}
          </select>
          <select className="border rounded px-3 py-2 text-sm" value={filters.assigned} onChange={(e) => setFilters((current) => ({ ...current, assigned: e.target.value }))}>
            <option value="all">All assignments</option>
            <option value="mine">My queue</option>
            <option value="unassigned">Unassigned</option>
          </select>
        </div>
        <div className="flex flex-wrap gap-3">
          {canReassign ? (
            <button className="px-3 py-2 border rounded text-sm disabled:opacity-50" disabled={!selectedIds.length || actionBusy} onClick={bulkAssignToMe}>
              Assign Selected To Me
            </button>
          ) : null}
          {canChangeStatus ? (
            <>
              <select className="border rounded px-3 py-2 text-sm" value={bulkStatus} onChange={(e) => setBulkStatus(e.target.value)}>
                {ALERT_STATUSES.map((status) => <option key={status} value={status}>{status}</option>)}
              </select>
              <button className="px-3 py-2 border rounded text-sm disabled:opacity-50" disabled={!selectedIds.length || actionBusy} onClick={bulkUpdateStatus}>
                Update Selected Status
              </button>
            </>
          ) : null}
        </div>
      </div>

      <div className="overflow-x-auto bg-white border rounded">
        <table className="w-full text-sm">
          <thead className="bg-slate-100">
            <tr>
              <th className="text-left p-2">
                <input type="checkbox" checked={selectedIds.length > 0 && selectedIds.length === filteredQueue.length} onChange={toggleAll} />
              </th>
              <th className="text-left p-2">Alert ID</th>
              <th className="text-left p-2">Priority</th>
              <th className="text-left p-2">Risk Score</th>
              <th className="text-left p-2">Assigned To</th>
              <th className="text-left p-2">Status</th>
              <th className="text-left p-2">Action</th>
            </tr>
          </thead>
          <tbody>
            {filteredQueue.map((item) => (
              <tr key={item.alert_id} className="border-t">
                <td className="p-2">
                  <input type="checkbox" checked={selectedIds.includes(item.alert_id)} onChange={() => toggleSelected(item.alert_id)} />
                </td>
                <td className="p-2">
                  <Link className="text-blue-600" to={`/investigation/alerts/${item.alert_id}`}>{item.alert_id}</Link>
                </td>
                <td className="p-2">{item.priority}</td>
                <td className="p-2">{Number(item.risk_score || 0).toFixed(2)}</td>
                <td className="p-2">{item.assigned_to || 'Unassigned'}</td>
                <td className="p-2">
                  {canChangeStatus ? (
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
                  {canReassign ? (
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
            {filteredQueue.length === 0 ? (
              <tr>
                <td className="p-3 text-slate-500" colSpan={7}>No alerts in queue for this role/team.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  )
}
