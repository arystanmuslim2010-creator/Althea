import { useState, useEffect } from 'react'
import { api } from '../services/api'
import { useAnalystCapacity } from '../contexts/AnalystCapacityContext'

export function OpsGovernance() {
  const [ops, setOps] = useState(null)
  const [health, setHealth] = useState(null)
  const { capacity, setCapacity, maxCapacity } = useAnalystCapacity()
  const [queue, setQueue] = useState([])
  const [slaBreachesApi, setSlaBreachesApi] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setError(null)
    Promise.all([
      api.getOpsMetrics(capacity),
      api.getHealth(),
      api.getWorkQueue(),
      api.getSlaBreaches().catch(() => ({ breaches: [] })),
    ])
      .then(([opsRes, healthRes, queueRes, slaRes]) => {
        setOps(opsRes)
        setHealth(healthRes)
        setQueue(queueRes?.queue || [])
        setSlaBreachesApi(slaRes?.breaches || [])
      })
      .catch((e) => {
        setOps(null)
        setHealth(null)
        setQueue([])
        setSlaBreachesApi([])
        setError(e?.message || 'Failed to load metrics')
      })
      .finally(() => setLoading(false))
  }, [capacity])

  const now = Date.now()
  const openQueue = queue.filter((item) => item.status !== 'closed')
  const highRiskOpen = openQueue.filter((item) => Number(item.risk_score || 0) >= 80)
  const slaBreaches = highRiskOpen.filter((item) => {
    const created = item.created_at ? new Date(item.created_at).getTime() : null
    if (!created || Number.isNaN(created)) return false
    return now - created > 24 * 60 * 60 * 1000
  })
  const investigatorStats = openQueue.reduce((acc, item) => {
    const assignee = item.assigned_to || 'Unassigned'
    if (!acc[assignee]) {
      acc[assignee] = { assignee, total: 0, escalated: 0, avgRisk: 0, _riskSum: 0 }
    }
    acc[assignee].total += 1
    if ((item.status || '').toLowerCase() === 'escalated') acc[assignee].escalated += 1
    acc[assignee]._riskSum += Number(item.risk_score || 0)
    acc[assignee].avgRisk = acc[assignee]._riskSum / acc[assignee].total
    return acc
  }, {})
  const investigatorRows = Object.values(investigatorStats)
    .map((row) => ({ ...row, avgRisk: Number(row.avgRisk || 0).toFixed(1) }))
    .sort((a, b) => b.total - a.total)

  if (loading) return <div className="max-w-[1200px] mx-auto"><div className="py-10 text-center text-[var(--muted)] text-[0.9375rem]">Loading...</div></div>

  return (
    <div className="max-w-[1200px] mx-auto">
      <h1 className="text-[1.375rem] font-medium mb-5 text-[var(--text)]">Operations & Governance</h1>
      {error && (
        <div className="mb-4 p-4 rounded-lg border border-red-500/25 bg-red-500/10 text-red-600 dark:text-red-400 text-sm">
          {error}
        </div>
      )}
      <section className="p-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-4">
        <h2 className="text-lg font-semibold mt-0 mb-4 text-[var(--text)]">Operational Metrics</h2>
        <div className="flex items-center gap-4 my-3">
          <label className="text-sm text-[var(--text)]">Analyst Capacity</label>
          <input
            type="range"
            min={1}
            max={maxCapacity}
            value={capacity}
            onChange={(e) => setCapacity(Number(e.target.value))}
            className="flex-1 max-w-[220px] h-1.5 accent-blue-500"
          />
          <span className="text-sm font-medium text-[var(--text)] min-w-8">{capacity}</span>
        </div>
        {ops && (
          <div className="flex flex-wrap gap-5 my-3">
            <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Precision@K</span><span className="font-semibold text-[0.9375rem]">{(ops.precision_k * 100).toFixed(1)}%</span></div>
            <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Alerts/Case</span><span className="font-semibold text-[0.9375rem]">{ops.alerts_per_case?.toFixed(1) ?? '-'}</span></div>
            <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Suppression Rate</span><span className="font-semibold text-[0.9375rem]">{((ops.suppression_rate || 0) * 100).toFixed(1)}%</span></div>
          </div>
        )}
      </section>
      <section className="p-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-4">
        <h2 className="text-lg font-semibold mt-0 mb-4 text-[var(--text)]">Model Health</h2>
        <div className="flex flex-col gap-0.5">
          <span className="text-[0.7rem] text-[var(--muted)] uppercase">Overall Health</span>
          <span className="font-semibold text-[0.9375rem]">{health?.status ?? 'N/A'}</span>
        </div>
      </section>
      <section className="p-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-4">
        <h2 className="text-lg font-semibold mt-0 mb-4 text-[var(--text)]">SLA Metrics</h2>
        <div className="flex flex-wrap gap-5 my-3">
          <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Open Queue</span><span className="font-semibold text-[0.9375rem]">{openQueue.length}</span></div>
          <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">High Risk Open</span><span className="font-semibold text-[0.9375rem]">{highRiskOpen.length}</span></div>
          <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">SLA Breaches (&gt;24h)</span><span className="font-semibold text-[0.9375rem]">{slaBreaches.length}</span></div>
          <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Workflow SLA Breaches</span><span className="font-semibold text-[0.9375rem]">{slaBreachesApi.length}</span></div>
        </div>
      </section>
      <section className="p-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md mb-4">
        <h2 className="text-lg font-semibold mt-0 mb-4 text-[var(--text)]">Investigator Analytics</h2>
        {investigatorRows.length === 0 ? (
          <p className="text-sm text-[var(--muted)]">No active investigation assignments available.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">Investigator</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">Open Alerts</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">Escalated</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">Avg Risk</th>
                </tr>
              </thead>
              <tbody>
                {investigatorRows.map((row) => (
                  <tr key={row.assignee} className="border-b border-[var(--border)]">
                    <td className="py-2 px-3 text-[0.8125rem]">{row.assignee}</td>
                    <td className="py-2 px-3 text-[0.8125rem]">{row.total}</td>
                    <td className="py-2 px-3 text-[0.8125rem]">{row.escalated}</td>
                    <td className="py-2 px-3 text-[0.8125rem]">{row.avgRisk}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}
