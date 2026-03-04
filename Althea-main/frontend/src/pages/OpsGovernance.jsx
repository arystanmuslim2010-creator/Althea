import { useState, useEffect } from 'react'
import { api } from '../services/api'

export function OpsGovernance() {
  const [ops, setOps] = useState(null)
  const [health, setHealth] = useState(null)
  const [capacity, setCapacity] = useState(50)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    setError(null)
    Promise.all([
      api.getOpsMetrics(capacity),
      api.getHealth(),
    ])
      .then(([opsRes, healthRes]) => {
        setOps(opsRes)
        setHealth(healthRes)
      })
      .catch((e) => {
        setOps(null)
        setHealth(null)
        setError(e?.message || 'Failed to load metrics')
      })
      .finally(() => setLoading(false))
  }, [capacity])

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
            max={500}
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
    </div>
  )
}
