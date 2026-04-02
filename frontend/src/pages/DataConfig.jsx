import { useState } from 'react'
import { api } from '../services/api'

export function DataConfig() {
  const [source, setSource] = useState('synthetic')
  const [loading, setLoading] = useState(false)
  const [msg, setMsg] = useState(null)
  const [file, setFile] = useState(null)
  const [nRows, setNRows] = useState(400)

  const runPipeline = async () => {
    setLoading(true)
    setMsg(null)
    try {
      const res = await api.runPipeline()
      if (res.job_id && res.status !== 'completed') {
        setMsg(`Pipeline job queued: ${res.job_id}. Waiting for completion...`)
        let final = res
        // Local development with full models can exceed 2 minutes for medium datasets.
        const maxWaitSeconds = 360
        for (let i = 0; i < maxWaitSeconds; i += 1) {
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => setTimeout(resolve, 1000))
          // eslint-disable-next-line no-await-in-loop
          final = await api.getPipelineJob(res.job_id)
          if (final.status === 'completed' || final.status === 'failed') break
        }
        if (final.status === 'failed') {
          throw new Error(final.detail || 'Pipeline failed')
        }
        if (final.status === 'discarded') {
          throw new Error(
            `Pipeline job was discarded (job: ${res.job_id}). Ensure API and workers use the same PostgreSQL/Redis connection settings.`
          )
        }
        if (final.status !== 'completed') {
          throw new Error(
            `Pipeline is still ${final.status || 'processing'} (job: ${res.job_id}). Check worker/redis and try refresh.`
          )
        }
        setMsg(`Pipeline complete. ${final.alerts ?? '--'} alerts generated.`)
        return final
      }
      setMsg(`Pipeline complete. ${res.alerts ?? res.row_count ?? '--'} alerts generated.`)
      return res
    } catch (e) {
      setMsg(`Error: ${e.message}`)
      throw e
    } finally {
      setLoading(false)
    }
  }

  const generateSynthetic = async () => {
    setLoading(true)
    setMsg(null)
    try {
      await api.generateSynthetic(nRows)
      await runPipeline()
    } catch (e) {
      setMsg(`Error: ${e.message}`)
    } finally {
      setLoading(false)
    }
  }

  const uploadCsv = async () => {
    if (!file) {
      setMsg('Select a file first.')
      return
    }
    setLoading(true)
    setMsg(null)
    try {
      if (source === 'alert_jsonl') {
        const res = await api.uploadAlertJsonl(file)
        setMsg(
          `Loaded ${file.name}. Run ${res.run_id}: ${res.success_count}/${res.total_alerts} alerts ingested (${res.failed_count} failed).`
        )
        return
      }

      if (source === 'bank') {
        await api.uploadBankCsv(file)
      } else {
        await api.uploadCsv(file)
      }
      setMsg(`Loaded ${file.name}.`)
      await runPipeline()
    } catch (e) {
      setMsg(`Error: ${e.message}`)
    } finally {
      setLoading(false)
    }
  }

  const clearRun = async () => {
    setLoading(true)
    try {
      await api.clearRun()
      setMsg('Run cleared.')
    } catch (e) {
      setMsg(`Error: ${e.message}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="max-w-[1200px] mx-auto">
      <h1 className="text-[1.375rem] font-medium mb-5 text-[var(--text)]">System & Data</h1>
      <div className="p-6 max-w-[560px] rounded-lg border border-[var(--border)] bg-[var(--surface)] shadow-md">
        <h3 className="mt-0 text-[0.9375rem] font-semibold mb-4">Data Source</h3>
        <div className="flex flex-col gap-2 my-3 mb-4">
          <label className="flex items-center gap-2 cursor-pointer text-sm text-[var(--text)]">
            <input
              type="radio"
              name="source"
              checked={source === 'synthetic'}
              onChange={() => {
                setSource('synthetic')
                setFile(null)
              }}
              className="accent-blue-500"
            />
            Synthetic
          </label>
          <label className="flex items-center gap-2 cursor-pointer text-sm text-[var(--text)]">
            <input
              type="radio"
              name="source"
              checked={source === 'csv'}
              onChange={() => {
                setSource('csv')
                setFile(null)
              }}
              className="accent-blue-500"
            />
            CSV Upload
          </label>
          <label className="flex items-center gap-2 cursor-pointer text-sm text-[var(--text)]">
            <input
              type="radio"
              name="source"
              checked={source === 'bank'}
              onChange={() => {
                setSource('bank')
                setFile(null)
              }}
              className="accent-blue-500"
            />
            Bank Alerts CSV
          </label>
          <label className="flex items-center gap-2 cursor-pointer text-sm text-[var(--text)]">
            <input
              type="radio"
              name="source"
              checked={source === 'alert_jsonl'}
              onChange={() => {
                setSource('alert_jsonl')
                setFile(null)
              }}
              className="accent-blue-500"
            />
            Alert JSONL
          </label>
        </div>
        <div className="mb-4 rounded-md border border-[var(--border)] bg-[var(--surface2)] px-4 py-3 text-sm text-[var(--muted)]">
          Pipeline execution now goes through a persisted job flow. Local runs can still complete inline, while enterprise deployments can switch to Redis/RQ workers without changing the UI contract.
        </div>
        {source === 'synthetic' && (
          <>
            <div className="flex items-center gap-4 my-3">
              <label className="text-sm text-[var(--text)]">Rows</label>
              <input
                type="number"
                min={100}
                max={5000}
                className="w-24 h-9 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md"
                value={nRows}
                onChange={(e) => setNRows(Number(e.target.value) || 400)}
              />
            </div>
            <button className="px-4 py-2 text-sm font-medium rounded-md bg-[var(--accent2)] text-white dark:bg-white dark:text-[#0c0c0c] mr-2 mb-2 disabled:opacity-60" onClick={generateSynthetic} disabled={loading}>
              {loading ? 'Running...' : 'Generate Demo Data & Run Pipeline'}
            </button>
          </>
        )}
        {(source === 'csv' || source === 'bank' || source === 'alert_jsonl') && (
          <>
            <input
              type="file"
              accept={source === 'alert_jsonl' ? '.jsonl,.json' : '.csv'}
              onChange={(e) => setFile(e.target.files?.[0])}
              className="my-3 text-sm text-[var(--muted)] block"
            />
            <button className="px-4 py-2 text-sm font-medium rounded-md bg-[var(--accent2)] text-white dark:bg-white dark:text-[#0c0c0c] mr-2 mb-2 disabled:opacity-60" onClick={uploadCsv} disabled={loading || !file}>
              {loading ? 'Running...' : 'Upload & Run Pipeline'}
            </button>
          </>
        )}
        <button className="px-4 py-2 text-sm font-medium rounded-md border border-[var(--border)] bg-transparent text-[var(--text)] mr-2 mb-2 disabled:opacity-60" onClick={clearRun} disabled={loading}>
          Clear active run
        </button>
      </div>
      {msg && <div className="mt-4 p-3 rounded-md border border-[var(--border)] bg-[var(--surface)] text-sm text-[var(--text)]">{msg}</div>}
    </div>
  )
}
