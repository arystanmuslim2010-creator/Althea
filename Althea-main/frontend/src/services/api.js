/**
 * Single API client for the app. All backend requests go through this module.
 * Base URL: VITE_API_BASE_URL (e.g. https://your-app.onrender.com). No hardcoded hosts or ports.
 * When empty, requests use relative /api (Vite proxy or same-origin).
 * Optional debug: set VITE_DEBUG=true to enable any future request logging.
 */
const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '')
const API = API_BASE ? `${API_BASE}/api` : '/api'

/** Generic message when the service is unreachable (no port or server instructions). */
export const CONNECTION_ERROR_MESSAGE = 'Cannot connect to backend service. Please try again.'

/** Returns true if the error message indicates a connection/network failure. */
export function isConnectionError(message) {
  if (!message || typeof message !== 'string') return false
  return /failed to fetch|networkerror|connection refused|err_connection|not found|internal server error/i.test(message)
}

async function parseResponse(res) {
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail || res.statusText)
  }
  if (res.status === 204 || res.headers.get('content-length') === '0') return {}
  const text = await res.text()
  return text ? JSON.parse(text) : {}
}

async function req(method, path, body = null) {
  const opts = { method }
  if (body !== null && body !== undefined) {
    opts.headers = { 'Content-Type': 'application/json' }
    opts.body = JSON.stringify(body)
  }
  let res
  try {
    res = await fetch(`${API}${path}`, opts)
  } catch (e) {
    const msg = (e && e.message) || ''
    if (isConnectionError(msg)) throw new Error(CONNECTION_ERROR_MESSAGE)
    throw new Error(msg || 'Network error')
  }
  return parseResponse(res)
}

async function reqForm(path, formData) {
  let res
  try {
    res = await fetch(`${API}${path}`, { method: 'POST', body: formData })
  } catch (e) {
    const msg = (e && e.message) || ''
    if (isConnectionError(msg)) throw new Error(CONNECTION_ERROR_MESSAGE)
    throw new Error(msg || 'Network error')
  }
  return parseResponse(res)
}

/** API client. Endpoints match backend OpenAPI: /api/health, /api/run-info, /api/alerts, etc. */
export const api = {
  getHealth: () => req('GET', '/health'),
  getRunInfo: () => req('GET', '/run-info'),
  getQueueMetrics: () => req('GET', '/queue-metrics'),
  getAlerts: (params) => {
    const clean = Object.fromEntries(
      Object.entries(params || {}).filter(([, v]) => v !== undefined && v !== null)
    )
    const q = new URLSearchParams(clean).toString()
    return req('GET', q ? `/alerts?${q}` : '/alerts')
  },
  getAlert: (id) => req('GET', `/alerts/${id}`),
  getAiSummary: (alertId) => req('GET', `/alerts/${alertId}/ai-summary`),
  generateAiSummary: (alertId) => req('POST', `/alerts/${alertId}/ai-summary`),
  clearAiSummary: (alertId) => req('DELETE', `/alerts/${alertId}/ai-summary`),
  getRuns: () => req('GET', '/runs'),
  getCases: () => req('GET', '/cases'),
  getCaseAudit: (caseId) => req('GET', `/cases/${caseId}/audit`),
  createCase: (alertIds, actor = 'Analyst_1') =>
    req('POST', '/cases', { alert_ids: alertIds, actor }),
  updateCase: (caseId, payload) => req('PUT', `/cases/${caseId}`, payload),
  deleteCase: (caseId) => req('DELETE', `/cases/${caseId}`),
  getActor: () => req('GET', '/actor'),
  setActor: (actor) => req('PUT', '/actor', { actor }),
  getOpsMetrics: (cap = 50) => req('GET', `/ops-metrics?analyst_capacity=${cap}`),
  generateSynthetic: (n = 400) =>
    req('POST', `/data/generate-synthetic?n_rows=${n}`),
  uploadCsv: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    return reqForm('/data/upload-csv', fd)
  },
  uploadBankCsv: async (file) => {
    const fd = new FormData()
    fd.append('file', file)
    return reqForm('/data/upload-bank-csv', fd)
  },
  runPipeline: () => req('POST', '/pipeline/run'),
  clearRun: () => req('POST', '/pipeline/clear'),
}
