/**
 * API client with access+refresh token lifecycle.
 */
const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '')
const API_CANDIDATES = API_BASE
  ? [`${API_BASE}/api`, '/api', 'http://127.0.0.1:8000/api', 'http://localhost:8000/api']
  : ['/api', 'http://127.0.0.1:8000/api', 'http://localhost:8000/api']
const ACCESS_TOKEN_KEY = 'althea_auth_token'
const REFRESH_TOKEN_KEY = 'althea_refresh_token'
const REQUEST_TIMEOUT_MS = 10000

let refreshInFlight = null

export const CONNECTION_ERROR_MESSAGE = 'Cannot connect to backend service. Please try again.'

export function isConnectionError(message) {
  if (!message || typeof message !== 'string') return false
  return /failed to fetch|networkerror|connection refused|err_connection|econnrefused|load failed|network request failed/i.test(
    message
  )
}

function withTimeout() {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  return {
    signal: controller.signal,
    done: () => clearTimeout(timeoutId),
  }
}

async function parseResponse(res) {
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail || res.statusText)
  }
  if (res.status === 204 || res.headers.get('content-length') === '0') return {}
  const text = await res.text()
  if (!text) return {}
  try {
    return JSON.parse(text)
  } catch {
    throw new Error('Backend returned an invalid response.')
  }
}

function getAccessToken() {
  return localStorage.getItem(ACCESS_TOKEN_KEY)
}

function getRefreshToken() {
  return localStorage.getItem(REFRESH_TOKEN_KEY)
}

function setTokens(accessToken, refreshToken) {
  if (accessToken) localStorage.setItem(ACCESS_TOKEN_KEY, accessToken)
  if (refreshToken) localStorage.setItem(REFRESH_TOKEN_KEY, refreshToken)
}

function clearTokens() {
  localStorage.removeItem(ACCESS_TOKEN_KEY)
  localStorage.removeItem(REFRESH_TOKEN_KEY)
}

async function refreshAccessToken(apiBase) {
  if (refreshInFlight) return refreshInFlight

  const refreshToken = getRefreshToken()
  if (!refreshToken) return false

  refreshInFlight = (async () => {
    const timeout = withTimeout()
    try {
      const res = await fetch(`${apiBase}/auth/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: refreshToken }),
        signal: timeout.signal,
      })
      if (!res.ok) {
        clearTokens()
        return false
      }
      const payload = await parseResponse(res)
      setTokens(payload.access_token, payload.refresh_token)
      return Boolean(payload.access_token)
    } catch {
      clearTokens()
      return false
    } finally {
      timeout.done()
      refreshInFlight = null
    }
  })()

  return refreshInFlight
}

async function runFetchWithLifecycle(apiBase, path, options, allowRefresh = true) {
  let res = await fetch(`${apiBase}${path}`, options)
  if (res.status !== 401 || !allowRefresh) return res

  const refreshed = await refreshAccessToken(apiBase)
  if (!refreshed) return res

  const retriedHeaders = { ...(options.headers || {}) }
  const token = getAccessToken()
  if (token) retriedHeaders.Authorization = `Bearer ${token}`
  res = await fetch(`${apiBase}${path}`, { ...options, headers: retriedHeaders })
  return res
}

async function req(method, path, body = null, allowRefresh = true) {
  const token = getAccessToken()
  const headers = token ? { Authorization: `Bearer ${token}` } : {}
  if (body !== null && body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }

  const options = {
    method,
    headers,
    body: body !== null && body !== undefined ? JSON.stringify(body) : undefined,
  }

  let lastNetworkError = null
  for (const apiBase of API_CANDIDATES) {
    const timeout = withTimeout()
    try {
      const res = await runFetchWithLifecycle(apiBase, path, { ...options, signal: timeout.signal }, allowRefresh)
      return await parseResponse(res)
    } catch (e) {
      const msg = (e && e.message) || ''
      if (e?.name === 'AbortError' || isConnectionError(msg)) {
        lastNetworkError = e
        continue
      }
      throw new Error(msg || 'Network error')
    } finally {
      timeout.done()
    }
  }

  if (lastNetworkError) throw new Error(CONNECTION_ERROR_MESSAGE)
  throw new Error(CONNECTION_ERROR_MESSAGE)
}

async function reqForm(path, formData) {
  const token = getAccessToken()
  const headers = token ? { Authorization: `Bearer ${token}` } : undefined
  let lastNetworkError = null

  for (const apiBase of API_CANDIDATES) {
    const timeout = withTimeout()
    try {
      const res = await runFetchWithLifecycle(
        apiBase,
        path,
        { method: 'POST', body: formData, headers, signal: timeout.signal },
        true
      )
      return await parseResponse(res)
    } catch (e) {
      const msg = (e && e.message) || ''
      if (e?.name === 'AbortError' || isConnectionError(msg)) {
        lastNetworkError = e
        continue
      }
      throw new Error(msg || 'Network error')
    } finally {
      timeout.done()
    }
  }

  if (lastNetworkError) throw new Error(CONNECTION_ERROR_MESSAGE)
  throw new Error(CONNECTION_ERROR_MESSAGE)
}

export const api = {
  setToken: (token) => setTokens(token, null),
  getToken: () => getAccessToken(),
  clearToken: () => clearTokens(),
  setTokens: (accessToken, refreshToken) => setTokens(accessToken, refreshToken),
  getAccessToken: () => getAccessToken(),
  getRefreshToken: () => getRefreshToken(),
  clearTokens: () => clearTokens(),
  refresh: () => req('POST', '/auth/refresh', { refresh_token: getRefreshToken() }, false),
  register: (payload) => req('POST', '/auth/register', payload, false),
  login: (payload) => req('POST', '/auth/login', payload, false),
  me: () => req('GET', '/auth/me'),
  getWorkQueue: () => req('GET', '/work/queue'),
  assignAlert: (alertId, assignedTo) => req('POST', `/alerts/${alertId}/assign`, { assigned_to: assignedTo }),
  updateAlertStatus: (alertId, status) => req('POST', `/alerts/${alertId}/status`, { status }),
  addAlertNote: (alertId, noteText) => req('POST', `/alerts/${alertId}/note`, { note_text: noteText }),
  getAlertNotes: (alertId) => req('GET', `/alerts/${alertId}/notes`),
  createInvestigationCase: (alertId) => req('POST', '/cases/create', { alert_id: alertId }),
  getInvestigationCase: (caseId) => req('GET', `/cases/${caseId}`),
  updateInvestigationCaseStatus: (caseId, status) => req('POST', `/cases/${caseId}/status`, { status }),
  getAdminUsers: () => req('GET', '/admin/users'),
  updateUserRole: (userId, role) => req('POST', `/admin/users/${userId}/role`, { role }),
  getHealth: () => req('GET', '/health'),
  getRunInfo: () => req('GET', '/run-info'),
  getQueueMetrics: () => req('GET', '/queue-metrics'),
  getAlerts: (params) => {
    const clean = Object.fromEntries(Object.entries(params || {}).filter(([, v]) => v !== undefined && v !== null))
    const q = new URLSearchParams(clean).toString()
    return req('GET', q ? `/alerts?${q}` : '/alerts')
  },
  getAlert: (id) => req('GET', `/alerts/${id}`),
  getAlertExplain: (id) => req('GET', `/alerts/${id}/explain`),
  getAiSummary: (alertId) => req('GET', `/alerts/${alertId}/ai-summary`),
  generateAiSummary: (alertId) => req('POST', `/alerts/${alertId}/ai-summary`),
  clearAiSummary: (alertId) => req('DELETE', `/alerts/${alertId}/ai-summary`),
  getRuns: () => req('GET', '/runs'),
  getCases: () => req('GET', '/cases'),
  getCaseAudit: (caseId) => req('GET', `/cases/${caseId}/audit`),
  createCase: (alertIds, actor = 'Analyst_1') => req('POST', '/cases', { alert_ids: alertIds, actor }),
  updateCase: (caseId, payload) => req('PUT', `/cases/${caseId}`, payload),
  deleteCase: (caseId) => req('DELETE', `/cases/${caseId}`),
  getActor: () => req('GET', '/actor'),
  setActor: (actor) => req('PUT', '/actor', { actor }),
  getOpsMetrics: (cap = 50) => req('GET', `/ops-metrics?analyst_capacity=${cap}`),
  generateSynthetic: (n = 400) => req('POST', `/data/generate-synthetic?n_rows=${n}`),
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
  getPipelineJob: (jobId) => req('GET', `/pipeline/jobs/${jobId}`),
  clearRun: () => req('POST', '/pipeline/clear'),
}
