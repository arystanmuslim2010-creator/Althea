import { describe, expect, it, vi, beforeEach } from 'vitest'
import { api } from './api'

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('api client contracts', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
    if (globalThis.localStorage && typeof globalThis.localStorage.clear === 'function') {
      globalThis.localStorage.clear()
    }
  })

  it('maps legacy case statuses to backend workflow-safe statuses', async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ case_id: 'CASE_1', status: 'closed' }))
    vi.stubGlobal('fetch', fetchMock)

    await api.updateCase('CASE_1', { status: 'CLOSED_FP' })

    const [, options] = fetchMock.mock.calls[0]
    const body = JSON.parse(options.body)
    expect(body.status).toBe('closed')
  })

  it('normalizes health payload to include status', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => jsonResponse({ ok: true, checks: { database: true }, queue_depth: 1 })))
    const payload = await api.getHealth()
    expect(payload.status).toBe('healthy')
    expect(payload.ok).toBe(true)
  })

  it('refreshes access token on 401 and retries request', async () => {
    api.setTokens('expired-access', 'valid-refresh')
    const fetchMock = vi.fn(async (url) => {
      if (String(url).endsWith('/auth/refresh')) {
        return jsonResponse({ access_token: 'new-access', refresh_token: 'new-refresh' })
      }
      const auth = (fetchMock.mock.calls.at(-1)?.[1]?.headers || {}).Authorization
      if (auth === 'Bearer expired-access') {
        return jsonResponse({ detail: 'Unauthorized' }, 401)
      }
      return jsonResponse({ user_id: 'u1' })
    })
    vi.stubGlobal('fetch', fetchMock)

    const me = await api.me()
    expect(me.user_id).toBe('u1')
    expect(api.getAccessToken()).toBe('new-access')
    expect(api.getRefreshToken()).toBe('new-refresh')
  })

  it('calls network graph endpoint and returns normalized payload', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse({
          alert_id: 'A1',
          nodes: [{ id: 'n1', label: 'Node', type: 'customer', meta: { x: 1 } }],
          edges: [{ source: 'n1', target: 'n2', relation: 'transaction' }],
        }),
      ),
    )

    const graph = await api.getNetworkGraph('A1')
    expect(graph.alert_id).toBe('A1')
    expect(graph.nodes[0].meta.x).toBe(1)
    expect(graph.edges[0].type).toBe('transaction')
  })

  it('calls narrative draft endpoint and returns normalized payload', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        jsonResponse({
          alert_id: 'A1',
          title: 'Investigation Narrative Draft',
          narrative: 'Draft text',
          sections: { activity_summary: 'summary' },
          source_signals: { reason_codes: ['R1'] },
        }),
      ),
    )

    const draft = await api.getNarrativeDraft('A1')
    expect(draft.alert_id).toBe('A1')
    expect(draft.title).toBe('Investigation Narrative Draft')
    expect(draft.sections.activity_summary).toBe('summary')
    expect(draft.source_signals.reason_codes).toEqual(['R1'])
  })
})
