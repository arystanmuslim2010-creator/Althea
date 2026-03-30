import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { AlertDetails } from './AlertDetails'

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useParams: () => ({ id: 'A1' }),
    useNavigate: () => vi.fn(),
  }
})

vi.mock('../services/api', () => ({
  api: {
    getAlert: vi.fn(),
    getAlertExplain: vi.fn(),
    getAlertNotes: vi.fn(),
    getWorkQueue: vi.fn(),
    getInvestigationContext: vi.fn(),
    getNetworkGraph: vi.fn(),
    getNarrativeDraft: vi.fn(),
    getAlertOutcome: vi.fn(),
    addAlertNote: vi.fn(),
    createInvestigationCase: vi.fn(),
    workflowAssignAlert: vi.fn(),
    workflowEscalateAlert: vi.fn(),
    workflowCloseAlert: vi.fn(),
    recordAlertOutcome: vi.fn(),
  },
}))

vi.mock('../contexts/AuthContext', () => ({
  useAuth: () => ({ user: { user_id: 'u1', role: 'analyst' } }),
}))

describe('AlertDetails', () => {
  beforeEach(async () => {
    const { api } = await import('../services/api')
    api.getAlert.mockResolvedValue({ alert_id: 'A1', risk_score: 88, risk_band: 'high' })
    api.getAlertExplain.mockResolvedValue({ primary_drivers: ['rule_hit'] })
    api.getAlertNotes.mockResolvedValue({ notes: [] })
    api.getWorkQueue.mockResolvedValue({ queue: [{ alert_id: 'A1', status: 'open' }] })
    api.getInvestigationContext.mockResolvedValue({
      alert_id: 'A1',
      investigation_summary: { key_observations: ['test'] },
      risk_explanation: {},
      network_graph: {
        node_count: 2,
        edge_count: 1,
        nodes: [
          { id: 'A1', type: 'alert', label: 'Alert A1' },
          { id: 'customer:U1', type: 'customer', label: 'Customer U1' },
        ],
        edges: [{ source: 'A1', target: 'customer:U1', relation: 'user_id' }],
        risk_signals: ['customer_high_risk'],
      },
      investigation_steps: {},
      sar_draft: {},
      global_signals: [],
      model_metadata: { model_version: 'model-v1' },
    })
    api.getAlertOutcome.mockResolvedValue(null)
    api.getNetworkGraph.mockResolvedValue({
      alert_id: 'A1',
      nodes: [
        { id: 'alert:A1', type: 'alert', label: 'Alert A1', risk: 'high', meta: {} },
        { id: 'customer:U1', type: 'customer', label: 'Customer U1', risk: 'medium', meta: {} },
      ],
      edges: [{ source: 'alert:A1', target: 'customer:U1', type: 'associated_with', relation: 'associated_with', weight: 1, meta: {} }],
      summary: { node_count: 2, edge_count: 1, high_risk_nodes: 1 },
      node_count: 2,
      edge_count: 1,
      risk_signals: ['customer'],
    })
    api.getNarrativeDraft.mockResolvedValue({
      alert_id: 'A1',
      title: 'Investigation Narrative Draft',
      narrative: 'Draft narrative text.',
      sections: {
        activity_summary: 'Activity summary text.',
        risk_indicators: ['Indicator 1'],
        recommended_follow_up: ['Follow up 1'],
      },
      source_signals: { risk_score: 88, reason_codes: ['R1'], countries: ['US'] },
    })
  })

  it('loads and renders investigation intelligence sections', async () => {
    render(
      <MemoryRouter>
        <AlertDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Investigation Intelligence')).not.toBeNull())
    expect(screen.getByText('Network Graph')).not.toBeNull()
    expect(screen.getByText('Risk signals:')).not.toBeNull()
    expect(screen.getAllByText('Investigation Narrative Draft').length).toBeGreaterThan(0)
    expect(screen.getByText('Draft narrative text.')).not.toBeNull()
    expect(screen.getAllByText('customer').length).toBeGreaterThan(0)
    expect(screen.getByText('Outcome Feedback')).not.toBeNull()
  })

  it('keeps alert details page usable when graph and narrative endpoints fail', async () => {
    const { api } = await import('../services/api')
    api.getNetworkGraph.mockRejectedValueOnce(new Error('graph failed'))
    api.getNarrativeDraft.mockRejectedValueOnce(new Error('narrative failed'))

    render(
      <MemoryRouter>
        <AlertDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Alert Information')).not.toBeNull())
    expect(screen.getByText('Network Graph')).not.toBeNull()
    expect(screen.getAllByText('Investigation Narrative Draft').length).toBeGreaterThan(0)
    expect(screen.getByText('graph failed')).not.toBeNull()
    expect(screen.getByText('narrative failed')).not.toBeNull()
    expect(screen.getByText('Outcome Feedback')).not.toBeNull()
  })
})
