import { describe, expect, it, vi, beforeEach } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { AlertDetails } from './AlertDetails'

let mockUser = {
  user_id: 'u1',
  role: 'analyst',
  permissions: ['add_investigation_notes', 'work_cases', 'change_alert_status', 'reassign_alerts'],
}

let mockLocationState = {}

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useParams: () => ({ id: 'A1' }),
    useNavigate: () => vi.fn(),
    useLocation: () => ({ state: mockLocationState }),
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
  useAuth: () => ({ user: mockUser }),
}))

describe('AlertDetails', () => {
  beforeEach(async () => {
    mockUser = {
      user_id: 'u1',
      role: 'analyst',
      permissions: ['add_investigation_notes', 'work_cases', 'change_alert_status', 'reassign_alerts'],
    }
    mockLocationState = {}

    const { api } = await import('../services/api')
    Object.values(api).forEach((fn) => fn.mockReset?.())

    api.getAlert.mockResolvedValue({
      alert_id: 'A1',
      risk_score: 88,
      risk_band: 'high',
      typology: 'structuring',
      segment: 'retail',
      model_version: 'model-v1',
      risk_explain_json: JSON.stringify({
        feature_attribution: [{ feature: 'amount', value: 0.7, shap_value: 0.7 }],
        explanation_method: 'shap',
        explanation_status: 'ok',
        risk_reason_codes: ['R1'],
      }),
      rules_json: JSON.stringify([{ rule_id: 'RULE-1', description: 'Large cash movement' }]),
    })
    api.getAlertExplain.mockResolvedValue({
      risk_explanation: {
        feature_attribution: [{ feature: 'amount', value: 0.7, shap_value: 0.7 }],
        explanation_method: 'shap',
        explanation_status: 'ok',
        risk_reason_codes: ['R1'],
      },
      human_interpretation_view: {
        headline: 'Alert drivers indicate elevated structuring risk that requires analyst review.',
        reasons: ['Rapid increase in transaction amount', 'Triggered structuring rule set'],
        patterns: ['Potential structuring'],
      },
      analyst_focus_points: ['Validate the source of funds.'],
    })
    api.getAlertNotes.mockResolvedValue({ notes: [] })
    api.getWorkQueue.mockResolvedValue({
      queue: [{
        alert_id: 'A1',
        status: 'open',
        assigned_to: 'u1',
        alert_age_hours: 6,
        overdue_review: false,
        case_id: 'CASE-1',
        case_status: 'under_review',
      }],
    })
    api.getInvestigationContext.mockResolvedValue({
      alert_id: 'A1',
      investigation_summary: {
        customer: 'Customer U1',
        key_observations: ['High risk score requires priority review', 'Triggered structuring rule set'],
      },
      risk_explanation: {
        feature_attribution: [{ feature: 'amount', value: 0.7, shap_value: 0.7 }],
        explanation_method: 'shap',
        explanation_status: 'ok',
      },
      network_graph: {
        node_count: 2,
        edge_count: 1,
        nodes: [
          { id: 'alert:A1', type: 'alert', label: 'Alert A1' },
          { id: 'customer:U1', type: 'customer', label: 'Customer U1' },
        ],
        edges: [{ source: 'alert:A1', target: 'customer:U1', relation: 'associated_with' }],
        risk_signals: ['customer'],
      },
      investigation_steps: {
        steps: [
          { step: 1, description: 'Review linked transactions over the last 90 days.' },
          { step: 2, description: 'Validate the source of funds.' },
        ],
      },
      sar_draft: {
        narrative: 'Preliminary draft text for compliance review only.',
        risk_indicators: ['Risk score above threshold'],
        disclaimer: 'This is a preliminary system-generated draft for analyst support only.',
      },
      narrative_draft: {
        alert_id: 'A1',
        title: 'Investigation Narrative Draft',
        narrative: 'Draft narrative text.',
        sections: {
          activity_summary: 'Customer U1 moved funds to a linked destination account.',
          risk_indicators: ['High-risk typology signal'],
          recommended_follow_up: ['Validate counterparties'],
        },
        source_signals: { risk_score: 88, reason_codes: ['R1'], countries: ['US'] },
      },
      global_signals: [{ signal_type: 'device_fingerprint', description: 'Device fingerprint seen across peer institutions', tenant_count: 3, alert_count: 7 }],
      outcome: null,
      case_status: { case_id: 'CASE-1', status: 'under_review', assigned_to: 'u1' },
      model_metadata: { model_version: 'model-v1', approval_state: 'approved' },
    })
    api.getAlertOutcome.mockResolvedValue(null)
    api.getNetworkGraph.mockResolvedValue({
      alert_id: 'A1',
      nodes: [{ id: 'alert:A1', type: 'alert', label: 'Alert A1' }],
      edges: [],
      summary: { node_count: 1, edge_count: 0, high_risk_nodes: 1 },
      risk_signals: [],
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

  it('renders a cleaned post-case workflow and hides raw payloads by default', async () => {
    const { api } = await import('../services/api')
    mockLocationState = { created_case_id: 'CASE-1' }

    render(
      <MemoryRouter>
        <AlertDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Analyst Summary')).not.toBeNull())
    expect(screen.getByText('Case CASE-1 created and ready for investigation.')).not.toBeNull()
    expect(screen.getByText('View Case')).not.toBeNull()
    expect(screen.queryByText('Create Case')).toBeNull()
    expect(screen.getByText('Recommended Next Steps')).not.toBeNull()
    expect(screen.getByText('Narrative Draft')).not.toBeNull()
    expect(screen.getByText('Preliminary SAR/STR Support Draft')).not.toBeNull()
    expect(screen.queryByText('Some investigation context is temporarily unavailable.')).toBeNull()
    expect(screen.queryByText('"feature_attribution"')).toBeNull()

    fireEvent.click(screen.getByText('View raw explanation'))
    expect(screen.getByText((content) => content.includes('feature_attribution'))).not.toBeNull()
    expect(api.getNetworkGraph).not.toHaveBeenCalled()
    expect(api.getNarrativeDraft).not.toHaveBeenCalled()
  })

  it('shows scoped fallback messages when graph and narrative data remain unavailable', async () => {
    const { api } = await import('../services/api')
    api.getInvestigationContext.mockResolvedValueOnce({
      alert_id: 'A1',
      investigation_summary: { key_observations: ['High risk score requires priority review'] },
      risk_explanation: {},
      investigation_steps: { steps: [] },
      sar_draft: {},
      global_signals: [],
      outcome: null,
      case_status: null,
      model_metadata: { model_version: 'model-v1' },
    })
    api.getNetworkGraph.mockRejectedValueOnce(new Error('graph failed'))
    api.getNarrativeDraft.mockRejectedValueOnce(new Error('narrative failed'))

    render(
      <MemoryRouter>
        <AlertDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Network Graph')).not.toBeNull())
    await waitFor(() => expect(screen.getByText('Network graph is temporarily unavailable.')).not.toBeNull())
    expect(screen.getByText('Narrative draft is temporarily unavailable.')).not.toBeNull()
    expect(screen.queryByText('Some investigation context is temporarily unavailable.')).toBeNull()
  })

  it('renders model-based explanation messaging for SHAP payloads', async () => {
    render(
      <MemoryRouter>
        <AlertDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Supporting Evidence')).not.toBeNull())
    expect(screen.getByText('Model-based explanation available.')).not.toBeNull()
  })

  it('renders heuristic disclaimer when explanation falls back', async () => {
    const { api } = await import('../services/api')
    api.getAlert.mockResolvedValueOnce({
      alert_id: 'A1',
      risk_score: 88,
      risk_band: 'high',
      risk_explain_json: JSON.stringify({
        feature_attribution: [{ feature: 'amount', value: 10000 }],
        explanation_method: 'numeric_fallback',
        explanation_status: 'fallback',
      }),
    })
    api.getAlertExplain.mockResolvedValueOnce({
      risk_explanation: {
        feature_attribution: [{ feature: 'amount', value: 10000 }],
        explanation_method: 'numeric_fallback',
        explanation_status: 'fallback',
      },
    })

    render(
      <MemoryRouter>
        <AlertDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Supporting Evidence')).not.toBeNull())
    expect(screen.getByText(/Heuristic feature highlights are shown because model attribution is not available/)).not.toBeNull()
  })

  it('hides mutation controls when permissions are missing', async () => {
    mockUser = { user_id: 'u1', role: 'analyst', permissions: [] }

    render(
      <MemoryRouter>
        <AlertDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Current Status & Actions')).not.toBeNull())
    expect(screen.getByText(/Workflow actions require backend permissions/)).not.toBeNull()
    expect(screen.getByText(/Outcome recording is disabled/)).not.toBeNull()
  })
})
