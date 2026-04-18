import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { AnalystCapacityProvider } from '../contexts/AnalystCapacityContext'
import { AlertQueue } from './AlertQueue'

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useNavigate: () => vi.fn(),
  }
})

vi.mock('../services/api', () => ({
  api: {
    getAlerts: vi.fn(),
    getQueueMetrics: vi.fn(),
    getRunInfo: vi.fn(),
    getAlert: vi.fn(),
    getAiSummary: vi.fn(),
    getAlertOutcome: vi.fn(),
  },
  isConnectionError: vi.fn(() => false),
}))

vi.mock('../contexts/AuthContext', () => ({
  useAuth: () => ({
    user: { user_id: 'u1', role: 'analyst', permissions: ['view_assigned_alerts'] },
  }),
}))

vi.mock('../contexts/LanguageContext', () => ({
  useLanguage: () => ({
    language: 'en',
    t: {
      layout: {
        pageTitles: {
          '/alert-queue': 'Alert Queue',
        },
      },
    },
  }),
}))

describe('AlertQueue', () => {
  beforeEach(async () => {
    globalThis.localStorage.setItem('althea.analystCapacity', '100')

    const { api } = await import('../services/api')
    api.getAlerts.mockReset()
    api.getQueueMetrics.mockReset()
    api.getRunInfo.mockReset()
    api.getAlert.mockReset()
    api.getAiSummary.mockReset()
    api.getAlertOutcome.mockReset()

    api.getAlerts.mockResolvedValue({
      alerts: Array.from({ length: 100 }, (_, index) => ({
        alert_id: `A-${index + 1}`,
        risk_band: 'high',
        risk_score: 95 - index,
        user_id: `U-${index + 1}`,
        segment: 'retail',
        typology: 'structuring',
        governance_status: 'eligible',
      })),
      total_available: 240,
      total: 100,
    })
    api.getQueueMetrics.mockResolvedValue({
      total_alerts: 240,
      in_queue: 240,
      suppressed: 0,
      high_risk: 240,
    })
    api.getRunInfo.mockResolvedValue({ run_id: 'run-1' })
    api.getAiSummary.mockResolvedValue({ summary: null })
    api.getAlertOutcome.mockResolvedValue(null)
  })

  it('loads alert queue using the shared analyst capacity as the page size', async () => {
    const { api } = await import('../services/api')

    render(
      <MemoryRouter>
        <AnalystCapacityProvider>
          <AlertQueue />
        </AnalystCapacityProvider>
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('100 / 240 alerts')).not.toBeNull())
    expect(api.getAlerts).toHaveBeenCalledWith(expect.objectContaining({ limit: 100, response_mode: 'queue' }))
  })
})
