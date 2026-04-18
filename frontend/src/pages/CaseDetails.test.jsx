import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { CaseDetails } from './CaseDetails'

let mockUser = {
  user_id: 'manager-1',
  role: 'manager',
  permissions: ['work_cases', 'manager_approval'],
}

let mockCaseResponse = {
  case: {
    case_id: 'CASE_00005',
    status: 'open',
    alert_id: 'ALT000394',
    created_by: 'manager-1',
    created_at: '2026-04-18T03:42:05.738978+00:00',
  },
  timeline: [],
}

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useParams: () => ({ id: 'CASE_00005' }),
  }
})

vi.mock('../services/api', () => ({
  api: {
    getInvestigationCase: vi.fn(),
    updateInvestigationCaseStatus: vi.fn(),
  },
}))

vi.mock('../contexts/AuthContext', () => ({
  useAuth: () => ({ user: mockUser }),
}))

describe('CaseDetails', () => {
  beforeEach(async () => {
    mockUser = {
      user_id: 'manager-1',
      role: 'manager',
      permissions: ['work_cases', 'manager_approval'],
    }
    mockCaseResponse = {
      case: {
        case_id: 'CASE_00005',
        status: 'open',
        alert_id: 'ALT000394',
        created_by: 'manager-1',
        created_at: '2026-04-18T03:42:05.738978+00:00',
      },
      timeline: [],
    }

    const { api } = await import('../services/api')
    api.getInvestigationCase.mockReset()
    api.updateInvestigationCaseStatus.mockReset()
    api.getInvestigationCase.mockImplementation(async () => mockCaseResponse)
    api.updateInvestigationCaseStatus.mockImplementation(async (_caseId, nextStatus) => {
      mockCaseResponse = {
        ...mockCaseResponse,
        case: {
          ...mockCaseResponse.case,
          status: nextStatus,
        },
      }
      return { case_id: 'CASE_00005', status: nextStatus }
    })
  })

  it('does not expose SAR approval from open cases', async () => {
    render(
      <MemoryRouter>
        <CaseDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Case CASE_00005')).not.toBeNull())
    expect(screen.queryByText('Approve SAR')).toBeNull()
    expect(screen.getByText('SAR approval becomes available after the case moves to review or escalation.')).not.toBeNull()

    const statusOptions = Array.from(screen.getAllByRole('option')).map((option) => option.textContent)
    expect(statusOptions).toEqual(['Open', 'Under Review', 'Escalated', 'Closed'])
  })

  it('exposes SAR approval only after the case reaches an allowed state', async () => {
    mockCaseResponse = {
      ...mockCaseResponse,
      case: {
        ...mockCaseResponse.case,
        status: 'escalated',
      },
    }

    const { api } = await import('../services/api')

    render(
      <MemoryRouter>
        <CaseDetails />
      </MemoryRouter>,
    )

    await waitFor(() => expect(screen.getByText('Approve SAR')).not.toBeNull())
    const statusOptions = Array.from(screen.getAllByRole('option')).map((option) => option.textContent)
    expect(statusOptions).toEqual(['Escalated', 'Under Review', 'Sar Filed', 'Closed'])

    fireEvent.click(screen.getByText('Approve SAR'))

    await waitFor(() => expect(api.updateInvestigationCaseStatus).toHaveBeenCalledWith('CASE_00005', 'sar_filed'))
  })
})
