import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { AuthProvider, useAuth } from './AuthContext'

vi.mock('../services/api', () => {
  const mockApi = {
    getAccessToken: vi.fn(() => null),
    getRefreshToken: vi.fn(() => null),
    clearTokens: vi.fn(),
    setTokens: vi.fn(),
    refresh: vi.fn(),
    me: vi.fn(),
    login: vi.fn(),
  }
  return { api: mockApi }
})

function Probe() {
  const { user, isAuthenticated } = useAuth()
  return (
    <div>
      <span data-testid="auth">{String(isAuthenticated)}</span>
      <span data-testid="user">{user?.email || 'none'}</span>
    </div>
  )
}

describe('AuthContext', () => {
  beforeEach(async () => {
    const mod = await import('../services/api')
    mod.api.getAccessToken.mockReset()
    mod.api.getRefreshToken.mockReset()
    mod.api.clearTokens.mockReset()
    mod.api.setTokens.mockReset()
    mod.api.refresh.mockReset()
    mod.api.me.mockReset()
    mod.api.login.mockReset()
  })

  it('hydrates user from /auth/me when token exists', async () => {
    const mod = await import('../services/api')
    mod.api.getAccessToken.mockReturnValue('token')
    mod.api.getRefreshToken.mockReturnValue('refresh')
    mod.api.me.mockResolvedValue({ email: 'analyst@bank.com', user_id: 'u1', role: 'analyst' })

    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    )

    await waitFor(() => expect(screen.getByTestId('user').textContent).toBe('analyst@bank.com'))
    expect(screen.getByTestId('auth').textContent).toBe('true')
  })
})
