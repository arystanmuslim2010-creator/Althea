import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import { api } from '../services/api'

const AuthContext = createContext(null)

function decodeJwtPayload(token) {
  if (!token) return null
  try {
    const parts = token.split('.')
    if (parts.length !== 3) return null
    const payload = parts[1]
    const base64 = payload.replace(/-/g, '+').replace(/_/g, '/')
    const normalized = base64.padEnd(Math.ceil(base64.length / 4) * 4, '=')
    const decoded = atob(normalized)
    return JSON.parse(decoded)
  } catch {
    return null
  }
}

function buildUserFromJwt(token) {
  const jwtPayload = decodeJwtPayload(token)
  if (!jwtPayload) return null
  return {
    user_id: jwtPayload.user_id || jwtPayload.sub || null,
    id: jwtPayload.sub || jwtPayload.user_id || null,
    role: jwtPayload.role,
    team: jwtPayload.team,
    tenant_id: jwtPayload.tenant_id,
    source: 'jwt',
  }
}

function getTokenExpiryMs(token) {
  const payload = decodeJwtPayload(token)
  if (!payload?.exp) return null
  return Number(payload.exp) * 1000
}

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [loading, setLoading] = useState(true)
  const [refreshTick, setRefreshTick] = useState(0)

  useEffect(() => {
    const init = async () => {
      const token = api.getAccessToken()
      if (!token) {
        setLoading(false)
        return
      }

      const jwtUser = buildUserFromJwt(token)
      if (jwtUser) {
        setUser(jwtUser)
      }
      setLoading(false)

      try {
        const exp = getTokenExpiryMs(token)
        if (exp && Date.now() >= exp - 15000 && api.getRefreshToken()) {
          const refreshed = await api.refresh()
          if (refreshed?.access_token) {
            api.setTokens(refreshed.access_token, refreshed.refresh_token)
          }
        }
        const me = await api.me()
        setUser({ ...me, source: 'api' })
      } catch {
        api.clearTokens()
        setUser(null)
      }
    }

    init()
  }, [refreshTick])

  useEffect(() => {
    const accessToken = api.getAccessToken()
    const refreshToken = api.getRefreshToken()
    if (!accessToken || !refreshToken) return undefined

    const expMs = getTokenExpiryMs(accessToken)
    if (!expMs) return undefined

    const msUntilRefresh = Math.max(5000, expMs - Date.now() - 60000)
    const timer = setTimeout(async () => {
      try {
        const refreshed = await api.refresh()
        if (refreshed?.access_token) {
          api.setTokens(refreshed.access_token, refreshed.refresh_token)
          setRefreshTick((v) => v + 1)
        } else {
          api.clearTokens()
          setUser(null)
        }
      } catch {
        api.clearTokens()
        setUser(null)
      }
    }, msUntilRefresh)

    return () => clearTimeout(timer)
  }, [user, refreshTick])

  const login = async ({ email, password }) => {
    const res = await api.login({ email, password })
    api.setTokens(res.access_token, res.refresh_token)
    const resolvedUser = res.user
      ? { ...res.user, user_id: res.user.user_id || res.user.id, source: 'login' }
      : buildUserFromJwt(res.access_token)
    setUser(resolvedUser || null)

    api.me()
      .then((me) => setUser({ ...me, source: 'api' }))
      .catch(() => {})

    return resolvedUser
  }

  const logout = async () => {
    try {
      await api.logout()
    } catch {
      // Ignore logout transport errors and clear local auth state anyway.
    } finally {
      api.clearTokens()
      setUser(null)
    }
  }

  const value = useMemo(
    () => ({
      user,
      loading,
      isAuthenticated: Boolean(api.getAccessToken() && user),
      login,
      logout,
      decodeJwtPayload,
    }),
    [user, loading],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
