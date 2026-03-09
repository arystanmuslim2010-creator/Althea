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

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const init = async () => {
      const token = api.getToken()
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
        const me = await api.me()
        setUser({ ...me, source: 'api' })
      } catch {
        api.clearToken()
        setUser(null)
      }
    }

    init()
  }, [])

  const login = async ({ email, password }) => {
    const res = await api.login({ email, password })
    api.setToken(res.access_token)
    const resolvedUser = res.user
      ? { ...res.user, user_id: res.user.user_id || res.user.id, source: 'login' }
      : buildUserFromJwt(res.access_token)
    setUser(resolvedUser || null)

    api.me()
      .then((me) => setUser({ ...me, source: 'api' }))
      .catch(() => {})

    return resolvedUser
  }

  const logout = () => {
    api.clearToken()
    setUser(null)
  }

  const value = useMemo(
    () => ({
      user,
      loading,
      isAuthenticated: Boolean(api.getToken() && user),
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
