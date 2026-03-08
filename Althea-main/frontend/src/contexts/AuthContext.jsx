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

      const jwtPayload = decodeJwtPayload(token)
      if (jwtPayload) {
        setUser({
          user_id: jwtPayload.user_id,
          role: jwtPayload.role,
          team: jwtPayload.team,
          source: 'jwt',
        })
      }

      try {
        const me = await api.me()
        setUser({ ...me, source: 'api' })
      } catch {
        api.clearToken()
        setUser(null)
      } finally {
        setLoading(false)
      }
    }

    init()
  }, [])

  const login = async ({ email, password }) => {
    const res = await api.login({ email, password })
    api.setToken(res.access_token)
    const jwtPayload = decodeJwtPayload(res.access_token)
    if (jwtPayload) {
      setUser({
        user_id: jwtPayload.user_id,
        role: jwtPayload.role,
        team: jwtPayload.team,
        source: 'jwt',
      })
    }
    const me = await api.me()
    setUser({ ...me, source: 'api' })
    return me
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
