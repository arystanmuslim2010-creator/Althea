import { useEffect, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

function getDefaultPathForRole(role) {
  if (role === 'analyst') return '/'
  return '/investigation/dashboard'
}

function getNextPath(role, fromPath) {
  if (role === 'analyst') return '/'
  return fromPath || getDefaultPathForRole(role)
}

export function Login() {
  const navigate = useNavigate()
  const location = useLocation()
  const { login, isAuthenticated, loading, user } = useAuth()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    if (!loading && isAuthenticated) {
      const nextPath = getNextPath(user?.role, location.state?.from)
      navigate(nextPath, { replace: true })
    }
  }, [loading, isAuthenticated, navigate, location.state, user?.role])

  const onSubmit = async (e) => {
    e.preventDefault()
    setError('')
    setSubmitting(true)
    try {
      const me = await login({ email, password })
      const nextPath = getNextPath(me?.role, location.state?.from)
      navigate(nextPath, { replace: true })
    } catch (err) {
      setError(err.message || 'Login failed')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-slate-100 p-4">
      <form onSubmit={onSubmit} className="w-full max-w-md bg-white border border-slate-200 rounded-xl p-6 shadow-sm space-y-4">
        <h1 className="text-xl font-semibold">ALTHEA Login</h1>
        <p className="text-sm text-slate-600">Authenticate to access investigation workflow and dashboards.</p>
        <div className="space-y-2">
          <label className="text-sm font-medium" htmlFor="email">Email</label>
          <input
            id="email"
            type="email"
            className="w-full border rounded px-3 py-2"
            placeholder="analyst@bank.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />
        </div>
        <div className="space-y-2">
          <label className="text-sm font-medium" htmlFor="password">Password</label>
          <input
            id="password"
            type="password"
            className="w-full border rounded px-3 py-2"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
        </div>
        {error && <p className="text-sm text-red-600">{error}</p>}
        <button
          type="submit"
          className="w-full bg-slate-900 text-white rounded py-2 disabled:opacity-50"
          disabled={submitting}
        >
          {submitting ? 'Signing in...' : 'Login'}
        </button>
        <p className="text-xs text-slate-500">
          If you do not have an account yet, use the backend registration endpoint and then login here.
        </p>
        <Link to="/" className="text-xs text-blue-600">Back to app</Link>
      </form>
    </div>
  )
}
