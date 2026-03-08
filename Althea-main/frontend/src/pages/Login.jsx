import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../services/api'

export function Login() {
  const navigate = useNavigate()
  const [mode, setMode] = useState('login')
  const [form, setForm] = useState({ email: '', password: '', role: 'analyst', team: 'team-a' })
  const [error, setError] = useState('')

  const onChange = (key, value) => setForm((prev) => ({ ...prev, [key]: value }))

  const onSubmit = async (e) => {
    e.preventDefault()
    setError('')
    try {
      const payload = mode === 'register'
        ? { email: form.email, password: form.password, role: form.role, team: form.team }
        : { email: form.email, password: form.password }
      const res = mode === 'register' ? await api.register(payload) : await api.login(payload)
      api.setToken(res.access_token)
      navigate('/investigation/dashboard')
    } catch (err) {
      setError(err.message || 'Authentication failed')
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-slate-100 p-4">
      <form onSubmit={onSubmit} className="w-full max-w-md bg-white border border-slate-200 rounded-xl p-6 shadow-sm space-y-4">
        <h1 className="text-xl font-semibold">ALTHEA Investigation Login</h1>
        <div className="flex gap-2">
          <button type="button" onClick={() => setMode('login')} className={`px-3 py-1 rounded ${mode === 'login' ? 'bg-slate-800 text-white' : 'bg-slate-200'}`}>Login</button>
          <button type="button" onClick={() => setMode('register')} className={`px-3 py-1 rounded ${mode === 'register' ? 'bg-slate-800 text-white' : 'bg-slate-200'}`}>Register</button>
        </div>
        <input className="w-full border rounded px-3 py-2" placeholder="Email" value={form.email} onChange={(e) => onChange('email', e.target.value)} />
        <input className="w-full border rounded px-3 py-2" placeholder="Password" type="password" value={form.password} onChange={(e) => onChange('password', e.target.value)} />
        {mode === 'register' && (
          <>
            <select className="w-full border rounded px-3 py-2" value={form.role} onChange={(e) => onChange('role', e.target.value)}>
              <option value="analyst">analyst</option>
              <option value="lead">lead</option>
              <option value="manager">manager</option>
              <option value="admin">admin</option>
            </select>
            <input className="w-full border rounded px-3 py-2" placeholder="Team" value={form.team} onChange={(e) => onChange('team', e.target.value)} />
          </>
        )}
        {error && <p className="text-sm text-red-600">{error}</p>}
        <button className="w-full bg-slate-900 text-white rounded py-2">{mode === 'register' ? 'Create account' : 'Sign in'}</button>
      </form>
    </div>
  )
}
