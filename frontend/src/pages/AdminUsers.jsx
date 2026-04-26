import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'

const ROLES = ['analyst', 'investigator', 'manager', 'admin']
const SAFE_USER_FIELDS = ['id', 'user_id', 'email', 'role', 'roles', 'permissions', 'team', 'is_active', 'created_at', 'last_login_at']

function sanitizeUserRow(user) {
  return Object.fromEntries(SAFE_USER_FIELDS.filter((key) => key in (user || {})).map((key) => [key, user[key]]))
}

export function AdminUsers() {
  const { user } = useAuth()
  const [users, setUsers] = useState([])
  const [error, setError] = useState('')

  const load = async () => {
    try {
      const res = await api.getAdminUsers()
      setUsers((res.users || []).map(sanitizeUserRow))
    } catch (err) {
      setError(err.message || 'Failed to load users')
    }
  }

  useEffect(() => {
    if (user?.role === 'admin') {
      load()
    }
  }, [user?.role])

  const updateRole = async (userId, role) => {
    await api.updateUserRole(userId, role)
    await load()
  }

  if (user?.role !== 'admin') {
    return (
      <div className="p-6">
        <p className="text-sm text-red-600">This page is available for admins only.</p>
        <Link to="/investigation/dashboard" className="inline-block mt-3 px-3 py-1 border rounded">Back</Link>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">User Management</h1>
        <Link to="/investigation/dashboard" className="px-3 py-1 border rounded">Back</Link>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      <div className="overflow-x-auto border rounded bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-100">
            <tr>
              <th className="text-left p-2">Email</th>
              <th className="text-left p-2">Team</th>
              <th className="text-left p-2">Role</th>
              <th className="text-left p-2">Action</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id} className="border-t">
                <td className="p-2">{u.email}</td>
                <td className="p-2">{u.team}</td>
                <td className="p-2">{u.role}</td>
                <td className="p-2">
                  <select
                    className="border rounded px-2 py-1"
                    defaultValue={u.role}
                    onChange={(e) => updateRole(u.id, e.target.value)}
                  >
                    {ROLES.map((r) => (
                      <option key={r} value={r}>{r}</option>
                    ))}
                  </select>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
