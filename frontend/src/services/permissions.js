export function hasPermission(user, permission) {
  if (!permission) return false
  const role = String(user?.role || '').toLowerCase()
  if (role === 'admin') return true
  const granted = Array.isArray(user?.permissions) ? user.permissions : []
  return granted.includes(permission)
}

export function hasAnyPermission(user, permissions) {
  const required = Array.isArray(permissions) ? permissions : [permissions]
  return required.some((permission) => hasPermission(user, permission))
}
