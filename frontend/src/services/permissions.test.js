import { describe, expect, it } from 'vitest'
import { hasAnyPermission, hasPermission } from './permissions'

describe('permissions helpers', () => {
  it('checks explicit permissions for non-admin users', () => {
    const user = { role: 'analyst', permissions: ['work_cases', 'change_alert_status'] }
    expect(hasPermission(user, 'work_cases')).toBe(true)
    expect(hasPermission(user, 'reassign_alerts')).toBe(false)
    expect(hasAnyPermission(user, ['reassign_alerts', 'change_alert_status'])).toBe(true)
  })

  it('treats admin as fully authorized', () => {
    const user = { role: 'admin', permissions: [] }
    expect(hasPermission(user, 'manage_users')).toBe(true)
    expect(hasAnyPermission(user, ['view_dashboards', 'manager_approval'])).toBe(true)
  })
})
