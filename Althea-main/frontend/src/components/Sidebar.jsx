import { useState, useEffect } from 'react'
import { NavLink } from 'react-router-dom'
import { api } from '../services/api'
import { useLanguage } from '../contexts/LanguageContext'

const nav = [
  { path: '/', icon: '⌂' },
  { path: '/alert-queue', icon: '◍' },
  { path: '/cases', icon: '▣' },
  { path: '/ops', icon: '◈' },
  { path: '/data', icon: '☰' },
]

const ACTORS = ['Analyst_1', 'Analyst_2', 'Manager']

export function Sidebar({ title = 'Althea' }) {
  const [actor, setActor] = useState('Analyst_1')
  const [collapsed, setCollapsed] = useState(false)
  const { t } = useLanguage()

  useEffect(() => {
    api.getActor().then((r) => setActor(r.actor || 'Analyst_1')).catch(() => {})
  }, [])

  const handleActorChange = (val) => {
    setActor(val)
    api.setActor(val).catch(() => {})
  }

  return (
    <aside
      className={`md:sticky md:top-14 w-full ${
        collapsed ? 'md:w-[84px]' : 'md:w-60'
      } flex-shrink-0 md:min-h-[calc(100vh-56px)] bg-[var(--surface)] border-b md:border-b-0 md:border-r border-[var(--border)] p-4 md:py-5 transition-all duration-300`}
    >
      <div className={`flex items-center ${collapsed ? 'justify-center' : 'justify-between'} px-3 mb-4 pb-3 border-b border-[var(--border)]`}>
        {!collapsed && (
          <h1 className="text-[0.9375rem] font-semibold m-0 text-[var(--text)]">
            {title}
          </h1>
        )}
        <button
          type="button"
          onClick={() => setCollapsed((prev) => !prev)}
          aria-label={collapsed ? t.sidebar.expand : t.sidebar.collapse}
          className="h-9 w-9 rounded-[var(--radius-md)] border border-[var(--border)] bg-[var(--surface2)] text-[var(--text)] inline-flex items-center justify-center hover:bg-[var(--surface)] transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--surface)]"
        >
          ☰
        </button>
      </div>
      <nav className={`flex flex-col gap-0.5 ${collapsed ? 'px-1' : 'px-3'} mb-4`}>
        {!collapsed && (
          <span className="text-[0.65rem] font-semibold uppercase tracking-wider text-[var(--muted)] mt-0 mx-0 mb-1 ml-2 block">
            {t.sidebar.navigation}
          </span>
        )}
        {nav.map(({ path, icon }) => (
          <NavLink
            key={path}
            to={path}
            className={({ isActive }) =>
              `${
                collapsed ? 'px-2 py-2.5 justify-center' : 'px-3 py-2'
              } text-sm rounded-md transition-colors duration-300 flex items-center gap-2 ${
                isActive
                  ? 'text-[var(--text)] bg-[var(--surface2)] font-medium border border-[var(--border)]'
                  : 'text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--surface2)]'
              }`
            }
            aria-label={t.sidebar.nav[path]}
            title={collapsed ? t.sidebar.nav[path] : undefined}
            end={path === '/'}
          >
            <span className="text-base leading-none" aria-hidden>
              {icon}
            </span>
            {!collapsed && t.sidebar.nav[path]}
          </NavLink>
        ))}
      </nav>
      {!collapsed && (
        <div className="mt-4 pt-4 px-4 border-t border-[var(--border)]">
          <span className="text-[0.65rem] font-semibold uppercase tracking-wider text-[var(--muted)] mt-0 mx-0 mb-1 ml-2 block">
            {t.sidebar.analyst}
          </span>
          <select
            className="w-full mt-2 px-3 py-2 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md text-[var(--text)]"
            value={actor}
            onChange={(e) => handleActorChange(e.target.value)}
          >
            {ACTORS.map((a) => (
              <option key={a} value={a}>{a}</option>
            ))}
          </select>
        </div>
      )}
    </aside>
  )
}
