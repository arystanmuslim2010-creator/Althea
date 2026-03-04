import { Outlet, useLocation } from 'react-router-dom'
import { Sidebar } from './Sidebar'
import { ThemeToggle } from './ThemeToggle'
import { LanguageSelect } from './LanguageSelect'
import { useLanguage } from '../contexts/LanguageContext'

export function Layout() {
  const { pathname } = useLocation()
  const { t } = useLanguage()
  const pageTitle = t.layout.pageTitles[pathname] ?? t.layout.defaultTitle

  return (
    <div className="flex flex-col min-h-screen bg-[var(--bg)] text-[var(--text)] transition-colors duration-300">
      <header className="sticky top-0 z-[100] flex items-center justify-between h-14 px-6 bg-[var(--surface)] border-b border-[var(--border)] transition-colors duration-300">
        <h1 className="m-0 text-[1.125rem] font-semibold text-[var(--text)] tracking-tight">
          {t.layout.defaultTitle}
        </h1>
        <div className="flex items-center gap-2">
          <LanguageSelect />
          <ThemeToggle />
        </div>
      </header>
      <div className="flex flex-1 min-h-0 flex-col md:flex-row">
        <Sidebar title={pageTitle} />
        <main className="flex-1 min-w-0 p-5 md:p-6 md:px-8 overflow-auto bg-[var(--bg)] transition-colors duration-300">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
