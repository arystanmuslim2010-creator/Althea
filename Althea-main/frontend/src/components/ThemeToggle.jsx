import { useTheme } from '../contexts/ThemeContext'

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()

  return (
    <div
      className="flex gap-0.5 p-1 bg-[var(--surface2)] border border-[var(--border)] rounded-2xl shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] transition-colors duration-300"
      role="group"
      aria-label="Theme"
    >
      <button
        type="button"
        className={`px-3.5 py-1.5 text-[0.8125rem] font-medium rounded-l-[10px] transition-all duration-300 ${
          theme === 'light'
            ? 'bg-neutral-800 text-white shadow-sm'
            : 'text-[var(--muted)] bg-transparent hover:text-[var(--text)] hover:bg-[var(--bg)]'
        }`}
        onClick={() => setTheme('light')}
        aria-pressed={theme === 'light'}
        aria-label="Light theme"
      >
        Light
      </button>
      <button
        type="button"
        className={`px-3.5 py-1.5 text-[0.8125rem] font-medium rounded-r-[10px] transition-all duration-300 ${
          theme === 'dark'
            ? 'bg-white text-neutral-950 shadow-sm'
            : 'text-[var(--muted)] bg-transparent hover:text-[var(--text)] hover:bg-[var(--bg)]'
        }`}
        onClick={() => setTheme('dark')}
        aria-pressed={theme === 'dark'}
        aria-label="Dark theme"
      >
        Dark
      </button>
    </div>
  )
}
