import { useLanguage } from '../contexts/LanguageContext'

export function LanguageSelect() {
  const { language, setLanguage, languages, t } = useLanguage()

  return (
    <label className="inline-flex items-center gap-2 text-[0.8125rem] text-[var(--muted)]">
      <span className="hidden sm:inline">{t.layout.languageLabel}</span>
      <select
        value={language}
        onChange={(e) => setLanguage(e.target.value)}
        aria-label={t.layout.languageLabel}
        className="min-h-[34px] px-2.5 py-1.5 text-[0.8125rem] bg-[var(--surface2)] border border-[var(--border)] rounded-lg text-[var(--text)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--surface)]"
      >
        {languages.map((item) => (
          <option key={item.code} value={item.code}>
            {item.label}
          </option>
        ))}
      </select>
    </label>
  )
}
