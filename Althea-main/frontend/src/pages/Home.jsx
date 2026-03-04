import { Link } from 'react-router-dom'
import { useLanguage } from '../contexts/LanguageContext'

export function Home() {
  const { t } = useLanguage()

  return (
    <div className="max-w-[1200px] mx-auto transition-colors duration-300">
      <header className="py-12 md:py-20">
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-10 lg:gap-14 items-center">
          <div className="flex flex-col gap-6">
            <p className="m-0 text-sm uppercase tracking-[0.22em] text-[var(--muted)]">
              {t.home.heroTag}
            </p>
            <h1 className="m-0 text-[clamp(3rem,9vw,5.5rem)] leading-[0.98] tracking-tight text-[var(--text)]">
              {t.home.heroTitleTop}
              <br />
              {t.home.heroTitleBottom}
            </h1>
            <p className="m-0 text-base leading-relaxed text-[var(--muted)] max-w-[52ch]">
              {t.home.heroText}
            </p>
            <div className="flex flex-wrap gap-3">
              <Link
                to="/alert-queue"
                className="inline-flex items-center justify-center min-h-[48px] px-7 text-base font-medium rounded-[var(--btn-radius)] bg-[var(--accent2)] text-[var(--bg)] border border-transparent transition-all hover:brightness-110 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--bg)]"
              >
                {t.home.startDemo}
              </Link>
              <Link
                to="/cases"
                className="inline-flex items-center justify-center min-h-[48px] px-7 text-base font-medium rounded-[var(--btn-radius)] border border-[var(--border)] text-[var(--text)] bg-transparent transition-all hover:bg-[var(--surface2)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--bg)]"
              >
                {t.home.viewCases}
              </Link>
            </div>
          </div>
          <div
            className="min-h-[360px] rounded-[var(--radius-xl)] border border-[var(--border)] bg-[linear-gradient(145deg,var(--surface2),var(--surface))] p-6 flex items-end"
            aria-label={t.home.mediaAria}
          >
            <div className="w-full border border-[var(--border)] rounded-[var(--radius-lg)] bg-[var(--surface)]/70 p-5 backdrop-blur-sm">
              <p className="m-0 text-sm uppercase tracking-[0.2em] text-[var(--muted)]">
                {t.home.mediaTag}
              </p>
              <p className="m-0 mt-2 text-base text-[var(--text)]">
                {t.home.mediaText}
              </p>
            </div>
          </div>
        </section>
      </header>

      <section
        className="my-8 md:my-12 p-6 md:p-8 rounded-[var(--radius-xl)] border border-[var(--border)] bg-[linear-gradient(170deg,var(--surface),var(--surface2))]"
        aria-label={t.home.capabilitiesAria}
      >
        <h2 className="m-0 text-[clamp(1.9rem,5vw,3rem)] leading-tight tracking-tight text-[var(--text)]">
          {t.home.capabilitiesTitle}
        </h2>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mt-6">
          {t.home.capabilities.map((item) => (
            <article
              key={item.title}
              className="rounded-[var(--radius-lg)] border border-[var(--border)] bg-[var(--surface)]/70 p-4 md:p-5 min-h-[180px] flex flex-col gap-3"
            >
              <span
                className="w-10 h-10 rounded-[var(--radius-md)] border border-[var(--border)] bg-[var(--surface2)] text-[var(--text)] text-base flex items-center justify-center"
                aria-hidden
              >
                {item.icon}
              </span>
              <h3 className="m-0 text-base font-semibold text-[var(--text)]">{item.title}</h3>
              <p className="m-0 text-sm leading-relaxed text-[var(--muted)]">{item.desc}</p>
            </article>
          ))}
        </div>
      </section>

      <section
        className="my-8 md:my-12 p-6 md:p-8 rounded-[var(--radius-xl)] border border-[var(--border)] bg-[linear-gradient(165deg,var(--surface2),var(--surface))]"
        aria-label={t.home.impactAria}
      >
        <h2 className="m-0 text-[clamp(1.9rem,5vw,3rem)] leading-tight tracking-tight text-[var(--text)]">
          {t.home.impactTitle}
        </h2>
        <div className="flex gap-2 flex-wrap mt-4">
          <span className="px-3 py-1 rounded-full text-sm border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)]">
            low: 0-35
          </span>
          <span className="px-3 py-1 rounded-full text-sm border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)]">
            medium: 36-70
          </span>
          <span className="px-3 py-1 rounded-full text-sm border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)]">
            high: 71-100
          </span>
        </div>
        <ul className="mt-6 mb-0 pl-5 text-base text-[var(--muted)] leading-relaxed">
          {t.home.impactBullets.map((bullet) => (
            <li key={bullet} className="mb-2 last:mb-0">
              {bullet}
            </li>
          ))}
        </ul>
      </section>

      <section className="my-8 md:my-12 p-6 md:p-8 border border-[var(--border)] rounded-[var(--radius-xl)] bg-[var(--surface)]">
        <div className="grid grid-cols-1 md:grid-cols-[1.2fr_1fr] gap-6 items-center">
          <div>
            <h2 className="m-0 text-[clamp(1.9rem,5vw,3rem)] leading-tight tracking-tight text-[var(--text)]">
              {t.home.missionTitle}
            </h2>
            <p className="m-0 mt-4 text-base leading-relaxed text-[var(--muted)] max-w-[54ch]">
              {t.home.missionTextOne}
            </p>
            <p className="m-0 mt-3 text-base leading-relaxed text-[var(--muted)] max-w-[54ch]">
              {t.home.missionTextTwo}
            </p>
          </div>
          <div
            className="min-h-[240px] rounded-[var(--radius-lg)] border border-[var(--border)] bg-[linear-gradient(150deg,var(--surface2),var(--surface))] flex items-center justify-center p-6 text-center"
            aria-label={t.home.missionAria}
          >
            <p className="m-0 text-sm uppercase tracking-[0.2em] text-[var(--muted)]">
              {t.home.missionMedia}
            </p>
          </div>
        </div>
      </section>

      <section className="my-10 md:my-14 rounded-[var(--radius-xl)] border border-[var(--border)] bg-[var(--surface2)] p-6 md:p-9 text-center">
        <h2 className="m-0 text-[clamp(1.9rem,5vw,3rem)] leading-tight tracking-tight text-[var(--text)]">
          {t.home.ctaTitle}
        </h2>
        <p className="m-0 mt-4 text-base text-[var(--muted)] max-w-[58ch] mx-auto">
          {t.home.ctaText}
        </p>
        <div className="mt-6 flex justify-center">
          <Link
            to="/alert-queue"
            className="inline-flex items-center justify-center min-h-[48px] px-8 text-base font-medium rounded-[var(--btn-radius)] bg-[var(--accent2)] text-[var(--bg)] border border-transparent hover:brightness-110 transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--surface2)]"
          >
            {t.home.tryService}
          </Link>
        </div>
      </section>

      <footer className="mt-12 p-6 md:p-8 rounded-[var(--radius-xl)] border border-[var(--border)] bg-[var(--surface)] text-sm text-[var(--muted)]">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
          <p className="m-0">contact@althea.ai · +1 (305) 555-0142</p>
          <div className="flex flex-wrap gap-4">
            <Link to="/cases" className="text-[var(--muted)] hover:text-[var(--text)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--surface)] rounded-sm">
              {t.home.footerFaq}
            </Link>
            <Link to="/cases" className="text-[var(--muted)] hover:text-[var(--text)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--surface)] rounded-sm">
              {t.home.footerReviews}
            </Link>
            <Link to="/ops" className="text-[var(--muted)] hover:text-[var(--text)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--text)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--surface)] rounded-sm">
              {t.home.footerPolicy}
            </Link>
          </div>
        </div>
      </footer>
    </div>
  )
}
