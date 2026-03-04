import { Routes, Route } from 'react-router-dom'
import { ThemeProvider } from './contexts/ThemeContext'
import { LanguageProvider } from './contexts/LanguageContext'
import { Layout } from './components/Layout'
import { Home } from './pages/Home'
import { AlertQueue } from './pages/AlertQueue'
import { Cases } from './pages/Cases'
import { OpsGovernance } from './pages/OpsGovernance'
import { DataConfig } from './pages/DataConfig'

export default function App() {
  return (
    <ThemeProvider>
      <LanguageProvider>
        <div className="min-h-screen bg-[var(--bg)] text-[var(--text)] transition-colors duration-300">
          <Routes>
            <Route path="/" element={<Layout />}>
              <Route index element={<Home />} />
              <Route path="alert-queue" element={<AlertQueue />} />
              <Route path="cases" element={<Cases />} />
              <Route path="ops" element={<OpsGovernance />} />
              <Route path="data" element={<DataConfig />} />
            </Route>
          </Routes>
        </div>
      </LanguageProvider>
    </ThemeProvider>
  )
}
