import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { api, isConnectionError } from '../services/api'
import { normalizeExplanationPayload } from '../services/contracts'
import { useLanguage } from '../contexts/LanguageContext'
import { useAuth } from '../contexts/AuthContext'
import { hasPermission } from '../services/permissions'

function _parseJson(val) {
  if (!val) return null
  if (typeof val === 'object') return val
  try {
    return typeof val === 'string' ? JSON.parse(val) : null
  } catch {
    return null
  }
}

function _formatExplainSummary(rawExplain) {
  const explain = normalizeExplanationPayload(rawExplain)
  if (!explain || typeof explain !== 'object') return 'No explanation data.'
  const parts = []
  if (typeof explain.base_prob === 'number') {
    parts.push(`base_prob=${explain.base_prob.toFixed(3)}`)
  }
  if (explain.model_version) {
    parts.push(`model=${explain.model_version}`)
  }
  const reasons = Array.isArray(explain.risk_reason_codes) ? explain.risk_reason_codes.filter(Boolean) : []
  if (reasons.length) {
    parts.push(`reasons=${reasons.slice(0, 4).join(', ')}`)
  }
  const top = Array.isArray(explain.feature_attribution) && explain.feature_attribution.length
    ? explain.feature_attribution
    : (Array.isArray(explain.contributions) ? explain.contributions : [])
  if (top.length) {
    const head = top.slice(0, 3).map((item) => {
      if (!item || typeof item !== 'object') return String(item)
      const f = item.feature || item.name || 'feature'
      const v = typeof item.value === 'number' ? item.value.toFixed(3) : String(item.value ?? '')
      return `${f}:${v}`
    })
    parts.push(`top=${head.join(' | ')}`)
  }
  return parts.join('; ') || 'No explanation data.'
}

/**
 * Get explanation method from raw explanation JSON.
 * Returns: "shap", "numeric_fallback", "unknown", or null
 */
function _getExplanationMethod(rawExplain) {
  const explain = normalizeExplanationPayload(rawExplain)
  if (!explain || typeof explain !== 'object') return null
  return explain.explanation_method || 'unknown'
}

/**
 * Check if explanation is using fallback method.
 * Returns true if explanation is heuristic/numeric fallback (not model-based).
 */
function _isExplanationFallback(rawExplain) {
  const explain = normalizeExplanationPayload(rawExplain)
  return Boolean(explain?.is_fallback)
}

/**
 * Get explanation warning message if present.
 */
function _getExplanationWarning(rawExplain) {
  const explain = normalizeExplanationPayload(rawExplain)
  if (!explain || typeof explain !== 'object') return null
  return explain.explanation_warning || null
}

function _deriveBehavioralSignals(alertDetail) {
  const direct = _parseJson(alertDetail?.ml_signals_json)
  if (direct && typeof direct === 'object' && Object.keys(direct).length > 0) return direct
  const explain = _parseJson(alertDetail?.risk_explain_json)
  const contrib = _parseJson(alertDetail?.top_feature_contributions_json)
  const top = Array.isArray(explain?.feature_attribution) && explain.feature_attribution.length
    ? explain.feature_attribution
    : (Array.isArray(explain?.contributions) && explain.contributions.length
      ? explain.contributions
      : (Array.isArray(contrib) ? contrib : []))
  if (!top.length) return null
  return {
    model_version: explain?.model_version || alertDetail?.model_version || 'unknown',
    top_feature_contributions: top,
    risk_reason_codes: Array.isArray(explain?.risk_reason_codes) ? explain.risk_reason_codes : [],
  }
}

function badgeClass(riskBand) {
  const b = (riskBand || '').toLowerCase()
  const base = 'inline-block px-2 py-0.5 text-[0.68rem] font-semibold rounded uppercase tracking-wide'
  if (b === 'critical') return `${base} bg-red-500/20 text-red-500`
  if (b === 'high') return `${base} bg-orange-500/20 text-orange-500`
  if (b === 'medium') return `${base} bg-orange-500/10 text-orange-500`
  if (b === 'low') return `${base} bg-green-500/20 text-green-500`
  return `${base} bg-[var(--surface2)] text-[var(--muted)]`
}

function govBadgeClass(status) {
  const s = (status || '').toLowerCase().replace(/[^a-z0-9]/g, '-')
  const base = 'inline-block px-2 py-0.5 text-[0.68rem] font-semibold rounded uppercase tracking-wide'
  if (s === 'eligible') return `${base} bg-green-500/15 text-green-600 dark:text-green-400`
  if (s === 'mandatory-review') return `${base} bg-orange-500/15 text-orange-600 dark:text-orange-400`
  if (s === 'suppressed') return `${base} bg-slate-500/20 text-[var(--muted)]`
  return `${base} bg-[var(--surface2)] text-[var(--muted)]`
}

function RawDataExpandable({ data, keys, parse }) {
  const [open, setOpen] = useState({})
  const toggle = (k) => setOpen((o) => ({ ...o, [k]: !o[k] }))
  return (
    <div className="flex flex-col gap-2">
      {keys.map((k) => {
        const val = data[k]
        const parsed = parse(val)
        const isOpen = open[k]
        return (
          <div key={k}>
            <button
              type="button"
              className="w-full py-1.5 px-2 text-[0.8125rem] bg-[var(--surface2)] border border-[var(--border)] rounded text-left text-[var(--text)] cursor-pointer"
              onClick={() => toggle(k)}
            >
              {isOpen ? '▼' : '▶'} {k}
            </button>
            {isOpen && (
              <pre className="m-0 mt-2 text-xs overflow-x-auto max-h-[120px] whitespace-pre-wrap break-words bg-[var(--surface2)] p-2 rounded">
                {parsed ? JSON.stringify(parsed, null, 2) : (val || '—')}
              </pre>
            )}
          </div>
        )
      })}
    </div>
  )
}

const TAB_IDS = ['overview', 'why', 'governance', 'evidence', 'audit', 'ai']

const ALERT_QUEUE_I18N = {
  en: {
    loading: 'Loading...',
    connectionHint: 'Service temporarily unavailable. Please try again later.',
    noActiveRun: 'No active run.',
    noActiveRunHint: 'Use System & Data to generate or load data, then click Run Pipeline / Ingest & Score.',
    searchPlaceholder: 'Type to search...',
    eligible: 'Eligible',
    suppressed: 'Suppressed',
    all: 'All',
    moreFilters: 'More Filters',
    minRisk: 'Min Risk',
    typology: 'Typology',
    segment: 'Segment',
    alerts: 'alerts',
    selectAlert: 'Select an alert from the queue to view details.',
    tabs: {
      overview: 'Alert Overview',
      why: 'Why This Alert',
      governance: 'Governance Decision',
      evidence: 'Evidence',
      audit: 'Audit & History',
      ai: 'AI Summary',
    },
  },
  ru: {
    loading: 'Загрузка...',
    connectionHint: 'Сервис временно недоступен. Попробуйте позже.',
    noActiveRun: 'Нет активного запуска.',
    noActiveRunHint: 'Откройте System & Data, загрузите или сгенерируйте данные, затем нажмите Run Pipeline / Ingest & Score.',
    searchPlaceholder: 'Поиск...',
    eligible: 'Допущен',
    suppressed: 'Подавлен',
    all: 'Все',
    moreFilters: 'Еще фильтры',
    minRisk: 'Мин. риск',
    typology: 'Типология',
    segment: 'Сегмент',
    alerts: 'алертов',
    selectAlert: 'Выберите алерт из очереди, чтобы посмотреть детали.',
    tabs: {
      overview: 'Обзор алерта',
      why: 'Почему этот алерт',
      governance: 'Решение governance',
      evidence: 'Доказательства',
      audit: 'Аудит и история',
      ai: 'AI сводка',
    },
  },
  zh: {
    loading: '加载中...',
    connectionHint: '服务暂时不可用，请稍后重试。',
    noActiveRun: '没有活动运行。',
    noActiveRunHint: '请在 System & Data 生成或加载数据，然后点击 Run Pipeline / Ingest & Score。',
    searchPlaceholder: '输入搜索...',
    eligible: '可处理',
    suppressed: '已抑制',
    all: '全部',
    moreFilters: '更多筛选',
    minRisk: '最低风险',
    typology: '类型',
    segment: '分群',
    alerts: '告警',
    selectAlert: '从队列中选择一个告警以查看详情。',
    tabs: {
      overview: '告警概览',
      why: '为何触发',
      governance: '治理决策',
      evidence: '证据',
      audit: '审计与历史',
      ai: 'AI 摘要',
    },
  },
  ja: {
    loading: '読み込み中...',
    connectionHint: 'サービスが一時的に利用できません。しばらくしてからお試しください。',
    noActiveRun: '実行中のランがありません。',
    noActiveRunHint: 'System & Data でデータを作成または読み込み、Run Pipeline / Ingest & Score を実行してください。',
    searchPlaceholder: '検索...',
    eligible: '対象',
    suppressed: '抑制',
    all: 'すべて',
    moreFilters: '詳細フィルター',
    minRisk: '最小リスク',
    typology: 'タイポロジ',
    segment: 'セグメント',
    alerts: 'アラート',
    selectAlert: 'キューからアラートを選択して詳細を表示します。',
    tabs: {
      overview: 'アラート概要',
      why: 'このアラートの理由',
      governance: 'ガバナンス判定',
      evidence: 'エビデンス',
      audit: '監査と履歴',
      ai: 'AI サマリー',
    },
  },
  tr: {
    loading: 'Yukleniyor...',
    connectionHint: 'Hizmet gecici olarak kullanilamiyor. Lutfen daha sonra tekrar deneyin.',
    noActiveRun: 'Aktif calisma yok.',
    noActiveRunHint: 'System & Data ile veri yukleyin veya uretin, sonra Run Pipeline / Ingest & Score tiklayin.',
    searchPlaceholder: 'Aramak icin yazin...',
    eligible: 'Uygun',
    suppressed: 'Bastirilmis',
    all: 'Tum',
    moreFilters: 'Daha Fazla Filtre',
    minRisk: 'Min Risk',
    typology: 'Tipoloji',
    segment: 'Segment',
    alerts: 'uyari',
    selectAlert: 'Detaylari gormek icin kuyruktan bir uyari secin.',
    tabs: {
      overview: 'Uyari Ozeti',
      why: 'Neden Bu Uyari',
      governance: 'Yonetisim Karari',
      evidence: 'Kanit',
      audit: 'Denetim ve Gecmis',
      ai: 'AI Ozeti',
    },
  },
  kk: {
    loading: 'Жүктелуде...',
    connectionHint: 'Қызмет уақытша қолжетімсіз. Кейінірек қайталап көріңіз.',
    noActiveRun: 'Белсенді іске қосу жоқ.',
    noActiveRunHint: 'System & Data арқылы дерек жүктеп не жасап, кейін Run Pipeline / Ingest & Score батырмасын басыңыз.',
    searchPlaceholder: 'Іздеу...',
    eligible: 'Қолжетімді',
    suppressed: 'Басылған',
    all: 'Барлығы',
    moreFilters: 'Қосымша фильтр',
    minRisk: 'Мин. тәуекел',
    typology: 'Типология',
    segment: 'Сегмент',
    alerts: 'ескерту',
    selectAlert: 'Толығырақ көру үшін кезектен ескертуді таңдаңыз.',
    tabs: {
      overview: 'Ескерту шолуы',
      why: 'Неге бұл ескерту',
      governance: 'Басқару шешімі',
      evidence: 'Дәлелдер',
      audit: 'Аудит және тарих',
      ai: 'AI қысқаша',
    },
  },
}

export function AlertQueue() {
  const navigate = useNavigate()
  const { user } = useAuth()
  const { language, t } = useLanguage()
  const ui = ALERT_QUEUE_I18N[language] ?? ALERT_QUEUE_I18N.en
  const tabs = TAB_IDS.map((id) => ({ id, label: ui.tabs[id] ?? id }))
  const [alerts, setAlerts] = useState([])
  const [metrics, setMetrics] = useState(null)
  const [runInfo, setRunInfo] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [warning, setWarning] = useState(null)
  const [selected, setSelected] = useState(null)
  const [alertDetail, setAlertDetail] = useState(null)
  const [caseCreated, setCaseCreated] = useState(null)
  const [aiSummary, setAiSummary] = useState(null)
  const [aiSummaryLoading, setAiSummaryLoading] = useState(false)
  const [outcome, setOutcome] = useState(null)
  const [outcomeReason, setOutcomeReason] = useState('')
  const [outcomeLoading, setOutcomeLoading] = useState(false)
  const [activeTab, setActiveTab] = useState('overview')
  const [moreFiltersOpen, setMoreFiltersOpen] = useState(false)
  const [filters, setFilters] = useState({
    status: 'Eligible',
    minRisk: 0,
    typology: 'All',
    segment: 'All',
    search: '',
  })
  const [totalAlerts, setTotalAlerts] = useState(0)
  const canCreateCase = hasPermission(user, 'work_cases')
  const canRecordOutcome = hasPermission(user, 'change_alert_status')

  const load = async () => {
    setLoading(true)
    setError(null)
    setWarning(null)
    try {
      const params = {
        status_filter: filters.status,
        min_risk: filters.minRisk,
        typology: filters.typology || 'All',
        segment: filters.segment || 'All',
        search: filters.search,
        response_mode: 'queue',
      }
      const [alertsRes, metricsRes, runRes] = await Promise.allSettled([
        api.getAlerts(params),
        api.getQueueMetrics(),
        api.getRunInfo(),
      ])

      if (alertsRes.status !== 'fulfilled') {
        throw alertsRes.reason
      }

      const alertsPayload = alertsRes.value || {}
      const metricsPayload = metricsRes.status === 'fulfilled' ? (metricsRes.value || null) : null
      const runPayload = runRes.status === 'fulfilled' ? (runRes.value || null) : null

      setAlerts(alertsPayload.alerts || [])
      setTotalAlerts(metricsPayload?.total_alerts ?? alertsPayload.total_available ?? alertsPayload.total ?? alertsPayload.alerts?.length ?? 0)
      setMetrics(metricsPayload)
      setRunInfo(runPayload)

      if (metricsRes.status === 'rejected' || runRes.status === 'rejected') {
        setWarning('Some dashboard metrics are temporarily unavailable.')
      }
    } catch (e) {
      setError(e.message)
      setAlerts([])
      setMetrics(null)
      setRunInfo(null)
      setWarning(null)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [filters.status, filters.minRisk, filters.typology, filters.segment, filters.search])

  const loadDetail = async (id) => {
    setSelected(id)
    setAiSummary(null)
    setOutcome(null)
    setWarning(null)
    const fallbackAlert = (alerts || []).find((item) => String(item?.alert_id) === String(id)) || null
    try {
      const [detailRes, summaryRes, outcomeRes] = await Promise.allSettled([
        api.getAlert(id),
        api.getAiSummary(id),
        api.getAlertOutcome(id),
      ])

      if (detailRes.status === 'fulfilled') {
        setAlertDetail(detailRes.value)
      } else if (fallbackAlert) {
        setAlertDetail(fallbackAlert)
        setWarning('Full alert details are temporarily unavailable; showing summary fields from queue.')
      } else {
        setAlertDetail(null)
      }

      setAiSummary(summaryRes.status === 'fulfilled' ? (summaryRes.value?.summary || null) : null)
      setOutcome(outcomeRes.status === 'fulfilled' ? outcomeRes.value : null)

      if (summaryRes.status === 'rejected' || outcomeRes.status === 'rejected') {
        setWarning('Some side panels are temporarily unavailable.')
      }
    } catch {
      if (fallbackAlert) {
        setAlertDetail(fallbackAlert)
        setWarning('Full alert details are temporarily unavailable; showing summary fields from queue.')
      } else {
        setAlertDetail(null)
      }
      setAiSummary(null)
      setOutcome(null)
    }
  }

  const handleGenerateSummary = async () => {
    if (!selected) return
    setAiSummaryLoading(true)
    try {
      const res = await api.generateAiSummary(selected)
      setAiSummary(res.summary || null)
    } catch (e) {
      setError(e.message)
    } finally {
      setAiSummaryLoading(false)
    }
  }

  const handleClearSummary = async () => {
    if (!selected) return
    setAiSummaryLoading(true)
    try {
      await api.clearAiSummary(selected)
      setAiSummary(null)
    } catch (e) {
      setError(e.message)
    } finally {
      setAiSummaryLoading(false)
    }
  }

  const handleCreateCase = async () => {
    if (!selected || !canCreateCase) return
    try {
      const res = await api.createCase([selected])
      setCaseCreated(res.case_id)
      if (res?.case_id) {
        navigate(`/investigation/alerts/${selected}`, {
          state: {
            created_case_id: res.case_id,
            source_alert_id: selected,
          },
        })
        return
      }
      setTimeout(() => setCaseCreated(null), 3000)
    } catch (e) {
      setError(e.message)
    }
  }

  const submitOutcome = async (decision) => {
    if (!selected || !decision || !canRecordOutcome) return
    setOutcomeLoading(true)
    try {
      const payload = {
        analyst_decision: decision,
        decision_reason: outcomeReason || null,
        risk_score_at_decision: Number(alertDetail?.risk_score ?? 0),
      }
      const res = await api.recordAlertOutcome(selected, payload)
      setOutcome(res)
      setOutcomeReason('')
    } catch (e) {
      setError(e.message)
    } finally {
      setOutcomeLoading(false)
    }
  }

  if (loading && !alerts.length) {
    return (
      <div className="max-w-[1200px] mx-auto">
        <div className="py-10 text-center text-[var(--muted)] text-[0.9375rem]">{ui.loading}</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="max-w-[1200px] mx-auto">
        <div className="p-4 px-5 bg-red-500/10 border border-red-500/25 rounded-lg text-red-600 dark:text-red-400 text-sm">
          {error}
          {isConnectionError(error) && (
            <p className="mt-3 text-[0.85rem] text-slate-500">
              {ui.connectionHint}
            </p>
          )}
        </div>
      </div>
    )
  }

  if (!runInfo?.run_id) {
    return (
      <div className="max-w-[1200px] mx-auto">
        <h1 className="text-[1.375rem] font-medium mb-5 text-[var(--text)]">{t.layout.pageTitles['/alert-queue']}</h1>
        <div className="p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)] text-sm leading-relaxed">
          {ui.noActiveRun} {ui.noActiveRunHint}
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-[1200px] mx-auto">
      <h1 className="text-[1.375rem] font-medium mb-5 text-[var(--text)]">{t.layout.pageTitles['/alert-queue']}</h1>
      {warning && (
        <div className="mb-4 p-3 rounded-md border border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-400 text-sm">
          {warning}
        </div>
      )}
      <div className="grid grid-cols-1 lg:grid-cols-[1.8fr_2.2fr] gap-6">
        <div className="flex flex-col gap-3">
          <div className="flex flex-wrap gap-2 items-center">
            <input
              type="text"
              className="flex-1 min-w-[180px] h-9 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md text-[var(--text)] placeholder-[var(--muted)] focus:outline-none focus:ring-2 focus:ring-blue-500/25 focus:border-blue-500"
              placeholder={ui.searchPlaceholder}
              value={filters.search}
              onChange={(e) => setFilters((f) => ({ ...f, search: e.target.value }))}
            />
            <select
              className="min-w-[130px] h-9 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md text-[var(--text)]"
              value={filters.status}
              onChange={(e) => setFilters((f) => ({ ...f, status: e.target.value }))}
            >
              <option value="Eligible">{ui.eligible}</option>
              <option value="Suppressed">{ui.suppressed}</option>
              <option value="All">{ui.all}</option>
            </select>
            <button
              type="button"
              className={`px-3.5 py-1.5 text-[0.8125rem] rounded-md border transition-colors ${
                moreFiltersOpen
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400 bg-blue-500/5'
                  : 'border-[var(--border)] bg-transparent text-[var(--text)] hover:bg-[var(--surface2)]'
              }`}
              onClick={() => setMoreFiltersOpen((v) => !v)}
            >
              {moreFiltersOpen ? '▼' : '▶'} {ui.moreFilters}
            </button>
            {moreFiltersOpen && (
              <div className="w-full flex flex-wrap gap-2 items-center p-3 mt-1 bg-[var(--surface2)] border border-[var(--border)] rounded-md">
                <label className="flex items-center gap-2">
                  {ui.minRisk}:
                  <input
                    type="number"
                    min={0}
                    max={100}
                    className="w-14 h-9 px-2 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md"
                    value={filters.minRisk}
                    onChange={(e) => setFilters((f) => ({ ...f, minRisk: Number(e.target.value) || 0 }))}
                  />
                </label>
                <input
                  type="text"
                  placeholder={ui.typology}
                  className="min-w-[100px] h-9 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md"
                  value={filters.typology}
                  onChange={(e) => setFilters((f) => ({ ...f, typology: e.target.value }))}
                />
                <input
                  type="text"
                  placeholder={ui.segment}
                  className="min-w-[100px] h-9 px-3 text-sm bg-[var(--surface)] border border-[var(--border)] rounded-md"
                  value={filters.segment}
                  onChange={(e) => setFilters((f) => ({ ...f, segment: e.target.value }))}
                />
              </div>
            )}
          </div>
          <p className="text-xs text-[var(--muted)] my-1">
            {totalAlerts > 0 ? totalAlerts.toLocaleString() : alerts.length} {ui.alerts}
          </p>
          <div className="overflow-x-auto border border-[var(--border)] rounded-lg bg-[var(--surface)] shadow-md">
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  <th className="w-10 text-center py-2 px-3 text-left text-xs font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]" />
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">risk_band</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">risk_score</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">user_id</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">segment</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">typology</th>
                  <th className="py-2 px-3 text-left text-[0.7rem] font-semibold text-[var(--muted)] uppercase tracking-wider bg-[var(--surface2)]">governance</th>
                </tr>
              </thead>
              <tbody>
                {alerts.map((a) => (
                  <tr
                    key={a.alert_id}
                    className={`cursor-pointer transition-colors ${
                      selected === a.alert_id
                        ? 'bg-blue-500/10 dark:bg-blue-400/15'
                        : 'even:bg-black/[0.02] dark:even:bg-white/[0.02] hover:bg-[var(--surface2)]'
                    }`}
                    onClick={() => loadDetail(a.alert_id)}
                  >
                    <td className="w-10 text-center py-2 px-3" onClick={(e) => e.stopPropagation()}>
                      <input
                        type="checkbox"
                        className="accent-blue-500 cursor-pointer"
                        checked={selected === a.alert_id}
                        onChange={() => {
                          if (selected === a.alert_id) {
                            setSelected(null)
                            setAlertDetail(null)
                          } else {
                            loadDetail(a.alert_id)
                          }
                        }}
                      />
                    </td>
                    <td className="py-2 px-3 text-[0.8125rem]">
                      <span className={badgeClass(a.risk_band)}>{a.risk_band || '-'}</span>
                    </td>
                    <td className="py-2 px-3 text-[0.8125rem]">{a.risk_score != null ? Number(a.risk_score).toFixed(4) : '-'}</td>
                    <td className="py-2 px-3 text-[0.8125rem]">{a.user_id || '-'}</td>
                    <td className="py-2 px-3 text-[0.8125rem]">{a.segment || '-'}</td>
                    <td className="py-2 px-3 text-[0.8125rem]">{a.typology || '-'}</td>
                    <td className="py-2 px-3 text-[0.8125rem]">
                      <span className={govBadgeClass(a.governance_status)}>
                        {(a.governance_status || '').toLowerCase()}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="p-5 border border-[var(--border)] rounded-lg bg-[var(--surface)] shadow-md overflow-hidden">
          {selected && alertDetail ? (
            <>
              <div className="flex items-center justify-between flex-wrap gap-2 pb-4 border-b border-[var(--border)]">
                <h3 className="m-0 text-base font-semibold text-[var(--text)]">Alert {alertDetail.alert_id}</h3>
                <div className="flex gap-2">
                  <span className={badgeClass(alertDetail.risk_band)}>{alertDetail.risk_band || '-'}</span>
                  <span className={govBadgeClass(alertDetail.governance_status)}>{alertDetail.governance_status || '-'}</span>
                </div>
              </div>
              <div className="flex flex-wrap gap-4 py-2 pb-4 text-[0.8rem] text-[var(--muted)]">
                <span>RISK BAND: <span className={badgeClass(alertDetail.risk_band)}>{alertDetail.risk_band || '-'}</span></span>
                <span>IN QUEUE: {alertDetail.in_queue ? 'Yes' : 'No'}</span>
                <span>STATUS: {(alertDetail.governance_status || '').toLowerCase()}</span>
              </div>
              <div className="flex flex-wrap gap-1 py-2 border-b border-[var(--border)] mb-4">
                {tabs.map((t) => (
                  <button
                    key={t.id}
                    type="button"
                    className={`px-2.5 py-1.5 text-[0.75rem] font-medium rounded transition-colors ${
                      activeTab === t.id
                        ? 'text-blue-600 dark:text-blue-300 bg-blue-500/10 dark:bg-blue-400/15 font-semibold'
                        : 'text-[var(--muted)] bg-transparent hover:text-[var(--text)] hover:bg-[var(--surface2)]'
                    }`}
                    onClick={() => setActiveTab(t.id)}
                  >
                    {t.label}
                  </button>
                ))}
              </div>
              <div className="min-h-[120px]">
                {activeTab === 'overview' && (
                  <div className="py-2">
                    <div className="flex flex-wrap gap-5 my-3">
                      <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">RISK SCORE</span><span className="font-semibold text-[0.9375rem]">{alertDetail.risk_score != null ? Number(alertDetail.risk_score).toFixed(1) : '-'}</span></div>
                      <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">RISK PROB</span><span className="font-semibold text-[0.9375rem]">{alertDetail.risk_prob != null ? Number(alertDetail.risk_prob).toFixed(3) : '-'}</span></div>
                      <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">AGE / SLA</span><span className="font-semibold text-[0.9375rem]">{alertDetail.sla_hours != null ? `${Number(alertDetail.sla_hours).toFixed(1)} h` : '—'}</span></div>
                      <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">RANK</span><span className="font-semibold text-[0.9375rem]">{alertDetail.risk_score_rank ?? alertDetail.queue_rank ?? '—'}</span></div>
                    </div>
                    <div className="[&>p]:m-0.5 [&>p]:text-sm [&>p]:text-[var(--text)]">
                      <p>Segment: {alertDetail.segment ?? '-'}</p>
                      <p>Typology: {alertDetail.typology ?? '-'}</p>
                      <p>User ID: {alertDetail.user_id ?? '-'}</p>
                      <p>Tx Ref: {alertDetail.tx_ref ?? '—'}</p>
                    </div>
                    {_parseJson(alertDetail.rules_json) && Object.keys(_parseJson(alertDetail.rules_json)).length > 0 && (
                      <div className="mt-4 p-3 bg-[var(--surface2)] rounded-md text-[0.8125rem]">
                        <h5 className="m-0 mb-2 text-[0.8125rem]">Rule hits</h5>
                        <pre className="m-0 text-xs overflow-x-auto max-h-[180px] whitespace-pre-wrap break-words">{JSON.stringify(_parseJson(alertDetail.rules_json), null, 2)}</pre>
                      </div>
                    )}
                    {_parseJson(alertDetail.risk_explain_json) && (
                      <div className="mt-3 space-y-3">
                        {/* Explanation warning banner for fallback methods */}
                        {_isExplanationFallback(alertDetail.risk_explain_json) && (
                          <div className="p-3 rounded-md bg-amber-500/10 border-l-4 border-amber-500 text-[var(--text)] text-[0.75rem]">
                            <strong>Heuristic feature highlights:</strong> {_getExplanationWarning(alertDetail.risk_explain_json) || 'Not model contribution attribution.'}
                          </div>
                        )}
                        {/* Explanation summary */}
                        <div className={`p-4 rounded-md border-l-4 text-[var(--text)] text-[0.8125rem] ${
                          _isExplanationFallback(alertDetail.risk_explain_json)
                            ? 'bg-amber-500/5 border-amber-400'
                            : 'bg-blue-500/10 border-blue-500'
                          }`}>
                          <div className="flex items-center gap-2 mb-1">
                            <span className="font-semibold">Feature Signals</span>
                            {_getExplanationMethod(alertDetail.risk_explain_json) === 'shap' && (
                              <span className="text-[0.65rem] px-1.5 py-0.5 rounded bg-blue-500/20 text-blue-600 dark:text-blue-400 font-medium">MODEL ATTRIBUTION (SHAP)</span>
                            )}
                            {_getExplanationMethod(alertDetail.risk_explain_json) === 'numeric_fallback' && (
                              <span className="text-[0.65rem] px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-600 dark:text-amber-400 font-medium">HEURISTIC</span>
                            )}
                          </div>
                          Summary: {_formatExplainSummary(alertDetail.risk_explain_json)}
                        </div>
                      </div>
                    )}
                    <div className="mt-3 p-4 rounded-md bg-amber-500/10 border-l-4 border-amber-500 text-[var(--text)] text-[0.8125rem]">
                      Outcome feedback: {outcome?.analyst_decision || 'not yet recorded'}
                    </div>
                    <div className="flex items-center gap-2 mt-3">
                      {canCreateCase ? (
                        <button type="button" className="px-4 py-2 text-sm font-medium rounded-md bg-[var(--accent2)] text-white dark:bg-white dark:text-[#0c0c0c] hover:brightness-105" onClick={handleCreateCase}>
                          Create Case
                        </button>
                      ) : (
                        <span className="text-xs text-[var(--muted)]">Case creation requires `work_cases` permission.</span>
                      )}
                      {caseCreated && <span className="text-sm text-green-600 dark:text-green-400">Case {caseCreated} created</span>}
                    </div>
                  </div>
                )}
                {activeTab === 'why' && (
                  <div className="py-2">
                    <div className="bg-blue-500/5 border-l-4 border-blue-500 p-4 rounded-md mb-4">
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Why This Alert Is Prioritized / Suppressed</h5>
                      <ul className="m-2 mt-2 pl-5 [&>li]:my-1">
                        <li>Alert is queued for analyst review</li>
                        <li>Critical risk score ({alertDetail.risk_score != null ? Math.round(Number(alertDetail.risk_score)) : 0}/100) — immediate review required</li>
                        <li>Rules triggered: {_parseJson(alertDetail.rules_json) ? Object.keys(_parseJson(alertDetail.rules_json)).join(', ') || '—' : '—'}</li>
                        <li>Baseline: Individual historical baseline (confidence: {alertDetail.baseline_confidence != null ? `${(Number(alertDetail.baseline_confidence) * 100).toFixed(0)}%` : '100%'})</li>
                      </ul>
                    </div>
                    <div className="mt-4 p-3 bg-[var(--surface2)] rounded-md">
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Baseline Mode</h5>
                      <div className="flex flex-wrap gap-5 my-2">
                        <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">MODE</span><span className="font-semibold">{alertDetail.baseline_level || 'User'}</span></div>
                        <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">CONFIDENCE</span><span className="font-semibold">{alertDetail.baseline_confidence != null ? `${(Number(alertDetail.baseline_confidence) * 100).toFixed(0)}%` : '100%'}</span></div>
                        <div className="flex flex-col gap-0.5"><span className="text-[0.65rem] text-[var(--muted)] uppercase">HISTORY POINTS</span><span className="font-semibold">{alertDetail.n_hist ?? alertDetail.history_size ?? '—'}</span></div>
                      </div>
                      <p className="text-xs text-[var(--muted)] mt-2 m-0">Individual historical baseline — window: 30 days</p>
                    </div>
                    {_parseJson(alertDetail.risk_explain_json) && (
                      <div className="mt-4 p-3 bg-[var(--surface2)] rounded-md">
                        <h5 className="m-0 mb-2 text-[0.8125rem]">Risk Component Breakdown</h5>
                        <table className="w-full text-[0.8125rem] border-collapse">
                          <thead><tr><th className="py-1.5 px-2 text-left text-[var(--muted)] font-semibold">Component</th><th className="py-1.5 px-2 text-left text-[var(--muted)] font-semibold">Score</th></tr></thead>
                          <tbody>
                            {Object.entries(_parseJson(alertDetail.risk_explain_json)).map(([k, v]) => (
                              <tr key={k} className="border-b border-[var(--border)]"><td className="py-1.5 px-2">{k}</td><td className="py-1.5 px-2">{typeof v === 'object' ? JSON.stringify(v) : String(v)}</td></tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )}
                {activeTab === 'governance' && (
                  <div className="py-2">
                    <h5 className="m-0 mb-2 text-[0.8125rem]">Governance Decision</h5>
                    <div className="[&>p]:my-2 [&>p]:text-sm">
                      <p>Status: <span className={govBadgeClass(alertDetail.governance_status)}>{alertDetail.governance_status ?? '-'}</span></p>
                      <p>Suppression Reason: {alertDetail.suppression_reason || 'None'}</p>
                      <p>In Queue: {alertDetail.in_queue ? 'Yes' : 'No'}</p>
                      <p>Suppression Code: {alertDetail.suppression_code || '—'}</p>
                      <p>Policy Version: {alertDetail.policy_version ?? '1.0'}</p>
                      <p>Hard Constraint: NO</p>
                    </div>
                  </div>
                )}
                {activeTab === 'evidence' && (
                  <div className="py-2">
                    <div className="mb-4">
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Rule Evidence</h5>
                      <p className="m-0 text-sm">{_parseJson(alertDetail.rule_evidence_json) && Object.keys(_parseJson(alertDetail.rule_evidence_json)).length > 0 ? `${Object.keys(_parseJson(alertDetail.rule_evidence_json)).length} rule(s) triggered` : 'No rule evidence.'}</p>
                    </div>
                    <div className="mb-4">
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Behavioral Signals</h5>
                      <p className="m-0 text-sm">{_deriveBehavioralSignals(alertDetail) ? JSON.stringify(_deriveBehavioralSignals(alertDetail)) : 'No behavioral deviation data.'}</p>
                    </div>
                    <div>
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Raw Data (Audit / Debug)</h5>
                      <RawDataExpandable data={alertDetail} keys={['rules_json', 'rule_evidence_json', 'ml_signals_json', 'features_json', 'risk_explain_json', 'top_feature_contributions_json', 'top_features_json']} parse={_parseJson} />
                    </div>
                  </div>
                )}
                {activeTab === 'audit' && (
                  <div className="py-2">
                    <div className="mb-4">
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Audit Log</h5>
                      <p className="text-xs text-[var(--muted)] m-0">No audit history. Create a case to start tracking.</p>
                    </div>
                    <div className="mb-4">
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Actions</h5>
                      <div className="flex items-center gap-2">
                        {canCreateCase ? <button type="button" className="px-4 py-2 text-sm font-medium rounded-md bg-[var(--accent2)] text-white dark:bg-white dark:text-[#0c0c0c]" onClick={handleCreateCase}>Create Case</button> : null}
                        {caseCreated && <span className="text-sm text-green-600">Case {caseCreated} created</span>}
                        <input type="text" className="flex-1 min-w-[140px] py-1.5 px-2 text-[0.8125rem] rounded-md border border-[var(--border)] bg-[var(--surface)]" placeholder="Quick Note: Add a note..." />
                      </div>
                    </div>
                    <div className="mb-4">
                      <h5 className="m-0 mb-2 text-[0.8125rem]">Outcome Feedback</h5>
                      <p className="text-xs text-[var(--muted)] m-0 mb-2">Record TP or FP. Stored for analysis.</p>
                      {canRecordOutcome ? (
                        <>
                          <input
                            type="text"
                            className="w-full min-w-[140px] py-1.5 px-2 text-[0.8125rem] rounded-md border border-[var(--border)] bg-[var(--surface)] mb-2"
                            placeholder="Outcome reason (optional)"
                            value={outcomeReason}
                            onChange={(e) => setOutcomeReason(e.target.value)}
                          />
                          <div className="flex gap-2">
                            <button type="button" className="px-4 py-2 text-sm font-medium rounded-md border border-[var(--border)] bg-transparent disabled:opacity-60" disabled={outcomeLoading} onClick={() => submitOutcome('true_positive')}>Mark TP</button>
                            <button type="button" className="px-4 py-2 text-sm font-medium rounded-md border border-[var(--border)] bg-transparent disabled:opacity-60" disabled={outcomeLoading} onClick={() => submitOutcome('false_positive')}>Mark FP</button>
                          </div>
                        </>
                      ) : (
                        <p className="text-xs text-[var(--muted)] m-0">Outcome recording requires `change_alert_status` permission.</p>
                      )}
                      <p className="text-xs text-[var(--muted)] mt-2 m-0">
                        {outcome ? `Recorded: ${outcome.analyst_decision}` : 'No outcome recorded yet.'}
                      </p>
                    </div>
                  </div>
                )}
                {activeTab === 'ai' && (
                  <div className="py-2">
                    <div className="flex gap-2 mb-4">
                      <button type="button" className="px-4 py-2 text-sm font-medium rounded-md bg-[var(--accent2)] text-white dark:bg-white dark:text-[#0c0c0c] disabled:opacity-60" onClick={handleGenerateSummary} disabled={aiSummaryLoading}>
                        {aiSummaryLoading ? 'Generating...' : 'Generate Summary'}
                      </button>
                      <button type="button" className="px-4 py-2 text-sm font-medium rounded-md border border-[var(--border)] bg-transparent disabled:opacity-60" onClick={handleClearSummary} disabled={aiSummaryLoading || !aiSummary}>
                        Clear Summary
                      </button>
                    </div>
                    {aiSummary ? (
                      <div className="p-4 rounded-md bg-blue-500/10 border-l-4 border-blue-500">
                        <pre className="m-0 whitespace-pre-wrap font-sans text-sm leading-relaxed">{aiSummary}</pre>
                      </div>
                    ) : (
                      <div className="mt-4 p-4 rounded-md bg-blue-500/10 border-l-4 border-blue-500">
                        No AI summary yet. Click Generate Summary to create one.
                      </div>
                    )}
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="p-4 px-5 rounded-lg border border-[var(--border)] bg-[var(--surface)] text-[var(--muted)] text-sm">
              {ui.selectAlert}
            </div>
          )}
          {metrics && (
            <div className="mt-5 pt-4 border-t border-[var(--border)] p-4 rounded-md bg-[var(--surface2)] border border-[var(--border)] mb-3">
              <h4 className="m-0 mb-3 text-sm font-semibold">Queue Metrics</h4>
              <div className="flex flex-wrap gap-5">
                <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Total</span><span className="font-semibold text-[0.9375rem]">{metrics.total_alerts?.toLocaleString() ?? 0}</span></div>
                <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">In Queue</span><span className="font-semibold text-[0.9375rem]">{metrics.in_queue?.toLocaleString() ?? 0}</span></div>
                <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">Suppressed</span><span className="font-semibold text-[0.9375rem]">{metrics.suppressed?.toLocaleString() ?? 0}</span></div>
                <div className="flex flex-col gap-0.5"><span className="text-[0.7rem] text-[var(--muted)] uppercase">High Risk</span><span className="font-semibold text-[0.9375rem]">{metrics.high_risk?.toLocaleString() ?? 0}</span></div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
