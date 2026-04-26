import { useEffect, useMemo, useState } from 'react'
import { Link, useLocation, useNavigate, useParams } from 'react-router-dom'
import { api } from '../services/api'
import { useAuth } from '../contexts/AuthContext'
import { InvestigationGraph } from '../components/InvestigationGraph'
import { normalizeExplanationPayload } from '../services/contracts'
import { hasPermission } from '../services/permissions'

function tryParseJson(value, fallback) {
  if (value == null) return fallback
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value)
  } catch {
    return fallback
  }
}

function toArray(value) {
  return Array.isArray(value) ? value : []
}

function toObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {}
}

function normalizeWhitespace(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim()
}

function isPlaceholderText(value) {
  const text = normalizeWhitespace(value).toLowerCase()
  if (!text) return true
  if (['-', 'n/a', 'na', 'none', 'null', 'undefined', 'unknown', '[]', '{}'].includes(text)) return true
  if (text.startsWith('unknown ')) return true
  return false
}

function cleanText(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return String(value)
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  const text = normalizeWhitespace(value)
  return isPlaceholderText(text) ? '' : text
}

function hasContent(value) {
  if (value == null) return false
  if (typeof value === 'string') return Boolean(cleanText(value))
  if (typeof value === 'number') return Number.isFinite(value)
  if (typeof value === 'boolean') return true
  if (Array.isArray(value)) return value.some((item) => hasContent(item))
  if (typeof value === 'object') return Object.values(value).some((item) => hasContent(item))
  return false
}

function formatDisplayValue(value, fallback = 'Not available') {
  const text = cleanText(value)
  return text || fallback
}

function titleCase(value) {
  return String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

function formatStatus(value, fallback = 'Not available') {
  const text = cleanText(value)
  return text ? titleCase(text) : fallback
}

function formatDecision(value) {
  const text = cleanText(value)
  if (!text) return 'No outcome recorded'
  const labels = {
    true_positive: 'True Positive',
    false_positive: 'False Positive',
    escalated: 'Escalated',
    sar_filed: 'SAR/STR Filing Recorded',
    benign_activity: 'Benign Activity',
    confirmed_suspicious: 'Confirmed Suspicious Activity',
  }
  return labels[text.toLowerCase()] || titleCase(text)
}

function formatTimestamp(value) {
  const text = cleanText(value)
  if (!text) return 'Not available'
  const parsed = new Date(text)
  if (Number.isNaN(parsed.getTime())) return text
  return parsed.toLocaleString()
}

function formatRiskScore(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 'Not available'
  return numeric.toFixed(numeric >= 100 ? 0 : 1)
}

function formatHours(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 'Not available'
  return `${numeric.toFixed(numeric >= 10 ? 0 : 1)}h`
}

function formatDays(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 'Not available'
  return `${numeric.toFixed(0)}d`
}

function formatCurrency(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 'Not available'
  return numeric.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 2 })
}

function formatPercent(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 'Not available'
  return `${(numeric * 100).toFixed(0)}%`
}

function formatBooleanState(value, labels = ['No', 'Yes']) {
  if (value == null) return 'Not available'
  return value ? labels[1] : labels[0]
}

function formatImpact(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 'Not available'
  return `${numeric >= 0 ? '+' : ''}${numeric.toFixed(3)}`
}

function looksLikeOpaqueId(value) {
  const text = cleanText(value)
  if (!text) return false
  return /^[0-9a-f]{24,}$/i.test(text) || /^[0-9a-f]{8,}-[0-9a-f-]{12,}$/i.test(text)
}

function formatAssignmentDisplay(value, fallback = 'Unassigned') {
  const text = cleanText(value)
  if (!text) return fallback
  if (looksLikeOpaqueId(text)) return 'Assigned analyst unavailable'
  return text
}

function uniqueTextList(items) {
  const seen = new Set()
  const out = []
  for (const item of toArray(items)) {
    const text = cleanText(item)
    if (!text) continue
    const key = text.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    out.push(text)
  }
  return out
}

function normalizeStep(step) {
  if (typeof step === 'string') return cleanText(step)
  const item = toObject(step)
  return cleanText(item.description || item.step || item.label)
}

function humanizeFeature(value) {
  const text = cleanText(value)
  return text ? titleCase(text) : 'Signal'
}

function summarizeRule(rule) {
  if (typeof rule === 'string') {
    const id = cleanText(rule)
    return id ? { id, description: '' } : null
  }
  const item = toObject(rule)
  const id = cleanText(item.rule_id || item.id || item.rule || item.name)
  const description = cleanText(item.description || item.reason || item.message)
  if (!id && !description) return null
  return {
    id: id || 'Rule Trigger',
    description,
  }
}

function riskBadgeClass(riskBand) {
  const value = String(riskBand || '').toLowerCase()
  const base = 'inline-flex items-center rounded-full px-2.5 py-1 text-[0.7rem] font-semibold uppercase tracking-wide'
  if (value === 'critical') return `${base} bg-red-500/15 text-red-700 dark:text-red-300`
  if (value === 'high') return `${base} bg-orange-500/15 text-orange-700 dark:text-orange-300`
  if (value === 'medium') return `${base} bg-amber-500/15 text-amber-700 dark:text-amber-300`
  if (value === 'low') return `${base} bg-emerald-500/15 text-emerald-700 dark:text-emerald-300`
  return `${base} bg-[var(--surface2)] text-[var(--muted)]`
}

function workflowBadgeClass(status) {
  const value = String(status || '').toLowerCase()
  const base = 'inline-flex items-center rounded-full px-2.5 py-1 text-[0.72rem] font-semibold'
  if (value === 'closed') return `${base} bg-slate-500/15 text-slate-700 dark:text-slate-300`
  if (value === 'sar_filed') return `${base} bg-fuchsia-500/15 text-fuchsia-700 dark:text-fuchsia-300`
  if (value === 'escalated') return `${base} bg-amber-500/15 text-amber-700 dark:text-amber-300`
  if (value === 'under_review') return `${base} bg-blue-500/15 text-blue-700 dark:text-blue-300`
  if (value === 'open') return `${base} bg-emerald-500/15 text-emerald-700 dark:text-emerald-300`
  return `${base} bg-[var(--surface2)] text-[var(--muted)]`
}

function SectionCard({ title, subtitle = '', actions = null, children }) {
  return (
    <section className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-5 shadow-md">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <h2 className="m-0 text-base font-semibold text-[var(--text)]">{title}</h2>
          {subtitle ? <p className="mt-1 mb-0 text-sm text-[var(--muted)]">{subtitle}</p> : null}
        </div>
        {actions}
      </div>
      {children}
    </section>
  )
}

function Metric({ label, value }) {
  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-3 py-2">
      <div className="text-[0.68rem] uppercase tracking-wide text-[var(--muted)]">{label}</div>
      <div className="mt-1 text-sm font-semibold text-[var(--text)]">{value}</div>
    </div>
  )
}

function DetailRow({ label, value }) {
  if (value == null || value === '' || value === 'Not available') return null
  if (!hasContent(value)) return null
  return (
    <div className="flex items-start justify-between gap-4 border-b border-[var(--border)] py-2 last:border-b-0">
      <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">{label}</div>
      <div className="text-right text-sm text-[var(--text)]">{value}</div>
    </div>
  )
}

function CompactList({ title, items, emptyLabel }) {
  const values = uniqueTextList(items)
  return (
    <div>
      <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">{title}</div>
      {values.length ? (
        <ul className="mb-0 mt-2 list-disc pl-5 text-sm text-[var(--text)]">
          {values.map((item) => (
            <li key={item} className="mt-1">{item}</li>
          ))}
        </ul>
      ) : (
        <p className="mb-0 mt-2 text-sm text-[var(--muted)]">{emptyLabel}</p>
      )}
    </div>
  )
}

function RawJsonDisclosure({ label, payload }) {
  const [open, setOpen] = useState(false)
  if (!hasContent(payload)) return null
  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface2)]">
      <button
        type="button"
        className="flex w-full items-center justify-between rounded-lg bg-transparent px-3 py-2 text-left text-sm font-medium text-[var(--text)]"
        onClick={() => setOpen((current) => !current)}
      >
        <span>{open ? `Hide ${label.toLowerCase()}` : label}</span>
        <span className="text-[var(--muted)]">{open ? '-' : '+'}</span>
      </button>
      {open ? (
        <pre className="m-0 overflow-x-auto border-t border-[var(--border)] px-3 py-3 text-xs text-[var(--text)] whitespace-pre-wrap">
          {JSON.stringify(payload, null, 2)}
        </pre>
      ) : null}
    </div>
  )
}

export function AlertDetails() {
  const { id } = useParams()
  const navigate = useNavigate()
  const location = useLocation()
  const { user } = useAuth()
  const [alert, setAlert] = useState(null)
  const [explain, setExplain] = useState(null)
  const [notes, setNotes] = useState([])
  const [noteText, setNoteText] = useState('')
  const [queueItem, setQueueItem] = useState(null)
  const [caseInfo, setCaseInfo] = useState(null)
  const [context, setContext] = useState(null)
  const [networkGraph, setNetworkGraph] = useState(null)
  const [graphLoading, setGraphLoading] = useState(false)
  const [graphError, setGraphError] = useState('')
  const [narrativeDraft, setNarrativeDraft] = useState(null)
  const [narrativeLoading, setNarrativeLoading] = useState(false)
  const [narrativeError, setNarrativeError] = useState('')
  const [copyStatus, setCopyStatus] = useState('')
  const [outcome, setOutcome] = useState(null)
  const [outcomeReason, setOutcomeReason] = useState('')
  const [actionBusy, setActionBusy] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [warnings, setWarnings] = useState([])
  const [caseNotice, setCaseNotice] = useState('')

  const canAddNotes = hasPermission(user, 'add_investigation_notes')
  const canCreateCase = hasPermission(user, 'work_cases')
  const canAssign = hasPermission(user, 'reassign_alerts')
  const canChangeStatus = hasPermission(user, 'change_alert_status')
  const canRecordOutcome = canChangeStatus

  useEffect(() => {
    const createdCaseId = cleanText(location.state?.created_case_id)
    if (createdCaseId) {
      setCaseNotice(`Case ${createdCaseId} created and ready for investigation.`)
    }
  }, [location.state])

  const loadGraph = async (alertId) => {
    setGraphLoading(true)
    setGraphError('')
    try {
      const graph = await api.getNetworkGraph(alertId)
      if (hasContent(graph?.nodes)) {
        setNetworkGraph(graph)
      } else {
        setNetworkGraph(null)
        setGraphError('Network graph is not available for this alert.')
      }
    } catch {
      setNetworkGraph(null)
      setGraphError('Network graph is temporarily unavailable.')
    } finally {
      setGraphLoading(false)
    }
  }

  const loadNarrativeDraft = async (alertId) => {
    setNarrativeLoading(true)
    setNarrativeError('')
    try {
      const draft = await api.getNarrativeDraft(alertId)
      if (hasContent(draft?.narrative) || hasContent(draft?.sections)) {
        setNarrativeDraft(draft)
      } else {
        setNarrativeDraft(null)
        setNarrativeError('Narrative draft is not available for this alert.')
      }
    } catch {
      setNarrativeDraft(null)
      setNarrativeError('Narrative draft is temporarily unavailable.')
    } finally {
      setNarrativeLoading(false)
    }
  }

  const copyNarrative = async () => {
    const text = cleanText(narrativeDraft?.narrative)
    if (!text || !navigator?.clipboard?.writeText) return
    try {
      await navigator.clipboard.writeText(text)
      setCopyStatus('Narrative copied')
    } catch {
      setCopyStatus('Copy failed')
    } finally {
      setTimeout(() => setCopyStatus(''), 1600)
    }
  }

  const load = async () => {
    setLoading(true)
    setError('')
    setWarnings([])

    try {
      const [alertResult, explainResult, notesResult, queueResult, contextResult, outcomeResult] = await Promise.allSettled([
        api.getAlert(id),
        api.getAlertExplain(id),
        api.getAlertNotes(id),
        api.getWorkQueue({ limit: 500 }),
        api.getInvestigationContext(id),
        api.getAlertOutcome(id),
      ])

      if (alertResult.status !== 'fulfilled') {
        throw alertResult.reason
      }

      const alertPayload = alertResult.value || null
      const explainPayload = explainResult.status === 'fulfilled' ? (explainResult.value || null) : null
      const notesPayload = notesResult.status === 'fulfilled' ? (notesResult.value || { notes: [] }) : { notes: [] }
      const queuePayload = queueResult.status === 'fulfilled' ? (queueResult.value || { queue: [] }) : { queue: [] }
      const contextPayload = contextResult.status === 'fulfilled' ? (contextResult.value || null) : null
      const outcomePayload = outcomeResult.status === 'fulfilled' ? (outcomeResult.value || null) : null

      setAlert(alertPayload)
      setExplain(explainPayload)
      setNotes(toArray(notesPayload.notes))
      setContext(contextPayload)
      setOutcome(outcomePayload)

      const matchedQueueItem = toArray(queuePayload.queue).find((item) => String(item?.alert_id) === String(id)) || null
      setQueueItem(matchedQueueItem)

      const resolvedCase =
        toObject(contextPayload?.case_status).case_id
          ? toObject(contextPayload.case_status)
          : matchedQueueItem?.case_id
            ? {
                case_id: matchedQueueItem.case_id,
                status: matchedQueueItem.case_status || matchedQueueItem.status || null,
                assigned_to: matchedQueueItem.assigned_to || null,
              }
            : null
      setCaseInfo(resolvedCase)

      const loadWarnings = []
      const hasExplainFallback = hasContent(explainPayload?.risk_explanation) || hasContent(alertPayload?.risk_explain_json)
      if (explainResult.status === 'rejected' && !hasExplainFallback) {
        loadWarnings.push('Detailed explanation data is temporarily unavailable.')
      }
      if (notesResult.status === 'rejected') {
        loadWarnings.push('Investigation notes could not be loaded.')
      }
      if (queueResult.status === 'rejected') {
        loadWarnings.push('Current workflow status could not be refreshed.')
      }
      if (contextResult.status === 'rejected') {
        loadWarnings.push('Analyst summary, recommended next steps, and SAR support data are temporarily unavailable.')
      }
      const outcomeErrorMessage = cleanText(outcomeResult.status === 'rejected' ? outcomeResult.reason?.message : '')
      if (outcomeResult.status === 'rejected' && !/no outcome recorded/i.test(outcomeErrorMessage)) {
        loadWarnings.push('Recorded outcome details could not be loaded.')
      }
      setWarnings(loadWarnings)

      const embeddedGraph = contextPayload?.network_graph
      if (hasContent(embeddedGraph?.nodes)) {
        setNetworkGraph(embeddedGraph)
        setGraphError('')
      } else {
        setNetworkGraph(null)
        void loadGraph(id)
      }

      const embeddedNarrative = contextPayload?.narrative_draft
      if (hasContent(embeddedNarrative?.narrative) || hasContent(embeddedNarrative?.sections)) {
        setNarrativeDraft(embeddedNarrative)
        setNarrativeError('')
      } else {
        setNarrativeDraft(null)
        void loadNarrativeDraft(id)
      }
    } catch (err) {
      setError(err?.message || 'Failed to load alert details')
      setAlert(null)
      setExplain(null)
      setNotes([])
      setQueueItem(null)
      setCaseInfo(null)
      setContext(null)
      setOutcome(null)
      setNetworkGraph(null)
      setNarrativeDraft(null)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [id])

  const riskExplain = useMemo(() => tryParseJson(alert?.risk_explain_json, {}), [alert])
  const rulesJson = useMemo(() => toArray(tryParseJson(alert?.rules_json, [])), [alert])
  const normalizedExplanation = useMemo(
    () => normalizeExplanationPayload(explain?.risk_explanation || riskExplain || {}),
    [explain, riskExplain],
  )

  const explanationView = useMemo(() => toObject(explain?.human_interpretation_view), [explain])
  const explanationHeadline = useMemo(
    () => (
      cleanText(explanationView.headline) ||
      cleanText(explain?.summary_text) ||
      cleanText(explain?.human_interpretation?.summary_text) ||
      'Model and rule signals suggest this alert requires analyst review.'
    ),
    [explain, explanationView],
  )
  const explanationReasons = useMemo(
    () => uniqueTextList([
      ...toArray(explanationView.reasons),
      ...toArray(explain?.key_reasons),
      ...toArray(explain?.human_interpretation?.key_reasons),
      ...toArray(normalizedExplanation.risk_reason_codes).map((code) => `Reason code: ${code}`),
    ]).slice(0, 6),
    [explain, explanationView, normalizedExplanation],
  )
  const explanationPatterns = useMemo(
    () => uniqueTextList([
      ...toArray(explanationView.patterns),
      ...toArray(explain?.aml_patterns),
      ...toArray(explain?.human_interpretation?.aml_patterns),
    ]).slice(0, 4),
    [explain, explanationView],
  )
  const fallbackExplanationMessage = useMemo(() => {
    if (!normalizedExplanation.is_fallback) return ''
    const detail = cleanText(normalizedExplanation.explanation_warning)
    return detail
      ? `Heuristic feature highlights are shown because model attribution is not available. ${detail}`
      : 'Heuristic feature highlights are shown because model attribution is not available.'
  }, [normalizedExplanation])
  const driverRows = useMemo(() => {
    const directDrivers = toArray(normalizedExplanation.feature_attribution)
    const fallbackDrivers = toArray(explain?.feature_contributions)
    const source = directDrivers.length ? directDrivers : fallbackDrivers
    return source
      .map((item) => ({
        feature: humanizeFeature(item?.feature || item?.name),
        value: item?.value ?? item?.contribution ?? item?.shap_value ?? null,
        shapValue: item?.shap_value ?? item?.contribution ?? null,
        magnitude: Math.abs(Number(item?.magnitude ?? item?.shap_value ?? item?.value ?? item?.contribution ?? 0)),
      }))
      .filter((item) => item.feature && Number.isFinite(item.magnitude))
      .sort((left, right) => right.magnitude - left.magnitude)
      .slice(0, 6)
  }, [explain, normalizedExplanation])

  const ruleSignals = useMemo(
    () => rulesJson.map((item) => summarizeRule(item)).filter(Boolean),
    [rulesJson],
  )

  const investigationSummary = useMemo(() => toObject(context?.investigation_summary), [context])
  const customerProfile = useMemo(() => toObject(context?.customer_profile), [context])
  const accountProfile = useMemo(() => toObject(context?.account_profile), [context])
  const behaviorBaseline = useMemo(() => toObject(context?.behavior_baseline), [context])
  const counterpartySummary = useMemo(() => toObject(context?.counterparty_summary), [context])
  const geographySummary = useMemo(() => toObject(context?.geography_payment_summary), [context])
  const screeningSummary = useMemo(() => toObject(context?.screening_summary), [context])
  const dataAvailability = useMemo(() => toObject(context?.data_availability), [context])
  const summaryObservations = useMemo(
    () => uniqueTextList(toArray(investigationSummary.key_observations)).slice(0, 8),
    [investigationSummary],
  )
  const recommendedSteps = useMemo(
    () => uniqueTextList([
      ...toArray(context?.investigation_steps?.steps).map((step) => normalizeStep(step)),
      ...toArray(narrativeDraft?.sections?.recommended_follow_up),
      ...toArray(explain?.analyst_focus_points),
    ]).slice(0, 10),
    [context, explain, narrativeDraft],
  )

  const globalSignals = useMemo(() => toArray(context?.global_signals), [context])
  const modelMetadata = useMemo(() => toObject(context?.model_metadata), [context])
  const sarDraft = useMemo(() => toObject(context?.sar_draft), [context])
  const recordedOutcome = useMemo(() => toObject(outcome || context?.outcome), [context, outcome])

  const caseId = cleanText(caseInfo?.case_id || queueItem?.case_id)
  const caseStatus = cleanText(caseInfo?.status || queueItem?.case_status || null)
  const hasCase = Boolean(caseId)
  const riskBand = cleanText(alert?.risk_band || investigationSummary.risk_band)
  const currentAssignment = cleanText(caseInfo?.assigned_to || queueItem?.assigned_to)
  const assignmentDisplay = useMemo(
    () => formatAssignmentDisplay(customerProfile.assigned_analyst_label || currentAssignment),
    [customerProfile, currentAssignment],
  )
  const typology = cleanText(alert?.typology || investigationSummary.typology)
  const segment = cleanText(alert?.segment || investigationSummary.segment)
  const summaryCustomer = cleanText(
    customerProfile.customer_label || investigationSummary.customer || alert?.customer_name || alert?.user_id,
  )
  const workflowStatus = cleanText(queueItem?.status)
  const coverageStatus = cleanText(dataAvailability.coverage_status)
  const freshnessStatus = cleanText(dataAvailability.freshness_status)
  const missingSections = useMemo(
    () => uniqueTextList(toArray(dataAvailability.missing_sections).map((item) => titleCase(String(item || '').replace(/_/g, ' ')))),
    [dataAvailability],
  )
  const caseActionText = useMemo(() => {
    if (hasCase && caseStatus === 'closed') {
      return 'Case is closed. Review the final outcome and supporting notes if follow-up is required.'
    }
    if (hasCase && caseStatus === 'escalated') {
      return 'Case is escalated. Prepare supporting evidence for manager or compliance review.'
    }
    if (hasCase) {
      return 'Case is active. Continue investigator review, document findings, and update the governed workflow.'
    }
    return 'No case exists yet. Create a case to move this alert into the governed investigation workflow.'
  }, [caseStatus, hasCase])

  const screeningStatusLabel = useMemo(() => {
    const normalized = String(screeningSummary.screening_status || '').toLowerCase()
    if (normalized === 'hits_found') return 'Screening hits require review'
    if (normalized === 'no_hits') return 'No screening hits identified'
    return 'Screening data unavailable'
  }, [screeningSummary])

  const addNote = async () => {
    const text = noteText.trim()
    if (!text) return
    try {
      await api.addAlertNote(id, text)
      setNoteText('')
      await load()
    } catch (err) {
      setError(err?.message || 'Failed to add note')
    }
  }

  const createCase = async () => {
    try {
      setActionBusy(true)
      const created = await api.createInvestigationCase(id)
      if (created?.case_id) {
        setCaseNotice(`Case ${created.case_id} created and ready for investigation.`)
      }
      await load()
    } catch (err) {
      setError(err?.message || 'Failed to create case')
    } finally {
      setActionBusy(false)
    }
  }

  const assignToMe = async () => {
    if (!user?.user_id) return
    try {
      setActionBusy(true)
      await api.workflowAssignAlert(id, user.user_id)
      await load()
    } catch (err) {
      setError(err?.message || 'Failed to assign alert')
    } finally {
      setActionBusy(false)
    }
  }

  const escalateAlert = async () => {
    try {
      setActionBusy(true)
      await api.workflowEscalateAlert(id, 'manual_escalation')
      await load()
    } catch (err) {
      setError(err?.message || 'Failed to escalate alert')
    } finally {
      setActionBusy(false)
    }
  }

  const closeAlert = async () => {
    try {
      setActionBusy(true)
      await api.workflowCloseAlert(id, 'manual_close')
      await load()
    } catch (err) {
      setError(err?.message || 'Failed to close alert')
    } finally {
      setActionBusy(false)
    }
  }

  const saveOutcome = async (decision) => {
    if (!decision) return
    try {
      setActionBusy(true)
      const payload = {
        analyst_decision: decision,
        decision_reason: outcomeReason || null,
        model_version: cleanText(context?.model_metadata?.model_version || alert?.model_version) || null,
        risk_score_at_decision: Number(alert?.risk_score ?? 0),
      }
      const res = await api.recordAlertOutcome(id, payload)
      setOutcome(res)
      setOutcomeReason('')
    } catch (err) {
      setError(err?.message || 'Failed to record outcome')
    } finally {
      setActionBusy(false)
    }
  }

  if (loading && !alert) {
    return (
      <div className="max-w-[1200px] mx-auto px-6 py-8">
        <p className="text-sm text-[var(--muted)]">Loading alert and case context...</p>
      </div>
    )
  }

  if (!alert) {
    return (
      <div className="max-w-[1200px] mx-auto px-6 py-8">
        <div className="rounded-xl border border-red-500/25 bg-red-500/10 p-5 text-sm text-red-600 dark:text-red-400">
          {error || 'Alert details are not available.'}
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-[1200px] mx-auto px-6 py-6 space-y-5">
      <SectionCard
        title={`Alert ${alert.alert_id || id}`}
        subtitle={summaryCustomer ? `Analyst workspace for ${summaryCustomer}.` : 'Analyst investigation workspace.'}
        actions={(
          <div className="flex flex-wrap gap-2">
            <button type="button" onClick={() => navigate(-1)} className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] bg-transparent">
              Back
            </button>
            <Link to="/investigation/dashboard" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] no-underline">
              Dashboard
            </Link>
            {hasCase ? (
              <Link
                to={`/investigation/cases/${caseId}`}
                className="rounded-md bg-[var(--accent2)] px-3 py-2 text-sm font-medium text-white no-underline hover:brightness-105 dark:bg-white dark:text-[#0c0c0c]"
              >
                View Case
              </Link>
            ) : canCreateCase ? (
              <button
                type="button"
                className="rounded-md bg-[var(--accent2)] px-3 py-2 text-sm font-medium text-white hover:brightness-105 disabled:opacity-60 dark:bg-white dark:text-[#0c0c0c]"
                onClick={createCase}
                disabled={actionBusy}
              >
                Create Case
              </button>
            ) : null}
          </div>
        )}
      >
        <div className="flex flex-wrap items-center gap-2">
          <span className={riskBadgeClass(riskBand)}>{formatDisplayValue(riskBand, 'Unrated')}</span>
          <span className={workflowBadgeClass(caseStatus || workflowStatus || 'open')}>
            {formatStatus(caseStatus || workflowStatus || 'open')}
          </span>
          {hasCase ? (
            <span className="inline-flex items-center rounded-full bg-blue-500/10 px-2.5 py-1 text-[0.72rem] font-semibold text-blue-700 dark:text-blue-300">
              Case {caseId}
            </span>
          ) : (
            <span className="inline-flex items-center rounded-full bg-[var(--surface2)] px-2.5 py-1 text-[0.72rem] font-semibold text-[var(--muted)]">
              No Case
            </span>
          )}
          {currentAssignment ? (
            <span className="inline-flex items-center rounded-full bg-[var(--surface2)] px-2.5 py-1 text-[0.72rem] font-semibold text-[var(--muted)]">
              Assigned: {assignmentDisplay}
            </span>
          ) : null}
        </div>

        {caseNotice ? (
          <div className="mt-4 rounded-lg border border-emerald-500/25 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-700 dark:text-emerald-300">
            {caseNotice}
          </div>
        ) : null}

        {error ? (
          <div className="mt-4 rounded-lg border border-red-500/25 bg-red-500/10 px-4 py-3 text-sm text-red-600 dark:text-red-400">
            {error}
          </div>
        ) : null}

        <p className="mt-4 text-sm text-[var(--text)]">{caseActionText}</p>

        <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Metric label="Risk Score" value={formatRiskScore(alert.risk_score)} />
          <Metric label="Case Status" value={formatStatus(caseStatus, hasCase ? 'Open' : 'No case')} />
          <Metric label="Typology" value={formatDisplayValue(typology)} />
          <Metric label="Segment" value={formatDisplayValue(segment)} />
          <Metric label="Assignment" value={assignmentDisplay} />
          <Metric label="Workflow Status" value={formatStatus(workflowStatus, 'Open')} />
          <Metric label="Alert Age" value={formatHours(queueItem?.alert_age_hours)} />
          <Metric label="Customer" value={formatDisplayValue(summaryCustomer)} />
        </div>
      </SectionCard>

      {warnings.length ? (
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-700 dark:text-amber-300">
          <div className="font-semibold">Some supporting data could not be loaded.</div>
          <ul className="mt-2 mb-0 list-disc pl-5">
            {warnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="grid gap-5 lg:grid-cols-[1.6fr_1fr]">
        <SectionCard
          title="Analyst Summary"
          subtitle="Use this section first to understand why the alert matters before reviewing raw technical detail."
        >
          <p className="m-0 text-sm text-[var(--text)]">{explanationHeadline}</p>
          {summaryObservations.length ? (
            <ul className="mt-4 mb-0 list-disc pl-5 text-sm text-[var(--text)]">
              {summaryObservations.map((item) => (
                <li key={item} className="mt-1">{item}</li>
              ))}
            </ul>
          ) : (
            <p className="mt-4 mb-0 text-sm text-[var(--muted)]">No concise analyst summary is available for this alert.</p>
          )}
        </SectionCard>

        <SectionCard
          title="Current Status & Actions"
          subtitle="Current workflow state, case state, and investigator actions."
        >
          <div className="space-y-2 text-sm text-[var(--text)]">
            <div><strong>Case ID:</strong> {hasCase ? caseId : 'No case yet'}</div>
            <div><strong>Case Status:</strong> {formatStatus(caseStatus, hasCase ? 'Open' : 'No case')}</div>
            <div><strong>Assigned To:</strong> {assignmentDisplay}</div>
            <div><strong>Overdue Review:</strong> {queueItem?.overdue_review ? 'Yes' : 'No'}</div>
          </div>

          <p className="mt-4 mb-0 text-xs text-[var(--muted)]">
            {hasCase
              ? 'Case already exists. Use the primary View Case action above to open the governed case workspace.'
              : canCreateCase
                ? 'Use the primary Create Case action above to start the governed investigation workflow.'
                : 'Your current role cannot create cases.'}
          </p>

          <div className="mt-4">
            <h3 className="m-0 text-sm font-semibold text-[var(--text)]">Workflow Actions</h3>
            {canAssign || canChangeStatus ? (
              <div className="mt-2 flex flex-wrap gap-2">
                {canAssign ? (
                  <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60" onClick={assignToMe} disabled={actionBusy}>
                    Assign to Me
                  </button>
                ) : null}
                {canChangeStatus ? (
                  <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60" onClick={escalateAlert} disabled={actionBusy}>
                    Escalate
                  </button>
                ) : null}
                {canChangeStatus ? (
                  <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60" onClick={closeAlert} disabled={actionBusy}>
                    Close Alert
                  </button>
                ) : null}
              </div>
            ) : (
              <p className="mt-2 mb-0 text-xs text-[var(--muted)]">Workflow actions require backend permissions from your current session.</p>
            )}
          </div>
        </SectionCard>
      </div>

      <div className="grid gap-5 lg:grid-cols-2">
        <SectionCard
          title="Customer & Account Profile"
          subtitle="Known customer and account context available before deeper technical review."
        >
          {hasContent(customerProfile) || hasContent(accountProfile) ? (
            <div className="grid gap-4 lg:grid-cols-2">
              <div className="rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
                <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Customer Profile</div>
                <div className="mt-2">
                  <DetailRow label="Customer" value={formatDisplayValue(customerProfile.customer_label)} />
                  <DetailRow label="Segment" value={formatDisplayValue(customerProfile.segment)} />
                  <DetailRow label="Risk Tier" value={formatStatus(customerProfile.risk_tier)} />
                  <DetailRow label="Country" value={formatDisplayValue(customerProfile.country)} />
                  <DetailRow label="Business Purpose" value={formatDisplayValue(customerProfile.business_purpose)} />
                  <DetailRow label="KYC Status" value={formatStatus(customerProfile.kyc_status)} />
                  <DetailRow label="PEP Flag" value={formatBooleanState(customerProfile.pep_flag)} />
                  <DetailRow label="Sanctions Flag" value={formatBooleanState(customerProfile.sanctions_flag)} />
                  <DetailRow label="Onboarded" value={formatTimestamp(customerProfile.onboarded_at)} />
                </div>
              </div>
              <div className="rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
                <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Account Profile</div>
                <div className="mt-2">
                  <DetailRow label="Account" value={formatDisplayValue(accountProfile.account_label)} />
                  <DetailRow label="Account Type" value={formatStatus(accountProfile.account_type)} />
                  <DetailRow label="Status" value={formatStatus(accountProfile.account_status)} />
                  <DetailRow label="Opened" value={formatTimestamp(accountProfile.opened_at)} />
                  <DetailRow label="Account Age" value={formatDays(accountProfile.account_age_days)} />
                </div>
              </div>
            </div>
          ) : (
            <p className="m-0 text-sm text-[var(--muted)]">Customer and account profile details are not yet available for this alert.</p>
          )}
        </SectionCard>

        <SectionCard
          title="Behavior vs Baseline"
          subtitle="Recent activity compared with known account history and prior investigation workload."
        >
          {hasContent(behaviorBaseline) ? (
            <div className="space-y-4">
              <p className="m-0 text-sm text-[var(--text)]">
                {cleanText(behaviorBaseline.deviation_summary) || 'Recent account history is limited; baseline comparison is not yet established.'}
              </p>
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                <Metric label="Baseline Avg Amount" value={formatCurrency(behaviorBaseline.baseline_avg_amount)} />
                <Metric label="Current Window Outflow" value={formatCurrency(behaviorBaseline.current_window_outflow)} />
                <Metric label="Current Window Inflow" value={formatCurrency(behaviorBaseline.current_window_inflow)} />
                <Metric label="Baseline Tx Count" value={formatDisplayValue(behaviorBaseline.baseline_tx_count)} />
                <Metric label="Current Window Tx Count" value={formatDisplayValue(behaviorBaseline.current_window_tx_count)} />
                <Metric label="Prior Alerts 30d" value={formatDisplayValue(behaviorBaseline.prior_alert_count_30d)} />
                <Metric label="Prior Alerts 90d" value={formatDisplayValue(behaviorBaseline.prior_alert_count_90d)} />
                <Metric label="Prior Cases 90d" value={formatDisplayValue(behaviorBaseline.prior_case_count_90d)} />
                <Metric label="Baseline Monthly Outflow" value={formatCurrency(behaviorBaseline.baseline_monthly_outflow)} />
              </div>
            </div>
          ) : (
            <p className="m-0 text-sm text-[var(--muted)]">Behavioral baseline data is not yet available for this alert.</p>
          )}
        </SectionCard>
      </div>

      <div className="grid gap-5 lg:grid-cols-2">
        <SectionCard
          title="Counterparty & Geography"
          subtitle="Observed counterparties, jurisdictions, and payment context available for this alert."
        >
          {hasContent(counterpartySummary) || hasContent(geographySummary) ? (
            <div className="grid gap-4 lg:grid-cols-2">
              <CompactList
                title="Top Counterparties"
                items={counterpartySummary.top_counterparties}
                emptyLabel="Counterparty details unavailable."
              />
              <div className="rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
                <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Context Signals</div>
                <div className="mt-2">
                  <DetailRow label="New Counterparty Share" value={formatPercent(counterpartySummary.new_counterparty_share)} />
                  <DetailRow label="Recurring Counterparty Share" value={formatPercent(counterpartySummary.recurring_counterparty_share)} />
                  <DetailRow label="Counterparty Bank Count" value={formatDisplayValue(counterpartySummary.counterparty_bank_count)} />
                  <DetailRow label="Cross-Border" value={formatBooleanState(geographySummary.is_cross_border)} />
                </div>
              </div>
              <CompactList
                title="Countries Involved"
                items={geographySummary.countries_involved.length ? geographySummary.countries_involved : counterpartySummary.counterparty_countries}
                emptyLabel="Jurisdiction details unavailable."
              />
              <CompactList
                title="Payment & Currency Mix"
                items={[...toArray(geographySummary.payment_channels), ...toArray(geographySummary.currency_mix)]}
                emptyLabel="Payment channel and currency details unavailable."
              />
            </div>
          ) : (
            <p className="m-0 text-sm text-[var(--muted)]">Counterparty and geography context is not yet available for this alert.</p>
          )}
        </SectionCard>

        <SectionCard
          title="Screening & Data Coverage"
          subtitle="Screening visibility and enrichment coverage for this investigation workspace."
        >
          <div className="space-y-4">
            <div className="rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
              <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Screening Status</div>
              <p className="mb-0 mt-2 text-sm text-[var(--text)]">{screeningStatusLabel}</p>
              <div className="mt-3">
                <DetailRow label="Screening Checked" value={formatTimestamp(screeningSummary.screening_checked_at)} />
                <DetailRow label="Coverage Status" value={formatStatus(coverageStatus)} />
                <DetailRow label="Freshness Status" value={formatStatus(freshnessStatus)} />
              </div>
            </div>

            <CompactList title="Sanctions Hits" items={screeningSummary.sanctions_hits} emptyLabel={screeningSummary.screening_status === 'no_hits' ? 'No sanctions hits identified.' : 'Sanctions screening unavailable.'} />
            <CompactList title="Watchlist Hits" items={screeningSummary.watchlist_hits} emptyLabel={screeningSummary.screening_status === 'no_hits' ? 'No watchlist hits identified.' : 'Watchlist screening unavailable.'} />
            <CompactList title="PEP Indicators" items={screeningSummary.pep_hits} emptyLabel={screeningSummary.screening_status === 'no_hits' ? 'No PEP indicators identified.' : 'PEP screening unavailable.'} />
            <CompactList title="Coverage Gaps" items={missingSections} emptyLabel="No known enrichment coverage gaps." />
          </div>
        </SectionCard>
      </div>

      <SectionCard
        title="Recommended Next Steps"
        subtitle="Ordered actions for investigator follow-up and compliance escalation review."
      >
        {recommendedSteps.length ? (
          <ol className="mb-0 mt-0 list-decimal pl-5 text-sm text-[var(--text)]">
            {recommendedSteps.map((step) => (
              <li key={step} className="mt-1">{step}</li>
            ))}
          </ol>
        ) : (
          <p className="m-0 text-sm text-[var(--muted)]">No next-step guidance is currently available.</p>
        )}
      </SectionCard>

      <div className="grid gap-5 lg:grid-cols-2">
        <SectionCard
          title="Narrative Draft"
          subtitle="Draft wording for analyst review before case notes or escalation are finalized."
          actions={(
            <div className="flex items-center gap-2">
              <button
                type="button"
                className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60"
                onClick={copyNarrative}
                disabled={!cleanText(narrativeDraft?.narrative)}
              >
                Copy Narrative
              </button>
              {copyStatus ? <span className="text-xs text-[var(--muted)]">{copyStatus}</span> : null}
            </div>
          )}
        >
          <div className="rounded-lg border border-blue-500/20 bg-blue-500/5 px-4 py-3 text-sm text-[var(--text)]">
            Analyst-support draft only. This text does not determine that suspicious activity occurred and does not authorize any filing decision. Investigator validation and compliance approval remain required.
          </div>
          {narrativeLoading ? <p className="mt-4 mb-0 text-sm text-[var(--muted)]">Loading narrative draft...</p> : null}
          {narrativeError ? <p className="mt-4 mb-0 text-sm text-amber-700 dark:text-amber-300">{narrativeError}</p> : null}
          {cleanText(narrativeDraft?.sections?.activity_summary) ? (
            <div className="mt-4 rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
              <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Activity Summary</div>
              <p className="mb-0 mt-2 text-sm text-[var(--text)]">{narrativeDraft.sections.activity_summary}</p>
            </div>
          ) : null}
          <div className="mt-4 rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
            <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Draft Text</div>
            <p className="mb-0 mt-2 whitespace-pre-wrap text-sm leading-6 text-[var(--text)]">
              {cleanText(narrativeDraft?.narrative) || 'Narrative draft is not available.'}
            </p>
          </div>
        </SectionCard>

        <SectionCard
          title="Preliminary SAR/STR Support Draft"
          subtitle="Preliminary escalation support text for compliance review, subject to investigator validation."
        >
          <div className="rounded-lg border border-amber-500/25 bg-amber-500/10 px-4 py-3 text-sm text-[var(--text)]">
            {cleanText(sarDraft.disclaimer) ||
              'ALTHEA provides investigation support only. Final SAR/STR filing decisions must be made by authorized compliance staff.'}
          </div>
          {cleanText(sarDraft.narrative) ? (
            <div className="mt-4 rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
              <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Draft Narrative</div>
              <p className="mb-0 mt-2 whitespace-pre-wrap text-sm leading-6 text-[var(--text)]">{sarDraft.narrative}</p>
            </div>
          ) : (
            <p className="mt-4 mb-0 text-sm text-[var(--muted)]">No SAR/STR support draft is available for this alert.</p>
          )}
          {uniqueTextList(toArray(sarDraft.risk_indicators)).length ? (
            <div className="mt-4">
              <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Supporting Indicators</div>
              <ul className="mb-0 mt-2 list-disc pl-5 text-sm text-[var(--text)]">
                {uniqueTextList(toArray(sarDraft.risk_indicators)).map((item) => (
                  <li key={item} className="mt-1">{item}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </SectionCard>
      </div>

      <div className="grid gap-5 lg:grid-cols-[1.25fr_1fr]">
        <SectionCard
          title="Supporting Evidence"
          subtitle="Primary explanation summary, ranked drivers, and triggered rule evidence."
        >
          <div className="space-y-4">
            <div>
              <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Explanation Summary</div>
              <p className="mb-0 mt-2 text-sm text-[var(--text)]">{explanationHeadline}</p>
              {(normalizedExplanation.explanation_method === 'shap' || normalizedExplanation.explanation_method === 'tree_shap') ? (
                <p className="mb-0 mt-2 text-xs text-blue-700 dark:text-blue-300">Model-based explanation available.</p>
              ) : null}
              {normalizedExplanation.explanation_method === 'unavailable' ? (
                <p className="mb-0 mt-2 text-xs text-[var(--muted)]">Detailed model attribution is not available for this alert.</p>
              ) : null}
              {normalizedExplanation.is_fallback ? (
                <div className="mt-3 rounded-lg border border-amber-500/25 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
                  {fallbackExplanationMessage}
                </div>
              ) : null}
            </div>

            {explanationReasons.length ? (
              <div>
                <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Key Reasons</div>
                <ul className="mb-0 mt-2 list-disc pl-5 text-sm text-[var(--text)]">
                  {explanationReasons.map((item) => (
                    <li key={item} className="mt-1">{item}</li>
                  ))}
                </ul>
              </div>
            ) : null}

            {driverRows.length ? (
              <div>
                <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Top Drivers</div>
                <div className="mt-2 overflow-x-auto">
                  <table className="w-full border-collapse text-sm">
                    <thead>
                      <tr>
                        <th className="border-b border-[var(--border)] px-3 py-2 text-left text-[0.72rem] uppercase tracking-wide text-[var(--muted)]">Driver</th>
                        <th className="border-b border-[var(--border)] px-3 py-2 text-left text-[0.72rem] uppercase tracking-wide text-[var(--muted)]">Impact</th>
                      </tr>
                    </thead>
                    <tbody>
                      {driverRows.map((row) => (
                        <tr key={row.feature}>
                          <td className="border-b border-[var(--border)] px-3 py-2 text-[var(--text)]">{row.feature}</td>
                          <td className="border-b border-[var(--border)] px-3 py-2 text-[var(--text)]">{formatImpact(row.shapValue ?? row.value)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}

            {ruleSignals.length ? (
              <div>
                <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Triggered Rules</div>
                <ul className="mb-0 mt-2 list-disc pl-5 text-sm text-[var(--text)]">
                  {ruleSignals.map((rule) => (
                    <li key={`${rule.id}-${rule.description || 'rule'}`} className="mt-1">
                      <strong>{rule.id}</strong>
                      {rule.description ? ` - ${rule.description}` : ''}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <p className="m-0 text-sm text-[var(--muted)]">No rule hits are available for this alert.</p>
            )}

            {explanationPatterns.length ? (
              <div>
                <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Observed Patterns</div>
                <ul className="mb-0 mt-2 list-disc pl-5 text-sm text-[var(--text)]">
                  {explanationPatterns.map((item) => (
                    <li key={item} className="mt-1">{item}</li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>
        </SectionCard>

        <SectionCard
          title="Additional Intelligence"
          subtitle="Cross-tenant pattern signals and model context that may support deeper review."
        >
          <div>
            <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Cross-Tenant Signals</div>
            {globalSignals.length ? (
              <ul className="mb-0 mt-2 list-disc pl-5 text-sm text-[var(--text)]">
                {globalSignals.map((signal, index) => {
                  const item = toObject(signal)
                  const description = cleanText(item.description)
                  const signalType = cleanText(item.signal_type)
                  const tenants = Number(item.tenant_count)
                  const alerts = Number(item.alert_count)
                  const label = description || (signalType ? `${titleCase(signalType)} pattern identified across peer institutions.` : '')
                  return (
                    <li key={`${signalType || 'signal'}-${index}`} className="mt-1">
                      {label || 'Peer pattern signal available.'}
                      {Number.isFinite(tenants) ? ` Tenant matches: ${tenants}.` : ''}
                      {Number.isFinite(alerts) ? ` Alert matches: ${alerts}.` : ''}
                    </li>
                  )
                })}
              </ul>
            ) : (
              <p className="mb-0 mt-2 text-sm text-[var(--muted)]">No cross-tenant signals.</p>
            )}
          </div>

          <div className="mt-4 space-y-2 text-sm text-[var(--text)]">
            <div className="text-[0.72rem] font-semibold uppercase tracking-wide text-[var(--muted)]">Model Context</div>
            <div><strong>Model Version:</strong> {formatDisplayValue(modelMetadata.model_version || alert.model_version)}</div>
            <div><strong>Approval State:</strong> {formatDisplayValue(modelMetadata.approval_state)}</div>
            <div><strong>Scored At:</strong> {formatTimestamp(modelMetadata.scoring_timestamp)}</div>
            <div><strong>Monitoring Snapshot:</strong> {formatTimestamp(modelMetadata.monitoring_timestamp)}</div>
          </div>
        </SectionCard>
      </div>

      <SectionCard
        title="Network Graph"
        subtitle="Entity and relationship view for linked accounts, customers, and counterparties."
      >
        {graphLoading ? <p className="m-0 text-sm text-[var(--muted)]">Loading network graph...</p> : null}
        {graphError ? <p className="m-0 text-sm text-amber-700 dark:text-amber-300">{graphError}</p> : null}
        <InvestigationGraph graph={networkGraph} />
      </SectionCard>

      <div className="grid gap-5 lg:grid-cols-[1fr_1fr]">
        <SectionCard
          title="Outcome Feedback"
          subtitle="Record the investigator decision and retain supporting context for review."
        >
          <div className="space-y-2 text-sm text-[var(--text)]">
            <div><strong>Current Outcome:</strong> {formatDecision(recordedOutcome.analyst_decision)}</div>
            <div><strong>Reason:</strong> {formatDisplayValue(recordedOutcome.decision_reason, 'No outcome reason provided')}</div>
            <div><strong>Recorded By:</strong> {formatDisplayValue(recordedOutcome.analyst_id)}</div>
            <div><strong>Recorded At:</strong> {formatTimestamp(recordedOutcome.created_at || recordedOutcome.recorded_at)}</div>
          </div>

          {canRecordOutcome ? (
            <>
              <div className="mt-4 flex flex-wrap gap-2">
                <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60" onClick={() => saveOutcome('true_positive')} disabled={actionBusy}>Record True Positive</button>
                <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60" onClick={() => saveOutcome('false_positive')} disabled={actionBusy}>Record False Positive</button>
                <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60" onClick={() => saveOutcome('escalated')} disabled={actionBusy}>Record Escalated</button>
                <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)] disabled:opacity-60" onClick={() => saveOutcome('sar_filed')} disabled={actionBusy}>Record Human-Reviewed SAR/STR Filing</button>
              </div>
              <textarea
                className="mt-4 min-h-[88px] w-full rounded-md border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm text-[var(--text)]"
                value={outcomeReason}
                onChange={(event) => setOutcomeReason(event.target.value)}
                placeholder="Outcome rationale (optional)"
              />
            </>
          ) : (
            <p className="mt-4 mb-0 text-xs text-[var(--muted)]">Outcome recording is disabled for your current permission set.</p>
          )}
        </SectionCard>

        <SectionCard
          title="Investigation Notes"
          subtitle="Investigator-authored notes and rationale for the governed case record."
        >
          {canAddNotes ? (
            <div className="flex gap-2">
              <input
                className="flex-1 rounded-md border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm text-[var(--text)]"
                value={noteText}
                onChange={(event) => setNoteText(event.target.value)}
                placeholder="Add investigation note"
              />
              <button type="button" className="rounded-md border border-[var(--border)] px-3 py-2 text-sm text-[var(--text)]" onClick={addNote}>
                Save
              </button>
            </div>
          ) : (
            <p className="m-0 text-xs text-[var(--muted)]">Your role cannot add investigation notes.</p>
          )}

          {notes.length ? (
            <div className="mt-4 space-y-3">
              {notes.map((note) => (
                <div key={note.id} className="rounded-lg border border-[var(--border)] bg-[var(--surface2)] px-4 py-3">
                  <p className="m-0 text-sm text-[var(--text)]">{note.note_text}</p>
                  <p className="mb-0 mt-2 text-xs text-[var(--muted)]">
                    {formatDisplayValue(note.user_id)} | {formatTimestamp(note.created_at)}
                  </p>
                </div>
              ))}
            </div>
          ) : (
            <p className="mt-4 mb-0 text-sm text-[var(--muted)]">No investigation notes yet.</p>
          )}
        </SectionCard>
      </div>

      <SectionCard
        title="Raw Technical Details"
        subtitle="Secondary technical payloads for advanced review and troubleshooting. Hidden by default."
      >
        <div className="space-y-3">
          <RawJsonDisclosure label="View raw explanation" payload={normalizedExplanation} />
          <RawJsonDisclosure label="View raw intelligence payload" payload={context} />
          <RawJsonDisclosure label="View raw alert payload" payload={alert} />
          <RawJsonDisclosure label="View raw graph payload" payload={networkGraph} />
          <RawJsonDisclosure label="View raw narrative draft JSON" payload={narrativeDraft} />
          <RawJsonDisclosure label="View raw SAR draft JSON" payload={sarDraft} />
          <RawJsonDisclosure label="View raw outcome JSON" payload={recordedOutcome} />
        </div>
      </SectionCard>
    </div>
  )
}
