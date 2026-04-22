const CASE_STATUS_COMPATIBILITY_MAP = {
  OPEN: 'open',
  ASSIGNED: 'open',
  IN_PROGRESS: 'under_review',
  UNDER_REVIEW: 'under_review',
  ESCALATED: 'escalated',
  MANAGER_REVIEW: 'escalated',
  SAR_FILED: 'sar_filed',
  CLOSED: 'closed',
  CLOSED_TP: 'closed',
  CLOSED_FP: 'closed',
}

function toSafeArray(value) {
  return Array.isArray(value) ? value : []
}

function toSafeObject(value) {
  return value && typeof value === 'object' ? value : {}
}

function safeParseJson(value) {
  if (value == null) return {}
  if (typeof value === 'object') return toSafeObject(value)
  if (typeof value === 'string') {
    const text = value.trim()
    if (!text) return {}
    try {
      return toSafeObject(JSON.parse(text))
    } catch {
      return {}
    }
  }
  return {}
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function toNullableNumber(value) {
  if (value == null || value === '') return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toNullableBoolean(value) {
  if (value == null || value === '') return null
  if (typeof value === 'boolean') return value
  const normalized = String(value).trim().toLowerCase()
  if (!normalized) return null
  if (['true', '1', 'yes'].includes(normalized)) return true
  if (['false', '0', 'no'].includes(normalized)) return false
  return null
}

function toStringArray(value) {
  return toSafeArray(value).map((item) => String(item ?? '')).filter(Boolean)
}

function normalizeCustomerProfile(payload) {
  const profile = toSafeObject(payload)
  return {
    customer_label: String(profile.customer_label ?? ''),
    segment: String(profile.segment ?? ''),
    risk_tier: String(profile.risk_tier ?? ''),
    country: String(profile.country ?? ''),
    business_purpose: String(profile.business_purpose ?? ''),
    kyc_status: String(profile.kyc_status ?? ''),
    pep_flag: toNullableBoolean(profile.pep_flag),
    sanctions_flag: toNullableBoolean(profile.sanctions_flag),
    onboarded_at: profile.onboarded_at ?? null,
    assigned_analyst_label: String(profile.assigned_analyst_label ?? ''),
  }
}

function normalizeAccountProfile(payload) {
  const profile = toSafeObject(payload)
  return {
    account_label: String(profile.account_label ?? ''),
    account_type: String(profile.account_type ?? ''),
    account_status: String(profile.account_status ?? ''),
    opened_at: profile.opened_at ?? null,
    account_age_days: toNullableNumber(profile.account_age_days),
  }
}

function normalizeBehaviorBaseline(payload) {
  const baseline = toSafeObject(payload)
  return {
    baseline_avg_amount: toNullableNumber(baseline.baseline_avg_amount),
    baseline_monthly_inflow: toNullableNumber(baseline.baseline_monthly_inflow),
    baseline_monthly_outflow: toNullableNumber(baseline.baseline_monthly_outflow),
    baseline_tx_count: toNullableNumber(baseline.baseline_tx_count),
    current_window_inflow: toNullableNumber(baseline.current_window_inflow),
    current_window_outflow: toNullableNumber(baseline.current_window_outflow),
    current_window_tx_count: toNullableNumber(baseline.current_window_tx_count),
    deviation_summary: String(baseline.deviation_summary ?? ''),
    prior_alert_count_30d: toNullableNumber(baseline.prior_alert_count_30d),
    prior_alert_count_90d: toNullableNumber(baseline.prior_alert_count_90d),
    prior_case_count_90d: toNullableNumber(baseline.prior_case_count_90d),
  }
}

function normalizeCounterpartySummary(payload) {
  const summary = toSafeObject(payload)
  return {
    top_counterparties: toStringArray(summary.top_counterparties),
    new_counterparty_share: toNullableNumber(summary.new_counterparty_share),
    recurring_counterparty_share: toNullableNumber(summary.recurring_counterparty_share),
    counterparty_countries: toStringArray(summary.counterparty_countries),
    counterparty_bank_count: toNullableNumber(summary.counterparty_bank_count),
  }
}

function normalizeGeographyPaymentSummary(payload) {
  const summary = toSafeObject(payload)
  return {
    is_cross_border: toNullableBoolean(summary.is_cross_border),
    countries_involved: toStringArray(summary.countries_involved),
    payment_channels: toStringArray(summary.payment_channels),
    currency_mix: toStringArray(summary.currency_mix),
  }
}

function normalizeScreeningSummary(payload) {
  const summary = toSafeObject(payload)
  return {
    sanctions_hits: toStringArray(summary.sanctions_hits),
    watchlist_hits: toStringArray(summary.watchlist_hits),
    pep_hits: toStringArray(summary.pep_hits),
    adverse_media_hits: toStringArray(summary.adverse_media_hits),
    screening_checked_at: summary.screening_checked_at ?? null,
    screening_status: String(summary.screening_status ?? 'unavailable'),
  }
}

function normalizeDataAvailability(payload) {
  const summary = toSafeObject(payload)
  return {
    missing_sections: toStringArray(summary.missing_sections),
    coverage_status: String(summary.coverage_status ?? 'limited'),
    freshness_status: String(summary.freshness_status ?? 'legacy_only'),
  }
}

export function mapCaseStatusForUpdate(status) {
  const raw = String(status || '').trim()
  if (!raw) return undefined
  const upper = raw.toUpperCase()
  if (CASE_STATUS_COMPATIBILITY_MAP[upper]) return CASE_STATUS_COMPATIBILITY_MAP[upper]
  return raw.toLowerCase()
}

export function normalizeHealthResponse(payload) {
  const checks = toSafeObject(payload?.checks)
  const ok = typeof payload?.ok === 'boolean' ? payload.ok : Object.values(checks).every(Boolean)
  const status = String(payload?.status || (ok ? 'healthy' : 'degraded'))
  return {
    ok,
    status,
    checks,
    queue_depth: Number(payload?.queue_depth ?? 0),
    details: toSafeObject(payload?.details),
  }
}

export function normalizeExplanationPayload(payload) {
  const explain = safeParseJson(payload)
  const featureAttribution = toSafeArray(explain.feature_attribution || explain.contributions).map((item) => {
    const normalized = toSafeObject(item)
    return {
      feature: String(normalized.feature ?? normalized.name ?? ''),
      value: toNumber(normalized.value ?? normalized.contribution ?? normalized.shap_value, 0),
      shap_value: normalized.shap_value == null ? null : toNumber(normalized.shap_value, 0),
      magnitude: toNumber(normalized.magnitude ?? Math.abs(toNumber(normalized.value ?? normalized.contribution ?? normalized.shap_value, 0)), 0),
    }
  }).filter((item) => item.feature)

  let method = String(explain.explanation_method || '').trim().toLowerCase()
  if (!method) {
    const hasShap = featureAttribution.some((item) => item.shap_value != null)
    method = hasShap ? 'shap' : 'unknown'
  }

  let status = String(explain.explanation_status || '').trim().toLowerCase()
  if (!status) {
    if (method === 'numeric_fallback') {
      status = 'fallback'
    } else if (method === 'unavailable') {
      status = 'unavailable'
    } else if (method === 'shap' || method === 'tree_shap') {
      status = 'ok'
    } else {
      status = 'unknown'
    }
  }

  const warning = explain.explanation_warning || (
    method === 'numeric_fallback'
      ? 'Heuristic feature highlights; not model contribution attribution.'
      : null
  )

  const reasonCodes = toSafeArray(explain.risk_reason_codes).map((item) => String(item)).filter(Boolean)
  return {
    ...explain,
    feature_attribution: featureAttribution,
    contributions: featureAttribution,
    risk_reason_codes: reasonCodes,
    explanation_method: method || 'unknown',
    explanation_status: status || 'unknown',
    explanation_warning: warning,
    explanation_warning_code: explain.explanation_warning_code || null,
    is_fallback: status === 'fallback' || method === 'numeric_fallback' || method === 'unavailable',
  }
}

export function normalizeInvestigationContext(payload) {
  const context = toSafeObject(payload)
  return {
    alert_id: context.alert_id ?? null,
    investigation_summary: toSafeObject(context.investigation_summary),
    risk_explanation: toSafeObject(context.risk_explanation),
    network_graph: normalizeNetworkGraph(context.network_graph),
    investigation_steps: toSafeObject(context.investigation_steps),
    sar_draft: toSafeObject(context.sar_draft),
    narrative_draft: normalizeNarrativeDraft(context.narrative_draft, context.alert_id ?? null),
    global_signals: toSafeArray(context.global_signals),
    outcome: context.outcome ?? null,
    case_status: context.case_status ?? null,
    model_metadata: toSafeObject(context.model_metadata),
    customer_profile: normalizeCustomerProfile(context.customer_profile),
    account_profile: normalizeAccountProfile(context.account_profile),
    behavior_baseline: normalizeBehaviorBaseline(context.behavior_baseline),
    counterparty_summary: normalizeCounterpartySummary(context.counterparty_summary),
    geography_payment_summary: normalizeGeographyPaymentSummary(context.geography_payment_summary),
    screening_summary: normalizeScreeningSummary(context.screening_summary),
    data_availability: normalizeDataAvailability(context.data_availability),
    assembled_at: context.assembled_at ?? null,
    assembly_latency_seconds: toNumber(context.assembly_latency_seconds, 0),
  }
}

export function normalizeNetworkGraph(payload) {
  const graph = toSafeObject(payload)
  const nodes = toSafeArray(graph.nodes).map((node) => {
    const item = toSafeObject(node)
    return {
      id: String(item.id ?? ''),
      label: String(item.label ?? item.id ?? ''),
      type: String(item.type ?? 'entity'),
      risk: String(item.risk ?? 'low'),
      meta: toSafeObject(item.meta ?? item.properties),
      properties: toSafeObject(item.properties ?? item.meta),
    }
  }).filter((node) => node.id)

  const edges = toSafeArray(graph.edges).map((edge) => {
    const item = toSafeObject(edge)
    const edgeType = item.type ?? item.relation ?? 'link'
    return {
      source: String(item.source ?? ''),
      target: String(item.target ?? ''),
      type: String(edgeType),
      relation: String(edgeType),
      weight: toNumber(item.weight, 1),
      meta: toSafeObject(item.meta),
    }
  }).filter((edge) => edge.source && edge.target)

  const summary = toSafeObject(graph.summary)
  return {
    alert_id: graph.alert_id ?? null,
    nodes,
    edges,
    summary: {
      node_count: toNumber(summary.node_count, nodes.length),
      edge_count: toNumber(summary.edge_count, edges.length),
      high_risk_nodes: toNumber(summary.high_risk_nodes, nodes.filter((n) => n.risk === 'high').length),
    },
    node_count: toNumber(graph.node_count, nodes.length),
    edge_count: toNumber(graph.edge_count, edges.length),
    relationship_types: toSafeArray(graph.relationship_types),
    risk_signals: toSafeArray(graph.risk_signals),
    generated_at: graph.generated_at ?? null,
  }
}

export function normalizeNarrativeDraft(payload, alertId = null) {
  const draft = toSafeObject(payload)
  const sections = toSafeObject(draft.sections)
  return {
    alert_id: draft.alert_id ?? alertId ?? null,
    title: String(draft.title ?? 'Investigation Narrative Draft'),
    narrative: String(draft.narrative ?? ''),
    sections: {
      activity_summary: String(sections.activity_summary ?? ''),
      risk_indicators: toSafeArray(sections.risk_indicators).map((item) => String(item)),
      recommended_follow_up: toSafeArray(sections.recommended_follow_up).map((item) => String(item)),
    },
    generated_at: draft.generated_at ?? null,
    source_signals: {
      ...toSafeObject(draft.source_signals),
      reason_codes: toSafeArray(toSafeObject(draft.source_signals).reason_codes).map((item) => String(item)),
      countries: toSafeArray(toSafeObject(draft.source_signals).countries).map((item) => String(item)),
    },
  }
}
