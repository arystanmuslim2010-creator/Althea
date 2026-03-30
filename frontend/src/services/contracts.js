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

function toNumber(value, fallback = 0) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
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
