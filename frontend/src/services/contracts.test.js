import { describe, expect, it } from 'vitest'
import {
  mapCaseStatusForUpdate,
  normalizeExplanationPayload,
  normalizeInvestigationContext,
  normalizeNarrativeDraft,
  normalizeNetworkGraph,
} from './contracts'

describe('contracts mappers', () => {
  it('maps UI status variants to backend statuses', () => {
    expect(mapCaseStatusForUpdate('CLOSED_TP')).toBe('closed')
    expect(mapCaseStatusForUpdate('IN_PROGRESS')).toBe('under_review')
    expect(mapCaseStatusForUpdate('sar_filed')).toBe('sar_filed')
  })

  it('normalizes investigation context shape for UI safety', () => {
    const normalized = normalizeInvestigationContext({ alert_id: 'A1' })
    expect(normalized.alert_id).toBe('A1')
    expect(Array.isArray(normalized.global_signals)).toBe(true)
    expect(typeof normalized.model_metadata).toBe('object')
    expect(typeof normalized.investigation_summary).toBe('object')
    expect(typeof normalized.customer_profile).toBe('object')
    expect(Array.isArray(normalized.counterparty_summary.top_counterparties)).toBe(true)
    expect(normalized.screening_summary.screening_status).toBe('unavailable')
    expect(Array.isArray(normalized.data_availability.missing_sections)).toBe(true)
  })

  it('normalizes network graph with compatibility aliases', () => {
    const graph = normalizeNetworkGraph({
      alert_id: 'A1',
      nodes: [{ id: 'n1', label: 'Node 1', type: 'customer', properties: { score: 2 } }],
      edges: [{ source: 'n1', target: 'n2', relation: 'transaction', weight: '3' }],
    })
    expect(graph.alert_id).toBe('A1')
    expect(graph.nodes[0].meta.score).toBe(2)
    expect(graph.edges[0].type).toBe('transaction')
    expect(graph.summary.node_count).toBe(1)
    expect(graph.summary.edge_count).toBe(1)
  })

  it('normalizes narrative draft fallback fields', () => {
    const draft = normalizeNarrativeDraft({ sections: null, source_signals: null }, 'A9')
    expect(draft.alert_id).toBe('A9')
    expect(draft.title).toBe('Investigation Narrative Draft')
    expect(Array.isArray(draft.sections.risk_indicators)).toBe(true)
    expect(Array.isArray(draft.source_signals.reason_codes)).toBe(true)
  })

  it('normalizes explanation payload for SHAP method', () => {
    const explain = normalizeExplanationPayload({
      feature_attribution: [{ feature: 'amount', value: 0.77, shap_value: 0.77 }],
      risk_reason_codes: ['amount:increase'],
      explanation_method: 'shap',
      explanation_status: 'ok',
    })
    expect(explain.explanation_method).toBe('shap')
    expect(explain.explanation_status).toBe('ok')
    expect(explain.is_fallback).toBe(false)
    expect(explain.feature_attribution[0].feature).toBe('amount')
  })

  it('normalizes fallback explanation payload and provides disclaimer', () => {
    const explain = normalizeExplanationPayload({
      feature_attribution: [{ feature: 'amount', value: 10000 }],
      explanation_method: 'numeric_fallback',
    })
    expect(explain.explanation_method).toBe('numeric_fallback')
    expect(explain.explanation_status).toBe('fallback')
    expect(explain.is_fallback).toBe(true)
    expect(String(explain.explanation_warning || '').toLowerCase()).toContain('heuristic')
  })

  it('handles explanation payload with missing metadata from older records', () => {
    const explain = normalizeExplanationPayload([{ feature: 'amount', value: 5 }])
    expect(explain.explanation_method).toBe('unknown')
    expect(Array.isArray(explain.feature_attribution)).toBe(true)
  })
})
