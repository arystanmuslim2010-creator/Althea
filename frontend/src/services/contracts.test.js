import { describe, expect, it } from 'vitest'
import {
  mapCaseStatusForUpdate,
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
})
