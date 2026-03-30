import { useMemo } from 'react'

const NODE_COLORS = {
  alert: '#dc2626',
  customer: '#2563eb',
  account: '#0891b2',
  beneficiary: '#7c3aed',
  counterparty: '#d97706',
  device: '#0f766e',
  ip: '#4b5563',
}

function colorForNode(type) {
  return NODE_COLORS[String(type || '').toLowerCase()] || '#334155'
}

function buildLayout(graph) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : []
  const edges = Array.isArray(graph?.edges) ? graph.edges : []
  if (!nodes.length) return { nodes: [], edges: [], positions: {}, rootId: null }

  const root = nodes.find((n) => String(n?.type || '').toLowerCase() === 'alert') || nodes[0]
  const rest = nodes.filter((n) => String(n?.id) !== String(root?.id))
  const positions = {}
  positions[String(root.id)] = { x: 0, y: 0 }
  const radius = Math.max(95, Math.min(165, 75 + rest.length * 10))
  rest.forEach((node, idx) => {
    const angle = (Math.PI * 2 * idx) / Math.max(1, rest.length)
    positions[String(node.id)] = {
      x: Math.cos(angle) * radius,
      y: Math.sin(angle) * radius,
    }
  })

  return { nodes, edges, positions, rootId: String(root.id) }
}

export function InvestigationGraph({ graph }) {
  const model = useMemo(() => buildLayout(graph), [graph])
  const riskSignals = Array.isArray(graph?.risk_signals) ? graph.risk_signals : []
  const summary = graph?.summary || {}

  if (!model.nodes.length) {
    return (
      <div className="text-sm text-slate-500">
        No graph data available for this alert.
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <svg viewBox="-240 -180 480 360" className="w-full border rounded bg-slate-50">
        {model.edges.map((edge, idx) => {
          const src = model.positions[String(edge.source)]
          const dst = model.positions[String(edge.target)]
          if (!src || !dst) return null
          const mx = (src.x + dst.x) / 2
          const my = (src.y + dst.y) / 2
          return (
            <g key={`${edge.source}-${edge.target}-${idx}`}>
              <line x1={src.x} y1={src.y} x2={dst.x} y2={dst.y} stroke="#94a3b8" strokeWidth="1.5" />
              {edge.relation && (
                <text x={mx} y={my - 4} textAnchor="middle" fontSize="10" fill="#475569">
                  {String(edge.relation)}
                </text>
              )}
            </g>
          )
        })}
        {model.nodes.map((node) => {
          const p = model.positions[String(node.id)]
          if (!p) return null
          const isRoot = String(node.id) === model.rootId
          return (
            <g key={node.id}>
              <circle
                cx={p.x}
                cy={p.y}
                r={isRoot ? 21 : 15}
                fill={colorForNode(node.type)}
                fillOpacity={isRoot ? 0.95 : 0.9}
              />
              <text x={p.x} y={p.y + (isRoot ? 33 : 27)} textAnchor="middle" fontSize="11" fill="#0f172a">
                {String(node.label || node.id)}
              </text>
            </g>
          )
        })}
      </svg>

      <div className="flex gap-3 flex-wrap text-xs">
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <span key={type} className="inline-flex items-center gap-1 border rounded px-2 py-1">
            <span className="inline-block w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }} />
            {type}
          </span>
        ))}
      </div>

      <div className="text-xs text-slate-600">
        <strong>Summary:</strong> {summary.node_count ?? model.nodes.length} nodes, {summary.edge_count ?? model.edges.length} edges, {summary.high_risk_nodes ?? 0} high risk nodes
      </div>

      <div className="text-xs text-slate-600">
        <strong>Risk signals:</strong> {riskSignals.length ? riskSignals.join(', ') : 'none'}
      </div>
    </div>
  )
}
