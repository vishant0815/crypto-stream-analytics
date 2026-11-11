import { useState } from 'react'
import { formatBytes, formatNumber } from '../format.js'

const STORAGE_NAMES = {
  trades: 'Raw trades',
  ticker: 'Ticker snapshots',
  candles_1m: '1-minute candles',
  candle_indicators: 'Indicators',
  alerts: 'Alerts',
}

// Trades stored per minute over the last 30 minutes, as bars with a hover tooltip.
function TradesPerMinute({ rows }) {
  const [hover, setHover] = useState(null)
  if (!rows?.length) return null
  const width = 560
  const height = 120
  const max = Math.max(...rows.map((r) => r.trades), 1)
  const slot = width / rows.length
  const barW = Math.max(slot - 2, 2) // 2px gap between bars
  return (
    <div style={{ position: 'relative' }}>
      <svg viewBox={`0 0 ${width} ${height + 18}`} width="100%" role="img" aria-label="Trades stored per minute, last 30 minutes">
        <line x1="0" x2={width} y1={height} y2={height} stroke="var(--rule)" />
        {rows.map((r, i) => {
          const h = (r.trades / max) * (height - 8)
          const x = i * slot + 1
          const isLast = i === rows.length - 1
          return (
            <g key={r.time} onMouseEnter={() => setHover({ i, r })} onMouseLeave={() => setHover(null)}>
              <rect x={i * slot} y="0" width={slot} height={height} fill="transparent" />
              <path
                d={h > 0 ? `M${x},${height} v${-(h - 2)} q0,-2 2,-2 h${barW - 4} q2,0 2,2 v${h - 2} z` : ''}
                fill={isLast ? 'var(--ink-3)' : 'var(--accent)'}
                opacity={hover && hover.i !== i ? 0.45 : 1}
              />
            </g>
          )
        })}
        <text x="0" y={height + 14} fontSize="11" fill="var(--ink-3)">30 min ago</text>
        <text x={width} y={height + 14} fontSize="11" fill="var(--ink-3)" textAnchor="end">now</text>
      </svg>
      {hover && (
        <div className="bars-tooltip" style={{ left: `${((hover.i + 0.5) / rows.length) * 100}%`, top: 0 }}>
          {new Date(hover.r.time * 1000).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })}: {formatNumber(hover.r.trades)} trades
          {hover.i === rows.length - 1 ? ' so far' : ''}
        </div>
      )}
    </div>
  )
}

export default function PipelineDetails({ pipeline }) {
  const latency = pipeline?.latency
  const total = pipeline?.trades_per_minute?.slice(0, -1).reduce((s, r) => s + r.trades, 0)
  return (
    <section className="details" aria-label="Pipeline details">
      <div>
        <h3>Trades stored per minute</h3>
        <p className="lede">
          {total == null ? 'Loading…' : `${formatNumber(total)} trades written to TimescaleDB in the last 29 full minutes. The grey bar is the minute in progress.`}
        </p>
        <TradesPerMinute rows={pipeline?.trades_per_minute} />
      </div>
      <div>
        <h3>Ingestion delay</h3>
        <p className="lede">From a trade on Coinbase to our pipeline receiving it, last 5 minutes.</p>
        <div className="figures">
          {[
            ['p50_ms', 'median'],
            ['p95_ms', '95th percentile'],
            ['p99_ms', '99th percentile'],
          ].map(([key, label]) => (
            <div key={key}>
              <div className="figure-value">{latency?.[key] == null ? '–' : `${formatNumber(latency[key])} ms`}</div>
              <div className="figure-label">{label}</div>
            </div>
          ))}
        </div>
      </div>
      <div>
        <h3>Storage</h3>
        <p className="lede">TimescaleDB hypertables, compressed after a few days.</p>
        <table className="storage">
          <tbody>
            {(pipeline?.storage ?? []).map((s) => (
              <tr key={s.name}>
                <td>{STORAGE_NAMES[s.name] ?? s.name}</td>
                <td>{formatBytes(s.bytes)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}
