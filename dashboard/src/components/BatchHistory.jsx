import { useState } from 'react'
import { formatNumber, formatPct, formatPrice } from '../format.js'

const SHORT = (symbol) => symbol.replace('-USD', '')

function formatDay(dt) {
  return new Date(`${dt}T00:00:00Z`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })
}

function formatUsd(v) {
  if (v == null) return '–'
  if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`
  if (v >= 1e6) return `$${(v / 1e6).toFixed(2)}M`
  if (v >= 1e3) return `$${(v / 1e3).toFixed(1)}K`
  return `$${v.toFixed(0)}`
}

// One sentence on the last nightly run, including whether HDFS and TimescaleDB agree
function RunStatus({ run }) {
  if (!run) {
    return <p className="lede">No nightly run yet. The first one runs at 00:30 UTC for the day before.</p>
  }
  const when = run.finished_at
    ? new Date(run.finished_at * 1000).toLocaleString('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' })
    : null
  if (run.status === 'failed') {
    return (
      <p className="lede">
        <span className="down">The last run, for {formatDay(run.dt)}, failed.</span> {run.error ?? 'See the spark-batch logs.'}
      </p>
    )
  }
  const diff = (run.rows_hdfs ?? 0) - (run.rows_db ?? 0)
  return (
    <p className="lede">
      Last run for {formatDay(run.dt)}
      {run.partial_day ? ' (day so far)' : ''}
      {when ? `, finished ${when} UTC` : ''}: {formatNumber(run.rows_hdfs)} trades in HDFS and{' '}
      {formatNumber(run.rows_db)} in TimescaleDB,{' '}
      {run.status === 'ok' ? (
        <strong>counts match</strong>
      ) : (
        <strong className="down">
          {formatNumber(Math.abs(diff))} {diff > 0 ? 'more' : 'fewer'} in HDFS
        </strong>
      )}
      .
      {run.files_before > run.files_after
        ? ` ${formatNumber(run.files_before)} small files compacted into ${formatNumber(run.files_after)}`
        : ''}
      {run.seconds != null ? `${run.files_before > run.files_after ? ',' : ''} ${run.seconds.toFixed(1)} s.` : ''}
    </p>
  )
}

function DailyTable({ rows }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th scope="col">Pair</th>
          <th scope="col">Close</th>
          <th scope="col">Day</th>
          <th scope="col" title="Square root of the summed squared one-minute log returns">Realized volatility</th>
          <th scope="col">High–low range</th>
          <th scope="col">Traded value</th>
          <th scope="col">Trades</th>
          <th scope="col" title="Share of the volume bought by takers">Buy share</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.symbol}>
            <th scope="row">{r.symbol}</th>
            <td>{formatPrice(r.close)}</td>
            <td className={r.return_pct > 0 ? 'up' : r.return_pct < 0 ? 'down' : ''}>{formatPct(r.return_pct)}</td>
            <td>{r.realized_vol_pct == null ? '–' : `${r.realized_vol_pct.toFixed(2)}%`}</td>
            <td>{`${r.range_pct.toFixed(2)}%`}</td>
            <td>{formatUsd(r.notional_usd)}</td>
            <td>{formatNumber(r.trades)}</td>
            <td>{`${r.buy_volume_pct.toFixed(0)}%`}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// Text on a heatmap cell: dark on light fills, light on dark fills
const inkFor = (t, varName) => `var(${t > 0.55 ? varName : '--ink'})`

function CorrelationMatrix({ rows }) {
  const [hover, setHover] = useState(null)
  const pairs = [...new Set(rows.flatMap((r) => [r.pair_a, r.pair_b]))]
  const lookup = new Map()
  for (const r of rows) {
    lookup.set(`${r.pair_a}|${r.pair_b}`, r)
    lookup.set(`${r.pair_b}|${r.pair_a}`, r)
  }
  if (pairs.length < 2) return <p className="lede">Needs at least two pairs trading on the same day.</p>
  const fill = (c) => {
    // Diverging: red for -1, neutral at 0, blue for +1
    const t = Math.min(Math.abs(c), 1)
    return `color-mix(in oklab, var(--div-mid), var(${c >= 0 ? '--div-pos' : '--div-neg'}) ${Math.round(t * 100)}%)`
  }
  return (
    <div className="heat-wrap">
      <table className="heat corr" aria-label="Correlation of one-minute returns between pairs">
        <thead>
          <tr>
            <th aria-hidden="true" />
            {pairs.map((p) => (
              <th key={p} scope="col">{SHORT(p)}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {pairs.map((a) => (
            <tr key={a}>
              <th scope="row">{SHORT(a)}</th>
              {pairs.map((b) => {
                if (a === b) return <td key={b} className="diag" aria-label={`${a} with itself`} />
                const r = lookup.get(`${a}|${b}`)
                const c = r?.correlation
                return (
                  <td
                    key={b}
                    style={c == null ? undefined : { background: fill(c), color: inkFor(Math.abs(c), '--heat-ink-strong') }}
                    onMouseEnter={() => r && setHover(r)}
                    onMouseLeave={() => setHover(null)}
                    aria-label={c == null ? `${a} and ${b}: not enough data` : `${a} and ${b}: ${c.toFixed(2)}`}
                  >
                    {c == null ? '–' : c.toFixed(2)}
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <p className="heat-note">
        {hover
          ? `${hover.pair_a} and ${hover.pair_b}: ${hover.correlation.toFixed(3)}, over ${formatNumber(hover.minutes)} minutes when both traded.`
          : '1 means two coins moved together minute by minute; 0 means no relation.'}
      </p>
    </div>
  )
}

function HourlyHeatmap({ rows }) {
  const [hover, setHover] = useState(null)
  const pairs = [...new Set(rows.map((r) => r.symbol))]
  const cell = new Map(rows.map((r) => [`${r.symbol}|${r.hour}`, r]))
  const max = Math.max(...rows.map((r) => r.share_pct ?? 0), 1)
  const hours = Array.from({ length: 24 }, (_, h) => h)
  return (
    <div className="heat-wrap">
      <table className="heat hourly" aria-label="Share of each pair's traded value by UTC hour">
        <thead>
          <tr>
            <th aria-hidden="true" />
            {hours.map((h) => (
              <th key={h} scope="col">{h % 3 === 0 ? String(h).padStart(2, '0') : ''}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {pairs.map((p) => (
            <tr key={p}>
              <th scope="row">{SHORT(p)}</th>
              {hours.map((h) => {
                const r = cell.get(`${p}|${h}`)
                const t = r ? r.share_pct / max : 0
                return (
                  <td
                    key={h}
                    style={{ background: r ? `color-mix(in oklab, var(--seq-low), var(--seq-high) ${Math.round(t * 100)}%)` : undefined }}
                    onMouseEnter={() => setHover({ p, h, r })}
                    onMouseLeave={() => setHover(null)}
                    aria-label={`${p} ${h}:00 UTC: ${r ? `${r.share_pct.toFixed(1)}% of traded value` : 'no trades'}`}
                  />
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="heat-scale" aria-hidden="true">
        <span>0%</span>
        <span className="heat-ramp" />
        <span>{max.toFixed(0)}% of a pair's daily value</span>
      </div>
      <p className="heat-note">
        {hover
          ? `${hover.p}, ${String(hover.h).padStart(2, '0')}:00–${String(hover.h + 1).padStart(2, '0')}:00 UTC: ${
              hover.r ? `${hover.r.share_pct.toFixed(1)}% of traded value, ${formatNumber(hover.r.trades)} trades` : 'no trades'
            }.`
          : 'Hours in UTC. Darker means more of that coin’s trading happened in that hour.'}
      </p>
    </div>
  )
}

export default function BatchHistory({ analytics }) {
  if (!analytics) return null
  const latest = analytics.latest_dt
  const today = (analytics.daily ?? []).filter((r) => r.dt === latest)
  const days = new Set((analytics.daily ?? []).map((r) => r.dt)).size
  return (
    <section className="history" aria-label="History from the data lake">
      <div className="history-head">
        <h2>History from the data lake</h2>
        <p className="lede">
          Every night Spark reads the previous day’s trades from the HDFS archive, compacts them and computes these
          figures, which are stored in Hive and TimescaleDB.
        </p>
        {/* The run that produced the day on show, not just the newest run */}
        <RunStatus run={analytics.runs?.find((r) => r.dt === latest && r.status !== 'running') ?? analytics.runs?.[0]} />
      </div>
      {latest && (
        <div className="history-grid">
          <div className="history-day">
            <h3>{formatDay(latest)}, per pair</h3>
            <div className="table-scroll">
              <DailyTable rows={today} />
            </div>
          </div>
          <div>
            <h3>How the coins move together</h3>
            <CorrelationMatrix rows={analytics.correlation ?? []} />
          </div>
          <div className="history-hours">
            <h3>
              When each coin trades{' '}
              <span className="muted" style={{ fontWeight: 400 }}>
                {days > 1 ? `last ${days} days` : formatDay(latest)}
              </span>
            </h3>
            <HourlyHeatmap rows={analytics.hourly ?? []} />
          </div>
        </div>
      )}
    </section>
  )
}
