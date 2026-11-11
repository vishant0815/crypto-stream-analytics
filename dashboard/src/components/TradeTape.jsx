import { useEffect, useState } from 'react'
import { getJSON } from '../live.js'
import { formatClock, formatPrice, formatSize } from '../format.js'

const ROWS = 12

// The most recent trades for the selected pair, newest on top.
export default function TradeTape({ pair, subscribe }) {
  const [trades, setTrades] = useState([])
  const [freshIds, setFreshIds] = useState(new Set())

  useEffect(() => {
    let cancelled = false
    setTrades([])
    getJSON('/api/trades/recent', { pair, limit: ROWS })
      .then((rows) => !cancelled && setTrades((live) => mergeTrades(live, rows)))
      .catch(() => {})
    const unsubscribe = subscribe((msg) => {
      const incoming = msg.type === 'update' ? msg.pairs[pair]?.trades : null
      if (!incoming?.length) return
      setTrades((prev) => mergeTrades(incoming, prev))
      setFreshIds(new Set(incoming.map((t) => String(t.trade_id))))
    })
    return () => {
      cancelled = true
      unsubscribe()
    }
  }, [pair, subscribe])

  return (
    <section className="panel side-section" aria-label={`${pair} recent trades`}>
      <h3>Latest trades</h3>
      {trades.length === 0 ? (
        <p className="muted" style={{ margin: 0, fontSize: 'var(--t-sm)' }}>
          No trades in the last day. They appear here as soon as Coinbase reports one.
        </p>
      ) : (
        <div className="tape-wrap">
          <table className="tape">
            <thead>
              <tr>
                <th scope="col">Time</th>
                <th scope="col">Price</th>
                <th scope="col">Size</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t) => (
                <tr key={t.trade_id} className={freshIds.has(String(t.trade_id)) ? 'fresh' : ''}>
                  <td className="muted">{formatClock(t.time_ms)}</td>
                  <td className={t.side === 'BUY' ? 'up' : 'down'}>
                    {formatPrice(t.price)}
                    <span className="visually-hidden">{t.side === 'BUY' ? ' buy' : ' sell'}</span>
                  </td>
                  <td>{formatSize(t.size)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

function mergeTrades(newer, older) {
  const seen = new Set()
  const out = []
  for (const t of [...newer, ...older]) {
    const id = String(t.trade_id)
    if (seen.has(id)) continue
    seen.add(id)
    out.push(t)
  }
  return out.sort((a, b) => b.time_ms - a.time_ms).slice(0, ROWS)
}
