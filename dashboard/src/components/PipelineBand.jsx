import { useEffect, useState } from 'react'
import { formatAge, formatNumber } from '../format.js'

// The live pipeline: one stage per hop, each with the figure that proves it's working.
// A pulse runs along the track every time the server pushes an update.
export default function PipelineBand({ pipeline, lastPush, status, now }) {
  const [pulseKey, setPulseKey] = useState(0)
  useEffect(() => {
    if (lastPush) setPulseKey((k) => k + 1)
  }, [lastPush])

  const live = pipeline?.live
  const rates = live?.messages_per_s ?? {}
  const totalRate = Object.values(rates).reduce((a, b) => a + b, 0)
  const candleAge = pipeline?.freshness?.candles_age_s
  const latency = pipeline?.latency
  const pushAge = lastPush ? (now - lastPush.at) / 1000 : null

  const minutes = pipeline?.trades_per_minute
  const lastFullMinute = minutes && minutes.length > 1 ? minutes[minutes.length - 2].trades : null

  const stages = [
    {
      name: 'Coinbase',
      figure: formatNumber(rates.trade, 1),
      unit: 'trades/s',
      note: `reach us in ${latency?.p50_ms == null ? '–' : formatNumber(latency.p50_ms, 0)} ms (median)`,
      ok: live ? (live.last_message_age_s ?? 99) < 30 : null,
    },
    {
      name: 'Kafka',
      figure: formatNumber(totalRate, 1),
      unit: 'msg/s',
      note: 'trades, tickers, candles, indicators',
      ok: live ? live.kafka_connected : null,
    },
    {
      name: 'Spark',
      figure: candleAge == null ? '–' : candleAge < 10 ? formatNumber(candleAge, 1) : formatAge(candleAge),
      unit: candleAge != null && candleAge < 10 ? 's' : '',
      note: 'since the live candle last changed',
      ok: candleAge == null ? null : candleAge < 30,
    },
    {
      name: 'TimescaleDB',
      figure: formatNumber(lastFullMinute, 0),
      unit: 'trades/min',
      note: 'written in the last full minute',
      ok: pipeline ? (pipeline.freshness?.trades_age_s ?? 99) < 30 : null,
    },
    {
      name: 'This page',
      figure: pushAge == null ? '–' : formatNumber(Math.max(pushAge, 0), 1),
      unit: 's',
      note: `since the last push, ${live?.ws_clients ?? '–'} viewer${live?.ws_clients === 1 ? '' : 's'}`,
      ok: status === 'live' ? (pushAge ?? 99) < 5 : status === 'down' ? false : null,
    },
  ]

  return (
    <section className="pipeline" aria-label="Data pipeline, live">
      <div className="pipeline-track" aria-hidden="true">
        <span key={pulseKey} className={`pipeline-pulse ${pulseKey ? 'run' : ''}`} />
      </div>
      <ol className="pipeline-stages">
        {stages.map((s) => (
          <li key={s.name} className="stage" data-ok={s.ok == null ? undefined : String(s.ok)}>
            <div className="stage-name">{s.name}</div>
            <div className="stage-figure">
              {s.figure}
              {s.unit && <small>{s.unit}</small>}
            </div>
            <div className="stage-note">{s.note}</div>
          </li>
        ))}
      </ol>
    </section>
  )
}
