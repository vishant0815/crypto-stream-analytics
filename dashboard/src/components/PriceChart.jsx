import { useEffect, useRef, useState } from 'react'
import { CandlestickSeries, ColorType, CrosshairMode, HistogramSeries, LineSeries, createChart, createSeriesMarkers } from 'lightweight-charts'
import { getJSON } from '../live.js'
import { decimalsFor, formatNumber, formatPrice } from '../format.js'

const INTERVALS = { '1m': 60, '5m': 300, '1h': 3600 }

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim()
}

function themeColors() {
  return {
    ink: cssVar('--ink'),
    ink3: cssVar('--ink-3'),
    rule: cssVar('--rule'),
    panel: cssVar('--panel'),
    up: cssVar('--up'),
    down: cssVar('--down'),
    sma15: cssVar('--sma15'),
    sma60: cssVar('--sma60'),
  }
}

function withAlpha(hex, alpha) {
  const h = hex.replace('#', '')
  const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16))
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

// A moving-average line with a break wherever minutes are missing (the pipeline was down, say).
// Without it, a gap of hours would look like a smooth slide from one price to another.
// lightweight-charts ignores whitespace points inside a line and draws through them, but a
// point's colour applies to the segment that starts at it: so the last point before each gap
// gets a transparent colour. (Checked by rendering both options.)
const GAP = 'rgba(0,0,0,0)'

function lineWithGaps(points, step) {
  return points.map((p, i) => {
    const next = points[i + 1]
    return next && next.time - p.time > step ? { ...p, color: GAP } : p
  })
}

function volumeBar(c, colors) {
  return { time: c.time, value: c.volume, color: withAlpha(c.close >= c.open ? colors.up : colors.down, 0.35) }
}

/**
 * Candles + SMA 15 / SMA 60 + volume for one pair.
 * Loads history over REST, then applies live candles from the WebSocket.
 * For 5m and 1h, live 1-minute candles are rolled up into the current bar.
 */
export default function PriceChart({ pair, interval, onIntervalChange, subscribe, livePrice, alerts = [] }) {
  const box = useRef(null)
  const chartRef = useRef(null)
  const series = useRef({})
  const markers = useRef(null)
  const lastTime = useRef(0)
  const rollup = useRef({ bucket: null, minutes: new Map() })
  const [hover, setHover] = useState(null)
  const [state, setState] = useState('loading') // loading | ready | empty | error
  const [scheme, setScheme] = useState(0)

  // Re-theme when the OS or the page switches light/dark
  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    const bump = () => setScheme((s) => s + 1)
    mq.addEventListener('change', bump)
    const obs = new MutationObserver(bump)
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] })
    return () => {
      mq.removeEventListener('change', bump)
      obs.disconnect()
    }
  }, [])

  // Create the chart once
  useEffect(() => {
    const colors = themeColors()
    const chart = createChart(box.current, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: colors.ink3,
        fontFamily: "'IBM Plex Sans', system-ui, sans-serif",
        fontSize: 12,
      },
      grid: { vertLines: { visible: false }, horzLines: { color: colors.rule } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.08, bottom: 0.24 } },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false, rightOffset: 4 },
      crosshair: { mode: CrosshairMode.Magnet },
      localization: { priceFormatter: (p) => formatPrice(p) },
    })
    series.current = {
      // Rising candles hollow, falling candles filled: direction is readable without colour
      candles: chart.addSeries(CandlestickSeries, {
        upColor: 'rgba(0,0,0,0)',
        downColor: colors.down,
        borderUpColor: colors.up,
        borderDownColor: colors.down,
        wickUpColor: colors.up,
        wickDownColor: colors.down,
      }),
      sma15: chart.addSeries(LineSeries, { color: colors.sma15, lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }),
      sma60: chart.addSeries(LineSeries, { color: colors.sma60, lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }),
      volume: chart.addSeries(HistogramSeries, { priceScaleId: 'volume', priceLineVisible: false, lastValueVisible: false }),
    }
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } })
    markers.current = createSeriesMarkers(series.current.candles, [])

    chart.subscribeCrosshairMove((param) => {
      const bar = param.time ? param.seriesData.get(series.current.candles) : null
      if (!bar) return setHover(null)
      const vol = param.seriesData.get(series.current.volume)
      const s15 = param.seriesData.get(series.current.sma15)
      const s60 = param.seriesData.get(series.current.sma60)
      setHover({ ...bar, volume: vol?.value, sma15: s15?.value, sma60: s60?.value })
    })
    chartRef.current = chart
    return () => chart.remove()
  }, [])

  // Apply theme colours
  useEffect(() => {
    const chart = chartRef.current
    if (!chart || !scheme) return
    const c = themeColors()
    chart.applyOptions({ layout: { textColor: c.ink3 }, grid: { horzLines: { color: c.rule } } })
    series.current.candles.applyOptions({ downColor: c.down, borderUpColor: c.up, borderDownColor: c.down, wickUpColor: c.up, wickDownColor: c.down })
    series.current.sma15.applyOptions({ color: c.sma15 })
    series.current.sma60.applyOptions({ color: c.sma60 })
  }, [scheme])

  // Load history whenever the pair or interval changes
  useEffect(() => {
    let cancelled = false
    setState('loading')
    setHover(null)
    const limit = interval === '1m' ? 360 : 300
    const step = INTERVALS[interval]
    Promise.all([
      getJSON('/api/candles', { pair, interval, limit }),
      // For 5m and 1h: the 1-minute candles of the newest bar, so live minutes can rebuild it exactly
      interval === '1m' ? null : getJSON('/api/candles', { pair, interval: '1m', limit: step / 60 }),
    ])
      .then(([data, minutes]) => {
        if (cancelled) return
        const colors = themeColors()
        const { candles, sma15, sma60, volume } = series.current
        const decimals = decimalsFor(data.candles.at(-1)?.close)
        candles.applyOptions({ priceFormat: { type: 'price', precision: decimals, minMove: 10 ** -decimals } })
        candles.setData(data.candles.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })))
        volume.setData(data.candles.map((c) => volumeBar(c, colors)))
        sma15.setData(lineWithGaps(data.indicators.filter((i) => i.sma_15 != null).map((i) => ({ time: i.time, value: i.sma_15 })), 60))
        sma60.setData(lineWithGaps(data.indicators.filter((i) => i.sma_60 != null).map((i) => ({ time: i.time, value: i.sma_60 })), 60))
        lastTime.current = data.candles.at(-1)?.time ?? 0

        const newest = data.candles.at(-1)?.time
        rollup.current = { bucket: newest ?? null, minutes: new Map() }
        for (const m of minutes?.candles ?? []) {
          if (newest != null && m.time >= newest) rollup.current.minutes.set(m.time, m)
        }
        chartRef.current.timeScale().fitContent()
        setState(data.candles.length ? 'ready' : 'empty')
      })
      .catch(() => !cancelled && setState('error'))
    return () => {
      cancelled = true
    }
  }, [pair, interval])

  // Unusual minutes, pinned above their candle (rolled up to the 5m / 1h bar they fall in)
  useEffect(() => {
    if (!markers.current) return
    const step = INTERVALS[interval]
    const color = cssVar('--alert')
    const byBar = new Map()
    for (const a of alerts) {
      const t = Math.floor(a.time / step) * step
      if (!byBar.has(t)) byBar.set(t, a)
    }
    markers.current.setMarkers(
      [...byBar.entries()]
        .sort((x, y) => x[0] - y[0])
        .map(([time, a]) => ({
          time,
          position: 'aboveBar',
          shape: 'arrowDown',
          color,
          text: a.alert_type === 'VOLUME_SPIKE' ? 'Volume' : a.alert_type === 'PRICE_JUMP' ? 'Jump' : 'Unusual',
        })),
    )
  }, [alerts, interval, state, scheme])

  // Live updates
  useEffect(() => {
    const step = INTERVALS[interval]
    return subscribe((msg) => {
      if (msg.type !== 'update') return
      const data = msg.pairs[pair]
      if (!data) return
      const colors = themeColors()
      const { candles, sma15, sma60, volume } = series.current

      for (const c of data.candles ?? []) {
        let bar = c
        if (interval !== '1m') {
          // Roll 1-minute candles up into the current 5m / 1h bar. Each minute is
          // stored once by its start time, so a minute that updates is replaced, not added again.
          const bucket = Math.floor(c.time / step) * step
          if (rollup.current.bucket !== bucket) {
            if (rollup.current.bucket != null && bucket < rollup.current.bucket) continue
            rollup.current = { bucket, minutes: new Map() }
          }
          rollup.current.minutes.set(c.time, c)
          const mins = [...rollup.current.minutes.values()].sort((a, b) => a.time - b.time)
          bar = {
            time: bucket,
            open: mins[0].open,
            high: Math.max(...mins.map((m) => m.high)),
            low: Math.min(...mins.map((m) => m.low)),
            close: mins.at(-1).close,
            volume: mins.reduce((sum, m) => sum + m.volume, 0),
          }
        }
        if (bar.time < lastTime.current) continue // older than the chart: ignore
        candles.update({ time: bar.time, open: bar.open, high: bar.high, low: bar.low, close: bar.close })
        volume.update(volumeBar(bar, colors))
        lastTime.current = bar.time
        if (state === 'empty') setState('ready')
      }
      if (interval === '1m') {
        for (const i of data.indicators ?? []) {
          for (const [line, value] of [[sma15, i.sma_15], [sma60, i.sma_60]]) {
            if (value == null) continue
            try {
              const last = line.data().at(-1)
              // Live data resumed after a gap: hide the segment bridging it
              if (last && 'value' in last && i.time - last.time > 60) line.update({ ...last, color: GAP })
              line.update({ time: i.time, value })
            } catch {
              // an indicator older than the line's last point: already drawn
            }
          }
        }
      }
    })
  }, [pair, interval, subscribe, state])

  const shown = hover
  return (
    <section className="panel chart-panel" aria-label={`${pair} price chart`}>
      <div className="chart-head">
        <div className="chart-title">
          <h2>{pair}</h2>
          <span className="chart-price">{formatPrice(livePrice)}</span>
        </div>
        <div className="segmented" role="group" aria-label="Candle interval">
          {Object.keys(INTERVALS).map((key) => (
            <button key={key} type="button" aria-pressed={interval === key} onClick={() => onIntervalChange(key)}>
              {key}
            </button>
          ))}
        </div>
      </div>

      <div className="legend" aria-live="off">
        {shown ? (
          <>
            <span>{new Date(shown.time * 1000).toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'short' })}</span>
            <span>Open {formatPrice(shown.open)}</span>
            <span>High {formatPrice(shown.high)}</span>
            <span>Low {formatPrice(shown.low)}</span>
            <span className={shown.close >= shown.open ? 'up' : 'down'}>Close {formatPrice(shown.close)}</span>
            {shown.volume != null && <span>Volume {formatNumber(shown.volume, 4)}</span>}
          </>
        ) : (
          <>
            <span className="legend-item">
              <span className="legend-candle" style={{ border: '1.5px solid var(--up)' }} /> Rising minute
            </span>
            <span className="legend-item">
              <span className="legend-candle" style={{ background: 'var(--down)' }} /> Falling minute
            </span>
            {interval === '1m' && (
              <>
                <span className="legend-item">
                  <span className="legend-swatch" style={{ background: 'var(--sma15)' }} /> 15-minute average
                </span>
                <span className="legend-item">
                  <span className="legend-swatch" style={{ background: 'var(--sma60)' }} /> 60-minute average
                </span>
              </>
            )}
          </>
        )}
      </div>

      <div className="chart-box">
        <div ref={box} style={{ position: 'absolute', inset: 0 }} />
        {state !== 'ready' && (
          <div className="chart-empty">
            {state === 'loading' && 'Loading candles…'}
            {state === 'empty' && `No candles for ${pair} yet. The first one appears within a minute of trades arriving.`}
            {state === 'error' && 'Candles could not be loaded. The API may be restarting; this page will reconnect on its own.'}
          </div>
        )}
      </div>
    </section>
  )
}
