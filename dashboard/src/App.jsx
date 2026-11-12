import { useCallback, useEffect, useMemo, useState } from 'react'
import { useLive, useNow, usePolling } from './live.js'
import { formatAge } from './format.js'
import PipelineBand from './components/PipelineBand.jsx'
import TickerRow from './components/TickerRow.jsx'
import PriceChart from './components/PriceChart.jsx'
import IndicatorPanel from './components/IndicatorPanel.jsx'
import TradeTape from './components/TradeTape.jsx'
import PipelineDetails from './components/PipelineDetails.jsx'
import BatchHistory from './components/BatchHistory.jsx'
import AlertsPanel from './components/AlertsPanel.jsx'

const REPO = 'https://github.com/vishant0815/crypto-stream-analytics'

function readHash() {
  const pair = window.location.hash.replace('#', '').toUpperCase()
  return /^[A-Z0-9]{2,10}-[A-Z]{2,5}$/.test(pair) ? pair : 'BTC-USD'
}

function ThemeToggle() {
  const [theme, setTheme] = useState(() => {
    try {
      return localStorage.getItem('theme')
    } catch {
      return null
    }
  })
  useEffect(() => {
    if (theme) document.documentElement.dataset.theme = theme
    else delete document.documentElement.dataset.theme
    try {
      if (theme) localStorage.setItem('theme', theme)
      else localStorage.removeItem('theme')
    } catch {
      // storage unavailable: the choice lasts for this visit
    }
  }, [theme])
  const isDark = theme ? theme === 'dark' : window.matchMedia('(prefers-color-scheme: dark)').matches
  return (
    <button type="button" className="theme-toggle" onClick={() => setTheme(isDark ? 'light' : 'dark')}>
      {isDark ? 'Light theme' : 'Dark theme'}
    </button>
  )
}

export default function App() {
  const [pair, setPair] = useState(readHash)
  const [interval, setCandleInterval] = useState('1m')
  const live = useLive()
  const now = useNow(500)
  const overview = usePolling('/api/overview', null, 30000)
  const pipeline = usePolling('/api/pipeline', null, 5000)
  const analytics = usePolling('/api/analytics', { days: 14 }, 300000) // changes once a night
  const storedAlerts = usePolling('/api/alerts', { limit: 50 }, 60000)
  const modelInfo = usePolling('/api/model', null, 60000)

  // Alerts from the database plus any pushed live since the last poll, newest first, one per pair-minute
  const alerts = useMemo(() => {
    const seen = new Map()
    for (const a of [...live.liveAlerts, ...(storedAlerts ?? [])]) {
      const key = `${a.symbol}-${a.time}`
      if (!seen.has(key)) seen.set(key, a)
    }
    return [...seen.values()].sort((a, b) => b.time - a.time)
  }, [live.liveAlerts, storedAlerts])
  const subscribe = useCallback(live.subscribe, []) // stable identity for effects

  const selectPair = (p) => {
    setPair(p)
    window.history.replaceState(null, '', `#${p}`)
  }

  const row = overview?.find((r) => r.symbol === pair)
  const livePrice = live.tickers[pair]?.price ?? row?.price
  const pushAge = live.lastPush ? (now - live.lastPush.at) / 1000 : null
  const statusText =
    live.status === 'live'
      ? pushAge == null
        ? 'Connected, waiting for data'
        : `Live, updated ${formatAge(pushAge)} ago`
      : live.status === 'down'
        ? 'Disconnected, reconnecting'
        : 'Connecting'

  return (
    <main className="page">
      <header className="masthead">
        <div>
          <h1>Crypto Stream Analytics</h1>
          <p>
            Live Coinbase trades, processed by Kafka and Spark and stored in TimescaleDB. Every number on this page
            updates as it happens.
          </p>
        </div>
        <div className="masthead-side">
          <span className="connection" data-state={live.status} role="status">
            <span className="connection-dot" aria-hidden="true" />
            {statusText}
          </span>
          <ThemeToggle />
          <a href={REPO} target="_blank" rel="noreferrer" style={{ fontSize: 'var(--t-sm)' }}>
            Source on GitHub
          </a>
        </div>
      </header>

      <PipelineBand pipeline={pipeline} lastPush={live.lastPush} status={live.status} now={now} />

      <TickerRow overview={overview} tickers={live.tickers} selected={pair} onSelect={selectPair} />

      <div className="market">
        <PriceChart
          pair={pair}
          interval={interval}
          onIntervalChange={setCandleInterval}
          subscribe={subscribe}
          livePrice={livePrice}
          alerts={alerts.filter((a) => a.symbol === pair)}
        />
        <div className="side">
          <IndicatorPanel
            pair={pair}
            indicators={live.indicators[pair] ?? (row ? { ...row, time: null } : null)}
            price={livePrice}
          />
          <TradeTape pair={pair} subscribe={subscribe} />
        </div>
      </div>

      <AlertsPanel alerts={alerts} modelInfo={modelInfo} onSelect={selectPair} />

      <PipelineDetails pipeline={pipeline} />

      <BatchHistory analytics={analytics} />

      <p className="colophon">
        Built by Vishantkumar Bhavsar with Python, Kafka, Spark Structured Streaming, Spark SQL and MLlib, Hadoop HDFS,
        Hive, PostgreSQL with TimescaleDB,
        FastAPI, Redis and React. Market data from the Coinbase public feed; not financial advice.
      </p>
    </main>
  )
}
