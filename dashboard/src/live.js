// REST helpers and the live WebSocket connection.
import { useEffect, useRef, useState } from 'react'

// Same origin by default (Vite proxy in dev, nginx in Docker).
// For a separately hosted frontend, set VITE_API_URL at build time.
const API_URL = (import.meta.env.VITE_API_URL || '').replace(/\/$/, '')

export async function getJSON(path, params) {
  const query = params ? `?${new URLSearchParams(params)}` : ''
  const res = await fetch(`${API_URL}${path}${query}`)
  if (!res.ok) throw new Error(`${path} returned ${res.status}`)
  return res.json()
}

function wsUrl() {
  const base = API_URL || window.location.origin
  return `${base.replace(/^http/, 'ws')}/ws/live`
}

// Poll a REST endpoint every `everyMs`; returns the latest data or null.
export function usePolling(path, params, everyMs) {
  const [data, setData] = useState(null)
  const key = JSON.stringify(params ?? {})
  useEffect(() => {
    let stop = false
    let timer
    const load = async () => {
      try {
        const value = await getJSON(path, params)
        if (!stop) setData(value)
      } catch {
        // keep the last good data; the connection indicator shows the outage
      }
      if (!stop) timer = setTimeout(load, everyMs)
    }
    load()
    return () => {
      stop = true
      clearTimeout(timer)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, key, everyMs])
  return data
}

/**
 * The live feed. Components subscribe to raw messages through `subscribe`
 * (the chart and trade tape update imperatively, without re-rendering React),
 * while `tickers`, `indicators`, `status` and `lastPush` are React state.
 */
export function useLive() {
  const [status, setStatus] = useState('connecting') // connecting | live | down
  const [tickers, setTickers] = useState({})
  const [indicators, setIndicators] = useState({})
  const [lastPush, setLastPush] = useState(null) // { at: ms, pushes: n }
  const [liveAlerts, setLiveAlerts] = useState([]) // alerts pushed since the page opened
  const listeners = useRef(new Set())

  useEffect(() => {
    let ws
    let retry = 1000
    let timer
    let closed = false
    let pushes = 0

    const apply = (pairs) => {
      const t = {}
      const ind = {}
      for (const [pair, v] of Object.entries(pairs)) {
        if (v.ticker) t[pair] = v.ticker
        const latestInd = v.indicators ? v.indicators[v.indicators.length - 1] : v.indicators
        if (latestInd) ind[pair] = latestInd
      }
      const fresh = Object.values(pairs).flatMap((v) => v.alerts ?? [])
      if (fresh.length) setLiveAlerts((prev) => [...fresh, ...prev].slice(0, 100))
      if (Object.keys(t).length) setTickers((prev) => ({ ...prev, ...t }))
      if (Object.keys(ind).length) setIndicators((prev) => ({ ...prev, ...ind }))
    }

    const connect = () => {
      ws = new WebSocket(wsUrl())
      ws.onopen = () => {
        retry = 1000
        setStatus('live')
      }
      ws.onmessage = (event) => {
        const msg = JSON.parse(event.data)
        if (msg.type === 'snapshot') {
          const pairs = {}
          for (const [pair, v] of Object.entries(msg.pairs)) {
            pairs[pair] = { ticker: v.ticker, indicators: v.indicators ? [v.indicators] : undefined }
          }
          apply(pairs)
        } else if (msg.type === 'update') {
          apply(msg.pairs)
          pushes += 1
          setLastPush({ at: Date.now(), pushes })
        }
        listeners.current.forEach((fn) => fn(msg))
      }
      ws.onclose = () => {
        setStatus('down')
        if (!closed) {
          timer = setTimeout(connect, retry)
          retry = Math.min(retry * 2, 15000)
        }
      }
    }
    connect()
    return () => {
      closed = true
      clearTimeout(timer)
      ws?.close()
    }
  }, [])

  const subscribe = (fn) => {
    listeners.current.add(fn)
    return () => listeners.current.delete(fn)
  }

  return { status, tickers, indicators, lastPush, liveAlerts, subscribe }
}

// Re-render every `ms` so "updated 3 s ago" style text stays current.
export function useNow(ms = 1000) {
  const [now, setNow] = useState(Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), ms)
    return () => clearInterval(id)
  }, [ms])
  return now
}
