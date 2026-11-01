import { useEffect, useRef, useState } from 'react'
import { directionClass, formatPct, formatPrice } from '../format.js'
import Sparkline from './Sparkline.jsx'

function Ticker({ row, live, selected, onSelect }) {
  const price = live?.price ?? row.price
  const change = live?.change_24h_pct ?? row.change_24h_pct
  const [flash, setFlash] = useState('')
  const prev = useRef(price)

  // Briefly tint the price when it moves, in the direction it moved
  useEffect(() => {
    if (prev.current != null && price != null && price !== prev.current) {
      setFlash(price > prev.current ? 'flash-up' : 'flash-down')
      const id = setTimeout(() => setFlash(''), 60)
      prev.current = price
      return () => clearTimeout(id)
    }
    prev.current = price
  }, [price])

  return (
    <button
      type="button"
      className="ticker"
      aria-pressed={selected}
      onClick={() => onSelect(row.symbol)}
      aria-label={`${row.symbol} ${formatPrice(price)}, ${formatPct(change)} in 24 hours`}
    >
      <span className="ticker-symbol">{row.symbol}</span>
      <span className={`ticker-change ${directionClass(change)}`}>{formatPct(change)}</span>
      <span className={`ticker-price ${flash}`}>{formatPrice(price)}</span>
      <span className="ticker-spark">
        <Sparkline values={row.spark} />
      </span>
    </button>
  )
}

export default function TickerRow({ overview, tickers, selected, onSelect }) {
  if (!overview) return <div className="tickers" aria-busy="true" />
  return (
    <nav className="tickers" aria-label="Trading pairs">
      {overview.map((row) => (
        <Ticker
          key={row.symbol}
          row={row}
          live={tickers[row.symbol]}
          selected={row.symbol === selected}
          onSelect={onSelect}
        />
      ))}
    </nav>
  )
}
