import { formatNumber, formatPct, formatPrice } from '../format.js'

// Latest indicators for the selected pair, each with one line saying what it means.
export default function IndicatorPanel({ pair, indicators, price }) {
  const i = indicators ?? {}
  const rsi = i.rsi_14
  const rsiText =
    rsi == null ? 'Needs 14 minutes of trading.' : rsi >= 70 ? 'Above 70: buying has been strong.' : rsi <= 30 ? 'Below 30: selling has been strong.' : 'Between 30 and 70: no strong pressure either way.'
  const vsVwap = i.vwap_15 && price ? (price / i.vwap_15 - 1) * 100 : null
  const updated = i.time ? new Date(i.time * 1000).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }) : null

  return (
    <section className="panel side-section" aria-label={`${pair} indicators`}>
      <h3>
        Indicators{' '}
        <span className="muted" style={{ fontWeight: 400, fontSize: 'var(--t-sm)' }}>
          {updated ? `for the minute ending ${updated}` : 'waiting for the first closed minute'}
        </span>
      </h3>
      <dl className="indicator-list">
        <div className="indicator">
          <dt>Relative strength (14 min)</dt>
          <dd>{formatNumber(rsi, 1)}</dd>
          <div className="rsi-bar" aria-hidden="true">
            <span className="rsi-zone" />
            {rsi != null && <span className="rsi-mark" style={{ left: `${rsi}%` }} />}
          </div>
          <p className="explain">{rsiText}</p>
        </div>
        <div className="indicator">
          <dt>Volatility (15 min)</dt>
          <dd>{i.volatility_15 == null ? '–' : `${formatNumber(i.volatility_15, 3)}%`}</dd>
          <p className="explain">Typical size of a one-minute price move.</p>
        </div>
        <div className="indicator">
          <dt>Price vs 15-min VWAP</dt>
          <dd className={vsVwap > 0 ? 'up' : vsVwap < 0 ? 'down' : ''}>{formatPct(vsVwap, 3)}</dd>
          <p className="explain">
            {i.vwap_15 == null ? 'VWAP is' : `VWAP ${formatPrice(i.vwap_15)} is`} the average price paid, weighted by size.
          </p>
        </div>
        <div className="indicator">
          <dt>Volume vs 15-min average</dt>
          <dd>{i.volume_ratio_15 == null ? '–' : `${formatNumber(i.volume_ratio_15, 2)}×`}</dd>
          <p className="explain">Above 1 means the last minute was busier than usual.</p>
        </div>
      </dl>
    </section>
  )
}
