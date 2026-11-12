import { formatNumber } from '../format.js'

const TYPE_LABEL = { VOLUME_SPIKE: 'Volume spike', PRICE_JUMP: 'Price jump', OTHER: 'One-sided trading' }

// What made the minute unusual, in plain words, from the feature that drove the alert
export function explainAlert(a) {
  const f = a.features ?? {}
  const v = f[a.driver]
  if (v == null) return TYPE_LABEL[a.alert_type] ?? a.alert_type
  switch (a.driver) {
    case 'log_volume_ratio':
      return `Volume ${formatNumber(Math.exp(v), 0)}× the past hour’s average`
    case 'log_trades_ratio':
      return `${formatNumber(Math.exp(v), 0)}× the usual number of trades`
    case 'abs_ret_z':
      return `Price moved ${formatNumber(v, 1)}× its usual one-minute move`
    case 'range_z':
      return `High–low range ${formatNumber(v, 1)}× the usual`
    case 'vwap_dev_z':
      return `Closed ${formatNumber(v, 1)}× the usual move away from its average price`
    case 'buy_imbalance':
      return `${Math.round(50 + v * 50)}% of volume on one side`
    default:
      return TYPE_LABEL[a.alert_type] ?? a.alert_type
  }
}

function detectedBy(modelInfo) {
  if (!modelInfo) return null
  if (modelInfo.mode === 'rules' || !modelInfo.model) {
    return 'Fixed rules for now: the K-Means model trains nightly once the archive holds enough history.'
  }
  const m = modelInfo.model
  const trained = new Date(m.trained_at * 1000).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })
  return `K-Means model (Spark MLlib, ${m.k} clusters) trained ${trained} on ${formatNumber(m.training_rows)} pair-minutes from the HDFS archive. It flags about ${(m.alert_rate * 100).toFixed(1)}% of minutes.`
}

export default function AlertsPanel({ alerts, modelInfo, onSelect }) {
  const rows = alerts.slice(0, 8)
  return (
    <section className="alerts" aria-label="Unusual minutes">
      <div className="alerts-head">
        <h2>Unusual minutes</h2>
        <p className="lede">
          {detectedBy(modelInfo)}
          {modelInfo ? ` ${formatNumber(modelInfo.alerts_24h)} in the last 24 hours.` : ''}
        </p>
      </div>
      {rows.length === 0 ? (
        <p className="lede">Nothing unusual yet. Each minute is scored once it closes and has 30 minutes of history.</p>
      ) : (
        <div className="table-scroll">
          <table className="data-table alerts-table">
            <thead>
              <tr>
                <th scope="col">Minute (UTC)</th>
                <th scope="col">Pair</th>
                <th scope="col">What happened</th>
                <th scope="col" title="Distance from the nearest normal pattern; above the threshold is unusual">Score</th>
                <th scope="col">Flagged by</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((a) => (
                <tr key={`${a.symbol}-${a.time}`}>
                  <td>
                    {new Date(a.time * 1000).toLocaleString('en-GB', {
                      day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', timeZone: 'UTC',
                    })}
                  </td>
                  <th scope="row">
                    <button type="button" className="link-button" onClick={() => onSelect(a.symbol)}>
                      {a.symbol}
                    </button>
                  </th>
                  <td className="alerts-what">
                    <span className="alert-badge" data-type={a.alert_type}>{TYPE_LABEL[a.alert_type] ?? a.alert_type}</span>
                    {explainAlert(a)}
                  </td>
                  <td>{formatNumber(a.score, 2)}</td>
                  <td className="muted">{a.model_ver}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
