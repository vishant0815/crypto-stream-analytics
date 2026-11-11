// Number and time formatting shared by all components.

export function decimalsFor(price) {
  if (price == null) return 2
  if (price >= 1000) return 2
  if (price >= 1) return 4
  return 6
}

export function formatPrice(price, decimals = decimalsFor(price)) {
  if (price == null || Number.isNaN(price)) return '–'
  return price.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
}

export function formatPct(pct, digits = 2) {
  if (pct == null || Number.isNaN(pct)) return '–'
  const sign = pct > 0 ? '+' : pct < 0 ? '−' : ''
  return `${sign}${Math.abs(pct).toFixed(digits)}%`
}

export function formatNumber(n, digits = 0) {
  if (n == null || Number.isNaN(n)) return '–'
  return n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

export function formatSize(size) {
  if (size == null) return '–'
  if (size >= 1000) return formatNumber(size, 0)
  if (size >= 1) return formatNumber(size, 3)
  // Three significant digits, never exponent notation: 0.00000009, not 9.00e-8
  return size.toLocaleString('en-US', { maximumSignificantDigits: 3, maximumFractionDigits: 12 })
}

export function formatBytes(bytes) {
  if (bytes == null) return '–'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  let v = bytes
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i += 1
  }
  return `${v.toFixed(v >= 100 || i === 0 ? 0 : 1)} ${units[i]}`
}

export function formatClock(ms) {
  return new Date(ms).toLocaleTimeString('en-GB', { hour12: false })
}

export function formatAge(seconds) {
  if (seconds == null) return '–'
  if (seconds < 1) return 'under 1 s'
  if (seconds < 60) return `${Math.round(seconds)} s`
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`
  return `${Math.round(seconds / 3600)} h`
}

export function directionClass(value) {
  if (value == null || value === 0) return ''
  return value > 0 ? 'up' : 'down'
}
