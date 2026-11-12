// The "venom" theme switch, spreading like ink in water.
//
// Switching to dark: black ink bleeds out of the theme button (the dot) unevenly,
// and splashes are flung out of the dot across the page, where they land, bloom
// and run into the main stain until everything is black. Switching to light: the
// same in reverse; the splashes shrink and fly back into the dot, and the main
// stain is drawn back into it. Every switch spreads differently.
//
// How: the View Transitions API snapshots the page before and after the theme
// changes. The ink is a clip-path on one of the two snapshots: one path made of
// several closed shapes (the main stain plus the splashes), animated through a
// series of frames. Each shape keeps the same number of points in every frame, so
// the browser can morph smoothly from one frame to the next.

const FRAMES = 56 // shapes the animation passes through
const COVER_MS = 2400
const RETRACT_MS = 2200

function rand(seed) {
  // small deterministic generator: one switch's shapes stay consistent across frames
  let s = seed >>> 0
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0
    return s / 2 ** 32
  }
}

const between = (r, lo, hi) => lo + r() * (hi - lo)
const clamp01 = (x) => Math.min(1, Math.max(0, x))
// fast bloom that creeps to a stop, like a drop soaking into paper
const bloom = (x) => 1 - Math.pow(1 - clamp01(x), 2.4)

// An uneven edge: a few waves of random size and direction that drift over time
function makeEdge(r, waves, roughness) {
  return Array.from({ length: waves }, () => ({
    k: Math.floor(between(r, 2, 11)), // whole numbers keep the outline closed
    amp: between(r, 0.3, 1) * roughness,
    phase: between(r, 0, Math.PI * 2),
    drift: between(r, -3, 3),
  }))
}

// Fingers of ink that run ahead of the edge in a few directions
function makeRuns(r, count) {
  return Array.from({ length: count }, () => ({
    angle: between(r, 0, Math.PI * 2),
    length: between(r, 0.25, 1.1),
    width: between(r, 0.08, 0.22),
  }))
}

// A stain lands at (x, y). If it is a splash it is first flung there from the
// dot at (fromX, fromY), taking `fly` of the animation to arrive.
function makeStain(r, { x, y, fromX = x, fromY = y, fly = 0, size, start, end, points, roughness, runs, curve = bloom }) {
  return {
    x, y, fromX, fromY, fly, size, start, end, points, curve,
    dir: Math.atan2(y - fromY, x - fromX),
    edge: makeEdge(r, 5, roughness),
    runs: makeRuns(r, runs),
  }
}

const easeOut = (x) => 1 - Math.pow(1 - clamp01(x), 3)

// One closed, smooth outline for one stain at time t (0..1)
function stainPath(s, t) {
  // where the stain is: still in the dot, in flight, or landed
  const flight = s.fly > 0 ? clamp01((t - s.start) / s.fly) : 1
  const along = easeOut(flight)
  const x = s.fromX + (s.x - s.fromX) * along
  const y = s.fromY + (s.y - s.fromY) * along
  const flying = t > s.start && flight < 1 ? 1 - along : 0
  const grow = s.curve((t - s.start) / (s.end - s.start))
  // a drop in flight keeps a visible core even before it starts to bleed
  const core = t > s.start ? Math.min(s.size * 0.09, 14) * clamp01((t - s.start) / 0.03) : 0
  const radius = Math.max(s.size * grow, core, 0.3)
  // the edge is rough while the ink is running, and calms down as it settles
  const calm = 1 - 0.55 * clamp01((t - s.start) / (s.end - s.start))
  const pts = []
  for (let i = 0; i < s.points; i++) {
    const a = (i / s.points) * Math.PI * 2
    let n = 0
    for (const w of s.edge) n += w.amp * Math.sin(w.k * a + w.phase + w.drift * t)
    for (const run of s.runs) {
      const d = Math.atan2(Math.sin(a - run.angle), Math.cos(a - run.angle))
      n += run.length * Math.exp(-(d * d) / (2 * run.width * run.width)) * Math.sin(Math.PI * clamp01(grow * 1.2))
    }
    // while flying, the drop trails a tail back towards the dot
    const back = Math.max(0, -Math.cos(a - s.dir))
    const tail = 1 + 2.6 * flying * back * back
    const r = radius * Math.max(0.12, 1 + n * calm) * tail
    pts.push([x + r * Math.cos(a), y + r * Math.sin(a)])
  }
  const mid = (p, q) => [(p[0] + q[0]) / 2, (p[1] + q[1]) / 2]
  const f = (v) => v.toFixed(1)
  const first = mid(pts[pts.length - 1], pts[0])
  let d = `M${f(first[0])} ${f(first[1])}`
  for (let i = 0; i < pts.length; i++) {
    const m = mid(pts[i], pts[(i + 1) % pts.length])
    d += `Q${f(pts[i][0])} ${f(pts[i][1])} ${f(m[0])} ${f(m[1])}`
  }
  return d + 'Z'
}

function inkFrames(cx, cy) {
  const w = window.innerWidth
  const h = window.innerHeight
  const diag = Math.hypot(w, h)
  const r = rand(Date.now())

  // The main stain from the button. It spreads slowly at first so the splashes
  // show on their own, then gathers pace and swallows them. Its narrowest edge at
  // the end still clears the furthest corner, so the screen ends fully covered.
  const corner = Math.hypot(Math.max(cx, w - cx), Math.max(cy, h - cy))
  const stains = [
    makeStain(r, {
      x: cx,
      y: cy,
      size: corner / 0.76,
      start: 0,
      end: 1,
      points: 72,
      roughness: 0.13,
      runs: 5,
      curve: (x) => Math.pow(clamp01(x), 1.7),
    }),
  ]

  // Big splashes flung out of the dot, landing around the page and blooming
  const big = 4 + Math.floor(r() * 3)
  for (let i = 0; i < big; i++) {
    stains.push(
      makeStain(r, {
        x: between(r, 0.05, 0.95) * w,
        y: between(r, 0.08, 0.95) * h,
        fromX: cx,
        fromY: cy,
        fly: between(r, 0.14, 0.24),
        size: between(r, 0.08, 0.19) * diag,
        start: between(r, 0.06, 0.45),
        end: between(r, 0.6, 0.95),
        points: 44,
        roughness: 0.16,
        runs: 2,
      }),
    )
  }

  // Small drops flicked out of the dot
  const drops = 6 + Math.floor(r() * 5)
  for (let i = 0; i < drops; i++) {
    const angle = between(r, 0, Math.PI * 2)
    const dist = between(r, 0.1, 0.55) * diag
    stains.push(
      makeStain(r, {
        x: Math.min(w, Math.max(0, cx + Math.cos(angle) * dist)),
        y: Math.min(h, Math.max(0, cy + Math.sin(angle) * dist)),
        fromX: cx,
        fromY: cy,
        fly: between(r, 0.1, 0.2),
        size: between(r, 0.012, 0.05) * diag,
        start: between(r, 0.02, 0.3),
        end: between(r, 0.45, 0.85),
        points: 24,
        roughness: 0.18,
        runs: 1,
      }),
    )
  }

  const frames = []
  for (let i = 0; i <= FRAMES; i++) {
    const t = i / FRAMES
    frames.push({ clipPath: `path('${stains.map((s) => stainPath(s, t)).join('')}')` })
  }
  return frames
}

/**
 * Switch theme with the ink animation.
 * apply(): changes the theme on the page (must update the DOM synchronously).
 * toDark: true when going to the dark theme.
 * origin: the element the ink comes out of and goes back into.
 */
export function switchThemeWithVenom(apply, toDark, origin) {
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  if (!document.startViewTransition || reduced || !origin) {
    apply()
    return
  }
  const box = origin.getBoundingClientRect()
  const cx = box.left + box.width / 2
  const cy = box.top + box.height / 2
  const root = document.documentElement
  root.classList.add(toDark ? 'venom-cover' : 'venom-retract')

  let ink = null
  const transition = document.startViewTransition(() => apply())
  transition.ready.then(() => {
    const frames = inkFrames(cx, cy)
    if (toDark) {
      // the new, dark page shows through wherever the ink has spread
      ink = root.animate(frames, {
        duration: COVER_MS,
        easing: 'linear',
        fill: 'forwards',
        pseudoElement: '::view-transition-new(root)',
      })
    } else {
      // the old, dark page dries back in patches into the button
      ink = root.animate(frames.slice().reverse(), {
        duration: RETRACT_MS,
        easing: 'linear',
        fill: 'forwards',
        pseudoElement: '::view-transition-old(root)',
      })
    }
  })
  transition.finished.finally(() => {
    // A filled animation stays attached to the root and would clip the next
    // switch's snapshot, so clear it once this switch is over.
    ink?.cancel()
    root.classList.remove('venom-cover', 'venom-retract')
  })
}
