import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App.jsx'
import './styles.css'

// Apply a saved theme before the first paint, so the page doesn't flash
try {
  const saved = localStorage.getItem('theme')
  if (saved) document.documentElement.dataset.theme = saved
} catch {
  // storage unavailable
}

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
