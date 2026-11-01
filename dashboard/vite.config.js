import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In development, /api and /ws go to the FastAPI server on port 8000.
// In Docker, nginx does the same job (see nginx.conf).
const api = process.env.API_URL || 'http://localhost:8000'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': api,
      '/ws': { target: api.replace(/^http/, 'ws'), ws: true },
    },
  },
  preview: {
    port: 3000,
    proxy: {
      '/api': api,
      '/ws': { target: api.replace(/^http/, 'ws'), ws: true },
    },
  },
})
