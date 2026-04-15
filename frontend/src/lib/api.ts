export type TablesResponse = {
  schema: string
  tables: string[]
}

export type PageResponse = {
  schema: string
  table: string
  page: number
  page_size: number
  total_rows: number
  columns: string[]
  rows: Record<string, unknown>[]
}

function apiBase(): string {
  // In Vite, import.meta.env is baked at BUILD time.
  // In Docker+nginx deployments, runtime env vars won't affect the built bundle.
  // So we support:
  // 1) VITE_API_BASE baked at build time (preferred)
  // 2) a relative '/api' fallback (when nginx is configured to proxy /api -> backend)
  // 3) as a last resort, same-host :5000 (common backend port in your deployment)
  const baked = import.meta.env.VITE_API_BASE
  if (baked && baked.trim()) return baked.trim()

  if (typeof window !== 'undefined') {
    // If you don't set VITE_API_BASE, we try a sensible default for your AWS setup.
    return `${window.location.protocol}//${window.location.hostname}:5000`
  }

  return 'http://127.0.0.1:8000'
}

export async function fetchTables(schema?: string): Promise<TablesResponse> {
  const url = new URL('/tables', apiBase())
  if (schema) url.searchParams.set('schema', schema)
  const res = await fetch(url)
  if (!res.ok) throw new Error(`Failed to load tables (${res.status})`)
  return (await res.json()) as TablesResponse
}

export async function fetchRows(params: {
  table: string
  schema?: string
  page: number
  pageSize: number
  q?: string
}): Promise<PageResponse> {
  const url = new URL(`/tables/${encodeURIComponent(params.table)}/rows`, apiBase())
  if (params.schema) url.searchParams.set('schema', params.schema)
  url.searchParams.set('page', String(params.page))
  url.searchParams.set('page_size', String(params.pageSize))
  if (params.q && params.q.trim()) url.searchParams.set('q', params.q.trim())
  const res = await fetch(url)
  if (!res.ok) throw new Error(`Failed to load rows (${res.status})`)
  return (await res.json()) as PageResponse
}
