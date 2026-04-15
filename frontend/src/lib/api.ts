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
  const base = import.meta.env.VITE_API_BASE
  if (!base) {
    throw new Error(
      'Missing VITE_API_BASE. Set it in frontend/.env (dev) or as a build env var (prod).',
    )
  }
  return base
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
