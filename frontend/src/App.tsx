import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { fetchRows, fetchTables, type PageResponse } from './lib/api'

const HIDDEN_COLS = new Set(['id', 'transactionid', 'transaction_id', 'attom_id', 'attomid'])

function titleCase(s: string): string {
  return s
    .split(' ')
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ')
}

const TOKENS = [
  'property',
  'address',
  'house',
  'number',
  'street',
  'direction',
  'suffix',
  'post',
  'unit',
  'value',
  'city',
  'state',
  'zip',
  'zip4',
  'county',
  'code',
  'fips',
  'jurisdiction',
  'name',
  'situs',
  'document',
  'recording',
  'record',
  'instrument',
  'book',
  'page',
  'legal',
  'parcel',
  'formatted',
  'raw',
  'year',
  'added',
  'change',
  'previous',
  'tax',
  'assessed',
  'market',
  'value',
  'improvements',
  'land',
  'mortgage',
  'lender',
  'transfer',
  'amount',
  'date',
  'lat',
  'latitude',
  'long',
  'longitude',
  'geo',
  'quality',
  'census',
  'tract',
  'block',
  'group',
  'owner',
  'deed',
  'sale',
  'grantor',
  'grantee',
  'mail',
  'info',
  'privacy',
  'type',
  'flag',
]

function prettyTableName(name: string): string {
  const n = name.trim()
  if (!n) return ''
  if (n.includes('_')) return titleCase(n.replace(/_+/g, ' '))
  return titleCase(n.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/([a-zA-Z])(\d+)/g, '$1 $2'))
}

function prettyHeader(col: string): string {
  const c = col.trim()
  if (!c) return ''

  // snake_case -> words
  if (c.includes('_')) return titleCase(c.replace(/_+/g, ' '))

  // Split digits (zip4 -> zip 4)
  const withDigits = c.replace(/([a-zA-Z])(\d+)/g, '$1 $2')
  if (withDigits.includes(' ')) return titleCase(withDigits)

  // Best-effort greedy tokenization for concatenated lowercase headers
  const s = c.toLowerCase()
  const tokens = [...TOKENS].sort((a, b) => b.length - a.length)
  const parts: string[] = []
  let i = 0
  while (i < s.length) {
    let matched = ''
    for (const t of tokens) {
      if (s.startsWith(t, i)) {
        matched = t
        break
      }
    }
    if (matched) {
      parts.push(matched)
      i += matched.length
    } else {
      parts.push(s[i])
      i += 1
    }
  }

  // Merge single letters into previous token
  const merged: string[] = []
  for (const p of parts) {
    if (p.length === 1 && merged.length) merged[merged.length - 1] += p
    else merged.push(p)
  }

  return titleCase(merged.join(' '))
}

function formatCellValue(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (typeof v === 'string') {
    // ISO date-time like 2026-04-08T00:00:00 or 2026-04-08 00:00:00 -> show date only
    if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}/.test(v)) return v.slice(0, 10)
    return v
  }
  if (v instanceof Date && !Number.isNaN(v.getTime())) return v.toISOString().slice(0, 10)
  return String(v)
}

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value)
  useEffect(() => {
    const t = window.setTimeout(() => setDebounced(value), delayMs)
    return () => window.clearTimeout(t)
  }, [value, delayMs])
  return debounced
}

function App() {
  const [schema] = useState('attom_dataset')
  const [tables, setTables] = useState<string[]>([])
  const [activeTable, setActiveTable] = useState<string | null>(null)
  const [q, setQ] = useState('')
  const [rowQuery, setRowQuery] = useState('')
  const debouncedRowQuery = useDebouncedValue(rowQuery, 350)
  const [colFilterColumn, setColFilterColumn] = useState('')
  const [colFilterValue, setColFilterValue] = useState('')
  const [colFilters, setColFilters] = useState<Array<{ column: string; value: string }>>([])

  const [pageSize, setPageSize] = useState(50)
  const [page, setPage] = useState(1)

  const [data, setData] = useState<PageResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      setError(null)
      try {
        const resp = await fetchTables(schema)
        if (cancelled) return
        setTables(resp.tables)
        setActiveTable((prev) => prev ?? resp.tables[0] ?? null)
      } catch (e: any) {
        if (cancelled) return
        setError(e?.message ?? 'Failed to load tables')
      }
    })()
    return () => {
      cancelled = true
    }
  }, [schema])

  useEffect(() => {
    if (!activeTable) return
    let cancelled = false
    ;(async () => {
      setLoading(true)
      setError(null)
      try {
        const resp = await fetchRows({
          schema,
          table: activeTable,
          page,
          pageSize,
          q: debouncedRowQuery,
          filters: colFilters,
        })
        if (cancelled) return
        setData(resp)
      } catch (e: any) {
        if (cancelled) return
        setError(e?.message ?? 'Failed to load rows')
        setData(null)
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [schema, activeTable, page, pageSize, debouncedRowQuery, colFilters])

  const filteredTables = useMemo(() => {
    const needle = q.trim().toLowerCase()
    if (!needle) return tables
    return tables.filter((t) => t.toLowerCase().includes(needle))
  }, [tables, q])

  const totalPages = data ? Math.max(1, Math.ceil(data.total_rows / data.page_size)) : 1
  const visibleColumns = useMemo(() => {
    if (!data) return []
    return data.columns.filter((c) => !HIDDEN_COLS.has(String(c).toLowerCase()))
  }, [data])

  const pageShownCount = data ? data.rows.length : 0

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <div className="title">Data from attom</div>
          <div className="subtle">Schema: {schema}</div>
        </div>
        <div className="subtle">
          {activeTable ? `Table: ${prettyTableName(activeTable)}` : 'No table selected'}
        </div>
      </header>

      <div className="content">
        <aside className="sidebar">
          <div className="sidebarHeader">
            <input
              className="search"
              placeholder="Search tables…"
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
          </div>
          <div className="tableList">
            {filteredTables.map((t) => (
              <button
                key={t}
                className={
                  'tableItem ' + (t === activeTable ? 'tableItemActive' : '')
                }
                onClick={() => {
                  setActiveTable(t)
                  setPage(1)
                }}
              >
                <span className="badge">tbl</span>
                <span>{prettyTableName(t)}</span>
              </button>
            ))}
            {!filteredTables.length && (
              <div className="empty">No matching tables.</div>
            )}
          </div>
        </aside>

        <main className="main">
          <div className="panel">
            <div className="panelHeader">
              <div>
                <div className="panelTitle">
                  {activeTable ? prettyTableName(activeTable) : 'Select a table'}
                </div>
              </div>

              <div className="controls">
                <input
                  className="search"
                  style={{ width: 260 }}
                  placeholder="Search rows (any column)…"
                  value={rowQuery}
                  onChange={(e) => {
                    setRowQuery(e.target.value)
                    setPage(1)
                  }}
                />

                <select
                  className="select"
                  value={colFilterColumn}
                  onChange={(e) => setColFilterColumn(e.target.value)}
                >
                  <option value="">Filter column…</option>
                  {visibleColumns.map((c) => (
                    <option key={c} value={c}>
                      {prettyHeader(c)}
                    </option>
                  ))}
                </select>
                <input
                  className="search"
                  style={{ width: 220 }}
                  placeholder="Value…"
                  value={colFilterValue}
                  onChange={(e) => setColFilterValue(e.target.value)}
                />
                <button
                  className="btn"
                  disabled={!colFilterColumn || !colFilterValue.trim()}
                  onClick={() => {
                    const column = colFilterColumn.trim().toLowerCase()
                    const value = colFilterValue.trim()
                    setColFilters((prev) => [...prev, { column, value }])
                    setColFilterValue('')
                    setPage(1)
                  }}
                >
                  Add
                </button>
              </div>
            </div>

            <div className="tableWrap">
              {!activeTable && <div className="empty">Pick a table from the left.</div>}
              {activeTable && loading && <div className="empty">Loading…</div>}
              {activeTable && !loading && data && (
                <table className="data">
                  <thead>
                    <tr>
                      {visibleColumns.map((c) => (
                        <th key={c}>{prettyHeader(c)}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.rows.map((r, idx) => (
                      <tr key={idx}>
                        {visibleColumns.map((c) => (
                          <td key={c}>{formatCellValue((r as any)[c])}</td>
                        ))}
                      </tr>
                    ))}
                    {!data.rows.length && (
                      <tr>
                        <td colSpan={visibleColumns.length || 1} className="empty">
                          No rows.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              )}
            </div>

            {activeTable && data && (
              <div className="panelFooter">
                <div className="subtle">
                  Showing {pageShownCount.toLocaleString()} of {data.total_rows.toLocaleString()}{' '}
                  records • Page {data.page} of {totalPages}
                  {rowQuery.trim() || colFilters.length ? ` • Filtered` : ''}
                </div>

                <div className="controls">
                  {colFilters.length > 0 && (
                    <>
                      <div className="chipRow" aria-label="Active filters">
                        {colFilters.map((f, i) => (
                          <span className="chip" key={`${f.column}::${f.value}::${i}`}>
                            <span>
                              {prettyHeader(f.column)}: {f.value}
                            </span>
                            <button
                              className="chipRemove"
                              onClick={() => {
                                setColFilters((prev) => prev.filter((_, idx) => idx !== i))
                                setPage(1)
                              }}
                              title="Remove filter"
                              aria-label="Remove filter"
                              type="button"
                            >
                              ×
                            </button>
                          </span>
                        ))}
                      </div>

                      <button
                        className="btn"
                        onClick={() => {
                          setColFilters([])
                          setPage(1)
                        }}
                        type="button"
                      >
                        Clear filters
                      </button>
                    </>
                  )}

                  <label className="subtle">
                    Page size{' '}
                    <select
                      className="select"
                      value={pageSize}
                      onChange={(e) => {
                        setPageSize(Number(e.target.value))
                        setPage(1)
                      }}
                    >
                      {[25, 50, 100, 250, 500].map((n) => (
                        <option key={n} value={n}>
                          {n}
                        </option>
                      ))}
                    </select>
                  </label>

                  <button
                    className="btn"
                    disabled={!data || page <= 1 || loading}
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                  >
                    Prev
                  </button>
                  <button
                    className="btn"
                    disabled={!data || page >= totalPages || loading}
                    onClick={() => setPage((p) => p + 1)}
                  >
                    Next
                  </button>
                </div>
              </div>
            )}

            {error && <div className="error">{error}</div>}
          </div>
        </main>
      </div>
    </div>
  )
}

export default App
