# SolarIQ Operations Dashboard

React + TypeScript + Vite frontend for the SolarIQ serving layer. Consumes the
FastAPI service in [`../api`](../api) — no mocked data outside tests.

## Develop

```bash
npm install
cp .env.example .env.local   # point VITE_API_BASE_URL at your running API
npm run dev
```

The browser runs this app on the host, so `VITE_API_BASE_URL` must be a
host-reachable address (`http://localhost:8000`), even when the API itself
runs inside Docker Compose — the browser cannot resolve the Compose service
name `api`. See [`../docs/demo-runbook.md`](../docs/demo-runbook.md).

## Test

```bash
npm run test      # vitest
npm run lint       # eslint
npm run build      # tsc -b && vite build — also the production build
```

## Structure

```text
src/
├── api/        typed fetch client + response types, mirroring api/app/models/
├── components/ KpiCard, StatusBadge, DataState (loading/error/empty), charts/
├── hooks/      React Query hooks (5s polling for live data)
├── lib/        presentation-only formatting + CSV export
├── pages/      PortfolioPage, PlantPage, AlertsPage, DailyReportPage
└── tests/      vitest + Testing Library
```

## Pages

| Route | Shows |
|---|---|
| `/` | Live portfolio KPIs, today's reconciliation, plant ranking, active alerts |
| `/plants/:plantId` | Plant detail, power history, inverters, recent alerts |
| `/alerts` | Filterable operational alerts table |
| `/reports/daily` | Daily reconciliation report with CSV export |

Every data panel handles loading, error, empty and stale states explicitly
(`components/DataState.tsx`, `components/StatusBadge.tsx`) — no panel ever
fabricates a value the API did not return.
