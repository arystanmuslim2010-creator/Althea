# Deployment cleanup (Render + Vercel)

## Summary of changes

- **API layer**: Single client in `src/services/api.js`. Base URL from `import.meta.env.VITE_API_BASE_URL`; all requests use `${API_BASE}/api` (or relative `/api` when empty). No hardcoded hosts or ports.
- **Connection errors**: Generic message "Cannot connect to backend service. Please try again." and `isConnectionError()` helper. Alert Queue shows a generic "Service temporarily unavailable" hint (i18n) instead of dev-era "Backend may not be reachable / verify server / port" text.
- **Env**: `.env.example` documents production (Render); `.env.development` documents local-only and keeps localhost for dev.
- **No feature removal**: All pages, routes, and API usage unchanged; only messages and centralization updated.

## Files touched

| File | Change |
|------|--------|
| `src/services/api.js` | Centralized client; `API_BASE` from env; exported `CONNECTION_ERROR_MESSAGE`, `isConnectionError()`; endpoint order aligned with OpenAPI; comment on VITE_DEBUG. |
| `src/pages/AlertQueue.jsx` | Import `isConnectionError`; `backendHint` → `connectionHint` (generic copy in all 6 languages); error block uses `isConnectionError(error)` and `ui.connectionHint`. |
| `.env.example` | Comment added: production = Render URL. |
| `.env.development` | Comment added: local dev only; production uses Vercel env. |
| `DEPLOYMENT_CLEANUP.md` | This file. |

## Confirmation: API base URL

All API routes go through `src/services/api.js`:

- `const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '')`
- `const API = API_BASE ? \`${API_BASE}/api\` : '/api'`
- Every `req()` / `reqForm()` call uses `` `${API}${path}` `` (e.g. `${API}/health`, `${API}/alerts`).

So in production (Vercel), set `VITE_API_BASE_URL` to your Render backend (e.g. `https://your-app.onrender.com`); all requests will use `https://your-app.onrender.com/api/...`.

## Sanity check (manual)

1. Home loads.
2. `/api/health` called successfully (e.g. from Ops & Governance or any page that triggers health).
3. Alert Queue loads data via `/api/alerts` and `/api/queue-metrics` (and `/api/run-info`).
4. Runs: app uses `/api/run-info` and `/api/runs` where applicable.
5. Cases page uses `/api/cases`, `/api/cases/{id}/audit`, create/update/delete.
6. Actor selector uses `/api/actor` (GET on load, PUT on change).

Build: `npm run build` — passes. No ESLint errors on touched files.

## TODO (optional, no feature change)

- Consider a small `useApiState` or `useAsyncAction` hook to deduplicate loading/error/setError patterns across AlertQueue, DataConfig, OpsGovernance, and Cases if desired later.
