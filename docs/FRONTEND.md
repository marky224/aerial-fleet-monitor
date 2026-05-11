# Aerial Fleet Monitor — Frontend

The full frontend specification documents AFM's React 18 + TypeScript + Vite application, including ArcGIS Maps SDK integration, state-management strategy, view-mode handling, and component architecture.

## Topics covered in the full specification

- Project setup (toolchain, build/deploy via S3 + CloudFront, environment variables)
- Directory layout (`src/api/`, `src/components/` by domain, `src/pages/`, `src/lib/`, `src/store/`)
- Routing (React Router 6 structure)
- State model:
  - Server state via TanStack Query, with per-hook refetch intervals matched to data freshness
  - Client state via a single Zustand store for cross-component coordination
- Auth handling (session bootstrap from `/v1/auth/me`, customer-view switching via Salesforce OAuth, logout)
- ArcGIS integration patterns:
  - Map initialization with the Light Gray Canvas basemap
  - Three-layer architecture (aircraft GraphicsLayer, airport FeatureLayer, trail GraphicsLayer)
  - Click-and-select coordination via the Zustand store
  - Region-scoping render rules (out-of-scope = dimmed, in-scope = full opacity)
  - Performance gotchas (avoid popup templates at 5K markers, throttle hover, etc.)
- shadcn/ui component inventory and Recharts for the SLA-trend sparkline
- Layout primitives (`AppShell`, `DashboardPage`)
- View-mode switching (internal-ops vs customer view derived from `useMe()`)
- Time and timezone handling (UTC API, user-tz display)
- Markdown rendering (server-side for runbooks, client-side for briefs)
- Testing approach (Vitest unit + component, Playwright e2e in a separate directory)
- Accessibility baseline (WCAG AA on contrast, keyboard nav, no full a11y on the map)
- Explicitly out of v1 (mobile responsive, i18n, dark-mode toggle, live-trail mode)

The full frontend specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the ArcGIS layer implementations, the Zustand store shape, and the testing patterns, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
