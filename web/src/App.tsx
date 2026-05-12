// Phase 00 placeholder. Renders the "coming soon" screen so the build
// doc's acceptance criterion #4 is satisfied. Phase 03 replaces this
// with the real shell: header, KPI strip, map, drill-down, recent cases.

export default function App() {
  return (
    <main className="flex h-full items-center justify-center bg-slate-50 text-slate-900">
      <div className="text-center">
        <h1 className="text-3xl font-semibold tracking-tight">
          Aerial Fleet Monitor
        </h1>
        <p className="mt-2 text-sm text-slate-600">v1.0 — coming soon</p>
      </div>
    </main>
  );
}
