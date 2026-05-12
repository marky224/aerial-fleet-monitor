// Vite dev server + build config.
//
// Phase 00 scope: bring up the dev server at http://localhost:5173 rendering
// the "AFM v1.0 — coming soon" placeholder. ArcGIS chunking, asset
// fingerprinting, and prod-bundle tuning land in Phase 03.

import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    host: "0.0.0.0",
    port: 5173,
    strictPort: true,
  },
  preview: {
    host: "0.0.0.0",
    port: 4173,
    strictPort: true,
  },
  build: {
    outDir: "dist",
    sourcemap: true,
    target: "es2022",
  },
});
