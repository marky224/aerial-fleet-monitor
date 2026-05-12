// PostCSS config — wires Tailwind and Autoprefixer into Vite's CSS pipeline.
//
// Vite auto-discovers this file at the web/ root and applies the plugins
// to all CSS files (including the one imported by src/main.tsx).
//
// Kept as .js (not .ts) so Vite can require() it without ts-node. The
// config has no TypeScript value — it's just two plugin names — and
// adding ts-node as a devDependency only for this one file isn't worth
// the surface area.

export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
};
