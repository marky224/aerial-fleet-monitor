// PostCSS config — wires Tailwind and Autoprefixer into Vite's CSS pipeline.
//
// Vite auto-discovers this file at the web/ root and applies the plugins
// to all CSS files (including the one imported by src/main.tsx).

export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
};
