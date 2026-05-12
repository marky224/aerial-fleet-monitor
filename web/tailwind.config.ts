// Tailwind CSS config — Phase 00 minimal setup.
//
// Phase 03 will add the shadcn/ui design tokens (CSS variables for
// background / foreground / primary / accent / muted / destructive,
// border radius scale) under the same `theme.extend` block. For now
// the goal is just: utility classes work in App.tsx.

import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {},
  },
  plugins: [],
} satisfies Config;
