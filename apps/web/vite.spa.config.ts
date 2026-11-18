import babel from '@rolldown/plugin-babel'
import tailwindcss from '@tailwindcss/vite'
import viteReact, { reactCompilerPreset } from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  build: {
    outDir: 'dist-spa',
    emptyOutDir: true,
  },
  resolve: {
    tsconfigPaths: true,
  },
  optimizeDeps: {
    include: ['@pierre/diffs', '@pierre/diffs/edit', '@pierre/diffs/react'],
  },
  worker: {
    format: 'es',
  },
  plugins: [
    tailwindcss(),
    viteReact(),
    babel({ presets: [reactCompilerPreset()] }),
  ],
})
