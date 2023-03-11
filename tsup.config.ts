import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['src/index.ts'],
  sourcemap: true,
  clean: true,
  format: ['esm', 'cjs'],
  target: 'es2022',
  outDir: 'lib',
  dts: {
    resolve: true,
    compilerOptions: {
      moduleResolution: 'node',
      lib: ['es2022', 'node'],
      allowSyntheticDefaultImports: true,
    }
  },
})
