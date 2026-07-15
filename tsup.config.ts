import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm"],
  target: "node22",
  platform: "node",
  sourcemap: true,
  dts: true,
  clean: true,
  splitting: false,
  treeshake: true,
  minify: false,
  bundle: false,
  outDir: "dist",
  external: ["amqplib"],
});