import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],

  bundle: true,

  format: ["esm"],

  dts: true,

  splitting: false,

  clean: true,

  sourcemap: true,

  external: ["amqplib"],
});