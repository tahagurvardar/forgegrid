import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
const target = process.env.FORGEGRID_API_URL ?? "http://127.0.0.1:8080";
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/api": { target }, "/healthz": { target } },
  },
  preview: { proxy: { "/api": { target }, "/healthz": { target } } },
  test: {
    globals: true,
    environment: "jsdom",
    setupFiles: ["./src/test-setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
