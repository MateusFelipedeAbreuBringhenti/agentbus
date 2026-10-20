import react from "@vitejs/plugin-react";
import { loadEnv } from "vite";
import { defineConfig } from "vitest/config";

export default defineConfig(({ mode }) => {
  const environment = loadEnv(mode, ".", "");
  return ({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    proxy: {
      "/api": {
        target: environment.AGENTBUS_CONTROL_CENTER_GATEWAY ?? "http://127.0.0.1:8001",
        changeOrigin: false,
      },
      "/intent-api": {
        target: environment.AGENTBUS_APPROVAL_INTENT_SERVICE ?? "http://127.0.0.1:8002",
        changeOrigin: false,
      },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
  },
  });
});
