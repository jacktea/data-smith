import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// dev 端口 5173;/api 代理到本地 Go server(契约第 4 节)
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});
