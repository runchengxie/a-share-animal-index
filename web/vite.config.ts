import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base 使用相对路径，配合 HashRouter，使站点在任意子路径（含 GitHub Pages 项目页）下均可运行。
export default defineConfig({
  plugins: [react()],
  base: "./",
});
