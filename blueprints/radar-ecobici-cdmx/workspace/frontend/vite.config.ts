/**
 * Autor: Isacc Cabrera
 * Descripción: Configuración de Vite (dev server en el puerto 5173, que la API permite por CORS)
 *   y de Vitest (entorno node: las pruebas cubren lógica pura de escalas y tema).
 * Patrón: N/A
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
 */
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true },
  preview: { port: 4173 },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
    exclude: ["node_modules", "dist"],
  },
});
