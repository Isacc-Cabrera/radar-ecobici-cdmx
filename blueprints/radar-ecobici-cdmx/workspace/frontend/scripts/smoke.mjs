/**
 * Autor: Isacc Cabrera
 * Descripción: Prueba de humo del build: levanta `vite preview` con la API de Vite en el mismo
 *   proceso, pide "/" y sale con 0 solo si responde 200 con el contenedor #root. Un solo proceso
 *   evita servidores huérfanos en Windows. Requiere haber corrido `npm run build` antes.
 * Patrón: N/A
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
 */
import { preview } from "vite";

const server = await preview({ preview: { port: 4173, strictPort: false } });
let ok = false;
try {
  const url = server.resolvedUrls?.local[0] ?? "http://localhost:4173/";
  const response = await fetch(url);
  const html = await response.text();
  const hasRoot = html.includes('id="root"');
  ok = response.status === 200 && hasRoot;
  console.log(JSON.stringify({ url, status: response.status, root: hasRoot }));
} finally {
  await server.close();
}
process.exit(ok ? 0 : 1);
