/**
 * Autor: Isacc Cabrera
 * Descripción: Punto de entrada de React: monta <App /> en #root e importa los estilos globales
 *   (tokens del sistema de diseño y la hoja de MapLibre).
 * Patrón: N/A
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint.
 */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "maplibre-gl/dist/maplibre-gl.css";
import "./styles/theme.css";
import App from "./App";

const rootElement = document.getElementById("root");
if (rootElement === null) {
  throw new Error("No existe el elemento #root en index.html");
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
