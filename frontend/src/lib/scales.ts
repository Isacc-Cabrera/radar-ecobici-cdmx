/**
 * Autor: Isacc Cabrera
 * Descripción: Sistema de diseño en código: paletas por tema, rampa de bicis disponibles, clases
 *   de predicción (se vacía / estable / se llena), mapas base de OpenFreeMap y utilidades de
 *   contraste WCAG. Lógica pura, sin React, para que Vitest la pruebe en entorno node.
 * Patrón: N/A (design tokens + funciones puras)
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint (§7).
 */

export type Theme = "dark" | "light";
export type PredictionClass = "se_vacia" | "estable" | "se_llena";
export type Rgb = [number, number, number];

export const THEME_STORAGE_KEY = "radar-tema";

export const THEME_TOKENS: Record<Theme, Record<string, string>> = {
  dark: {
    bg: "#07120D",
    surface: "#0E1F17",
    surface2: "#14291F",
    border: "#1F3D2E",
    text: "#E3F5EA",
    textMuted: "#8DB8A1",
    primary: "#22C55E",
    primaryStrong: "#16A34A",
    primaryFg: "#07120D",
    accent: "#A3E635",
    focus: "#4ADE80",
    destructive: "#F87171",
  },
  light: {
    bg: "#F2FAF5",
    surface: "#FFFFFF",
    surface2: "#E7F4EC",
    border: "#C9E4D3",
    text: "#0D2A1C",
    textMuted: "#4A6B5A",
    primary: "#15803D",
    primaryStrong: "#166534",
    primaryFg: "#FFFFFF",
    accent: "#65A30D",
    focus: "#16A34A",
    destructive: "#B91C1C",
  },
};

/** Rampa secuencial verde para bicis disponibles: vacía → llena (igual en ambos temas). */
export const BIKE_RAMP: readonly string[] = ["#E5F5E0", "#A1D99B", "#41AB5D", "#006D2C"];

/** Paleta divergente de la predicción t+60 (delta = predicción − actual). */
export const PREDICTION_COLORS: Record<PredictionClass, string> = {
  se_vacia: "#F97316",
  estable: "#84CC16",
  se_llena: "#06B6D4",
};

/** Etiquetas e íconos: la leyenda nunca depende solo del color (WCAG 1.4.1). */
export const PREDICTION_LABELS: Record<PredictionClass, { label: string; icon: string }> = {
  se_vacia: { label: "Se vacía", icon: "▼" },
  estable: { label: "Estable", icon: "●" },
  se_llena: { label: "Se llena", icon: "▲" },
};

export const ANOMALY_COLOR = "#EF4444";
export const PREDICTION_THRESHOLD = 3;

export const BASEMAPS: Record<Theme, string> = {
  dark: "https://tiles.openfreemap.org/styles/dark",
  light: "https://tiles.openfreemap.org/styles/positron",
};

export const BASEMAP_ATTRIBUTION = "OpenFreeMap © OpenMapTiles Data from OpenStreetMap";

/** Convierte "#RRGGBB" en [r, g, b]. */
export function hexToRgb(hex: string): Rgb {
  const clean = hex.replace("#", "");
  return [
    parseInt(clean.slice(0, 2), 16),
    parseInt(clean.slice(2, 4), 16),
    parseInt(clean.slice(4, 6), 16),
  ];
}

/** Color de la columna según la proporción bicis/capacidad (cuatro tramos de BIKE_RAMP). */
export function bikeColor(bikes: number, capacity: number): Rgb {
  const ratio = capacity > 0 ? bikes / capacity : 0;
  let index = 3;
  if (bikes <= 0) {
    index = 0;
  } else if (ratio <= 1 / 3) {
    index = 1;
  } else if (ratio <= 2 / 3) {
    index = 2;
  }
  return hexToRgb(BIKE_RAMP[index] ?? "#E5F5E0");
}

/** Clasifica el delta predicho: ≤ −3 se vacía, ≥ +3 se llena, el resto estable. */
export function predictionClass(delta: number | null): PredictionClass | null {
  if (delta === null || Number.isNaN(delta)) {
    return null;
  }
  if (delta <= -PREDICTION_THRESHOLD) {
    return "se_vacia";
  }
  if (delta >= PREDICTION_THRESHOLD) {
    return "se_llena";
  }
  return "estable";
}

/** Tema inicial: lo guardado en localStorage gana; si no, prefers-color-scheme. */
export function resolveTheme(stored: string | null, prefersDark: boolean): Theme {
  if (stored === "dark" || stored === "light") {
    return stored;
  }
  return prefersDark ? "dark" : "light";
}

function channel(value: number): number {
  const c = value / 255;
  return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
}

/** Luminancia relativa WCAG 2.x de un color hex. */
export function relativeLuminance(hex: string): number {
  const [r, g, b] = hexToRgb(hex);
  return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
}

/** Razón de contraste WCAG entre dos colores hex (1 a 21). */
export function contrastRatio(a: string, b: string): number {
  const la = relativeLuminance(a);
  const lb = relativeLuminance(b);
  const [hi, lo] = la >= lb ? [la, lb] : [lb, la];
  return (hi + 0.05) / (lo + 0.05);
}
