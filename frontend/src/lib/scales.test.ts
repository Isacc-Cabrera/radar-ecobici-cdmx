/**
 * Autor: Isacc Cabrera
 * Descripción: Pruebas del sistema de diseño: contraste WCAG AA de cada par texto/fondo en ambos
 *   temas, umbrales de la clase de predicción, tramos de la rampa y resolución del tema inicial.
 * Patrón: N/A (pruebas unitarias)
 * Fecha de creación: 2026-11-08
 * Cambios:
 *     - 2026-11-08 · Isacc Cabrera · Creación desde el blueprint (§7 y §15).
 */
import { describe, expect, it } from "vitest";
import {
  BIKE_RAMP,
  THEME_TOKENS,
  type Theme,
  bikeColor,
  contrastRatio,
  hexToRgb,
  predictionClass,
  resolveTheme,
} from "./scales";

const THEMES: Theme[] = ["dark", "light"];

const TEXT_PAIRS: Array<[string, string]> = [
  ["text", "bg"],
  ["text", "surface"],
  ["text", "surface2"],
  ["textMuted", "bg"],
  ["textMuted", "surface"],
  ["textMuted", "surface2"],
  ["primary", "bg"],
  ["primaryFg", "primary"],
  ["destructive", "bg"],
];

describe("contraste WCAG 2.2 AA", () => {
  for (const theme of THEMES) {
    const tokens = THEME_TOKENS[theme];
    for (const [fg, bg] of TEXT_PAIRS) {
      it(`${theme}: ${fg} sobre ${bg} >= 4.5:1`, () => {
        expect(contrastRatio(tokens[fg] ?? "", tokens[bg] ?? "")).toBeGreaterThanOrEqual(4.5);
      });
    }
    it(`${theme}: focus sobre bg >= 3:1`, () => {
      expect(contrastRatio(tokens.focus ?? "", tokens.bg ?? "")).toBeGreaterThanOrEqual(3);
    });
  }
});

describe("predictionClass", () => {
  it("-3 o menos se vacía", () => {
    expect(predictionClass(-3)).toBe("se_vacia");
  });
  it("entre -3 y +3 es estable", () => {
    expect(predictionClass(-2.9)).toBe("estable");
    expect(predictionClass(2.9)).toBe("estable");
  });
  it("+3 o más se llena", () => {
    expect(predictionClass(3)).toBe("se_llena");
  });
  it("sin predicción devuelve null", () => {
    expect(predictionClass(null)).toBeNull();
  });
});

describe("bikeColor", () => {
  it("estación vacía usa el primer tramo", () => {
    expect(bikeColor(0, 10)).toEqual(hexToRgb(BIKE_RAMP[0] ?? ""));
  });
  it("estación llena usa el último tramo", () => {
    expect(bikeColor(10, 10)).toEqual(hexToRgb(BIKE_RAMP[3] ?? ""));
  });
  it("capacidad cero no divide entre cero", () => {
    expect(bikeColor(0, 0)).toEqual(hexToRgb(BIKE_RAMP[0] ?? ""));
  });
});

describe("resolveTheme", () => {
  it("lo guardado gana sobre el sistema", () => {
    expect(resolveTheme("light", true)).toBe("light");
  });
  it("sin valor guardado sigue prefers-color-scheme", () => {
    expect(resolveTheme(null, true)).toBe("dark");
    expect(resolveTheme(null, false)).toBe("light");
  });
  it("un valor inválido se ignora", () => {
    expect(resolveTheme("sepia", false)).toBe("light");
  });
});
