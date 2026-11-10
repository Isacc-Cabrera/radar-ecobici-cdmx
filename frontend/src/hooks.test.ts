/**
 * Autor: Isacc Cabrera
 * Descripción: Pruebas de la unión de estaciones con predicciones (mergeStations) y de las
 *   constantes de polling: cada estación conserva sus datos, recibe su predicción por
 *   station_id y queda con prediccion = null si la API no la trajo (p. ej. 503 sin modelo).
 * Patrón: N/A (pruebas unitarias)
 * Fecha de creación: 2026-11-11
 * Cambios:
 *     - 2026-11-11 · Isacc Cabrera · Creación (paso 14).
 */
import { describe, expect, it } from "vitest";
import { HISTORY_HOURS, POLL_MS, type PredictionOut, type StationOut, mergeStations } from "./hooks";

const station = (id: string, bikes: number): StationOut => ({
  station_id: id,
  nombre: `CE-${id}`,
  lat: 19.42,
  lon: -99.16,
  capacidad: 20,
  bicis_disponibles: bikes,
  docks_disponibles: 20 - bikes,
  ultimo_reporte_utc: "2026-11-10T15:50:33Z",
  anomalia: false,
});

const prediction = (id: string, delta: number): PredictionOut => ({
  station_id: id,
  bicis_actuales: 10,
  prediccion_t60: 10 + delta,
  delta,
  clase: delta <= -3 ? "se_vacia" : delta >= 3 ? "se_llena" : "estable",
  generado_utc: "2026-11-10T16:00:00Z",
  modelo_version: "mlflow:1",
});

describe("mergeStations", () => {
  it("une por station_id sin perder estaciones", () => {
    const merged = mergeStations([station("1", 10), station("2", 3)], [prediction("2", -4)]);
    expect(merged.map((s) => s.station_id)).toEqual(["1", "2"]);
    expect(merged[0]?.prediccion).toBeNull();
    expect(merged[1]?.prediccion?.clase).toBe("se_vacia");
    expect(merged[1]?.bicis_disponibles).toBe(3);
  });

  it("sin predicciones (API sin modelo) todas quedan en null", () => {
    const merged = mergeStations([station("1", 5)], []);
    expect(merged[0]?.prediccion).toBeNull();
  });

  it("ignora predicciones de estaciones que no vienen en /stations", () => {
    expect(mergeStations([station("1", 5)], [prediction("9", 5)])).toHaveLength(1);
  });
});

describe("constantes", () => {
  it("polling cada 60 s e historia de 6 h", () => {
    expect(POLL_MS).toBe(60000);
    expect(HISTORY_HOURS).toBe(6);
  });
});
