/**
 * Autor: Isacc Cabrera
 * Descripción: Hooks de datos y preferencias del mapa. useStations hace polling cada 60 s a
 *   /stations y /predictions de la API y los une por station_id (si /predictions falla —por
 *   ejemplo 503 sin modelo— el mapa sigue con las estaciones). useStationHistory lee las
 *   últimas 6 h de una estación. useTheme arranca con lo guardado en localStorage o, si no hay,
 *   con prefers-color-scheme, y lo persiste. useReducedMotion respeta prefers-reduced-motion.
 *   Tipos iguales al contrato de la API (blueprint §5).
 * Patrón: Custom Hook
 * Fecha de creación: 2026-11-11
 * Cambios:
 *     - 2026-11-11 · Isacc Cabrera · Creación (paso 14).
 */
import { useCallback, useEffect, useState } from "react";
import { THEME_STORAGE_KEY, type PredictionClass, type Theme, resolveTheme } from "./lib/scales";

export const API_URL: string = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
export const POLL_MS = 60000;
export const HISTORY_HOURS = 6;

/** `GET /stations` (StationOut). */
export interface StationOut {
  station_id: string;
  nombre: string;
  lat: number;
  lon: number;
  capacidad: number;
  bicis_disponibles: number;
  docks_disponibles: number;
  ultimo_reporte_utc: string;
  anomalia: boolean;
}

/** `GET /predictions` (PredictionOut). */
export interface PredictionOut {
  station_id: string;
  bicis_actuales: number;
  prediccion_t60: number;
  delta: number;
  clase: PredictionClass;
  generado_utc: string;
  modelo_version: string;
}

/** Estación con su predicción (null si la API no tiene modelo o la estación no tiene datos). */
export interface Station extends StationOut {
  prediccion: PredictionOut | null;
}

/** `GET /stations/{id}/history` (HistoryPointOut). */
export interface HistoryPoint {
  ts_utc: string;
  bicis_disponibles: number;
  docks_disponibles: number;
}

/** Une estaciones y predicciones por `station_id` (función pura, probada en hooks.test.ts). */
export function mergeStations(stations: StationOut[], predictions: PredictionOut[]): Station[] {
  const byId = new Map(predictions.map((p) => [p.station_id, p]));
  return stations.map((s) => ({ ...s, prediccion: byId.get(s.station_id) ?? null }));
}

async function getJson<T>(url: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} en ${url}`);
  }
  return (await response.json()) as T;
}

export interface StationsState {
  stations: Station[];
  loading: boolean;
  error: string | null;
  updatedAt: Date | null;
}

/** Estaciones + predicciones con polling cada 60 s. */
export function useStations(apiUrl: string): StationsState {
  const [state, setState] = useState<StationsState>({
    stations: [],
    loading: true,
    error: null,
    updatedAt: null,
  });

  useEffect(() => {
    let controller = new AbortController();
    const load = async (): Promise<void> => {
      controller.abort();
      controller = new AbortController();
      const { signal } = controller;
      try {
        const stations = await getJson<StationOut[]>(`${apiUrl}/stations`, signal);
        let predictions: PredictionOut[] = [];
        try {
          predictions = await getJson<PredictionOut[]>(`${apiUrl}/predictions`, signal);
        } catch (err) {
          if (signal.aborted) return;
          // sin modelo (503) el mapa sigue: columnas coloreadas por la rampa de bicis
          console.warn("predicciones no disponibles", err);
        }
        setState({
          stations: mergeStations(stations, predictions),
          loading: false,
          error: null,
          updatedAt: new Date(),
        });
      } catch (err) {
        if (signal.aborted) return;
        const message = err instanceof Error ? err.message : String(err);
        setState((prev) => ({ ...prev, loading: false, error: message }));
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), POLL_MS);
    return () => {
      window.clearInterval(timer);
      controller.abort();
    };
  }, [apiUrl]);

  return state;
}

export interface HistoryState {
  points: HistoryPoint[];
  loading: boolean;
  error: string | null;
}

/** Historia de las últimas 6 h de la estación seleccionada (vacía si no hay selección). */
export function useStationHistory(apiUrl: string, stationId: string | null): HistoryState {
  const [state, setState] = useState<HistoryState>({ points: [], loading: false, error: null });

  useEffect(() => {
    if (stationId === null) {
      setState({ points: [], loading: false, error: null });
      return undefined;
    }
    const controller = new AbortController();
    setState({ points: [], loading: true, error: null });
    const url = `${apiUrl}/stations/${encodeURIComponent(stationId)}/history?hours=${HISTORY_HOURS}`;
    getJson<HistoryPoint[]>(url, controller.signal)
      .then((points) => setState({ points, loading: false, error: null }))
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        const message = err instanceof Error ? err.message : String(err);
        setState({ points: [], loading: false, error: message });
      });
    return () => controller.abort();
  }, [apiUrl, stationId]);

  return state;
}

function readStoredTheme(): string | null {
  try {
    return window.localStorage.getItem(THEME_STORAGE_KEY);
  } catch {
    return null;
  }
}

/** Tema claro/oscuro: localStorage > prefers-color-scheme; escribe data-theme en <html>. */
export function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(() =>
    resolveTheme(readStoredTheme(), window.matchMedia("(prefers-color-scheme: dark)").matches),
  );

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // modo privado o almacenamiento bloqueado: el tema funciona igual, solo no se recuerda
    }
  }, [theme]);

  const toggle = useCallback(() => setTheme((t) => (t === "dark" ? "light" : "dark")), []);
  return [theme, toggle];
}

/** True si el usuario pidió reducir el movimiento (la cámara no se anima). */
export function useReducedMotion(): boolean {
  const query = "(prefers-reduced-motion: reduce)";
  const [reduced, setReduced] = useState<boolean>(() => window.matchMedia(query).matches);

  useEffect(() => {
    const media = window.matchMedia(query);
    const onChange = (event: MediaQueryListEvent): void => setReduced(event.matches);
    media.addEventListener("change", onChange);
    return () => media.removeEventListener("change", onChange);
  }, []);

  return reduced;
}
