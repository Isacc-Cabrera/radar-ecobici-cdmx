/**
 * Autor: Isacc Cabrera
 * Descripción: Mapa 3D de las estaciones. deck.gl dibuja en WebGL una columna por estación
 *   (ColumnLayer: altura = bicis disponibles, color = clase de la predicción t+60 o, si no hay
 *   predicción, la rampa de bicis) sobre un mapa base de OpenFreeMap con MapLibre, que cambia
 *   con el tema. Al seleccionar una estación la cámara vuela hacia ella con FlyToInterpolator
 *   (1200 ms, o 0 si el usuario pidió reducir el movimiento). Solo recibe props. El worker de
 *   MapLibre se registra con setWorkerUrl para que funcione en dev y en el build de Vite.
 * Patrón: Container/Presentational (presentacional)
 * Fecha de creación: 2026-11-11
 * Cambios:
 *     - 2026-11-11 · Isacc Cabrera · Creación (paso 14).
 */
import { FlyToInterpolator, type MapViewState, type PickingInfo } from "@deck.gl/core";
import { ColumnLayer } from "@deck.gl/layers";
import DeckGL from "@deck.gl/react";
import { setWorkerUrl } from "maplibre-gl";
import maplibreWorkerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";
import { useEffect, useMemo, useState } from "react";
import { Map as BaseMap } from "react-map-gl/maplibre";
import type { Station } from "../hooks";
import {
  BASEMAPS,
  PREDICTION_COLORS,
  PREDICTION_LABELS,
  type Rgb,
  type Theme,
  bikeColor,
  hexToRgb,
} from "../lib/scales";

// MapLibre 6 calcula la URL de su worker relativa a su propio módulo; con Vite esa ruta no
// existe (ni en dev, por el pre-empaquetado, ni en el build). Importarlo con ?url hace que Vite
// lo publique como asset y se lo pasamos explícitamente.
setWorkerUrl(maplibreWorkerUrl);

export const INITIAL_VIEW: MapViewState = {
  longitude: -99.1677,
  latitude: 19.4205,
  zoom: 12.5,
  pitch: 50,
  bearing: -20,
};
const FLY_MS = 1200;
const SELECTED_ZOOM = 15;

interface StationMapProps {
  stations: Station[];
  theme: Theme;
  selectedId: string | null;
  reducedMotion: boolean;
  onSelect: (stationId: string) => void;
}

/** Color de la columna: la clase predicha si existe; si no, la rampa de bicis. */
export function columnColor(station: Station): Rgb {
  if (station.prediccion !== null) {
    return hexToRgb(PREDICTION_COLORS[station.prediccion.clase]);
  }
  return bikeColor(station.bicis_disponibles, station.capacidad);
}

function tooltip(info: PickingInfo<Station>) {
  const s = info.object;
  if (!s) return null;
  const pred = s.prediccion;
  const lines = [
    `<strong>${s.nombre}</strong>`,
    `Bicis: ${s.bicis_disponibles} · Muelles: ${s.docks_disponibles}`,
    pred
      ? `En 1 h: ${pred.prediccion_t60.toFixed(1)} bicis · ${PREDICTION_LABELS[pred.clase].icon} ${PREDICTION_LABELS[pred.clase].label}`
      : "Sin predicción",
  ];
  return {
    html: lines.join("<br/>"),
    style: {
      backgroundColor: "var(--surface)",
      color: "var(--text)",
      border: "1px solid var(--border)",
      borderRadius: "var(--radius-sm)",
      fontFamily: "var(--font-sans)",
      fontSize: "13px",
      padding: "var(--space-2) var(--space-3)",
    },
  };
}

export default function StationMap({
  stations,
  theme,
  selectedId,
  reducedMotion,
  onSelect,
}: StationMapProps) {
  const [viewState, setViewState] = useState<MapViewState>(INITIAL_VIEW);

  useEffect(() => {
    if (selectedId === null) return;
    const target = stations.find((s) => s.station_id === selectedId);
    if (!target) return;
    setViewState((current) => ({
      ...current,
      longitude: target.lon,
      latitude: target.lat,
      zoom: Math.max(current.zoom, SELECTED_ZOOM),
      pitch: INITIAL_VIEW.pitch,
      transitionDuration: reducedMotion ? 0 : FLY_MS,
      transitionInterpolator: new FlyToInterpolator(),
    }));
    // solo al cambiar la selección (no en cada polling): la cámara no se mueve sola
  }, [selectedId, reducedMotion]);

  const layers = useMemo(
    () => [
      new ColumnLayer<Station>({
        id: "estaciones",
        data: stations,
        diskResolution: 12,
        radius: 40,
        extruded: true,
        elevationScale: 40,
        getPosition: (s) => [s.lon, s.lat],
        getElevation: (s) => s.bicis_disponibles,
        getFillColor: columnColor,
        pickable: true,
        autoHighlight: true,
        updateTriggers: { getFillColor: stations, getElevation: stations },
        onClick: (info) => {
          if (info.object) onSelect(info.object.station_id);
        },
      }),
    ],
    [stations, onSelect],
  );

  return (
    <DeckGL
      viewState={viewState}
      onViewStateChange={({ viewState: next }) => setViewState(next as MapViewState)}
      controller={true}
      layers={layers}
      getTooltip={tooltip}
      getCursor={({ isHovering }) => (isHovering ? "pointer" : "grab")}
    >
      <BaseMap mapStyle={BASEMAPS[theme]} attributionControl={false} />
    </DeckGL>
  );
}
