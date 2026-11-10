/**
 * Autor: Isacc Cabrera
 * Descripción: Contenedor de la aplicación: usa los hooks (estaciones con polling, historia,
 *   tema y movimiento reducido), guarda la estación seleccionada y el filtro, y pasa props a
 *   los componentes presentacionales StationMap y Panel. Incluye el enlace "Saltar al panel" y
 *   el layout responsivo (panel lateral de var(--panel-width) desde 768 px, hoja inferior en
 *   pantallas angostas); los estilos usan solo var(--token).
 * Patrón: Container/Presentational (contenedor)
 * Fecha de creación: 2026-11-10
 * Cambios:
 *     - 2026-11-10 · Isacc Cabrera · Creación (paso 14).
 */
import { useCallback, useMemo, useState } from "react";
import Panel from "./components/Panel";
import StationMap from "./components/StationMap";
import { API_URL, useReducedMotion, useStationHistory, useStations, useTheme } from "./hooks";

const LAYOUT_CSS = `
.app { position: relative; height: 100%; overflow: hidden; }
.map-area { position: absolute; inset: 0; }
.panel {
  position: absolute; z-index: 2; overflow-y: auto;
  background: var(--surface); color: var(--text);
  border: 1px solid var(--border); box-shadow: var(--shadow-panel);
  padding: var(--space-4); display: flex; flex-direction: column; gap: var(--space-3);
}
@media (min-width: 768px) {
  .panel { top: var(--space-4); right: var(--space-4); bottom: var(--space-4);
    width: var(--panel-width); border-radius: var(--radius-lg); }
}
@media (max-width: 767px) {
  .panel { left: 0; right: 0; bottom: 0; max-height: 45vh;
    border-radius: var(--radius-lg) var(--radius-lg) 0 0; }
}
.panel h1 { font-size: 18px; margin: 0; }
.panel h2 { font-size: 15px; margin: 0 0 var(--space-2); }
.panel h3 { font-size: 13px; margin: var(--space-2) 0; color: var(--text-muted); }
.panel p { margin: 0; }
.panel-header { display: flex; align-items: center; justify-content: space-between; gap: var(--space-2); }
.panel-section { border-top: 1px solid var(--border); padding-top: var(--space-3); }
.muted { color: var(--text-muted); font-size: 13px; }
.error { color: var(--destructive); font-size: 13px; }
.theme-toggle, .station-list button {
  min-height: 32px; min-width: 24px; cursor: pointer; font: inherit;
  color: var(--text); background: var(--surface-2);
  border: 1px solid var(--border); border-radius: var(--radius-sm);
}
.theme-toggle { padding: var(--space-1) var(--space-3); font-size: 13px; }
.theme-toggle[aria-pressed="true"] { border-color: var(--primary); }
.ramp { display: flex; height: 10px; border-radius: var(--radius-sm); overflow: hidden; margin-top: var(--space-2); }
.ramp span { flex: 1; }
.ramp-labels { display: flex; justify-content: space-between; }
.legend { list-style: none; margin: var(--space-2) 0 0; padding: 0; display: flex; gap: var(--space-3); flex-wrap: wrap; }
.legend li { display: flex; align-items: center; gap: var(--space-1); font-size: 13px; }
.swatch { font-size: 14px; }
.filter { display: flex; flex-direction: column; gap: var(--space-1); font-size: 13px; color: var(--text-muted); }
.filter input {
  font: inherit; color: var(--text); background: var(--bg);
  border: 1px solid var(--border); border-radius: var(--radius-sm); padding: var(--space-2);
}
.station-list { list-style: none; margin: var(--space-2) 0 0; padding: 0; display: flex; flex-direction: column; gap: var(--space-1); }
.station-list button {
  width: 100%; display: flex; justify-content: space-between; gap: var(--space-2);
  text-align: left; padding: var(--space-2);
}
.station-list button[aria-current="true"] { border-color: var(--primary); background: var(--surface); }
.station-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.station-list .num { flex-shrink: 0; white-space: nowrap; }
.stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: var(--space-2); margin: 0 0 var(--space-2); }
.stats dt { color: var(--text-muted); font-size: 12px; }
.stats dd { margin: 0; font-size: 20px; font-weight: 600; }
.chart { width: 100%; }
.attribution { color: var(--text-muted); font-size: 11px; margin-top: auto; }
.sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }
`;

export default function App() {
  const { stations, loading, error, updatedAt } = useStations(API_URL);
  const [theme, toggleTheme] = useTheme();
  const reducedMotion = useReducedMotion();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const history = useStationHistory(API_URL, selectedId);

  const selected = useMemo(
    () => stations.find((s) => s.station_id === selectedId) ?? null,
    [stations, selectedId],
  );
  const onSelect = useCallback((stationId: string) => setSelectedId(stationId), []);

  return (
    <div className="app">
      <style>{LAYOUT_CSS}</style>
      <a href="#panel" className="skip-link">
        Saltar al panel
      </a>
      <main className="map-area" aria-label="Mapa 3D de estaciones de ECOBICI">
        <StationMap
          stations={stations}
          theme={theme}
          selectedId={selectedId}
          reducedMotion={reducedMotion}
          onSelect={onSelect}
        />
      </main>
      <Panel
        stations={stations}
        loading={loading}
        error={error}
        updatedAt={updatedAt}
        theme={theme}
        onToggleTheme={toggleTheme}
        filter={filter}
        onFilter={setFilter}
        selected={selected}
        onSelect={onSelect}
        history={history}
      />
    </div>
  );
}
