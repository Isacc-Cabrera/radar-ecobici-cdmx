/**
 * Autor: Isacc Cabrera
 * Descripción: Panel lateral (hoja inferior en pantallas angostas): título, botón de tema, hora
 *   de la última actualización (aria-live), leyenda con texto + ícono (nunca solo color), lista
 *   de estaciones filtrable y operable con teclado, detalle de la seleccionada con la gráfica de
 *   bicis de las últimas 6 h (recharts) y la atribución del mapa base. Estados de carga, vacío y
 *   error en texto. Solo recibe props; los colores salen de scales.ts o de var(--token).
 * Patrón: Container/Presentational (presentacional)
 * Fecha de creación: 2026-11-11
 * Cambios:
 *     - 2026-11-11 · Isacc Cabrera · Creación (paso 14).
 *     - 2026-11-11 · Isacc Cabrera · "⚠ Anomalía" en la leyenda, la lista y el detalle (paso 15).
 */
import { useMemo } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { HistoryState, Station } from "../hooks";
import {
  ANOMALY_COLOR,
  BASEMAP_ATTRIBUTION,
  BIKE_RAMP,
  PREDICTION_COLORS,
  PREDICTION_LABELS,
  type PredictionClass,
  THEME_TOKENS,
  type Theme,
} from "../lib/scales";

const CLASSES: PredictionClass[] = ["se_vacia", "estable", "se_llena"];
const MAX_LIST = 60;
const TIME_FMT = new Intl.DateTimeFormat("es-MX", {
  hour: "2-digit",
  minute: "2-digit",
  timeZone: "America/Mexico_City",
});

interface PanelProps {
  stations: Station[];
  loading: boolean;
  error: string | null;
  updatedAt: Date | null;
  theme: Theme;
  onToggleTheme: () => void;
  filter: string;
  onFilter: (text: string) => void;
  selected: Station | null;
  onSelect: (stationId: string) => void;
  history: HistoryState;
}

function Legend() {
  return (
    <section aria-labelledby="leyenda-titulo" className="panel-section">
      <h2 id="leyenda-titulo">Leyenda</h2>
      <p className="muted">Altura de la columna = bicis disponibles ahora.</p>
      <div className="ramp" aria-hidden="true">
        {BIKE_RAMP.map((color) => (
          <span key={color} style={{ background: color }} />
        ))}
      </div>
      <p className="muted ramp-labels">
        <span>Vacía</span>
        <span>Llena</span>
      </p>
      <p className="muted">Color = tendencia en 1 hora:</p>
      <ul className="legend">
        {CLASSES.map((c) => (
          <li key={c}>
            <span className="swatch" style={{ color: PREDICTION_COLORS[c] }} aria-hidden="true">
              {PREDICTION_LABELS[c].icon}
            </span>
            {PREDICTION_LABELS[c].label}
          </li>
        ))}
        <li>
          <span className="swatch" style={{ color: ANOMALY_COLOR }} aria-hidden="true">
            ⚠
          </span>
          Anomalía
        </li>
      </ul>
    </section>
  );
}

function Detail({ station, history, theme }: { station: Station; history: HistoryState; theme: Theme }) {
  const tokens = THEME_TOKENS[theme];
  const data = history.points.map((p) => ({
    hora: TIME_FMT.format(new Date(p.ts_utc)),
    bicis: p.bicis_disponibles,
  }));
  const pred = station.prediccion;
  return (
    <section aria-labelledby="detalle-titulo" className="panel-section detail">
      <h2 id="detalle-titulo">{station.nombre}</h2>
      <dl className="stats">
        <div>
          <dt>Bicis</dt>
          <dd className="num">{station.bicis_disponibles}</dd>
        </div>
        <div>
          <dt>Muelles</dt>
          <dd className="num">{station.docks_disponibles}</dd>
        </div>
        <div>
          <dt>Capacidad</dt>
          <dd className="num">{station.capacidad}</dd>
        </div>
      </dl>
      <p>
        {pred ? (
          <>
            En 1 hora: <strong className="num">{pred.prediccion_t60.toFixed(1)}</strong> bicis ·{" "}
            <span style={{ color: PREDICTION_COLORS[pred.clase] }} aria-hidden="true">
              {PREDICTION_LABELS[pred.clase].icon}
            </span>{" "}
            {PREDICTION_LABELS[pred.clase].label}
          </>
        ) : (
          "Sin predicción para esta estación."
        )}
      </p>
      {station.anomalia && (
        <p>
          <span style={{ color: ANOMALY_COLOR }} aria-hidden="true">
            ⚠
          </span>{" "}
          Anomalía detectada en esta estación.
        </p>
      )}
      <h3>Bicis en las últimas 6 horas</h3>
      {history.loading && <p className="muted">Cargando historia…</p>}
      {history.error && <p className="error">No se pudo cargar la historia: {history.error}</p>}
      {!history.loading && !history.error && data.length === 0 && (
        <p className="muted">Sin datos recientes de esta estación.</p>
      )}
      {data.length > 0 && (
        <div className="chart" role="img" aria-label={`Bicis disponibles en las últimas 6 horas en ${station.nombre}`}>
          <ResponsiveContainer width="100%" height={160}>
            <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: -24 }}>
              <CartesianGrid stroke={tokens.border} strokeDasharray="3 3" />
              <XAxis dataKey="hora" stroke={tokens.textMuted} fontSize={11} minTickGap={24} />
              <YAxis allowDecimals={false} stroke={tokens.textMuted} fontSize={11} />
              <Tooltip
                contentStyle={{ background: tokens.surface, border: `1px solid ${tokens.border}`, color: tokens.text }}
              />
              <Line type="stepAfter" dataKey="bicis" stroke={tokens.primary} strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}

export default function Panel({
  stations,
  loading,
  error,
  updatedAt,
  theme,
  onToggleTheme,
  filter,
  onFilter,
  selected,
  onSelect,
  history,
}: PanelProps) {
  const filtered = useMemo(() => {
    const text = filter.trim().toLowerCase();
    const list = text ? stations.filter((s) => s.nombre.toLowerCase().includes(text)) : stations;
    return [...list].sort((a, b) => a.nombre.localeCompare(b.nombre, "es"));
  }, [stations, filter]);
  const dark = theme === "dark";

  return (
    <aside id="panel" className="panel" aria-label="Panel de estaciones" tabIndex={-1}>
      <header className="panel-header">
        <h1>Radar ECOBICI CDMX</h1>
        <button type="button" className="theme-toggle" aria-pressed={dark} onClick={onToggleTheme}>
          {dark ? "Tema oscuro" : "Tema claro"}
        </button>
      </header>
      <p className="muted" aria-live="polite">
        {updatedAt ? `Actualizado a las ${TIME_FMT.format(updatedAt)} (cada 60 s)` : "Conectando con la API…"}
      </p>
      {error && <p className="error">No se pudo leer la API: {error}</p>}

      {selected && <Detail station={selected} history={history} theme={theme} />}

      <Legend />

      <section aria-labelledby="lista-titulo" className="panel-section">
        <h2 id="lista-titulo">Estaciones</h2>
        <label className="filter">
          <span>Buscar por nombre</span>
          <input
            type="search"
            value={filter}
            onChange={(e) => onFilter(e.target.value)}
            placeholder="p. ej. Buenavista"
          />
        </label>
        {loading && <p className="muted">Cargando estaciones…</p>}
        {!loading && !error && filtered.length === 0 && (
          <p className="muted">Ninguna estación coincide con «{filter}».</p>
        )}
        <ul className="station-list">
          {filtered.slice(0, MAX_LIST).map((s) => {
            const clase = s.prediccion?.clase;
            return (
              <li key={s.station_id}>
                <button
                  type="button"
                  aria-current={selected?.station_id === s.station_id ? "true" : undefined}
                  onClick={() => onSelect(s.station_id)}
                >
                  <span className="station-name">{s.nombre}</span>
                  <span className="num">
                    {s.bicis_disponibles} bicis
                    {clase && (
                      <>
                        {" "}
                        <span style={{ color: PREDICTION_COLORS[clase] }} aria-hidden="true">
                          {PREDICTION_LABELS[clase].icon}
                        </span>
                        <span className="sr-only">{PREDICTION_LABELS[clase].label}</span>
                      </>
                    )}
                    {s.anomalia && (
                      <>
                        {" "}
                        <span style={{ color: ANOMALY_COLOR }} aria-hidden="true">
                          ⚠
                        </span>
                        <span className="sr-only">Anomalía</span>
                      </>
                    )}
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
        {filtered.length > MAX_LIST && (
          <p className="muted">
            Mostrando {MAX_LIST} de {filtered.length}; escribe en el buscador para encontrar las demás.
          </p>
        )}
      </section>

      <p className="attribution">{BASEMAP_ATTRIBUTION}</p>
    </aside>
  );
}
