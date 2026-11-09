---
description: Convenciones del mapa 3D (React + Vite + deck.gl + MapLibre)
paths:
  - "frontend/**"
---

# Frontend

- Contenedor/Presentacional: `App.tsx` (contenedor) usa los hooks `useStations` y `useTheme` de
  `src/hooks.ts`; `StationMap.tsx` y `Panel.tsx` solo reciben props.
- Colores, umbrales, mapas base y atribución SOLO desde `src/lib/scales.ts` y variables CSS de
  `src/styles/theme.css`. Ningún componente escribe un hex literal.
- Tokens (oscuro / claro): bg `#07120D`/`#F2FAF5`, surface `#0E1F17`/`#FFFFFF`, surface-2
  `#14291F`/`#E7F4EC`, border `#1F3D2E`/`#C9E4D3`, text `#E3F5EA`/`#0D2A1C`, text-muted
  `#8DB8A1`/`#4A6B5A`, primary `#22C55E`/`#15803D`, primary-strong `#16A34A`/`#166534`, accent
  `#A3E635`/`#65A30D`, focus `#4ADE80`/`#16A34A`, destructive `#F87171`/`#B91C1C`.
- Columnas: altura = bicis disponibles (`elevationScale` 40), color por rampa
  `#E5F5E0 → #A1D99B → #41AB5D → #006D2C`. Predicción (delta = pred − actual): ≤ −3 se vacía
  `#F97316` ▼, estable `#84CC16` ●, ≥ +3 se llena `#06B6D4` ▲. Anomalía `#EF4444` ⚠.
- Tema: default `prefers-color-scheme`, persistido en `localStorage["radar-tema"]`; el mapa base
  cambia con el tema (OpenFreeMap `dark` / `positron`). Atribución visible siempre:
  "OpenFreeMap © OpenMapTiles Data from OpenStreetMap".
- Cámara: `pitch` 50°, transición con `FlyToInterpolator` de 1200 ms; con
  `prefers-reduced-motion: reduce` la duración es 0.
- Polling cada 60 s a `VITE_API_URL` (default `http://localhost:8000`); sin WebSockets (no-goal).
- Accesibilidad: el botón de tema y la lista de estaciones son operables con teclado; foco visible
  con `--focus`; la leyenda usa texto + ícono, nunca solo color; objetivos de 24×24 px mínimo;
  `aria-live="polite"` en la hora de última actualización.
- Imports relativos sin extensión (`./components/Panel`); `moduleResolution: "bundler"`.
- Estados de carga, vacío y error visibles en el panel (texto, no solo spinner).
