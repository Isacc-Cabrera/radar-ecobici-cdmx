# Guion del video — Radar ECOBICI CDMX (90 segundos)

**Formato:** grabación de pantalla con voz en off, ~150 palabras por minuto. **Antes de grabar:**
levanta la API y el mapa, deja el recolector corriendo y graba en hora pico (7–9 am o 5–7 pm) para
que se vean estaciones cambiando.

| Tiempo | Sección | En pantalla | Voz en off |
|---|---|---|---|
| 00:00–00:10 | **Gancho** | Mapa 3D girando sobre la CDMX | "Llegas a la estación de ECOBICI y no hay bicis. ¿Y si el mapa te dijera cómo va a estar dentro de una hora?" |
| 00:10–00:22 | **Problema** | Una estación de Buenavista y su gráfica cayendo a cero | "El feed oficial solo dice cómo está cada estación ahora. Esta estación de Buenavista pasó de 52 bicis a cero en menos de una hora, y nadie te avisó." |
| 00:22–00:42 | **Arquitectura** | Diagrama de `docs/arquitectura.md`, recorrido de izquierda a derecha | "Cada minuto recolecto 677 estaciones y las mando por Kafka a un data lake de tres capas: bronze guarda lo crudo, silver lo valida y gold lo convierte en features. Airflow lo orquesta cada hora y todo corre local, sin costo." |
| 00:42–01:02 | **Demo del mapa** | Buscar una estación, vuelo de cámara, tooltip, panel, tema oscuro, anillos rojos | "La altura de cada columna son las bicis de ahora. El color es la predicción: naranja se vacía, azul se llena. Los anillos rojos son anomalías, como una estación que lleva seis días sin reportar." |
| 01:02–01:18 | **Modelo vs baseline** | Tabla de `docs/reportes/comparacion_modelos.md` y `real_vs_pred.html` | "Mi primer modelo perdía contra el baseline más simple: suponer que nada cambia. Al enseñarle a predecir el cambio y no el nivel, mejoró 5.6 por ciento, validando siempre con datos del futuro. Es un resultado honesto, con pocas horas de datos." |
| 01:18–01:30 | **Cierre** | Repositorio en GitHub con el badge de CI en verde | "155 pruebas, integración continua y todo documentado, incluidos los errores. El código está en GitHub: Radar ECOBICI CDMX." |

**Notas de grabación**

- Herramientas gratuitas y formato del GIF: sección "Capturas y GIF" de [frontend.md](frontend.md).
- Texto en pantalla sugerido para el cierre: `github.com/Isacc-Cabrera/radar-ecobici-cdmx`.
- Actualiza las cifras si vuelves a entrenar el modelo antes de grabar.
