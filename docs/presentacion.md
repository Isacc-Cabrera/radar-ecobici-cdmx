# Presentación — Radar ECOBICI CDMX

Diez diapositivas siguiendo el hilo **problema → evidencia → decisión → resultado**. Las cifras son
del 10 de noviembre de 2026; actualízalas si vuelves a entrenar el modelo antes de exponer.

## Diapositiva 1 — Portada

- **Radar ECOBICI CDMX:** cuántas bicis habrá en tu estación dentro de una hora
- Angel Isacc Cabrera Leal · Ingeniería de Software · IBERO
- Pipeline en tiempo real, modelo predictivo, API y mapa 3D
- Imagen: captura del mapa 3D sobre la CDMX

**Nota del orador:** "Construí de punta a punta un sistema con datos reales que están cambiando
ahora mismo. Les voy a contar qué problema resuelve, qué decidí y qué salió mal."

## Diapositiva 2 — Problema

- Llegas a la estación y no hay bicis, o vas a devolverla y no hay muelles
- El feed oficial dice cómo está cada estación **ahora**, no en una hora
- No guarda historia: nadie puede analizar qué estaciones se vacían ni cuándo
- Ejemplo real: la 266-267 de Buenavista pasó de ~52 bicis a cero en menos de una hora

**Nota del orador:** "Muestro la gráfica de Buenavista. Esa estación se vacía todas las mañanas y
hoy ningún sistema público te lo anticipa."

## Diapositiva 3 — Arquitectura

- Feed cada 60 s → Kafka → data lake bronze / silver / gold
- Airflow orquesta cada hora; MLflow registra el modelo
- FastAPI sirve estaciones y predicciones a un mapa 3D con deck.gl
- Patrones nombrados: Ports & Adapters, Strategy, Template Method, Chain of Responsibility, Repository
- Todo local y sin costo; AWS descrito en Terraform y sin aplicar

**Nota del orador:** "Kafka desacopla capturar de escribir: apagué el consumidor y no perdí datos.
Airflow no compite con Kafka: uno mueve eventos, el otro agenda lotes con reintentos."

## Diapositiva 4 — Datos

- 677 estaciones cada 60 s, 1.6 M de viajes de octubre y clima de Open-Meteo
- Guardar primero, validar después: bronze nunca se modifica
- La regla de capacidad se cumple en 99.7 %; lo demás va a cuarentena con su motivo
- ~78 % de lo que llega son minutos sin cambios
- Privacidad: lista blanca de columnas; género y edad nunca salen de bronze local

**Nota del orador:** "Una decisión clave fue 60 segundos y no 10: el modelo predice a una hora, y
10 segundos daban casi 6 millones de filas al día sin aportar nada."

## Diapositiva 5 — Modelo

- Objetivo: bicis por estación a t+60 min
- Features solo del pasado: lags, medias móviles, calendario, clima y demanda histórica
- Validación temporal con purga: nunca al azar
- LightGBM aprende el **cambio** respecto a las bicis actuales, no el nivel
- Baseline a vencer: "en una hora habrá lo mismo que ahora"

**Nota del orador:** "Validar al azar mete el futuro en el entrenamiento. Y además quito las filas
cuyo target cae dentro de la validación, porque si no hay una hora de fuga en cada frontera."

## Diapositiva 6 — Demo

- Mapa en vivo: altura = bicis, color = tendencia a una hora
- Buscar Buenavista: vuelo de cámara y gráfica de 6 horas
- Anillos rojos: anomalías detectadas
- Tema claro y oscuro; accesible con teclado
- Swagger en `/docs`

**Nota del orador:** "Si la demo falla, tengo el video de respaldo. Muestro primero Buenavista, luego
una estación con anillo rojo, y cierro con `/predict` en Swagger."

## Diapositiva 7 — Resultados

- MAE: **2.848** contra 3.018 del baseline, **5.6 % mejor**
- Gana en 3 de 4 folds; de madrugada gana la persistencia
- 31 estaciones congeladas: una lleva 6.6 días sin reportar
- 155 pruebas de Python y 34 del frontend, con CI en verde
- `/predictions` responde 677 estaciones en ~30 ms con caché

**Nota del orador:** "La historia que más me gusta: mi primer modelo perdía contra suponer que nada
cambia. Reformular el problema como predecir el cambio fue lo que lo hizo ganar."

## Diapositiva 8 — Limitaciones

- Solo 8 horas útiles de entrenamiento: el 5.6 % es preliminar
- El z-score necesita 3 días de historia; hoy no marca nada
- Isolation Forest marca la hora pico como rara: falsos positivos
- No conozco los camiones de rebalanceo
- Corre en una laptop: si se suspende, se pierden esas horas

**Nota del orador:** "Prefiero enseñar estas limitaciones a que me las encuentren. Cada una está
documentada con ejemplos reales de estaciones."

## Diapositiva 9 — Trabajo futuro

- Reentrenar con semanas de datos y programarlo en Airflow
- Desplegar el mapa y la API públicamente
- Detector "fuera de servicio" con `is_renting`
- WebSockets, si se publica en vivo
- Generalizar a otros sistemas GBFS del mundo

**Nota del orador:** "Lo primero que haría con una semana más es simplemente dejarlo juntar datos y
volver a medir: casi todas las limitaciones se atacan con más historia."

## Diapositiva 10 — Cierre y preguntas

- De un feed público a una predicción en un mapa 3D, de punta a punta
- Lección técnica: un baseline honesto cambia cómo planteas el problema
- Lección de diseño: guardar lo crudo primero permite equivocarse y reprocesar
- Lección de gestión: pasos pequeños, verificados y con checkpoint
- Repositorio: `github.com/Isacc-Cabrera/radar-ecobici-cdmx`

**Nota del orador:** "Gracias. Todo el código, las decisiones y los errores están en el repositorio.
¿Preguntas?"
