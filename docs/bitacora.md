`> Registro de avances, decisiones y aprendizajes. Se actualiza al terminar cada sesión o fase.`

`> Formato de decisiones: **qué decidí · alternativas · por qué · consecuencias**.`

---

## Fase 0 — Exploración de retos y decisión del proyecto`

**Fechas:** 25–26 sep 2026 · **Estado:** ✅ Completada`

### Qué hice`

- Revisé los 42 archivos de retos del curso (86 retos en total: 34 básicos, 31 intermedios, 21 avanzados; incluye el bloque nuevo de Ciencia de Datos, retos 1–50).`

- Identifiqué que el bloque nuevo de retos (1–50) está enfocado en ciencia e ingeniería de datos, y que los retos web clásicos (contador, cambio de color, etc.) aportan poco a mi nivel actual.`

- Filtré los retos por valor para mi perfil (full stack → ingeniería de datos) y para portafolio/LinkedIn.`

- Comparé 3 ideas de proyecto integrador.`

### Ideas evaluadas`

| Idea                       | Fuerte en                              | Dificultad de datos | Visual en LinkedIn      |
| -------------------------- | -------------------------------------- | ------------------- | ----------------------- |
| **Radar ECOBICI CDMX**     | Streaming (Kafka), ingeniería de datos | Baja (API lista)    | Muy alto (mapa en vivo) |
| Calidad del aire CDMX      | ML + tiempo real (WebSockets)          | Baja-media          | Alto                    |
| Lupa (compras de gobierno) | Big data + PLN + IA                    | Media-alta          | Medio-alto              |

### Decisiones`

**D-001 · Proyecto: Radar ECOBICI CDMX**`

- *Alternativas:* calidad del aire, detector de anomalías en compras públicas, hacer retos sueltos.`

- *Por qué:* es el que más directo refuerza la ruta de **ingeniería de datos**, los datos son fáciles de obtener (feed público GBFS en tiempo real + históricos), es muy visual para LinkedIn y un solo proyecto cubre muchos retos.`

- *Consecuencia:* debo diferenciarme de proyectos existentes (p. ej. *ecobici-pulse*) con predicción, MLOps y asistente con IA.`

**D-002 · Proyecto integral en vez de retos sueltos**`

- *Alternativas:* hacer 6–8 retos independientes.`

- *Por qué:* un proyecto completo cuenta una historia de principio a fin (ingesta → modelo → producto), es mejor para portafolio y entrevistas.`

- *Consecuencia:* ⚠️ confirmar con el profesor que un proyecto integral puede contar por varios retos y si se entrega por etapas.`

**D-003 · Herramientas gratuitas + alojamiento híbrido**`

- *Alternativas:* todo en AWS; todo local.`

- *Por qué:* AWS da créditos solo por tiempo limitado (plan gratuito de 6 meses / hasta $200 USD) y correr Kafka + Airflow 24/7 los consumiría; los servicios administrados (MSK, MWAA) son caros.`

- *Decisión:* desarrollo con Docker Compose local; servidor 24/7 en Oracle Cloud Always Free o laptop; AWS solo para aprender Terraform + S3 + IAM; frontend y API en servicios gratuitos; video demo para LinkedIn.`

- *Consecuencia:* alerta de presupuesto de $5 USD en AWS desde el día 1.`

**D-004 · Forma de trabajo: modo tutor**`

- *Por qué:* quiero aprender y poder explicar cada concepto, no solo tener el proyecto funcionando.`

- *Cómo:* yo escribo el código; Claude guía con preguntas y pistas; checkpoints antes de cambiar de fase; esta bitácora como memoria del proyecto dentro de un proyecto de Claude.`

### Aprendizajes`

- Los retos tienen "equivalencia": básicos = 1, intermedios = 2, avanzados = 3.`

- Un proyecto de portafolio vale más si muestra el ciclo completo y decisiones justificadas.`

- El modelo necesita historia: hay que **empezar a recolectar datos pronto** (semana 2).`

### Pendientes`

- [x] Preguntar al profesor Cardeña: ¿un proyecto integral puede contar por varios retos?, ¿cuántos retos se requieren?, ¿fechas de entrega?`

- [x] Crear proyecto en Claude y subir: plan, bitácora y archivos de retos.`

- [ ] Crear repositorio público en GitHub.`

- [ ] Crear cuenta de AWS + alerta de presupuesto (cuando llegue la fase 8, o antes si quiero practicar).`

---
## Fase 1 — Diseño y exploración del feed
**Fechas:** 29–30 sep 2026 · **Estado:** ⏳ En proceso (falta el entregable: repo, README y diagrama)

### Qué hice
- Exploré los dos feeds principales de GBFS de ECOBICI:
  - `station_information`: datos maestros de cada estación (id, nombre, latitud/longitud, capacidad). Casi nunca cambian.
  - `station_status`: estado de cada estación en un momento dado (bicis y docks disponibles/deshabilitados).
  - Se unen mediante `station_id`.
- Descubrí la regla de capacidad buscando una estación que no cuadraba (la 311: capacidad 18 aparente vs 19 real):
  `bicis disponibles + docks disponibles + docks deshabilitados (+ bicis deshabilitadas) = capacity`.
  Queda como hipótesis por verificar sobre todas las estaciones; en silver será un check automático.
- Revisé los datos abiertos de viajes: CSV mensuales donde cada fila es un viaje (origen, destino, horas).
- Calculé el volumen de datos según el intervalo de recolección (~680 estaciones).
- Definí la estrategia ante fallas del feed.

### Decisiones

**D-005 · Recolectar `station_status` cada 60 s y `station_information` una vez al día**
- *Alternativas:*
  - A: guardar cada 10 s (lo que permite el `ttl`) → 5,875,200 filas/día, ~176 M/mes.
  - B: guardar cada 60 s → 979,200 filas/día, ~29 M/mes.
  - C: guardar solo cuando algo cambia.
  - Para `station_information`: descargarlo con la misma frecuencia, o una vez al mes / cada 3 meses.
- *Por qué:*
  - El modelo predice la próxima hora; no necesita resolución de 10 s.
  - A multiplica por 6 el volumen (disco, Kafka, procesamiento) sin aportar al modelo.
  - C deja huecos que no se distinguen de "el recolector estaba caído".
  - De datos finos puedo sacar agregados gruesos, pero no al revés; por eso no bajo más allá de 1 min al recolectar.
  - `station_information` casi no cambia, pero esperar un mes deja fuera estaciones nuevas del mapa y de la regla de capacidad. Descargarlo diario cuesta casi nada.
- *Consecuencias:*
  - `station_information`: detecto cambios con un **hash de los datos de las estaciones** (sin `last_updated`, porque ese campo puede cambiar aunque el contenido sea igual). Solo actualizo si el hash cambió.
  - Si `station_status` trae un `station_id` que no conozco, refresco `station_information` en ese momento.
  - Pendiente: revisar con datos reales si 60 s es realmente necesario.

**D-006 · Guardar primero, validar después (arquitectura por capas)**
- *Alternativas:* validar cada registro antes de guardarlo y descartar los que fallen.
- *Por qué:*
  - `station_status` es una foto de un instante: si la borro, no la puedo recuperar.
  - Si un dato es erróneo, esas filas son la evidencia del error.
  - Si la validación depende de `station_information` y esa descarga falla, no quiero que se detenga la recolección.
- *Consecuencias:*
  - **Bronze:** todo lo que llega, tal cual, sin borrar nada. Incluye filas con `station_id` desconocido (se registran en log).
  - **Silver:** validación de `station_id`, regla de capacidad y deduplicación de minutos sin cambios.
  - **Gold:** agregados por hora y features para el modelo.
  - **Registro de ejecución (heartbeat):** en cada corrida guardo hora, resultado (sin cambios / guardado / error), código HTTP y número de estaciones. Así distingo "no cambió nada" de "no corrí".

**D-007 · Licencia MIT**
- *Alternativas:* Apache 2.0; GPL (copyleft).
- *Por qué:* mi objetivo es que me reconozcan como autor, y MIT y Apache lo garantizan por igual. Lo que Apache agrega (permiso de patentes y obligación de marcar cambios) no le aporta a un proyecto de portafolio sin nada patentable. MIT es más simple y la más reconocida. Descarté GPL porque obliga a quien use mi código a usar la misma licencia, y prefiero que cualquiera pueda usarlo libremente.
- *Consecuencias:* cualquiera puede usar, modificar o vender el código, siempre que conserve mi aviso de copyright. Usar Kafka/Airflow (Apache) o FastAPI (MIT) no condiciona esta elección.

**D-008 · Cambio de modo de trabajo: Claude genera el código, yo lo reviso y lo explico** *(8 nov 2026, reemplaza el "cómo" de D-004)*
- *Alternativas:* mantener D-004 (yo escribo todo el código y Claude solo guía con pistas).
- *Por qué:* quedan 5 días para la entrega (13 nov) y el alcance cubre 10 etapas; escribir todo a mano no cabe en el tiempo. El objetivo de aprender se conserva si Claude explica cada concepto, alternativa y razón antes de cada pieza, y yo debo poder explicar todo en la exposición.
- *Consecuencias:*
  - Claude genera el código con encabezado de autor (Isacc Cabrera), descripción, fecha y cambios, y aplica patrones de diseño donde aporten.
  - Al cerrar cada etapa: resumen, 2–3 preguntas de checkpoint que respondo con mis palabras y entrada para esta bitácora.
  - Riesgo: entender menos el código propio → mitigación: no se cierra una etapa sin contestar el checkpoint.
- *Otras decisiones del 8 nov:* el repo vive en `Proyectos_Linkedin/ECO_BIKES/radar-ecobici-cdmx` (separado de este vault de Obsidian); se publica en la cuenta de GitHub Isacc Cabrera.

### Respuestas del checkpoint (con mis palabras)
- **¿Por qué el feed separa información estática de dinámica?**
  Porque cambian a ritmos muy distintos: la ubicación y capacidad de una estación casi nunca cambian, pero sus bicis cambian cada minuto. Separarlos permite descargar cada uno con la frecuencia que necesita.
- **¿Qué indica `ttl`?**
  Indica el tiempo en segundos en que esos datos se consideran vigentes.
- **¿Qué hago si el feed no responde o manda datos repetidos?**
  - 4xx: el error es mío; reintentar no lo arregla. Registro en logs y corrijo el código.
  - 5xx: el error es del servidor; reintento con exponential backoff y un tope máximo de espera. El recolector nunca muere.
  - 429: me pidieron bajar el ritmo; respeto el encabezado `Retry-After`.
  - 200 pero `last_updated` congelado: no guardo duplicados y, si pasan varios `ttl` sin cambios, genero una alerta.



### Aprendizajes / dudas
- Las reglas de calidad de datos se descubren buscando los casos que no cuadran (estación 311).
- Los históricos de viajes **no sirven como variable objetivo**: no incluyen el rebalanceo que hacen las camionetas. Sirven como contexto de demanda (salidas/llegadas por hora).
- Que el feed permita consultar cada 10 s no significa que deba hacerlo: es una decisión de diseño.

### Pendientes
- [x] Escribir la respuesta sobre `ttl` con mis palabras.
- [x] Verificar la regla de capacidad sobre todas las estaciones (en fase 2).
- [x] Verificar si `last_updated` de `station_information` cambia aunque el contenido sea igual.
- [ ] Crear repo público con README, licencia y `.gitignore`.
- [ ] Dibujar mi diagrama de arquitectura (antes de ver el del plan).
---`
<!-- Plantilla para nuevas fases:

## Fase N — Nombre

**Fechas:** · **Estado:**

### Qué hice

### Decisiones

**D-00X · Título**

- *Alternativas:*

- *Por qué:*

- *Consecuencia:*

### Respuestas del checkpoint

### Aprendizajes / dudas

### Pendientes

-->
---
## Fase 2 — Arquitectura y construcción
**Fechas:** 8–13 nov 2026 · **Estado:** ⏳ En proceso

### Decisiones

**D-009 · SeaweedFS reemplaza a MinIO como S3 local**
- *Alternativas:* MinIO (repo archivado el 2026-04-25, sin imágenes comunitarias nuevas); Garage (requiere configurar layout y llaves con su CLI); RustFS (versión 1.0 con dos semanas de vida).
- *Por qué:* SeaweedFS corre en un solo contenedor (`weed server -s3`), tiene una API S3 madura, identidades con acciones por bucket (mínimo privilegio) y desarrollo muy activo.
- *Consecuencias:* las identidades viven en `docker/seaweedfs/s3.json` (generado e ignorado en git); el código habla S3 vía boto3, así que cambiar a AWS S3 solo cambia el endpoint.

**D-010 · Mapa base OpenFreeMap**
- *Alternativas:* CARTO basemaps; Mapbox; MapTiler.
- *Por qué:* OpenFreeMap no pide token ni tarjeta (estilos `dark` y `positron` responden 200 sin llave); CARTO se descartó porque exige API key.
- *Consecuencias:* atribución obligatoria "OpenFreeMap © OpenMapTiles Data from OpenStreetMap"; sus términos de uso no se leyeron aún (pendiente antes de publicar el video).
