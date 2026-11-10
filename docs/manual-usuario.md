# Manual de usuario — Mapa de Radar ECOBICI CDMX

El mapa muestra cómo están las estaciones de ECOBICI y cómo se espera que estén **dentro de una
hora**. El porqué de cada decisión del proyecto está en la [bitácora](bitacora.md).

## Requisitos

- Un navegador moderno con WebGL (Chrome, Edge o Firefox actualizados).
- El proyecto corriendo en tu máquina. Desde la raíz del repo:

  ```bash
  docker compose --profile api up -d --build --wait mlflow api
  npm --prefix frontend install
  npm --prefix frontend run dev
  ```

- Abre **http://localhost:5173**.
- Para ver datos recientes, el recolector y el pipeline horario deben estar corriendo. Las
  instrucciones completas están en el [README](../README.md); si algo falla, en el
  [runbook](runbook.md).

## Cómo usar el mapa

**Moverte por el mapa**

| Acción | Cómo |
|---|---|
| Desplazarte | arrastra con el botón izquierdo |
| Acercarte o alejarte | rueda del ratón |
| Girar e inclinar | arrastra con el botón derecho, o `Ctrl` + arrastrar |
| Ver el detalle rápido | pasa el cursor sobre una columna |
| Seleccionar una estación | haz clic en su columna o en la lista |

**Tema.** El botón de arriba a la derecha alterna entre tema claro y oscuro. Al entrar por primera
vez se usa el de tu sistema, y tu elección se recuerda para la próxima visita.

**Lista de estaciones.** Escribe parte del nombre en el buscador (por ejemplo, "Reforma") y elige
una estación. La lista muestra hasta 60; si hay más, sigue escribiendo para acotar. Se puede usar
solo con teclado: `Tab` para moverte y `Enter` para elegir. El enlace "Saltar al panel" lleva
directo a la lista.

**Detalle de la estación.** Al seleccionar una estación, la cámara vuela hacia ella y el panel
muestra sus bicis, muelles y capacidad, la predicción a una hora y una gráfica de las últimas
6 horas.

**Leyenda**

| Elemento | Significado |
|---|---|
| Altura de la columna | bicis disponibles ahora: más alta, más bicis |
| ▼ Naranja — Se vacía | se esperan al menos 3 bicis menos en una hora |
| ● Verde — Estable | se espera un cambio menor a 3 bicis |
| ▲ Azul — Se llena | se esperan al menos 3 bicis más en una hora |
| ⚠ Anillo rojo — Anomalía | la estación se comporta de forma inusual |

Si no hay predicción para una estación, la columna usa una escala de verdes según qué tan llena
está: del más claro (vacía) al más oscuro (llena).

La línea "Actualizado a las…" indica cuándo se pidieron los datos por última vez. El mapa se
actualiza solo cada 60 segundos.

## Cómo leer la predicción

Ejemplo: una estación tiene 18 bicis y el panel dice **"En 1 hora: 13.3 bicis · ▼ Se vacía"**.

- **13.3** es el número esperado. Es un promedio estimado, no un conteo exacto: léelo como "unas 13".
- **El margen de error típico es de unas 3 bicis** (MAE de 2.85). Entre 13 y 16 bicis la diferencia
  es normal; entre 13 y 0, no.
- **La clase** compara la predicción con las bicis actuales: se vacía (−3 o menos), se llena (+3 o
  más) o estable.
- **Es una ayuda para decidir, no una garantía.** El modelo se entrenó con pocas horas y no conoce
  los camiones que redistribuyen bicis, ni eventos especiales.

## Preguntas frecuentes

**El panel dice "No se pudo leer la API".** La API no está corriendo o no responde en el puerto
8000. Revisa `curl -s localhost:8000/health` y la sección correspondiente del [runbook](runbook.md).

**Veo columnas pero todas verdes en escala, sin predicción.** La API arrancó sin modelo. Sigue "La
API responde sin modelo" en el [runbook](runbook.md).

**Los datos parecen viejos.** El mapa muestra lo último que procesó el pipeline horario, no el
minuto actual. `curl -s localhost:8000/health` indica la antigüedad en
`segundos_desde_ultimo_dato`.

**¿Una estación marcada con ⚠ está descompuesta?** No necesariamente. Puede estar fuera de servicio
o sin reportar, pero también puede ser un falso positivo, como una estación que se llena a las 9 am.
Los ejemplos reales están en [anomalias.md](anomalias.md).

**¿Por qué una estación con 0 bicis casi no se ve?** Porque la altura representa las bicis
disponibles. Una estación vacía queda como un disco a ras del suelo.

**¿Se envían mis datos a algún lado?** No. El mapa solo consulta la API local y descarga los
mosaicos del mapa base de OpenFreeMap.

**¿Por qué el proyecto funciona así?** Las decisiones, sus alternativas y sus consecuencias están
en la [bitácora](bitacora.md).
