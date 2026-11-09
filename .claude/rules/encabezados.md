---
description: Encabezado obligatorio de autor en todo archivo fuente
paths:
  - "src/**"
  - "tests/**"
  - "scripts/**"
  - "dags/**"
  - "sql/**"
  - "docker/**"
  - "infra/**"
  - "frontend/src/**"
  - "frontend/scripts/**"
  - "frontend/vite.config.ts"
  - "docker-compose.yml"
  - ".github/**"
---

# Encabezado obligatorio

Todo archivo `.py`, `.ts`, `.tsx`, `.mjs`, `.sql`, `*.Dockerfile`, `.yml`/`.yaml`, `.tf` y `.sh`
empieza con este encabezado. `tests/unit/test_encabezados.py` revisa las primeras 25 líneas y
falla si falta `Autor: Isacc Cabrera`, `Descripción:`, `Patrón:`, `Fecha de creación:` o `Cambios:`.

Python (docstring de módulo; líneas de 100 caracteres como máximo):

```python
"""
Autor: Isacc Cabrera
Descripción: <qué hace este módulo y por qué existe>
Patrón: <patrón de diseño aplicado o "N/A">
Fecha de creación: <YYYY-MM-DD>
Cambios:
    - <YYYY-MM-DD> · Isacc Cabrera · <qué se hizo>
"""
```

TypeScript / TSX / MJS:

```ts
/**
 * Autor: Isacc Cabrera
 * Descripción: <qué hace>
 * Patrón: <patrón o "N/A">
 * Fecha de creación: <YYYY-MM-DD>
 * Cambios:
 *     - <YYYY-MM-DD> · Isacc Cabrera · <qué se hizo>
 */
```

SQL: mismas cinco líneas con prefijo `-- `. Dockerfile, YAML, Terraform y shell: prefijo `# `
(en `.sh` el encabezado va DESPUÉS de la línea `#!/usr/bin/env bash`).

- Al editar un archivo existente, agrega una línea nueva al final de `Cambios`; nunca borres las previas.
- `Patrón:` nombra el patrón real (Ports & Adapters, Factory, Strategy, Chain of Responsibility,
  Repository, Dependency Injection, Template Method, Singleton, Container/Presentational,
  Custom Hook) o `N/A`. No inventes un patrón para llenar el campo.
- Toda función y clase pública lleva docstring estilo Google en español (`Args:`, `Returns:`,
  `Raises:`).
- Los JSON y Markdown no llevan encabezado (JSON no admite comentarios).
